from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import sqlite3
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ToolExecutionMetric:
    tool_name: str
    task_type: str
    latency_ms: float
    memory_mb: float
    cpu_percent: float
    success: bool
    timestamp: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ToolEfficiencyProfile:
    tool_name: str
    execution_count: int
    success_ratio: float
    avg_latency_ms: float
    avg_memory_mb: float
    avg_cpu_percent: float
    efficiency_score: float
    updated_at: float


class HeuristicScoringEngine:
    """
    Lightweight heuristic scoring engine.

    No ML frameworks required.
    Optimized for low-memory VPS systems.
    """

    LATENCY_WEIGHT = 0.35
    SUCCESS_WEIGHT = 0.40
    MEMORY_WEIGHT = 0.15
    CPU_WEIGHT = 0.10

    MAX_LATENCY_MS = 30000
    MAX_MEMORY_MB = 1024
    MAX_CPU_PERCENT = 100

    def calculate_score(
        self,
        *,
        success_ratio: float,
        avg_latency_ms: float,
        avg_memory_mb: float,
        avg_cpu_percent: float,
    ) -> float:
        latency_score = (
            1.0
            - min(
                avg_latency_ms
                / self.MAX_LATENCY_MS,
                1.0,
            )
        )

        memory_score = (
            1.0
            - min(
                avg_memory_mb
                / self.MAX_MEMORY_MB,
                1.0,
            )
        )

        cpu_score = (
            1.0
            - min(
                avg_cpu_percent
                / self.MAX_CPU_PERCENT,
                1.0,
            )
        )

        efficiency = (
            (
                success_ratio
                * self.SUCCESS_WEIGHT
            )
            + (
                latency_score
                * self.LATENCY_WEIGHT
            )
            + (
                memory_score
                * self.MEMORY_WEIGHT
            )
            + (
                cpu_score
                * self.CPU_WEIGHT
            )
        )

        return round(
            max(0.0, min(efficiency, 1.0)),
            4,
        )


class ToolOptimizer:
    """
    Self-learning Tool Optimization Runtime.

    Features:
    - Heuristic scoring engine
    - SQLite WAL persistence
    - Dynamic efficiency scoring
    - Tool feedback loops
    - Performance-aware recommendations
    - RBAC-safe optimization
    - Default deny enforcement
    - Low-memory async-first design
    """

    DATABASE_NAME = (
        "tool_optimization.db"
    )

    CLEANUP_INTERVAL = 3600
    PROFILE_CACHE_LIMIT = 256

    SQLITE_BUSY_TIMEOUT = 5000

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        db_path: str = "./data",
    ) -> None:
        self.router = router

        self._db_dir = Path(db_path)
        self._db_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._db_file = (
            self._db_dir
            / self.DATABASE_NAME
        )

        self._connection: Optional[
            sqlite3.Connection
        ] = None

        self._lock = asyncio.Lock()

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._scoring_engine = (
            HeuristicScoringEngine()
        )

        self._profile_cache: Dict[
            str,
            ToolEfficiencyProfile,
        ] = {}

        self._feedback_buffer: Deque[
            ToolExecutionMetric
        ] = deque(maxlen=512)

    async def start(self) -> None:
        logger.info(
            "Starting ToolOptimizer"
        )

        await self._initialize_database()

        self._running = True

        self._tasks.append(
            asyncio.create_task(
                self._cleanup_loop()
            )
        )

    async def stop(self) -> None:
        logger.info(
            "Stopping ToolOptimizer"
        )

        self._running = False

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

        self._tasks.clear()

        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def record_execution(
        self,
        metric: ToolExecutionMetric,
    ) -> None:
        """
        Record execution metrics.
        """

        async with self._lock:
            self._feedback_buffer.append(
                metric
            )

            await asyncio.to_thread(
                self._insert_metric,
                metric,
            )

            await self._refresh_profile(
                metric.tool_name
            )

    async def recommend_tool(
        self,
        *,
        task_type: str,
        candidate_tools: List[str],
        context: RouteContext,
    ) -> Optional[str]:
        """
        Recommend highest-efficiency tool.

        RBAC + Default Deny enforced.
        """

        best_tool = None
        best_score = -1.0

        for tool_name in candidate_tools:
            route = await self.router.route(
                task=tool_name,
                context=context,
            )

            if (
                route.decision
                != RouteDecision.ALLOWED
            ):
                continue

            profile = await self.get_profile(
                tool_name
            )

            if not profile:
                continue

            if (
                profile.efficiency_score
                > best_score
            ):
                best_score = (
                    profile.efficiency_score
                )

                best_tool = tool_name

        return best_tool

    async def get_profile(
        self,
        tool_name: str,
    ) -> Optional[
        ToolEfficiencyProfile
    ]:
        cached = (
            self._profile_cache.get(
                tool_name
            )
        )

        if cached:
            return cached

        row = await asyncio.to_thread(
            self._fetch_profile,
            tool_name,
        )

        if not row:
            return None

        profile = ToolEfficiencyProfile(
            tool_name=row[0],
            execution_count=row[1],
            success_ratio=row[2],
            avg_latency_ms=row[3],
            avg_memory_mb=row[4],
            avg_cpu_percent=row[5],
            efficiency_score=row[6],
            updated_at=row[7],
        )

        self._store_profile_cache(
            tool_name,
            profile,
        )

        return profile

    async def refresh_all_profiles(
        self,
    ) -> None:
        tools = self.router.list_tools()

        for tool in tools:
            await self._refresh_profile(
                tool["tool_name"]
            )

    async def _refresh_profile(
        self,
        tool_name: str,
    ) -> None:
        stats = await asyncio.to_thread(
            self._aggregate_metrics,
            tool_name,
        )

        if not stats:
            return

        (
            execution_count,
            success_ratio,
            avg_latency_ms,
            avg_memory_mb,
            avg_cpu_percent,
        ) = stats

        efficiency_score = (
            self._scoring_engine.calculate_score(
                success_ratio=
                    success_ratio,
                avg_latency_ms=
                    avg_latency_ms,
                avg_memory_mb=
                    avg_memory_mb,
                avg_cpu_percent=
                    avg_cpu_percent,
            )
        )

        profile = ToolEfficiencyProfile(
            tool_name=tool_name,
            execution_count=
                execution_count,
            success_ratio=
                success_ratio,
            avg_latency_ms=
                avg_latency_ms,
            avg_memory_mb=
                avg_memory_mb,
            avg_cpu_percent=
                avg_cpu_percent,
            efficiency_score=
                efficiency_score,
            updated_at=time.time(),
        )

        async with self._lock:
            await asyncio.to_thread(
                self._upsert_profile,
                profile,
            )

        self._store_profile_cache(
            tool_name,
            profile,
        )

    async def optimization_candidates(
        self,
        *,
        threshold: float = 0.45,
    ) -> List[
        ToolEfficiencyProfile
    ]:
        rows = await asyncio.to_thread(
            self._fetch_low_profiles,
            threshold,
        )

        results: List[
            ToolEfficiencyProfile
        ] = []

        for row in rows:
            results.append(
                ToolEfficiencyProfile(
                    tool_name=row[0],
                    execution_count=row[1],
                    success_ratio=row[2],
                    avg_latency_ms=row[3],
                    avg_memory_mb=row[4],
                    avg_cpu_percent=row[5],
                    efficiency_score=row[6],
                    updated_at=row[7],
                )
            )

        return results

    async def export_rankings(
        self,
    ) -> List[Dict[str, Any]]:
        rows = await asyncio.to_thread(
            self._fetch_rankings
        )

        rankings = []

        for row in rows:
            rankings.append(
                {
                    "tool_name":
                        row[0],
                    "efficiency_score":
                        row[1],
                    "success_ratio":
                        row[2],
                    "avg_latency_ms":
                        row[3],
                    "execution_count":
                        row[4],
                }
            )

        return rankings

    async def _initialize_database(
        self,
    ) -> None:
        self._connection = sqlite3.connect(
            str(self._db_file),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure_database
        )

        await asyncio.to_thread(
            self._create_tables
        )

    def _configure_database(
        self,
    ) -> None:
        self._connection.execute(
            "PRAGMA journal_mode=WAL;"
        )

        self._connection.execute(
            "PRAGMA synchronous=NORMAL;"
        )

        self._connection.execute(
            "PRAGMA temp_store=MEMORY;"
        )

        self._connection.execute(
            "PRAGMA cache_size=-2000;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tool_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tool_name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                latency_ms REAL NOT NULL,
                memory_mb REAL NOT NULL,
                cpu_percent REAL NOT NULL,
                success INTEGER NOT NULL,
                created_at REAL NOT NULL,
                metadata TEXT
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tool_metrics_tool
            ON tool_metrics(tool_name)
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tool_metrics_time
            ON tool_metrics(created_at)
            """
        )

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tool_profiles (
                tool_name TEXT PRIMARY KEY,
                execution_count INTEGER NOT NULL,
                success_ratio REAL NOT NULL,
                avg_latency_ms REAL NOT NULL,
                avg_memory_mb REAL NOT NULL,
                avg_cpu_percent REAL NOT NULL,
                efficiency_score REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )

    def _insert_metric(
        self,
        metric: ToolExecutionMetric,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO tool_metrics (
                tool_name,
                task_type,
                latency_ms,
                memory_mb,
                cpu_percent,
                success,
                created_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                metric.tool_name,
                metric.task_type,
                metric.latency_ms,
                metric.memory_mb,
                metric.cpu_percent,
                int(metric.success),
                metric.timestamp,
                json.dumps(
                    metric.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _aggregate_metrics(
        self,
        tool_name: str,
    ) -> Optional[
        Tuple[
            int,
            float,
            float,
            float,
            float,
        ]
    ]:
        cursor = self._connection.execute(
            """
            SELECT
                COUNT(*),
                AVG(success),
                AVG(latency_ms),
                AVG(memory_mb),
                AVG(cpu_percent)
            FROM tool_metrics
            WHERE tool_name = ?
            """,
            (tool_name,),
        )

        row = cursor.fetchone()

        if not row or row[0] == 0:
            return None

        return (
            int(row[0]),
            round(float(row[1]), 4),
            round(float(row[2]), 2),
            round(float(row[3]), 2),
            round(float(row[4]), 2),
        )

    def _upsert_profile(
        self,
        profile: ToolEfficiencyProfile,
    ) -> None:
        self._connection.execute(
            """
            INSERT INTO tool_profiles (
                tool_name,
                execution_count,
                success_ratio,
                avg_latency_ms,
                avg_memory_mb,
                avg_cpu_percent,
                efficiency_score,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)

            ON CONFLICT(tool_name)
            DO UPDATE SET
                execution_count=excluded.execution_count,
                success_ratio=excluded.success_ratio,
                avg_latency_ms=excluded.avg_latency_ms,
                avg_memory_mb=excluded.avg_memory_mb,
                avg_cpu_percent=excluded.avg_cpu_percent,
                efficiency_score=excluded.efficiency_score,
                updated_at=excluded.updated_at
            """,
            (
                profile.tool_name,
                profile.execution_count,
                profile.success_ratio,
                profile.avg_latency_ms,
                profile.avg_memory_mb,
                profile.avg_cpu_percent,
                profile.efficiency_score,
                profile.updated_at,
            ),
        )

    def _fetch_profile(
        self,
        tool_name: str,
    ) -> Optional[Tuple]:
        cursor = self._connection.execute(
            """
            SELECT
                tool_name,
                execution_count,
                success_ratio,
                avg_latency_ms,
                avg_memory_mb,
                avg_cpu_percent,
                efficiency_score,
                updated_at
            FROM tool_profiles
            WHERE tool_name = ?
            LIMIT 1
            """,
            (tool_name,),
        )

        return cursor.fetchone()

    def _fetch_rankings(
        self,
    ) -> List[Tuple]:
        cursor = self._connection.execute(
            """
            SELECT
                tool_name,
                efficiency_score,
                success_ratio,
                avg_latency_ms,
                execution_count
            FROM tool_profiles
            ORDER BY efficiency_score DESC
            LIMIT 50
            """
        )

        return cursor.fetchall()

    def _fetch_low_profiles(
        self,
        threshold: float,
    ) -> List[Tuple]:
        cursor = self._connection.execute(
            """
            SELECT
                tool_name,
                execution_count,
                success_ratio,
                avg_latency_ms,
                avg_memory_mb,
                avg_cpu_percent,
                efficiency_score,
                updated_at
            FROM tool_profiles
            WHERE efficiency_score <= ?
            ORDER BY efficiency_score ASC
            """,
            (threshold,),
        )

        return cursor.fetchall()

    async def _cleanup_loop(
        self,
    ) -> None:
        while self._running:
            try:
                await asyncio.sleep(
                    self.CLEANUP_INTERVAL
                )

                await asyncio.to_thread(
                    self._cleanup_old_metrics
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Optimizer cleanup failure"
                )

    def _cleanup_old_metrics(
        self,
    ) -> None:
        retention_cutoff = (
            time.time()
            - (30 * 24 * 3600)
        )

        self._connection.execute(
            """
            DELETE FROM tool_metrics
            WHERE created_at < ?
            """,
            (retention_cutoff,),
        )

        self._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def _store_profile_cache(
        self,
        tool_name: str,
        profile: ToolEfficiencyProfile,
    ) -> None:
        if (
            len(self._profile_cache)
            >= self.PROFILE_CACHE_LIMIT
        ):
            oldest = next(
                iter(self._profile_cache)
            )

            self._profile_cache.pop(
                oldest,
                None,
            )

        self._profile_cache[
            tool_name
        ] = profile

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "cached_profiles":
                len(
                    self._profile_cache
                ),
            "feedback_buffer":
                len(
                    self._feedback_buffer
                ),
            "database":
                str(self._db_file),
            "timestamp":
                time.time(),
        }

    def clear_cache(
        self,
    ) -> None:
        self._profile_cache.clear()

    @property
    def database_path(
        self,
    ) -> str:
        return str(self._db_file)
