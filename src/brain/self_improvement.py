from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
import statistics
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Optional,
    Set,
)

from app.core.message_bus import (
    MessageBus,
)

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ExecutionFeedback:
    execution_id: str
    mitigation_type: str
    action_name: str
    success: bool
    latency_ms: float
    resource_cost: float
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class HeuristicProfile:
    profile_id: str
    mitigation_type: str
    success_weight: float
    latency_weight: float
    resource_weight: float
    confidence_score: float
    updated_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class LogicBoundaryValidator:
    """
    Strict RBAC + logic boundaries.

    Prevents:
    - Self-modifying code
    - Runtime source rewriting
    - Unauthorized tuning
    """

    IMMUTABLE_ACTIONS = {
        "filesystem.write_source",
        "code.modify",
        "runtime.patch",
        "self.modify",
    }

    ALLOWED_ACTIONS = {
        "heuristic.read",
        "heuristic.update",
        "heuristic.cache",
    }

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        *,
        action: str,
        permissions: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:
        if (
            action
            in self.IMMUTABLE_ACTIONS
        ):
            return False

        if (
            action
            not in self.ALLOWED_ACTIONS
        ):
            return False

        context = RouteContext(
            requester_id=
                "self_improvement",
            requester_roles={
                "system"
            },
            requester_permissions=
                permissions,
            task_type=action,
            metadata=metadata or {},
        )

        route = await self.router.route(
            task=action,
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class SQLiteFeedbackStore:
    """
    SQLite WAL heuristic feedback storage.
    """

    SQLITE_BUSY_TIMEOUT = 5000

    def __init__(
        self,
        *,
        database_path: str,
    ) -> None:
        self.database_path = (
            Path(database_path)
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(self) -> None:
        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure_database
        )

        await asyncio.to_thread(
            self._create_tables
        )

    async def close(self) -> None:
        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def store_feedback(
        self,
        feedback: ExecutionFeedback,
    ) -> None:
        await asyncio.to_thread(
            self._insert_feedback,
            feedback,
        )

    async def store_profile(
        self,
        profile: HeuristicProfile,
    ) -> None:
        await asyncio.to_thread(
            self._insert_profile,
            profile,
        )

    async def recent_feedback(
        self,
        *,
        limit: int = 200,
    ) -> List[ExecutionFeedback]:
        rows = await asyncio.to_thread(
            self._recent_feedback,
            limit,
        )

        results: List[
            ExecutionFeedback
        ] = []

        for row in rows:
            results.append(
                ExecutionFeedback(
                    execution_id=row[0],
                    mitigation_type=row[1],
                    action_name=row[2],
                    success=bool(row[3]),
                    latency_ms=row[4],
                    resource_cost=row[5],
                    created_at=row[6],
                    metadata=json.loads(
                        row[7]
                    ),
                )
            )

        return results

    async def load_profiles(
        self,
    ) -> List[HeuristicProfile]:
        rows = await asyncio.to_thread(
            self._load_profiles
        )

        results: List[
            HeuristicProfile
        ] = []

        for row in rows:
            results.append(
                HeuristicProfile(
                    profile_id=row[0],
                    mitigation_type=row[1],
                    success_weight=row[2],
                    latency_weight=row[3],
                    resource_weight=row[4],
                    confidence_score=row[5],
                    updated_at=row[6],
                    metadata=json.loads(
                        row[7]
                    ),
                )
            )

        return results

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
            CREATE TABLE IF NOT EXISTS execution_feedback (
                execution_id TEXT PRIMARY KEY,
                mitigation_type TEXT NOT NULL,
                action_name TEXT NOT NULL,
                success INTEGER NOT NULL,
                latency_ms REAL NOT NULL,
                resource_cost REAL NOT NULL,
                created_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS heuristic_profiles (
                profile_id TEXT PRIMARY KEY,
                mitigation_type TEXT UNIQUE NOT NULL,
                success_weight REAL NOT NULL,
                latency_weight REAL NOT NULL,
                resource_weight REAL NOT NULL,
                confidence_score REAL NOT NULL,
                updated_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

    def _insert_feedback(
        self,
        feedback: ExecutionFeedback,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO execution_feedback (
                execution_id,
                mitigation_type,
                action_name,
                success,
                latency_ms,
                resource_cost,
                created_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                feedback.execution_id,
                feedback.mitigation_type,
                feedback.action_name,
                int(feedback.success),
                feedback.latency_ms,
                feedback.resource_cost,
                feedback.created_at,
                json.dumps(
                    feedback.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _insert_profile(
        self,
        profile: HeuristicProfile,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO heuristic_profiles (
                profile_id,
                mitigation_type,
                success_weight,
                latency_weight,
                resource_weight,
                confidence_score,
                updated_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile.profile_id,
                profile.mitigation_type,
                profile.success_weight,
                profile.latency_weight,
                profile.resource_weight,
                profile.confidence_score,
                profile.updated_at,
                json.dumps(
                    profile.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _recent_feedback(
        self,
        limit: int,
    ) -> List[Any]:
        cursor = self._connection.execute(
            """
            SELECT
                execution_id,
                mitigation_type,
                action_name,
                success,
                latency_ms,
                resource_cost,
                created_at,
                metadata
            FROM execution_feedback
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

        return cursor.fetchall()

    def _load_profiles(
        self,
    ) -> List[Any]:
        cursor = self._connection.execute(
            """
            SELECT
                profile_id,
                mitigation_type,
                success_weight,
                latency_weight,
                resource_weight,
                confidence_score,
                updated_at,
                metadata
            FROM heuristic_profiles
            """
        )

        return cursor.fetchall()


class HeuristicLearningEngine:
    """
    Lightweight heuristic learning engine.

    No ML frameworks.
    """

    MIN_WEIGHT = 0.1
    MAX_WEIGHT = 5.0

    def analyze(
        self,
        feedback_items: List[
            ExecutionFeedback
        ],
    ) -> Dict[
        str,
        HeuristicProfile
    ]:
        grouped: Dict[
            str,
            List[
                ExecutionFeedback
            ],
        ] = {}

        for item in feedback_items:
            grouped.setdefault(
                item.mitigation_type,
                [],
            ).append(item)

        profiles: Dict[
            str,
            HeuristicProfile,
        ] = {}

        for (
            mitigation_type,
            items,
        ) in grouped.items():

            success_ratio = (
                self._success_ratio(
                    items
                )
            )

            avg_latency = (
                self._average_latency(
                    items
                )
            )

            avg_resource = (
                self._average_resource(
                    items
                )
            )

            success_weight = (
                self._clamp(
                    1.0
                    + (
                        success_ratio
                        * 2.0
                    )
                )
            )

            latency_weight = (
                self._clamp(
                    1.0
                    / max(
                        0.1,
                        avg_latency
                        / 1000,
                    )
                )
            )

            resource_weight = (
                self._clamp(
                    1.0
                    / max(
                        0.1,
                        avg_resource,
                    )
                )
            )

            confidence = (
                self._confidence(
                    items
                )
            )

            profiles[
                mitigation_type
            ] = HeuristicProfile(
                profile_id=
                    uuid.uuid4().hex,
                mitigation_type=
                    mitigation_type,
                success_weight=round(
                    success_weight,
                    4,
                ),
                latency_weight=round(
                    latency_weight,
                    4,
                ),
                resource_weight=round(
                    resource_weight,
                    4,
                ),
                confidence_score=round(
                    confidence,
                    4,
                ),
                updated_at=
                    time.time(),
                metadata={
                    "samples":
                        len(items),
                },
            )

        return profiles

    def _success_ratio(
        self,
        items: List[
            ExecutionFeedback
        ],
    ) -> float:
        if not items:
            return 0.0

        successful = sum(
            1
            for item in items
            if item.success
        )

        return successful / len(
            items
        )

    def _average_latency(
        self,
        items: List[
            ExecutionFeedback
        ],
    ) -> float:
        values = [
            item.latency_ms
            for item in items
        ]

        return (
            statistics.mean(
                values
            )
            if values
            else 1000.0
        )

    def _average_resource(
        self,
        items: List[
            ExecutionFeedback
        ],
    ) -> float:
        values = [
            item.resource_cost
            for item in items
        ]

        return (
            statistics.mean(
                values
            )
            if values
            else 1.0
        )

    def _confidence(
        self,
        items: List[
            ExecutionFeedback
        ],
    ) -> float:
        if len(items) < 2:
            return 0.25

        success_values = [
            1.0
            if item.success
            else 0.0
            for item in items
        ]

        variance = (
            statistics.pvariance(
                success_values
            )
        )

        confidence = (
            1.0
            / (1.0 + variance)
        )

        return min(
            confidence,
            1.0,
        )

    def _clamp(
        self,
        value: float,
    ) -> float:
        return max(
            self.MIN_WEIGHT,
            min(
                self.MAX_WEIGHT,
                value,
            ),
        )


class DynamicWeightAdapter:
    """
    Low-latency heuristic cache.
    """

    CACHE_LIMIT = 256

    def __init__(
        self,
    ) -> None:
        self._profiles: Dict[
            str,
            HeuristicProfile,
        ] = {}

        self._access_order: Deque[
            str
        ] = deque(
            maxlen=self.CACHE_LIMIT
        )

    def update_profiles(
        self,
        profiles: Dict[
            str,
            HeuristicProfile,
        ],
    ) -> None:
        for (
            mitigation_type,
            profile,
        ) in profiles.items():

            self._profiles[
                mitigation_type
            ] = profile

            self._access_order.append(
                mitigation_type
            )

    def get_profile(
        self,
        mitigation_type: str,
    ) -> Optional[
        HeuristicProfile
    ]:
        profile = (
            self._profiles.get(
                mitigation_type
            )
        )

        if profile:
            self._access_order.append(
                mitigation_type
            )

        return profile

    def snapshot(
        self,
    ) -> Dict[str, Dict[str, Any]]:
        output = {}

        for (
            mitigation_type,
            profile,
        ) in self._profiles.items():

            output[
                mitigation_type
            ] = {
                "success_weight":
                    profile.success_weight,
                "latency_weight":
                    profile.latency_weight,
                "resource_weight":
                    profile.resource_weight,
                "confidence_score":
                    profile.confidence_score,
            }

        return output


class SelfImprovementEngine:
    """
    Async-first Self-improvement Heuristics Engine.

    Features:
    - Heuristic learning engine
    - Feedback history analysis
    - Dynamic weight tuning
    - Low-latency profile caching
    - SQLite WAL persistence
    - Strict security boundaries
    - No self-modifying code
    """

    CLEANUP_INTERVAL = 3600

    DEFAULT_ALLOWED_PERMISSIONS = {
        "heuristic.read",
        "heuristic.update",
        "heuristic.cache",
    }

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: MessageBus,
        database_path: str = (
            "./data/self_improvement.db"
        ),
        learning_interval: int = 300,
        allowed_permissions: Optional[
            Set[str]
        ] = None,
    ) -> None:
        self.router = router

        self.message_bus = (
            message_bus
        )

        self.learning_interval = max(
            60,
            learning_interval,
        )

        self.allowed_permissions = (
            allowed_permissions
            or set(
                self.DEFAULT_ALLOWED_PERMISSIONS
            )
        )

        self._validator = (
            LogicBoundaryValidator(
                router
            )
        )

        self._store = (
            SQLiteFeedbackStore(
                database_path=
                    database_path
            )
        )

        self._learning = (
            HeuristicLearningEngine()
        )

        self._adapter = (
            DynamicWeightAdapter()
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._profile_cache: Deque[
            str
        ] = deque(maxlen=128)

    async def start(self) -> None:
        logger.info(
            "Starting SelfImprovementEngine"
        )

        await self._store.initialize()

        await self._restore_profiles()

        self._running = True

        self._tasks.append(
            asyncio.create_task(
                self._learning_loop()
            )
        )

        self._tasks.append(
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(self) -> None:
        logger.info(
            "Stopping SelfImprovementEngine"
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

        await self._store.close()

    async def record_feedback(
        self,
        feedback: ExecutionFeedback,
    ) -> bool:
        """
        Feedback ingestion pipeline.
        """

        allowed = (
            await self._validator.validate(
                action=
                    "heuristic.update",
                permissions=
                    self.allowed_permissions,
                metadata=
                    feedback.metadata,
            )
        )

        if not allowed:
            logger.warning(
                "Feedback rejected by RBAC"
            )

            return False

        await self._store.store_feedback(
            feedback
        )

        return True

    async def get_profile(
        self,
        mitigation_type: str,
    ) -> Optional[
        HeuristicProfile
    ]:
        allowed = (
            await self._validator.validate(
                action=
                    "heuristic.cache",
                permissions=
                    self.allowed_permissions,
            )
        )

        if not allowed:
            return None

        return (
            self._adapter.get_profile(
                mitigation_type
            )
        )

    async def _learning_loop(
        self,
    ) -> None:
        """
        Runtime heuristic optimization loop.
        """

        while self._running:
            try:
                allowed = (
                    await self._validator.validate(
                        action=
                            "heuristic.read",
                        permissions=
                            self.allowed_permissions,
                    )
                )

                if not allowed:
                    await asyncio.sleep(
                        self.learning_interval
                    )

                    continue

                feedback_items = (
                    await self._store.recent_feedback(
                        limit=500
                    )
                )

                generated_profiles = (
                    self._learning.analyze(
                        feedback_items
                    )
                )

                self._adapter.update_profiles(
                    generated_profiles
                )

                for profile in (
                    generated_profiles.values()
                ):
                    await self._store.store_profile(
                        profile
                    )

                    self._profile_cache.append(
                        profile.profile_id
                    )

                await self._broadcast_updates(
                    generated_profiles
                )

                logger.info(
                    "Heuristic profiles updated | count=%s",
                    len(
                        generated_profiles
                    ),
                )

                await asyncio.sleep(
                    self.learning_interval
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Self-improvement learning failure"
                )

                await asyncio.sleep(
                    self.learning_interval
                )

    async def _broadcast_updates(
        self,
        profiles: Dict[
            str,
            HeuristicProfile,
        ],
    ) -> None:
        payload = {
            "profiles":
                {
                    key: {
                        "success_weight":
                            value.success_weight,
                        "latency_weight":
                            value.latency_weight,
                        "resource_weight":
                            value.resource_weight,
                        "confidence":
                            value.confidence_score,
                    }
                    for key, value in profiles.items()
                },
            "timestamp":
                time.time(),
        }

        await self.message_bus.publish(
            topic=
                "heuristics.updated",
            payload=payload,
        )

    async def _restore_profiles(
        self,
    ) -> None:
        profiles = (
            await self._store.load_profiles()
        )

        mapping: Dict[
            str,
            HeuristicProfile,
        ] = {}

        for profile in profiles:
            mapping[
                profile.mitigation_type
            ] = profile

        self._adapter.update_profiles(
            mapping
        )

    async def _maintenance_loop(
        self,
    ) -> None:
        while self._running:
            try:
                await asyncio.sleep(
                    self.CLEANUP_INTERVAL
                )

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Self-improvement maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self._store._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "cached_profiles":
                len(
                    self._profile_cache
                ),
            "learning_interval":
                self.learning_interval,
            "heuristic_profiles":
                len(
                    self._adapter.snapshot()
                ),
            "timestamp":
                time.time(),
        }
