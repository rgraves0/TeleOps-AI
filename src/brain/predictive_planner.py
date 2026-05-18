from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import sqlite3
import statistics
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Optional,
    Sequence,
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


class PredictionLevel(str, Enum):
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class ThrottleAction(str, Enum):
    NONE = "none"
    REDUCE_CONCURRENCY = (
        "reduce_concurrency"
    )
    PAUSE_BACKGROUND_TASKS = (
        "pause_background_tasks"
    )
    ENABLE_DEGRADATION = (
        "enable_degradation"
    )


@dataclass(slots=True)
class MetricPoint:
    timestamp: float
    cpu_percent: float
    memory_percent: float
    throughput_rate: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class WorkloadPrediction:
    prediction_id: str
    created_at: float
    predicted_cpu: float
    predicted_memory: float
    predicted_throughput: float
    confidence_score: float
    prediction_level: PredictionLevel
    recommended_action: ThrottleAction
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class BoundaryValidator:
    """
    Default Deny + RBAC enforcement.
    """

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
        context = RouteContext(
            requester_id=
                "predictive_planner",
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


class SQLiteMetricsHistory:
    """
    Lightweight SQLite WAL metrics store.
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

    async def insert_metric(
        self,
        metric: MetricPoint,
    ) -> None:
        await asyncio.to_thread(
            self._insert_metric,
            metric,
        )

    async def fetch_recent_metrics(
        self,
        *,
        limit: int = 120,
    ) -> List[MetricPoint]:
        rows = await asyncio.to_thread(
            self._fetch_recent,
            limit,
        )

        results: List[
            MetricPoint
        ] = []

        for row in rows:
            results.append(
                MetricPoint(
                    timestamp=row[0],
                    cpu_percent=row[1],
                    memory_percent=row[2],
                    throughput_rate=row[3],
                    metadata=json.loads(
                        row[4]
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
            CREATE TABLE IF NOT EXISTS workload_metrics (
                timestamp REAL PRIMARY KEY,
                cpu_percent REAL NOT NULL,
                memory_percent REAL NOT NULL,
                throughput_rate REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_workload_ts
            ON workload_metrics(timestamp)
            """
        )

    def _insert_metric(
        self,
        metric: MetricPoint,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO workload_metrics (
                timestamp,
                cpu_percent,
                memory_percent,
                throughput_rate,
                metadata
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                metric.timestamp,
                metric.cpu_percent,
                metric.memory_percent,
                metric.throughput_rate,
                json.dumps(
                    metric.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _fetch_recent(
        self,
        limit: int,
    ) -> List[Any]:
        cursor = self._connection.execute(
            """
            SELECT
                timestamp,
                cpu_percent,
                memory_percent,
                throughput_rate,
                metadata
            FROM workload_metrics
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (limit,),
        )

        return cursor.fetchall()


class StatisticalWorkloadPredictor:
    """
    Lightweight statistical predictor.

    Uses:
    - Moving averages
    - Trend slope analysis
    - Variance heuristics
    """

    HIGH_CPU_THRESHOLD = 85
    HIGH_MEMORY_THRESHOLD = 85

    def predict(
        self,
        metrics: Sequence[
            MetricPoint
        ],
    ) -> WorkloadPrediction:
        if not metrics:
            return self._empty_prediction()

        cpu_values = [
            m.cpu_percent
            for m in metrics
        ]

        memory_values = [
            m.memory_percent
            for m in metrics
        ]

        throughput_values = [
            m.throughput_rate
            for m in metrics
        ]

        cpu_prediction = (
            self._moving_average(
                cpu_values
            )
            + self._trend_slope(
                cpu_values
            )
        )

        memory_prediction = (
            self._moving_average(
                memory_values
            )
            + self._trend_slope(
                memory_values
            )
        )

        throughput_prediction = (
            self._moving_average(
                throughput_values
            )
            + self._trend_slope(
                throughput_values
            )
        )

        confidence = (
            self._confidence_score(
                cpu_values,
                memory_values,
            )
        )

        level = (
            self._determine_level(
                cpu_prediction,
                memory_prediction,
            )
        )

        action = (
            self._determine_action(
                level
            )
        )

        return WorkloadPrediction(
            prediction_id=
                uuid.uuid4().hex,
            created_at=time.time(),
            predicted_cpu=round(
                cpu_prediction,
                2,
            ),
            predicted_memory=round(
                memory_prediction,
                2,
            ),
            predicted_throughput=round(
                throughput_prediction,
                2,
            ),
            confidence_score=round(
                confidence,
                4,
            ),
            prediction_level=
                level,
            recommended_action=
                action,
            metadata={
                "samples":
                    len(metrics),
            },
        )

    def _moving_average(
        self,
        values: Sequence[float],
        window: int = 5,
    ) -> float:
        if not values:
            return 0.0

        scoped = list(values)[
            :window
        ]

        return sum(scoped) / len(
            scoped
        )

    def _trend_slope(
        self,
        values: Sequence[float],
    ) -> float:
        if len(values) < 2:
            return 0.0

        latest = values[0]
        oldest = values[-1]

        slope = (
            latest - oldest
        ) / max(
            1,
            len(values),
        )

        return slope

    def _confidence_score(
        self,
        cpu_values: Sequence[
            float
        ],
        memory_values: Sequence[
            float
        ],
    ) -> float:
        if (
            len(cpu_values) < 2
            or len(memory_values)
            < 2
        ):
            return 0.25

        cpu_variance = (
            statistics.pvariance(
                cpu_values
            )
        )

        mem_variance = (
            statistics.pvariance(
                memory_values
            )
        )

        combined = (
            cpu_variance
            + mem_variance
        )

        normalized = max(
            0.05,
            1.0
            / (1.0 + combined),
        )

        return min(
            normalized,
            1.0,
        )

    def _determine_level(
        self,
        cpu_prediction: float,
        memory_prediction: float,
    ) -> PredictionLevel:
        if (
            cpu_prediction >= 95
            or memory_prediction
            >= 95
        ):
            return (
                PredictionLevel.CRITICAL
            )

        if (
            cpu_prediction
            >= self.HIGH_CPU_THRESHOLD
            or memory_prediction
            >= self.HIGH_MEMORY_THRESHOLD
        ):
            return (
                PredictionLevel.HIGH
            )

        if (
            cpu_prediction >= 60
            or memory_prediction
            >= 60
        ):
            return (
                PredictionLevel.MODERATE
            )

        return PredictionLevel.LOW

    def _determine_action(
        self,
        level: PredictionLevel,
    ) -> ThrottleAction:
        if (
            level
            == PredictionLevel.CRITICAL
        ):
            return (
                ThrottleAction.ENABLE_DEGRADATION
            )

        if (
            level
            == PredictionLevel.HIGH
        ):
            return (
                ThrottleAction.PAUSE_BACKGROUND_TASKS
            )

        if (
            level
            == PredictionLevel.MODERATE
        ):
            return (
                ThrottleAction.REDUCE_CONCURRENCY
            )

        return ThrottleAction.NONE

    def _empty_prediction(
        self,
    ) -> WorkloadPrediction:
        return WorkloadPrediction(
            prediction_id=
                uuid.uuid4().hex,
            created_at=time.time(),
            predicted_cpu=0.0,
            predicted_memory=0.0,
            predicted_throughput=0.0,
            confidence_score=0.0,
            prediction_level=
                PredictionLevel.LOW,
            recommended_action=
                ThrottleAction.NONE,
        )


class DynamicResourceThrottler:
    """
    Dynamic resource throttling trigger.
    """

    def __init__(
        self,
        *,
        message_bus: MessageBus,
    ) -> None:
        self.message_bus = (
            message_bus
        )

    async def trigger(
        self,
        prediction: WorkloadPrediction,
    ) -> None:
        if (
            prediction.recommended_action
            == ThrottleAction.NONE
        ):
            return

        payload = {
            "prediction_id":
                prediction.prediction_id,
            "action":
                prediction.recommended_action.value,
            "predicted_cpu":
                prediction.predicted_cpu,
            "predicted_memory":
                prediction.predicted_memory,
            "confidence":
                prediction.confidence_score,
            "timestamp":
                time.time(),
        }

        await self.message_bus.publish(
            topic="system.throttle",
            payload=payload,
        )

        logger.warning(
            "Throttle signal dispatched | action=%s",
            prediction.recommended_action.value,
        )


class PredictivePlanner:
    """
    Async-first Predictive Workload Planner.

    Features:
    - Statistical workload prediction
    - Moving average forecasting
    - Trend analysis heuristics
    - SQLite WAL metrics analysis
    - Dynamic throttling triggers
    - RBAC-safe boundaries
    - Low-memory production runtime
    """

    CLEANUP_INTERVAL = 3600

    DEFAULT_ALLOWED_PERMISSIONS = {
        "metrics.read",
        "system.throttle",
    }

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: MessageBus,
        database_path: str = (
            "./data/predictive_metrics.db"
        ),
        prediction_interval: int = 60,
        allowed_permissions: Optional[
            Set[str]
        ] = None,
    ) -> None:
        self.router = router

        self.message_bus = (
            message_bus
        )

        self.prediction_interval = (
            max(
                15,
                prediction_interval,
            )
        )

        self.allowed_permissions = (
            allowed_permissions
            or set(
                self.DEFAULT_ALLOWED_PERMISSIONS
            )
        )

        self._validator = (
            BoundaryValidator(
                router
            )
        )

        self._history = (
            SQLiteMetricsHistory(
                database_path=
                    database_path
            )
        )

        self._predictor = (
            StatisticalWorkloadPredictor()
        )

        self._throttler = (
            DynamicResourceThrottler(
                message_bus=
                    message_bus
            )
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._prediction_cache: Deque[
            str
        ] = deque(maxlen=128)

    async def start(self) -> None:
        logger.info(
            "Starting PredictivePlanner"
        )

        await self._history.initialize()

        self._running = True

        self._tasks.append(
            asyncio.create_task(
                self._prediction_loop()
            )
        )

        self._tasks.append(
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(self) -> None:
        logger.info(
            "Stopping PredictivePlanner"
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

        await self._history.close()

    async def ingest_metric(
        self,
        metric: MetricPoint,
    ) -> None:
        """
        Metrics ingestion pipeline.
        """

        allowed = (
            await self._validator.validate(
                action="metrics.read",
                permissions=
                    self.allowed_permissions,
                metadata=
                    metric.metadata,
            )
        )

        if not allowed:
            logger.warning(
                "Metrics ingestion blocked"
            )

            return

        await self._history.insert_metric(
            metric
        )

    async def generate_prediction(
        self,
    ) -> WorkloadPrediction:
        """
        Main predictive analysis pipeline.
        """

        allowed = (
            await self._validator.validate(
                action="metrics.read",
                permissions=
                    self.allowed_permissions,
            )
        )

        if not allowed:
            raise PermissionError(
                "Metrics access denied"
            )

        metrics = (
            await self._history.fetch_recent_metrics(
                limit=120
            )
        )

        prediction = (
            self._predictor.predict(
                metrics
            )
        )

        self._prediction_cache.append(
            prediction.prediction_id
        )

        return prediction

    async def _prediction_loop(
        self,
    ) -> None:
        while self._running:
            try:
                prediction = (
                    await self.generate_prediction()
                )

                logger.info(
                    "Prediction generated | cpu=%.2f memory=%.2f level=%s",
                    prediction.predicted_cpu,
                    prediction.predicted_memory,
                    prediction.prediction_level.value,
                )

                allowed = (
                    await self._validator.validate(
                        action=
                            "system.throttle",
                        permissions=
                            self.allowed_permissions,
                    )
                )

                if allowed:
                    await self._throttler.trigger(
                        prediction
                    )

                await asyncio.sleep(
                    self.prediction_interval
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Predictive planner loop failure"
                )

                await asyncio.sleep(
                    self.prediction_interval
                )

    async def recent_metrics(
        self,
        *,
        limit: int = 30,
    ) -> List[MetricPoint]:
        return (
            await self._history.fetch_recent_metrics(
                limit=limit
            )
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
                    "Predictive planner maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self._history._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "prediction_interval":
                self.prediction_interval,
            "cached_predictions":
                len(
                    self._prediction_cache
                ),
            "allowed_permissions":
                list(
                    self.allowed_permissions
                ),
            "timestamp":
                time.time(),
        }
