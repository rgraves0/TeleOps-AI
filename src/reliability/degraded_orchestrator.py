from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import time
import traceback
from collections import deque
from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
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


class DegradedLevel(
    str,
    Enum,
):
    NORMAL = "normal"
    DEGRADED = "degraded"
    CRITICAL = "critical"


class CircuitState(
    str,
    Enum,
):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(slots=True)
class DependencyHealth:
    dependency_name: str
    failure_count: int
    success_count: int
    last_failure_at: float
    last_success_at: float
    circuit_state: CircuitState
    degraded_level: DegradedLevel
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class DegradedDecision:
    allowed: bool
    degraded_level: DegradedLevel
    strategy: str
    reason: Optional[str]
    timestamp: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class DegradedRBACValidator:
    """
    Default Deny degraded-mode validator.
    """

    REQUIRED_PERMISSION = (
        "system.degraded.manage"
    )

    SYSTEM_ROLES = {
        "admin",
        "system",
        "core",
    }

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        task_type: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:

        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        if not (
            roles
            & self.SYSTEM_ROLES
        ):
            return False

        context = RouteContext(
            requester_id=
                requester_id,
            requester_roles=
                roles,
            requester_permissions=
                permissions,
            task_type=task_type,
            metadata=
                metadata or {},
        )

        route = await self.router.route(
            task=task_type,
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class SQLiteDegradedStateStore:
    """
    SQLite WAL degraded-state persistence.
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

    async def initialize(
        self,
    ) -> None:

        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure
        )

        await asyncio.to_thread(
            self._create_tables
        )

    async def close(
        self,
    ) -> None:

        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def persist_health(
        self,
        health: DependencyHealth,
    ) -> None:

        await asyncio.to_thread(
            self._persist_health,
            health,
        )

    async def load_health(
        self,
        dependency_name: str,
    ) -> Optional[
        DependencyHealth
    ]:

        row = await asyncio.to_thread(
            self._load_health,
            dependency_name,
        )

        if not row:
            return None

        return DependencyHealth(
            dependency_name=row[0],
            failure_count=row[1],
            success_count=row[2],
            last_failure_at=row[3],
            last_success_at=row[4],
            circuit_state=
                CircuitState(row[5]),
            degraded_level=
                DegradedLevel(row[6]),
            metadata=row[7],
        )

    def _configure(
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
            "PRAGMA cache_size=-1000;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS degraded_states (
                dependency_name TEXT PRIMARY KEY,
                failure_count INTEGER NOT NULL,
                success_count INTEGER NOT NULL,
                last_failure_at REAL NOT NULL,
                last_success_at REAL NOT NULL,
                circuit_state TEXT NOT NULL,
                degraded_level TEXT NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

    def _persist_health(
        self,
        health: DependencyHealth,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO degraded_states (
                dependency_name,
                failure_count,
                success_count,
                last_failure_at,
                last_success_at,
                circuit_state,
                degraded_level,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                health.dependency_name,
                health.failure_count,
                health.success_count,
                health.last_failure_at,
                health.last_success_at,
                health.circuit_state.value,
                health.degraded_level.value,
                str(
                    health.metadata
                ),
            ),
        )

    def _load_health(
        self,
        dependency_name: str,
    ) -> Optional[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                dependency_name,
                failure_count,
                success_count,
                last_failure_at,
                last_success_at,
                circuit_state,
                degraded_level,
                metadata
            FROM degraded_states
            WHERE dependency_name = ?
            LIMIT 1
            """,
            (dependency_name,),
        )

        return cursor.fetchone()


class ActiveCircuitBreaker:
    """
    Lightweight circuit breaker.
    """

    FAILURE_THRESHOLD = 5
    HALF_OPEN_TIMEOUT = 30

    def __init__(
        self,
    ) -> None:

        self._states: Dict[
            str,
            DependencyHealth,
        ] = {}

    async def record_success(
        self,
        dependency_name: str,
    ) -> DependencyHealth:

        state = (
            self._states.get(
                dependency_name
            )
        )

        if not state:
            state = self._new_state(
                dependency_name
            )

        state.success_count += 1

        state.last_success_at = (
            time.time()
        )

        if (
            state.circuit_state
            == CircuitState.HALF_OPEN
        ):
            state.circuit_state = (
                CircuitState.CLOSED
            )

            state.degraded_level = (
                DegradedLevel.NORMAL
            )

        self._states[
            dependency_name
        ] = state

        return state

    async def record_failure(
        self,
        dependency_name: str,
    ) -> DependencyHealth:

        state = (
            self._states.get(
                dependency_name
            )
        )

        if not state:
            state = self._new_state(
                dependency_name
            )

        state.failure_count += 1

        state.last_failure_at = (
            time.time()
        )

        if (
            state.failure_count
            >= self.FAILURE_THRESHOLD
        ):
            state.circuit_state = (
                CircuitState.OPEN
            )

            state.degraded_level = (
                DegradedLevel.DEGRADED
            )

        if (
            state.failure_count
            >= (
                self.FAILURE_THRESHOLD
                * 2
            )
        ):
            state.degraded_level = (
                DegradedLevel.CRITICAL
            )

        self._states[
            dependency_name
        ] = state

        return state

    async def should_allow(
        self,
        dependency_name: str,
    ) -> bool:

        state = (
            self._states.get(
                dependency_name
            )
        )

        if not state:
            return True

        if (
            state.circuit_state
            == CircuitState.CLOSED
        ):
            return True

        if (
            state.circuit_state
            == CircuitState.OPEN
        ):
            elapsed = (
                time.time()
                - state.last_failure_at
            )

            if (
                elapsed
                >= self.HALF_OPEN_TIMEOUT
            ):
                state.circuit_state = (
                    CircuitState.HALF_OPEN
                )

                return True

            return False

        return True

    def state(
        self,
        dependency_name: str,
    ) -> Optional[
        DependencyHealth
    ]:
        return self._states.get(
            dependency_name
        )

    def _new_state(
        self,
        dependency_name: str,
    ) -> DependencyHealth:

        return DependencyHealth(
            dependency_name=
                dependency_name,
            failure_count=0,
            success_count=0,
            last_failure_at=0.0,
            last_success_at=0.0,
            circuit_state=
                CircuitState.CLOSED,
            degraded_level=
                DegradedLevel.NORMAL,
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "tracked_dependencies":
                len(
                    self._states
                )
        }


class GracefulDegradationStrategies:
    """
    Lightweight fallback strategies.
    """

    LARGE_TASK_LIMIT = 2048

    async def apply(
        self,
        *,
        degraded_level: DegradedLevel,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:

        modified = dict(
            payload
        )

        if (
            degraded_level
            == DegradedLevel.DEGRADED
        ):
            modified[
                "cache_only"
            ] = True

            modified[
                "allow_external_calls"
            ] = False

            modified[
                "priority"
            ] = "reduced"

        elif (
            degraded_level
            == DegradedLevel.CRITICAL
        ):
            modified[
                "cache_only"
            ] = True

            modified[
                "allow_external_calls"
            ] = False

            modified[
                "fallback_response"
            ] = (
                "System temporarily degraded"
            )

            modified[
                "priority"
            ] = "minimal"

            if (
                len(
                    str(modified)
                )
                > self.LARGE_TASK_LIMIT
            ):
                modified = {
                    "fallback_response":
                        "Task paused due to critical degradation"
                }

        return modified


class ActiveStateMonitor:
    """
    Runtime overload monitor.
    """

    MEMORY_THRESHOLD_MB = 850
    CPU_THRESHOLD = 90.0

    def __init__(
        self,
    ) -> None:

        self._metrics_history: Deque[
            Dict[str, Any]
        ] = deque(maxlen=128)

    async def record_metrics(
        self,
        *,
        memory_mb: float,
        cpu_percent: float,
    ) -> None:

        self._metrics_history.append(
            {
                "memory_mb":
                    memory_mb,
                "cpu_percent":
                    cpu_percent,
                "timestamp":
                    time.time(),
            }
        )

    async def overloaded(
        self,
    ) -> bool:

        if not self._metrics_history:
            return False

        latest = (
            self._metrics_history[-1]
        )

        return (
            latest["memory_mb"]
            >= self.MEMORY_THRESHOLD_MB
            or latest["cpu_percent"]
            >= self.CPU_THRESHOLD
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "metric_samples":
                len(
                    self._metrics_history
                )
        }


class SystemDegradationController:
    """
    Dynamic degradation controller.
    """

    def __init__(
        self,
        *,
        breaker: ActiveCircuitBreaker,
        strategies:
            GracefulDegradationStrategies,
    ) -> None:

        self.breaker = breaker

        self.strategies = (
            strategies
        )

    async def evaluate(
        self,
        *,
        dependency_name: str,
        payload: Dict[str, Any],
    ) -> DegradedDecision:

        allowed = (
            await self.breaker.should_allow(
                dependency_name
            )
        )

        state = self.breaker.state(
            dependency_name
        )

        if not state:
            return DegradedDecision(
                allowed=True,
                degraded_level=
                    DegradedLevel.NORMAL,
                strategy=
                    "normal_execution",
                reason=None,
                timestamp=
                    time.time(),
            )

        if allowed:
            return DegradedDecision(
                allowed=True,
                degraded_level=
                    state.degraded_level,
                strategy=
                    "fallback_execution",
                reason=
                    "Degraded execution path",
                timestamp=
                    time.time(),
                metadata=
                    await self.strategies.apply(
                        degraded_level=
                            state.degraded_level,
                        payload=
                            payload,
                    ),
            )

        return DegradedDecision(
            allowed=False,
            degraded_level=
                state.degraded_level,
            strategy=
                "execution_blocked",
            reason=
                "Circuit breaker active",
            timestamp=
                time.time(),
        )


class DegradedOrchestrator:
    """
    Async-first Production Degraded-mode Orchestrator.

    Features:
    - Dynamic degraded mode transitions
    - Active circuit breakers
    - Graceful fallback strategies
    - Runtime overload protection
    - SQLite WAL persistent state
    - Default Deny security guardrails
    """

    MAINTENANCE_INTERVAL = 600

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: Optional[
            MessageBus
        ] = None,
        database_path: str = (
            "./data/degraded_mode.db"
        ),
    ) -> None:

        self.router = router

        self.message_bus = (
            message_bus
        )

        self._validator = (
            DegradedRBACValidator(
                router
            )
        )

        self._store = (
            SQLiteDegradedStateStore(
                database_path=
                    database_path
            )
        )

        self._breaker = (
            ActiveCircuitBreaker()
        )

        self._strategies = (
            GracefulDegradationStrategies()
        )

        self._monitor = (
            ActiveStateMonitor()
        )

        self._controller = (
            SystemDegradationController(
                breaker=
                    self._breaker,
                strategies=
                    self._strategies,
            )
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

        self._degraded_events = 0

        self._critical_events = 0

        self._blocked_events = 0

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting DegradedOrchestrator"
        )

        await self._store.initialize()

        self._running = True

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping DegradedOrchestrator"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        await self._store.close()

    async def record_dependency_failure(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        dependency_name: str,
    ) -> None:

        allowed = (
            await self._validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                task_type=
                    "degraded.failure.record",
            )
        )

        if not allowed:
            raise PermissionError(
                "Failure recording denied"
            )

        state = (
            await self._breaker.record_failure(
                dependency_name
            )
        )

        await self._store.persist_health(
            state
        )

        if (
            state.degraded_level
            == DegradedLevel.DEGRADED
        ):
            self._degraded_events += 1

        if (
            state.degraded_level
            == DegradedLevel.CRITICAL
        ):
            self._critical_events += 1

        await self._emit_event(
            "dependency_failure",
            {
                "dependency":
                    dependency_name,
                "degraded_level":
                    state.degraded_level.value,
            },
        )

    async def record_dependency_success(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        dependency_name: str,
    ) -> None:

        allowed = (
            await self._validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                task_type=
                    "degraded.success.record",
            )
        )

        if not allowed:
            raise PermissionError(
                "Success recording denied"
            )

        state = (
            await self._breaker.record_success(
                dependency_name
            )
        )

        await self._store.persist_health(
            state
        )

    async def evaluate_execution(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        dependency_name: str,
        payload: Dict[str, Any],
    ) -> DegradedDecision:

        allowed = (
            await self._validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                task_type=
                    "degraded.execution.evaluate",
            )
        )

        if not allowed:
            return DegradedDecision(
                allowed=False,
                degraded_level=
                    DegradedLevel.CRITICAL,
                strategy=
                    "denied",
                reason=
                    "RBAC denied",
                timestamp=
                    time.time(),
            )

        overloaded = (
            await self._monitor.overloaded()
        )

        if overloaded:
            self._critical_events += 1

            await self._emit_event(
                "system_overload_detected",
                {
                    "dependency":
                        dependency_name
                },
            )

            return DegradedDecision(
                allowed=False,
                degraded_level=
                    DegradedLevel.CRITICAL,
                strategy=
                    "resource_protection",
                reason=
                    "System overload detected",
                timestamp=
                    time.time(),
            )

        decision = (
            await self._controller.evaluate(
                dependency_name=
                    dependency_name,
                payload=payload,
            )
        )

        if not decision.allowed:
            self._blocked_events += 1

        return decision

    async def record_runtime_metrics(
        self,
        *,
        memory_mb: float,
        cpu_percent: float,
    ) -> None:

        await self._monitor.record_metrics(
            memory_mb=
                memory_mb,
            cpu_percent=
                cpu_percent,
        )

    async def _emit_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic=
                "degraded.events",
            payload={
                "type":
                    event_type,
                "timestamp":
                    time.time(),
                **payload,
            },
        )

    async def _maintenance_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await asyncio.sleep(
                    self.MAINTENANCE_INTERVAL
                )

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
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
            "degraded_events":
                self._degraded_events,
            "critical_events":
                self._critical_events,
            "blocked_events":
                self._blocked_events,
            "breaker":
                self._breaker.stats(),
            "monitor":
                self._monitor.stats(),
            "timestamp":
                time.time(),
        }


DEFAULT_DEGRADED_ORCHESTRATOR = (
    DegradedOrchestrator
)
