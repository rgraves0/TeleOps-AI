from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import importlib
import json
import logging
import signal
import sqlite3
import sys
import time
import types
import uuid
from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
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
class RuntimeModuleState:
    module_name: str
    checksum: str
    reloaded_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class RecoveryState:
    state_id: str
    component: str
    payload: Dict[str, Any]
    updated_at: float


class RuntimeRBACValidator:
    """
    Default Deny runtime validator.
    """

    REQUIRED_PERMISSION = (
        "runtime.reload"
    )

    ADMIN_ROLES = {
        "admin",
        "system",
        "core",
    }

    def __init__(
        self,
        router: DynamicToolRouter,
        *,
        signing_secret: str,
    ) -> None:
        self.router = router

        self.signing_secret = (
            signing_secret.encode(
                "utf-8"
            )
        )

    async def validate(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        signed_token: str,
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
            & self.ADMIN_ROLES
        ):
            return False

        if not self._verify_signature(
            requester_id,
            signed_token,
        ):
            return False

        context = RouteContext(
            requester_id=
                requester_id,
            requester_roles=
                roles,
            requester_permissions=
                permissions,
            task_type=
                "runtime.reload",
            metadata=metadata or {},
        )

        route = await self.router.route(
            task=
                "runtime.reload",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )

    def _verify_signature(
        self,
        requester_id: str,
        token: str,
    ) -> bool:
        expected = hmac.new(
            self.signing_secret,
            requester_id.encode(
                "utf-8"
            ),
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(
            expected,
            token,
        )


class RuntimeStateStore:
    """
    SQLite WAL restart recovery store.
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

    async def persist_state(
        self,
        state: RecoveryState,
    ) -> None:
        await asyncio.to_thread(
            self._persist_state,
            state,
        )

    async def load_states(
        self,
    ) -> List[RecoveryState]:
        rows = await asyncio.to_thread(
            self._load_states
        )

        return [
            RecoveryState(
                state_id=row[0],
                component=row[1],
                payload=json.loads(
                    row[2]
                ),
                updated_at=row[3],
            )
            for row in rows
        ]

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
            CREATE TABLE IF NOT EXISTS runtime_states (
                state_id TEXT PRIMARY KEY,
                component TEXT UNIQUE NOT NULL,
                payload TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )

    def _persist_state(
        self,
        state: RecoveryState,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO runtime_states (
                state_id,
                component,
                payload,
                updated_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                state.state_id,
                state.component,
                json.dumps(
                    state.payload,
                    ensure_ascii=False,
                ),
                state.updated_at,
            ),
        )

    def _load_states(
        self,
    ) -> List[Any]:
        cursor = self._connection.execute(
            """
            SELECT
                state_id,
                component,
                payload,
                updated_at
            FROM runtime_states
            """
        )

        return cursor.fetchall()


class DynamicModuleReloader:
    """
    Safe hot reload runtime.
    """

    def __init__(
        self,
    ) -> None:
        self._loaded: Dict[
            str,
            RuntimeModuleState,
        ] = {}

    async def reload_module(
        self,
        module_name: str,
    ) -> RuntimeModuleState:

        module = sys.modules.get(
            module_name
        )

        if not module:
            module = importlib.import_module(
                module_name
            )

        reloaded = (
            importlib.reload(
                module
            )
        )

        checksum = (
            self._calculate_checksum(
                reloaded
            )
        )

        state = RuntimeModuleState(
            module_name=
                module_name,
            checksum=checksum,
            reloaded_at=
                time.time(),
        )

        self._loaded[
            module_name
        ] = state

        return state

    def _calculate_checksum(
        self,
        module: types.ModuleType,
    ) -> str:

        content = str(
            sorted(
                dir(module)
            )
        ).encode("utf-8")

        return hashlib.sha256(
            content
        ).hexdigest()

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "loaded_modules":
                len(
                    self._loaded
                ),
        }


class SignalShutdownController:
    """
    Graceful signal controller.
    """

    def __init__(
        self,
    ) -> None:
        self.shutdown_event = (
            asyncio.Event()
        )

    def register(
        self,
    ) -> None:

        loop = (
            asyncio.get_event_loop()
        )

        for sig in (
            signal.SIGINT,
            signal.SIGTERM,
        ):
            with contextlib.suppress(
                NotImplementedError
            ):
                loop.add_signal_handler(
                    sig,
                    self.shutdown_event.set,
                )

    async def wait(
        self,
    ) -> None:
        await self.shutdown_event.wait()


class SafeRestartBootstrapper:
    """
    Persistent restart recovery.
    """

    def __init__(
        self,
        *,
        state_store: RuntimeStateStore,
    ) -> None:
        self.state_store = (
            state_store
        )

    async def restore(
        self,
    ) -> Dict[str, Any]:

        restored: Dict[
            str,
            Any,
        ] = {}

        states = (
            await self.state_store.load_states()
        )

        for state in states:
            restored[
                state.component
            ] = state.payload

        return restored


class RuntimeTaskRegistry:
    """
    Active task registry.
    """

    def __init__(
        self,
    ) -> None:
        self._tasks: Dict[
            str,
            asyncio.Task,
        ] = {}

    async def register(
        self,
        *,
        task_id: str,
        task: asyncio.Task,
    ) -> None:
        self._tasks[
            task_id
        ] = task

    async def unregister(
        self,
        task_id: str,
    ) -> None:
        self._tasks.pop(
            task_id,
            None,
        )

    async def graceful_cancel_all(
        self,
    ) -> None:

        tasks = list(
            self._tasks.values()
        )

        for task in tasks:
            task.cancel()

        for task in tasks:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

        self._tasks.clear()

    def snapshot(
        self,
    ) -> Dict[str, Any]:
        return {
            "active_tasks":
                len(
                    self._tasks
                )
        }


class RuntimeManager:
    """
    Async-first Production Reliability Runtime.

    Features:
    - Zero-downtime hot reload
    - Safe restart recovery
    - Graceful signal shutdown
    - Persistent runtime states
    - Active task preservation
    - Default Deny RBAC validation
    """

    MAINTENANCE_INTERVAL = 900

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: Optional[
            MessageBus
        ] = None,
        database_path: str = (
            "./data/runtime_states.db"
        ),
        signing_secret: str = (
            "runtime-secret"
        ),
    ) -> None:

        self.router = router

        self.message_bus = (
            message_bus
        )

        self._validator = (
            RuntimeRBACValidator(
                router,
                signing_secret=
                    signing_secret,
            )
        )

        self._store = (
            RuntimeStateStore(
                database_path=
                    database_path
            )
        )

        self._reloader = (
            DynamicModuleReloader()
        )

        self._shutdown = (
            SignalShutdownController()
        )

        self._bootstrapper = (
            SafeRestartBootstrapper(
                state_store=
                    self._store
            )
        )

        self._registry = (
            RuntimeTaskRegistry()
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

        self._reload_counter = 0

        self._recovery_counter = 0

        self._restored_state: Dict[
            str,
            Any,
        ] = {}

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting RuntimeManager"
        )

        await self._store.initialize()

        self._shutdown.register()

        self._restored_state = (
            await self._bootstrapper.restore()
        )

        self._running = True

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

        asyncio.create_task(
            self._watch_shutdown()
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping RuntimeManager"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        await self._registry.graceful_cancel_all()

        await self._store.close()

    async def hot_reload(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        signed_token: str,
        module_name: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:
        """
        Secure hot reload gateway.
        """

        allowed = (
            await self._validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                signed_token=
                    signed_token,
                metadata=
                    metadata,
            )
        )

        if not allowed:
            await self._emit_alert(
                event_type=
                    "runtime_reload_denied",
                payload={
                    "requester_id":
                        requester_id,
                    "module_name":
                        module_name,
                },
            )

            return False

        try:
            state = (
                await self._reloader.reload_module(
                    module_name
                )
            )

            self._reload_counter += 1

            await self._persist_runtime_state(
                component=
                    f"reload:{module_name}",
                payload={
                    "checksum":
                        state.checksum,
                    "reloaded_at":
                        state.reloaded_at,
                },
            )

            await self._emit_alert(
                event_type=
                    "runtime_module_reloaded",
                payload={
                    "module_name":
                        module_name,
                    "checksum":
                        state.checksum,
                },
            )

            return True

        except Exception as exc:
            logger.exception(
                "Hot reload failure"
            )

            await self._emit_alert(
                event_type=
                    "runtime_reload_failed",
                payload={
                    "module_name":
                        module_name,
                    "error":
                        str(exc),
                },
            )

            return False

    async def persist_component_state(
        self,
        *,
        component: str,
        payload: Dict[str, Any],
    ) -> None:

        await self._persist_runtime_state(
            component=component,
            payload=payload,
        )

    async def restore_states(
        self,
    ) -> Dict[str, Any]:

        self._recovery_counter += 1

        return dict(
            self._restored_state
        )

    async def register_task(
        self,
        *,
        task_id: str,
        task: asyncio.Task,
    ) -> None:
        await self._registry.register(
            task_id=task_id,
            task=task,
        )

    async def unregister_task(
        self,
        task_id: str,
    ) -> None:
        await self._registry.unregister(
            task_id
        )

    async def _persist_runtime_state(
        self,
        *,
        component: str,
        payload: Dict[str, Any],
    ) -> None:

        state = RecoveryState(
            state_id=
                uuid.uuid4().hex,
            component=
                component,
            payload=payload,
            updated_at=
                time.time(),
        )

        await self._store.persist_state(
            state
        )

    async def _watch_shutdown(
        self,
    ) -> None:

        await self._shutdown.wait()

        logger.warning(
            "Shutdown signal received"
        )

        await self.stop()

    async def _emit_alert(
        self,
        *,
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic=
                "runtime.events",
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
                logger.exception(
                    "Runtime maintenance failure"
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
            "reloads":
                self._reload_counter,
            "recoveries":
                self._recovery_counter,
            "task_registry":
                self._registry.snapshot(),
            "module_reloader":
                self._reloader.stats(),
            "restored_components":
                len(
                    self._restored_state
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_RUNTIME_MANAGER = (
    RuntimeManager
)
