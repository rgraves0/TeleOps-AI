from __future__ import annotations

import asyncio
import gc
import json
import logging
import sqlite3
import sys
import time
import traceback
from collections import defaultdict
from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
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
class MemoryObjectRecord:
    object_type: str
    module_name: str
    size_bytes: int
    reference_count: int
    preview: str


@dataclass(slots=True)
class SessionRecord:
    session_id: str
    owner_id: str
    state: str
    task_count: int
    subscriptions: List[str]
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class InspectionSnapshot:
    timestamp: float
    total_objects: int
    total_memory_bytes: int
    top_modules: List[
        Dict[str, Any]
    ]
    active_sessions: List[
        SessionRecord
    ]


class InspectionRBACValidator:
    """
    Default Deny RBAC validator.
    """

    REQUIRED_PERMISSION = (
        "console.memory.inspect"
    )

    SYSTEM_ROLES = {
        "admin",
        "superuser",
        "system",
    }

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        admin_ids: Set[int],
    ) -> None:

        self.router = router
        self.admin_ids = admin_ids

    async def validate(
        self,
        *,
        telegram_user_id: int,
        permissions: Set[str],
        roles: Set[str],
        task_type: str,
    ) -> bool:

        if (
            telegram_user_id
            not in self.admin_ids
        ):
            return False

        if not (
            roles & self.SYSTEM_ROLES
        ):
            return False

        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        context = RouteContext(
            requester_id=str(
                telegram_user_id
            ),
            requester_roles=roles,
            requester_permissions=
                permissions,
            task_type=task_type,
            metadata={},
        )

        route = await self.router.route(
            task=task_type,
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class SensitiveDataRedactor:
    """
    Automatic sensitive value redactor.
    """

    REDACTION_PATTERNS = {
        "api_key",
        "secret",
        "token",
        "password",
        "vault",
        "credential",
        "private_key",
        "master_key",
        "auth",
        "bearer",
    }

    REDACTION_TEXT = (
        "[REDACTED]"
    )

    def redact(
        self,
        value: Any,
    ) -> str:

        try:
            text = str(value)

            lowered = text.lower()

            for pattern in (
                self.REDACTION_PATTERNS
            ):
                if pattern in lowered:
                    return (
                        self.REDACTION_TEXT
                    )

            if len(text) > 180:
                text = (
                    text[:177] + "..."
                )

            return text

        except Exception:
            return (
                self.REDACTION_TEXT
            )


class SQLiteInspectionStore:
    """
    SQLite WAL inspection store.
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

    async def save_snapshot(
        self,
        snapshot: InspectionSnapshot,
    ) -> None:

        await asyncio.to_thread(
            self._save_snapshot,
            snapshot,
        )

    async def latest_snapshot(
        self,
    ) -> Optional[
        InspectionSnapshot
    ]:

        row = await asyncio.to_thread(
            self._latest_snapshot
        )

        if not row:
            return None

        return InspectionSnapshot(
            timestamp=row[0],
            total_objects=row[1],
            total_memory_bytes=row[2],
            top_modules=json.loads(
                row[3]
            ),
            active_sessions=[
                SessionRecord(
                    **item
                )
                for item in json.loads(
                    row[4]
                )
            ],
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
            CREATE TABLE IF NOT EXISTS inspection_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                total_objects INTEGER NOT NULL,
                total_memory_bytes INTEGER NOT NULL,
                top_modules TEXT NOT NULL,
                active_sessions TEXT NOT NULL
            )
            """
        )

    def _save_snapshot(
        self,
        snapshot: InspectionSnapshot,
    ) -> None:

        self._connection.execute(
            """
            INSERT INTO inspection_snapshots (
                timestamp,
                total_objects,
                total_memory_bytes,
                top_modules,
                active_sessions
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                snapshot.timestamp,
                snapshot.total_objects,
                snapshot.total_memory_bytes,
                json.dumps(
                    snapshot.top_modules,
                    ensure_ascii=False,
                ),
                json.dumps(
                    [
                        {
                            "session_id":
                                item.session_id,
                            "owner_id":
                                item.owner_id,
                            "state":
                                item.state,
                            "task_count":
                                item.task_count,
                            "subscriptions":
                                item.subscriptions,
                            "metadata":
                                item.metadata,
                        }
                        for item in snapshot.active_sessions
                    ],
                    ensure_ascii=False,
                ),
            ),
        )

    def _latest_snapshot(
        self,
    ) -> Optional[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                timestamp,
                total_objects,
                total_memory_bytes,
                top_modules,
                active_sessions
            FROM inspection_snapshots
            ORDER BY id DESC
            LIMIT 1
            """
        )

        return cursor.fetchone()


class InMemoryObjectInspector:
    """
    Runtime heap inspector.
    """

    MAX_OBJECTS = 5000

    def __init__(
        self,
        *,
        redactor: SensitiveDataRedactor,
    ) -> None:

        self.redactor = redactor

    async def inspect(
        self,
    ) -> Tuple[
        List[MemoryObjectRecord],
        int,
    ]:

        return await asyncio.to_thread(
            self._inspect_sync
        )

    def _inspect_sync(
        self,
    ) -> Tuple[
        List[MemoryObjectRecord],
        int,
    ]:

        records: List[
            MemoryObjectRecord
        ] = []

        total_memory = 0

        tracked = 0

        for obj in gc.get_objects():

            if tracked >= self.MAX_OBJECTS:
                break

            try:
                size = sys.getsizeof(
                    obj
                )

                total_memory += size

                module_name = (
                    getattr(
                        type(obj),
                        "__module__",
                        "unknown",
                    )
                )

                preview = (
                    self.redactor.redact(
                        repr(obj)
                    )
                )

                record = (
                    MemoryObjectRecord(
                        object_type=
                            type(obj).__name__,
                        module_name=
                            module_name,
                        size_bytes=
                            size,
                        reference_count=
                            sys.getrefcount(
                                obj
                            ),
                        preview=
                            preview,
                    )
                )

                records.append(
                    record
                )

                tracked += 1

            except Exception:
                continue

        return (
            records,
            total_memory,
        )


class ActiveSessionInspector:
    """
    Active session/task inspector.
    """

    def __init__(
        self,
        *,
        redactor: SensitiveDataRedactor,
    ) -> None:

        self.redactor = redactor

        self._sessions: Dict[
            str,
            SessionRecord,
        ] = {}

    async def register_session(
        self,
        *,
        session_id: str,
        owner_id: str,
        state: str,
        task_count: int,
        subscriptions: List[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> None:

        sanitized = {}

        for key, value in (
            metadata or {}
        ).items():
            sanitized[key] = (
                self.redactor.redact(
                    value
                )
            )

        self._sessions[
            session_id
        ] = SessionRecord(
            session_id=session_id,
            owner_id=owner_id,
            state=state,
            task_count=task_count,
            subscriptions=
                subscriptions,
            metadata=sanitized,
        )

    async def remove_session(
        self,
        session_id: str,
    ) -> None:

        self._sessions.pop(
            session_id,
            None,
        )

    async def sessions(
        self,
    ) -> List[
        SessionRecord
    ]:

        return list(
            self._sessions.values()
        )


class MemoryReportFormatter:
    """
    Telegram-safe formatter.
    """

    MAX_MODULES = 10

    async def format_snapshot(
        self,
        snapshot: InspectionSnapshot,
    ) -> str:

        lines = [
            "🧠 *Live Memory Inspection*",
            "",
            f"• Total Objects: `{snapshot.total_objects}`",
            f"• Heap Memory: `{snapshot.total_memory_bytes // 1024} KB`",
            "",
            "📦 *Top Modules*",
        ]

        for item in (
            snapshot.top_modules[
                : self.MAX_MODULES
            ]
        ):
            lines.append(
                f"- `{item['module']}` → `{item['size_kb']} KB`"
            )

        lines.append("")
        lines.append(
            "⚙️ *Active Sessions*"
        )

        for session in (
            snapshot.active_sessions[
                : 10
            ]
        ):
            lines.append(
                (
                    f"- `{session.session_id}` "
                    f"| State: `{session.state}` "
                    f"| Tasks: `{session.task_count}`"
                )
            )

        return "\n".join(
            lines
        )


class LiveMemoryInspectionEngine:
    """
    Lightweight memory inspection runtime.
    """

    def __init__(
        self,
        *,
        object_inspector:
            InMemoryObjectInspector,
        session_inspector:
            ActiveSessionInspector,
    ) -> None:

        self.object_inspector = (
            object_inspector
        )

        self.session_inspector = (
            session_inspector
        )

    async def snapshot(
        self,
    ) -> InspectionSnapshot:

        objects, total_memory = (
            await self.object_inspector.inspect()
        )

        grouped = defaultdict(
            int
        )

        for item in objects:
            grouped[
                item.module_name
            ] += item.size_bytes

        top_modules = sorted(
            [
                {
                    "module":
                        module,
                    "size_kb":
                        size // 1024,
                }
                for module, size in grouped.items()
            ],
            key=lambda x: x[
                "size_kb"
            ],
            reverse=True,
        )[:20]

        sessions = (
            await self.session_inspector.sessions()
        )

        return InspectionSnapshot(
            timestamp=time.time(),
            total_objects=len(
                objects
            ),
            total_memory_bytes=
                total_memory,
            top_modules=
                top_modules,
            active_sessions=
                sessions,
        )


class MemoryInspectorRuntime:
    """
    Async-first Telegram memory inspector.

    Features:
    - Runtime heap inspection
    - Active session inspection
    - Sensitive data auto-redaction
    - Telegram-safe rendering
    - SQLite WAL snapshots
    - Default Deny RBAC
    """

    SNAPSHOT_INTERVAL = 300
    WAL_CHECKPOINT_INTERVAL = 1800

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: Optional[
            MessageBus
        ] = None,
        admin_ids: Optional[
            Set[int]
        ] = None,
        database_path: str = (
            "./data/memory_inspector.db"
        ),
    ) -> None:

        self.router = router

        self.message_bus = (
            message_bus
        )

        self.admin_ids = (
            admin_ids or set()
        )

        self._validator = (
            InspectionRBACValidator(
                router=router,
                admin_ids=
                    self.admin_ids,
            )
        )

        self._redactor = (
            SensitiveDataRedactor()
        )

        self._store = (
            SQLiteInspectionStore(
                database_path=
                    database_path
            )
        )

        self._object_inspector = (
            InMemoryObjectInspector(
                redactor=
                    self._redactor
            )
        )

        self._session_inspector = (
            ActiveSessionInspector(
                redactor=
                    self._redactor
            )
        )

        self._engine = (
            LiveMemoryInspectionEngine(
                object_inspector=
                    self._object_inspector,
                session_inspector=
                    self._session_inspector,
            )
        )

        self._formatter = (
            MemoryReportFormatter()
        )

        self._running = False

        self._snapshot_task: Optional[
            asyncio.Task
        ] = None

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting MemoryInspectorRuntime"
        )

        await self._store.initialize()

        self._running = True

        self._snapshot_task = (
            asyncio.create_task(
                self._snapshot_loop()
            )
        )

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping MemoryInspectorRuntime"
        )

        self._running = False

        for task in (
            self._snapshot_task,
            self._maintenance_task,
        ):
            if task:
                task.cancel()

                with contextlib.suppress(
                    asyncio.CancelledError
                ):
                    await task

        await self._store.close()

    async def authorize(
        self,
        *,
        telegram_user_id: int,
        permissions: Set[str],
        roles: Set[str],
        task_type: str,
    ) -> bool:

        return await self._validator.validate(
            telegram_user_id=
                telegram_user_id,
            permissions=
                permissions,
            roles=roles,
            task_type=
                task_type,
        )

    async def inspect_now(
        self,
    ) -> str:

        snapshot = (
            await self._engine.snapshot()
        )

        await self._store.save_snapshot(
            snapshot
        )

        return (
            await self._formatter.format_snapshot(
                snapshot
            )
        )

    async def register_session(
        self,
        *,
        session_id: str,
        owner_id: str,
        state: str,
        task_count: int,
        subscriptions: List[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> None:

        await self._session_inspector.register_session(
            session_id=session_id,
            owner_id=owner_id,
            state=state,
            task_count=task_count,
            subscriptions=
                subscriptions,
            metadata=
                metadata,
        )

    async def remove_session(
        self,
        session_id: str,
    ) -> None:

        await self._session_inspector.remove_session(
            session_id
        )

    async def latest_snapshot(
        self,
    ) -> Optional[
        InspectionSnapshot
    ]:

        return (
            await self._store.latest_snapshot()
        )

    async def _snapshot_loop(
        self,
    ) -> None:

        while self._running:
            try:
                snapshot = (
                    await self._engine.snapshot()
                )

                await self._store.save_snapshot(
                    snapshot
                )

                await self._emit(
                    "console.memory.snapshot",
                    {
                        "total_objects":
                            snapshot.total_objects,
                        "memory_bytes":
                            snapshot.total_memory_bytes,
                    },
                )

                await asyncio.sleep(
                    self.SNAPSHOT_INTERVAL
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    async def _maintenance_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await asyncio.sleep(
                    self.WAL_CHECKPOINT_INTERVAL
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

    async def _emit(
        self,
        topic: str,
        payload: Dict[str, Any],
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic=topic,
            payload={
                "timestamp":
                    time.time(),
                **payload,
            },
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
            "admin_count":
                len(
                    self.admin_ids
                ),
            "tracked_sessions":
                len(
                    self._session_inspector._sessions
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_MEMORY_INSPECTOR = (
    MemoryInspectorRuntime
)
