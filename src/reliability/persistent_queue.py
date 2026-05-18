from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
import time
import traceback
import uuid
from collections import deque
from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
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


class QueueEventStatus(
    str,
    Enum,
):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    REPLAYED = "replayed"


@dataclass(slots=True)
class QueueEvent:
    event_id: str
    queue_name: str
    payload: Dict[str, Any]
    owner_id: str
    permissions: List[str]
    roles: List[str]
    status: QueueEventStatus
    created_at: float
    updated_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ReplayResult:
    success: bool
    replayed_events: int
    failed_events: int
    restored_state: Dict[str, Any]
    replay_started_at: float
    replay_finished_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class QueueRBACValidator:
    """
    Default Deny + RBAC validator.
    """

    ENQUEUE_PERMISSION = (
        "queue.enqueue"
    )

    DEQUEUE_PERMISSION = (
        "queue.dequeue"
    )

    REPLAY_PERMISSION = (
        "queue.replay"
    )

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
        action: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:

        permission_map = {
            "enqueue":
                self.ENQUEUE_PERMISSION,
            "dequeue":
                self.DEQUEUE_PERMISSION,
            "replay":
                self.REPLAY_PERMISSION,
        }

        required = (
            permission_map.get(
                action
            )
        )

        if not required:
            return False

        if required not in permissions:
            return False

        context = RouteContext(
            requester_id=
                requester_id,
            requester_roles=
                roles,
            requester_permissions=
                permissions,
            task_type=
                f"queue.{action}",
            metadata=
                metadata or {},
        )

        route = await self.router.route(
            task=
                f"queue.{action}",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class SQLiteQueueStore:
    """
    SQLite WAL persistent FIFO queue.
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

    async def insert_event(
        self,
        event: QueueEvent,
    ) -> None:

        await asyncio.to_thread(
            self._insert_event,
            event,
        )

    async def fetch_next(
        self,
        queue_name: str,
    ) -> Optional[
        QueueEvent
    ]:

        row = await asyncio.to_thread(
            self._fetch_next,
            queue_name,
        )

        if not row:
            return None

        return self._row_to_event(
            row
        )

    async def update_status(
        self,
        *,
        event_id: str,
        status: QueueEventStatus,
    ) -> None:

        await asyncio.to_thread(
            self._update_status,
            event_id,
            status.value,
        )

    async def replay_events(
        self,
        *,
        from_timestamp: Optional[
            float
        ] = None,
        from_event_id: Optional[
            str
        ] = None,
    ) -> List[
        QueueEvent
    ]:

        rows = await asyncio.to_thread(
            self._replay_events,
            from_timestamp,
            from_event_id,
        )

        return [
            self._row_to_event(
                row
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
            CREATE TABLE IF NOT EXISTS persistent_queue (
                event_id TEXT PRIMARY KEY,
                queue_name TEXT NOT NULL,
                payload TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                permissions TEXT NOT NULL,
                roles TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_queue_status
            ON persistent_queue(queue_name, status, created_at)
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_created
            ON persistent_queue(created_at)
            """
        )

    def _insert_event(
        self,
        event: QueueEvent,
    ) -> None:

        self._connection.execute(
            """
            INSERT INTO persistent_queue (
                event_id,
                queue_name,
                payload,
                owner_id,
                permissions,
                roles,
                status,
                created_at,
                updated_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.queue_name,
                json.dumps(
                    event.payload,
                    ensure_ascii=False,
                ),
                event.owner_id,
                json.dumps(
                    event.permissions
                ),
                json.dumps(
                    event.roles
                ),
                event.status.value,
                event.created_at,
                event.updated_at,
                json.dumps(
                    event.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _fetch_next(
        self,
        queue_name: str,
    ) -> Optional[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                event_id,
                queue_name,
                payload,
                owner_id,
                permissions,
                roles,
                status,
                created_at,
                updated_at,
                metadata
            FROM persistent_queue
            WHERE queue_name = ?
            AND status = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (queue_name,),
        )

        row = cursor.fetchone()

        if row:
            self._connection.execute(
                """
                UPDATE persistent_queue
                SET status = 'processing',
                    updated_at = ?
                WHERE event_id = ?
                """,
                (
                    time.time(),
                    row[0],
                ),
            )

        return row

    def _update_status(
        self,
        event_id: str,
        status: str,
    ) -> None:

        self._connection.execute(
            """
            UPDATE persistent_queue
            SET status = ?,
                updated_at = ?
            WHERE event_id = ?
            """,
            (
                status,
                time.time(),
                event_id,
            ),
        )

    def _replay_events(
        self,
        from_timestamp: Optional[
            float
        ],
        from_event_id: Optional[
            str
        ],
    ) -> List[Any]:

        query = """
        SELECT
            event_id,
            queue_name,
            payload,
            owner_id,
            permissions,
            roles,
            status,
            created_at,
            updated_at,
            metadata
        FROM persistent_queue
        WHERE 1=1
        """

        params: List[Any] = []

        if from_timestamp:
            query += (
                " AND created_at >= ?"
            )
            params.append(
                from_timestamp
            )

        if from_event_id:
            query += (
                " AND event_id >= ?"
            )
            params.append(
                from_event_id
            )

        query += (
            " ORDER BY created_at ASC"
        )

        cursor = self._connection.execute(
            query,
            tuple(params),
        )

        return cursor.fetchall()

    def _row_to_event(
        self,
        row: Any,
    ) -> QueueEvent:

        return QueueEvent(
            event_id=row[0],
            queue_name=row[1],
            payload=json.loads(
                row[2]
            ),
            owner_id=row[3],
            permissions=json.loads(
                row[4]
            ),
            roles=json.loads(
                row[5]
            ),
            status=QueueEventStatus(
                row[6]
            ),
            created_at=row[7],
            updated_at=row[8],
            metadata=json.loads(
                row[9]
            ),
        )


class EventJournalRecorder:
    """
    Event journal statistics.
    """

    def __init__(
        self,
    ) -> None:

        self._journal_count = 0

        self._failed_events = 0

        self._replayed_events = 0

        self._recent_events: Deque[
            str
        ] = deque(maxlen=256)

    async def record(
        self,
        event_id: str,
    ) -> None:

        self._journal_count += 1

        self._recent_events.append(
            event_id
        )

    async def failed(
        self,
    ) -> None:
        self._failed_events += 1

    async def replayed(
        self,
        count: int,
    ) -> None:
        self._replayed_events += count

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "journaled":
                self._journal_count,
            "failed":
                self._failed_events,
            "replayed":
                self._replayed_events,
            "recent":
                len(
                    self._recent_events
                ),
        }


class SQLiteBackedFIFOQueue:
    """
    Non-blocking SQLite FIFO queue.
    """

    def __init__(
        self,
        *,
        store: SQLiteQueueStore,
        validator: QueueRBACValidator,
        journal: EventJournalRecorder,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:

        self.store = store

        self.validator = (
            validator
        )

        self.journal = journal

        self.message_bus = (
            message_bus
        )

    async def enqueue(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        queue_name: str,
        payload: Dict[str, Any],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> str:

        allowed = (
            await self.validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                action="enqueue",
                metadata=
                    metadata,
            )
        )

        if not allowed:
            raise PermissionError(
                "Queue enqueue denied"
            )

        event = QueueEvent(
            event_id=
                uuid.uuid4().hex,
            queue_name=
                queue_name,
            payload=
                payload,
            owner_id=
                requester_id,
            permissions=list(
                permissions
            ),
            roles=list(
                roles
            ),
            status=
                QueueEventStatus.PENDING,
            created_at=
                time.time(),
            updated_at=
                time.time(),
            metadata=
                metadata or {},
        )

        await self.store.insert_event(
            event
        )

        await self.journal.record(
            event.event_id
        )

        await self._emit_event(
            "queue_event_created",
            {
                "event_id":
                    event.event_id,
                "queue":
                    queue_name,
            },
        )

        return event.event_id

    async def dequeue(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        queue_name: str,
    ) -> Optional[
        QueueEvent
    ]:

        allowed = (
            await self.validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                action="dequeue",
            )
        )

        if not allowed:
            raise PermissionError(
                "Queue dequeue denied"
            )

        return await self.store.fetch_next(
            queue_name
        )

    async def complete(
        self,
        event_id: str,
    ) -> None:

        await self.store.update_status(
            event_id=event_id,
            status=
                QueueEventStatus.COMPLETED,
        )

    async def fail(
        self,
        event_id: str,
    ) -> None:

        await self.journal.failed()

        await self.store.update_status(
            event_id=event_id,
            status=
                QueueEventStatus.FAILED,
        )

    async def _emit_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic="queue.events",
            payload={
                "type":
                    event_type,
                "timestamp":
                    time.time(),
                **payload,
            },
        )


class StateReplayProcessor:
    """
    Historical state replay engine.
    """

    SAFE_REPLAY_LIMIT = 5000

    def __init__(
        self,
        *,
        store: SQLiteQueueStore,
        validator: QueueRBACValidator,
        journal: EventJournalRecorder,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:

        self.store = store

        self.validator = (
            validator
        )

        self.journal = journal

        self.message_bus = (
            message_bus
        )

    async def replay(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        from_timestamp: Optional[
            float
        ] = None,
        from_event_id: Optional[
            str
        ] = None,
    ) -> ReplayResult:

        started = time.time()

        allowed = (
            await self.validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                action="replay",
            )
        )

        if not allowed:
            raise PermissionError(
                "Replay denied"
            )

        events = (
            await self.store.replay_events(
                from_timestamp=
                    from_timestamp,
                from_event_id=
                    from_event_id,
            )
        )

        if (
            len(events)
            > self.SAFE_REPLAY_LIMIT
        ):
            events = events[
                : self.SAFE_REPLAY_LIMIT
            ]

        restored_state: Dict[
            str,
            Any,
        ] = {}

        replayed = 0

        failed = 0

        for event in events:
            try:
                if (
                    requester_id
                    != event.owner_id
                    and "admin"
                    not in roles
                ):
                    continue

                restored_state[
                    event.event_id
                ] = {
                    "queue":
                        event.queue_name,
                    "payload":
                        event.payload,
                    "status":
                        event.status.value,
                }

                replayed += 1

                await self.store.update_status(
                    event_id=
                        event.event_id,
                    status=
                        QueueEventStatus.REPLAYED,
                )

            except Exception:
                failed += 1

                logger.error(
                    traceback.format_exc()
                )

        await self.journal.replayed(
            replayed
        )

        await self._emit_event(
            "queue_replay_completed",
            {
                "replayed":
                    replayed,
                "failed":
                    failed,
            },
        )

        return ReplayResult(
            success=
                failed == 0,
            replayed_events=
                replayed,
            failed_events=
                failed,
            restored_state=
                restored_state,
            replay_started_at=
                started,
            replay_finished_at=
                time.time(),
        )

    async def stream_replay(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        from_timestamp: Optional[
            float
        ] = None,
    ) -> AsyncIterator[
        QueueEvent
    ]:

        allowed = (
            await self.validator.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                action="replay",
            )
        )

        if not allowed:
            raise PermissionError(
                "Replay stream denied"
            )

        events = (
            await self.store.replay_events(
                from_timestamp=
                    from_timestamp
            )
        )

        for event in events:
            if (
                requester_id
                != event.owner_id
                and "admin"
                not in roles
            ):
                continue

            yield event

    async def _emit_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic="queue.replay",
            payload={
                "type":
                    event_type,
                "timestamp":
                    time.time(),
                **payload,
            },
        )


class PersistentQueueRuntime:
    """
    Async-first Production Persistent Queue Runtime.

    Features:
    - SQLite WAL FIFO queue
    - Persistent disk-backed events
    - Loss-less queue persistence
    - Historical event replay
    - Event journal tracking
    - Replay-safe restoration
    - Default Deny RBAC validation
    """

    MAINTENANCE_INTERVAL = 1800

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: Optional[
            MessageBus
        ] = None,
        database_path: str = (
            "./data/persistent_queue.db"
        ),
    ) -> None:

        self.router = router

        self.message_bus = (
            message_bus
        )

        self._store = (
            SQLiteQueueStore(
                database_path=
                    database_path
            )
        )

        self._validator = (
            QueueRBACValidator(
                router
            )
        )

        self._journal = (
            EventJournalRecorder()
        )

        self.queue = (
            SQLiteBackedFIFOQueue(
                store=self._store,
                validator=
                    self._validator,
                journal=
                    self._journal,
                message_bus=
                    message_bus,
            )
        )

        self.replay = (
            StateReplayProcessor(
                store=self._store,
                validator=
                    self._validator,
                journal=
                    self._journal,
                message_bus=
                    message_bus,
            )
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting PersistentQueueRuntime"
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
            "Stopping PersistentQueueRuntime"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        await self._store.close()

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
            "journal":
                self._journal.stats(),
            "timestamp":
                time.time(),
        }


DEFAULT_PERSISTENT_QUEUE = (
    PersistentQueueRuntime
)
