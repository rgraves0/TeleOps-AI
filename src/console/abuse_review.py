from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
import sqlite3
import time
import traceback
from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
from pathlib import Path
from typing import (
    Any,
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


class IncidentSeverity(
    str,
    Enum,
):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IncidentStatus(
    str,
    Enum,
):
    OPEN = "open"
    BLOCKED = "blocked"
    UNBLOCKED = "unblocked"
    REVOKED = "revoked"
    RESOLVED = "resolved"


@dataclass(slots=True)
class AbuseIncident:
    incident_id: str
    source_id: str
    source_type: str
    severity: IncidentSeverity
    status: IncidentStatus
    reason: str
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class AdminSession:
    session_token: str
    telegram_user_id: int
    permissions: Set[str]
    roles: Set[str]
    expires_at: float


class AbuseReviewRBAC:
    """
    Default Deny RBAC enforcement.
    """

    REQUIRED_PERMISSION = (
        "console.abuse.manage"
    )

    SYSTEM_ROLES = {
        "admin",
        "superuser",
        "security",
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


class SQLiteAbuseStore:
    """
    SQLite WAL abuse review storage.
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

    async def save_incident(
        self,
        incident: AbuseIncident,
    ) -> None:

        await asyncio.to_thread(
            self._save_incident,
            incident,
        )

    async def load_incident(
        self,
        incident_id: str,
    ) -> Optional[
        AbuseIncident
    ]:

        row = await asyncio.to_thread(
            self._load_incident,
            incident_id,
        )

        if not row:
            return None

        return AbuseIncident(
            incident_id=row[0],
            source_id=row[1],
            source_type=row[2],
            severity=
                IncidentSeverity(
                    row[3]
                ),
            status=
                IncidentStatus(
                    row[4]
                ),
            reason=row[5],
            created_at=row[6],
            metadata=json.loads(
                row[7]
            ),
        )

    async def list_incidents(
        self,
        limit: int = 20,
    ) -> List[
        AbuseIncident
    ]:

        rows = await asyncio.to_thread(
            self._list_incidents,
            limit,
        )

        incidents: List[
            AbuseIncident
        ] = []

        for row in rows:
            incidents.append(
                AbuseIncident(
                    incident_id=row[0],
                    source_id=row[1],
                    source_type=row[2],
                    severity=
                        IncidentSeverity(
                            row[3]
                        ),
                    status=
                        IncidentStatus(
                            row[4]
                        ),
                    reason=row[5],
                    created_at=row[6],
                    metadata=json.loads(
                        row[7]
                    ),
                )
            )

        return incidents

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
            "PRAGMA cache_size=-1200;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS abuse_incidents (
                incident_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

    def _save_incident(
        self,
        incident: AbuseIncident,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO abuse_incidents (
                incident_id,
                source_id,
                source_type,
                severity,
                status,
                reason,
                created_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                incident.incident_id,
                incident.source_id,
                incident.source_type,
                incident.severity.value,
                incident.status.value,
                incident.reason,
                incident.created_at,
                json.dumps(
                    incident.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _load_incident(
        self,
        incident_id: str,
    ) -> Optional[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                incident_id,
                source_id,
                source_type,
                severity,
                status,
                reason,
                created_at,
                metadata
            FROM abuse_incidents
            WHERE incident_id = ?
            LIMIT 1
            """,
            (incident_id,),
        )

        return cursor.fetchone()

    def _list_incidents(
        self,
        limit: int,
    ) -> List[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                incident_id,
                source_id,
                source_type,
                severity,
                status,
                reason,
                created_at,
                metadata
            FROM abuse_incidents
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

        return cursor.fetchall()


class AdminSessionManager:
    """
    Admin session/token manager.
    """

    SESSION_TTL = 1800

    def __init__(
        self,
    ) -> None:

        self._sessions: Dict[
            str,
            AdminSession,
        ] = {}

    async def create_session(
        self,
        *,
        telegram_user_id: int,
        permissions: Set[str],
        roles: Set[str],
    ) -> AdminSession:

        token = secrets.token_hex(
            24
        )

        session = AdminSession(
            session_token=token,
            telegram_user_id=
                telegram_user_id,
            permissions=
                permissions,
            roles=roles,
            expires_at=
                time.time()
                + self.SESSION_TTL,
        )

        self._sessions[token] = (
            session
        )

        return session

    async def validate(
        self,
        token: str,
    ) -> bool:

        session = (
            self._sessions.get(
                token
            )
        )

        if not session:
            return False

        if (
            time.time()
            > session.expires_at
        ):
            self._sessions.pop(
                token,
                None,
            )

            return False

        return True

    async def destroy(
        self,
        token: str,
    ) -> None:

        self._sessions.pop(
            token,
            None,
        )


class DynamicStateSynchronizer:
    """
    Runtime block/unblock synchronizer.
    """

    def __init__(
        self,
        *,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:

        self.message_bus = (
            message_bus
        )

        self._blocked_entities: Dict[
            str,
            Dict[str, Any],
        ] = {}

    async def block(
        self,
        *,
        entity_id: str,
        reason: str,
    ) -> None:

        self._blocked_entities[
            entity_id
        ] = {
            "blocked": True,
            "reason": reason,
            "updated_at":
                time.time(),
        }

        await self._emit(
            "security.entity.blocked",
            {
                "entity_id":
                    entity_id,
                "reason":
                    reason,
            },
        )

    async def unblock(
        self,
        *,
        entity_id: str,
    ) -> None:

        self._blocked_entities.pop(
            entity_id,
            None,
        )

        await self._emit(
            "security.entity.unblocked",
            {
                "entity_id":
                    entity_id,
            },
        )

    async def blocked_entities(
        self,
    ) -> Dict[str, Any]:

        return dict(
            self._blocked_entities
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


class TelegramIncidentReaderUI:
    """
    Telegram inline review UI.
    """

    async def render_incidents(
        self,
        incidents: List[
            AbuseIncident
        ],
    ) -> Dict[str, Any]:

        lines = [
            "🚨 Abuse Incident Review",
            "",
        ]

        keyboard: List[
            List[Dict[str, str]]
        ] = []

        for incident in incidents:

            lines.append(
                (
                    f"• `{incident.incident_id[:8]}` "
                    f"| {incident.severity.value.upper()} "
                    f"| {incident.status.value.upper()}"
                )
            )

            lines.append(
                (
                    f"  ↳ {incident.reason[:72]}"
                )
            )

            keyboard.append(
                [
                    {
                        "text":
                            f"🔒 Block {incident.source_id}",
                        "callback_data":
                            f"block:{incident.incident_id}",
                    },
                    {
                        "text":
                            f"🔓 Unblock {incident.source_id}",
                        "callback_data":
                            f"unblock:{incident.incident_id}",
                    },
                ]
            )

        return {
            "text":
                "\n".join(lines),
            "inline_keyboard":
                keyboard,
        }


class RevocationCallbackHandler:
    """
    Inline callback handler.
    """

    def __init__(
        self,
        *,
        store: SQLiteAbuseStore,
        synchronizer:
            DynamicStateSynchronizer,
    ) -> None:

        self.store = store

        self.synchronizer = (
            synchronizer
        )

    async def process_callback(
        self,
        *,
        callback_data: str,
    ) -> Dict[str, Any]:

        parts = callback_data.split(
            ":",
            1,
        )

        if len(parts) != 2:
            raise ValueError(
                "Invalid callback payload"
            )

        action, incident_id = (
            parts
        )

        incident = (
            await self.store.load_incident(
                incident_id
            )
        )

        if not incident:
            raise ValueError(
                "Incident not found"
            )

        if action == "block":

            incident.status = (
                IncidentStatus.BLOCKED
            )

            await self.synchronizer.block(
                entity_id=
                    incident.source_id,
                reason=
                    incident.reason,
            )

        elif action == "unblock":

            incident.status = (
                IncidentStatus.UNBLOCKED
            )

            await self.synchronizer.unblock(
                entity_id=
                    incident.source_id,
            )

        elif action == "revoke":

            incident.status = (
                IncidentStatus.REVOKED
            )

            await self.synchronizer.block(
                entity_id=
                    incident.source_id,
                reason=
                    "revoked",
            )

        else:
            raise ValueError(
                "Unsupported callback action"
            )

        await self.store.save_incident(
            incident
        )

        return {
            "incident_id":
                incident.incident_id,
            "status":
                incident.status.value,
            "source_id":
                incident.source_id,
        }


class AbuseReviewRuntime:
    """
    Async-first Telegram abuse review console.

    Features:
    - Abuse incident viewer
    - Telegram inline review UI
    - Block/unblock callbacks
    - Dynamic synchronization
    - Session token validation
    - SQLite WAL persistence
    - Default Deny RBAC
    """

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
            "./data/abuse_review.db"
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
            AbuseReviewRBAC(
                router=router,
                admin_ids=
                    self.admin_ids,
            )
        )

        self._store = (
            SQLiteAbuseStore(
                database_path=
                    database_path
            )
        )

        self._sessions = (
            AdminSessionManager()
        )

        self._synchronizer = (
            DynamicStateSynchronizer(
                message_bus=
                    message_bus
            )
        )

        self._ui = (
            TelegramIncidentReaderUI()
        )

        self._callbacks = (
            RevocationCallbackHandler(
                store=self._store,
                synchronizer=
                    self._synchronizer,
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
            "Starting AbuseReviewRuntime"
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
            "Stopping AbuseReviewRuntime"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

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

    async def create_admin_session(
        self,
        *,
        telegram_user_id: int,
        permissions: Set[str],
        roles: Set[str],
    ) -> AdminSession:

        return await self._sessions.create_session(
            telegram_user_id=
                telegram_user_id,
            permissions=
                permissions,
            roles=roles,
        )

    async def register_incident(
        self,
        *,
        source_id: str,
        source_type: str,
        severity: str,
        reason: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> AbuseIncident:

        incident = AbuseIncident(
            incident_id=
                secrets.token_hex(12),
            source_id=
                source_id,
            source_type=
                source_type,
            severity=
                IncidentSeverity(
                    severity
                ),
            status=
                IncidentStatus.OPEN,
            reason=reason,
            created_at=
                time.time(),
            metadata=
                metadata or {},
        )

        await self._store.save_incident(
            incident
        )

        await self._emit(
            "security.abuse.detected",
            {
                "incident_id":
                    incident.incident_id,
                "source_id":
                    source_id,
                "severity":
                    severity,
            },
        )

        return incident

    async def render_review_console(
        self,
        *,
        limit: int = 10,
    ) -> Dict[str, Any]:

        incidents = (
            await self._store.list_incidents(
                limit
            )
        )

        return (
            await self._ui.render_incidents(
                incidents
            )
        )

    async def process_callback(
        self,
        *,
        session_token: str,
        callback_data: str,
    ) -> Dict[str, Any]:

        valid = (
            await self._sessions.validate(
                session_token
            )
        )

        if not valid:
            raise PermissionError(
                "Admin session expired or invalid"
            )

        return (
            await self._callbacks.process_callback(
                callback_data=
                    callback_data
            )
        )

    async def blocked_entities(
        self,
    ) -> Dict[str, Any]:

        return (
            await self._synchronizer.blocked_entities()
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
            "active_sessions":
                len(
                    self._sessions._sessions
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_ABUSE_REVIEW = (
    AbuseReviewRuntime
)
