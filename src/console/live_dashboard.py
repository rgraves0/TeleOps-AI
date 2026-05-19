from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sqlite3
import time
import traceback
from collections import deque
from dataclasses import (
    dataclass,
    field,
)
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
class AgentStatus:
    agent_id: str
    lifecycle_state: str
    current_task: str
    memory_usage_mb: float
    cpu_percent: float
    active_since: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class SystemMetrics:
    cpu_percent: float
    ram_usage_mb: float
    active_tasks: int
    message_throughput: int
    uptime_seconds: float
    collected_at: float


@dataclass(slots=True)
class DashboardSession:
    chat_id: int
    message_id: int
    created_at: float
    last_render_at: float
    requester_id: int
    enabled: bool = True


class TelegramRBACValidator:
    """
    Telegram Admin whitelist validator.
    """

    REQUIRED_ROLE = "admin"

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
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:

        if (
            telegram_user_id
            not in self.admin_ids
        ):
            return False

        if (
            self.REQUIRED_ROLE
            not in roles
        ):
            return False

        if (
            "console.dashboard.read"
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
            metadata=metadata or {},
        )

        route = await self.router.route(
            task=task_type,
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class SQLiteDashboardStore:
    """
    Lightweight WAL dashboard state store.
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

    async def save_agent_status(
        self,
        status: AgentStatus,
    ) -> None:

        await asyncio.to_thread(
            self._save_agent_status,
            status,
        )

    async def load_agent_statuses(
        self,
    ) -> List[AgentStatus]:

        rows = await asyncio.to_thread(
            self._load_agent_statuses
        )

        statuses: List[
            AgentStatus
        ] = []

        for row in rows:
            statuses.append(
                AgentStatus(
                    agent_id=row[0],
                    lifecycle_state=row[1],
                    current_task=row[2],
                    memory_usage_mb=row[3],
                    cpu_percent=row[4],
                    active_since=row[5],
                    metadata=json.loads(
                        row[6]
                    ),
                )
            )

        return statuses

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
            CREATE TABLE IF NOT EXISTS agent_status (
                agent_id TEXT PRIMARY KEY,
                lifecycle_state TEXT NOT NULL,
                current_task TEXT NOT NULL,
                memory_usage_mb REAL NOT NULL,
                cpu_percent REAL NOT NULL,
                active_since REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

    def _save_agent_status(
        self,
        status: AgentStatus,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO agent_status (
                agent_id,
                lifecycle_state,
                current_task,
                memory_usage_mb,
                cpu_percent,
                active_since,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                status.agent_id,
                status.lifecycle_state,
                status.current_task,
                status.memory_usage_mb,
                status.cpu_percent,
                status.active_since,
                json.dumps(
                    status.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _load_agent_statuses(
        self,
    ) -> List[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                agent_id,
                lifecycle_state,
                current_task,
                memory_usage_mb,
                cpu_percent,
                active_since,
                metadata
            FROM agent_status
            ORDER BY cpu_percent DESC
            """
        )

        return cursor.fetchall()


class SystemMetricsExtractor:
    """
    Lightweight runtime metrics extractor.
    """

    def __init__(
        self,
    ) -> None:

        self._start_time = time.time()

        self._message_counter = 0

        self._recent_metrics: Deque[
            SystemMetrics
        ] = deque(maxlen=64)

    async def collect(
        self,
        *,
        active_tasks: int,
    ) -> SystemMetrics:

        ram_usage_mb = (
            self._read_memory_usage()
        )

        cpu_percent = (
            self._read_cpu_usage()
        )

        metrics = SystemMetrics(
            cpu_percent=cpu_percent,
            ram_usage_mb=
                ram_usage_mb,
            active_tasks=
                active_tasks,
            message_throughput=
                self._message_counter,
            uptime_seconds=
                (
                    time.time()
                    - self._start_time
                ),
            collected_at=
                time.time(),
        )

        self._recent_metrics.append(
            metrics
        )

        return metrics

    async def increment_messages(
        self,
    ) -> None:
        self._message_counter += 1

    def _read_memory_usage(
        self,
    ) -> float:

        try:
            with open(
                "/proc/self/statm",
                "r",
                encoding="utf-8",
            ) as statm:
                pages = int(
                    statm.readline().split()[1]
                )

            page_size = (
                os.sysconf(
                    "SC_PAGE_SIZE"
                )
                / 1024
                / 1024
            )

            return round(
                pages * page_size,
                2,
            )

        except Exception:
            return 0.0

    def _read_cpu_usage(
        self,
    ) -> float:

        try:
            load_avg = os.getloadavg()[0]

            cpu_count = (
                os.cpu_count() or 1
            )

            return round(
                (
                    load_avg
                    / cpu_count
                )
                * 100,
                2,
            )

        except Exception:
            return 0.0


class TelegramMessageEditor:
    """
    Telegram dynamic message renderer.
    """

    MAX_MESSAGE_LENGTH = 3500

    def __init__(
        self,
    ) -> None:

        self._last_render_hash: Dict[
            int,
            int,
        ] = {}

    async def render(
        self,
        *,
        metrics: SystemMetrics,
        agents: List[
            AgentStatus
        ],
    ) -> str:

        lines: List[str] = []

        lines.append(
            "*📡 TeleOps AI Dashboard*"
        )

        lines.append("")
        lines.append(
            f"*CPU:* `{metrics.cpu_percent}%`"
        )

        lines.append(
            f"*RAM:* `{metrics.ram_usage_mb} MB`"
        )

        lines.append(
            f"*Tasks:* `{metrics.active_tasks}`"
        )

        lines.append(
            f"*Throughput:* `{metrics.message_throughput}`"
        )

        lines.append(
            f"*Uptime:* `{int(metrics.uptime_seconds)}s`"
        )

        lines.append("")
        lines.append(
            "*🤖 Active Agents*"
        )

        if not agents:
            lines.append(
                "_No active agents_"
            )

        for agent in agents[:10]:

            lines.append(
                (
                    f"• `{agent.agent_id}` "
                    f"| {agent.lifecycle_state} "
                    f"| CPU `{agent.cpu_percent}%` "
                    f"| RAM `{agent.memory_usage_mb}MB`"
                )
            )

            lines.append(
                (
                    f"  ↳ Task: "
                    f"`{agent.current_task[:48]}`"
                )
            )

        rendered = "\n".join(
            lines
        )

        return rendered[
            : self.MAX_MESSAGE_LENGTH
        ]

    async def should_update(
        self,
        *,
        chat_id: int,
        content: str,
    ) -> bool:

        content_hash = hash(
            content
        )

        previous = (
            self._last_render_hash.get(
                chat_id
            )
        )

        if previous == content_hash:
            return False

        self._last_render_hash[
            chat_id
        ] = content_hash

        return True


class AgentVisibilityTracker:
    """
    Active agent visibility tracker.
    """

    def __init__(
        self,
        *,
        store: SQLiteDashboardStore,
    ) -> None:

        self.store = store

        self._active_agents: Dict[
            str,
            AgentStatus,
        ] = {}

    async def update_agent(
        self,
        *,
        agent_id: str,
        lifecycle_state: str,
        current_task: str,
        memory_usage_mb: float,
        cpu_percent: float,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> None:

        status = AgentStatus(
            agent_id=agent_id,
            lifecycle_state=
                lifecycle_state,
            current_task=
                current_task,
            memory_usage_mb=
                memory_usage_mb,
            cpu_percent=
                cpu_percent,
            active_since=
                time.time(),
            metadata=
                metadata or {},
        )

        self._active_agents[
            agent_id
        ] = status

        await self.store.save_agent_status(
            status
        )

    async def active_agents(
        self,
    ) -> List[AgentStatus]:

        return list(
            self._active_agents.values()
        )


class LiveOperationalDashboard:
    """
    Async-first Telegram operational dashboard.

    Features:
    - Dynamic Telegram dashboard rendering
    - Real-time message editing
    - Agent visibility tracking
    - Lightweight metrics extraction
    - WAL-backed persistence
    - Telegram RBAC authorization
    """

    DASHBOARD_REFRESH = 5
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
            "./data/live_dashboard.db"
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
            TelegramRBACValidator(
                router=router,
                admin_ids=
                    self.admin_ids,
            )
        )

        self._store = (
            SQLiteDashboardStore(
                database_path=
                    database_path
            )
        )

        self._metrics = (
            SystemMetricsExtractor()
        )

        self._renderer = (
            TelegramMessageEditor()
        )

        self._tracker = (
            AgentVisibilityTracker(
                store=self._store
            )
        )

        self._running = False

        self._dashboard_sessions: Dict[
            int,
            DashboardSession,
        ] = {}

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

        self._render_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting LiveOperationalDashboard"
        )

        await self._store.initialize()

        self._running = True

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

        self._render_task = (
            asyncio.create_task(
                self._render_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping LiveOperationalDashboard"
        )

        self._running = False

        for task in (
            self._maintenance_task,
            self._render_task,
        ):
            if task:
                task.cancel()

                with contextlib.suppress(
                    asyncio.CancelledError
                ):
                    await task

        await self._store.close()

    async def authorize_dashboard(
        self,
        *,
        telegram_user_id: int,
        permissions: Set[str],
        roles: Set[str],
    ) -> bool:

        return await self._validator.validate(
            telegram_user_id=
                telegram_user_id,
            permissions=
                permissions,
            roles=roles,
            task_type=
                "telegram.dashboard.view",
        )

    async def register_dashboard(
        self,
        *,
        chat_id: int,
        message_id: int,
        requester_id: int,
    ) -> None:

        self._dashboard_sessions[
            chat_id
        ] = DashboardSession(
            chat_id=chat_id,
            message_id=message_id,
            requester_id=
                requester_id,
            created_at=
                time.time(),
            last_render_at=0.0,
        )

    async def update_agent_status(
        self,
        *,
        agent_id: str,
        lifecycle_state: str,
        current_task: str,
        memory_usage_mb: float,
        cpu_percent: float,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> None:

        await self._tracker.update_agent(
            agent_id=agent_id,
            lifecycle_state=
                lifecycle_state,
            current_task=
                current_task,
            memory_usage_mb=
                memory_usage_mb,
            cpu_percent=
                cpu_percent,
            metadata=
                metadata,
        )

    async def build_dashboard_text(
        self,
    ) -> str:

        agents = (
            await self._tracker.active_agents()
        )

        metrics = (
            await self._metrics.collect(
                active_tasks=
                    len(agents)
            )
        )

        return await self._renderer.render(
            metrics=metrics,
            agents=agents,
        )

    async def _render_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await asyncio.sleep(
                    self.DASHBOARD_REFRESH
                )

                dashboard = (
                    await self.build_dashboard_text()
                )

                for (
                    chat_id,
                    session,
                ) in list(
                    self._dashboard_sessions.items()
                ):

                    if (
                        not session.enabled
                    ):
                        continue

                    should_update = (
                        await self._renderer.should_update(
                            chat_id=
                                chat_id,
                            content=
                                dashboard,
                        )
                    )

                    if not should_update:
                        continue

                    await self._emit_dashboard_update(
                        chat_id=
                            chat_id,
                        message_id=
                            session.message_id,
                        content=
                            dashboard,
                    )

                    session.last_render_at = (
                        time.time()
                    )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    async def _emit_dashboard_update(
        self,
        *,
        chat_id: int,
        message_id: int,
        content: str,
    ) -> None:

        if not self.message_bus:
            return

        await self.message_bus.publish(
            topic=
                "telegram.dashboard.update",
            payload={
                "chat_id":
                    chat_id,
                "message_id":
                    message_id,
                "content":
                    content,
                "parse_mode":
                    "MarkdownV2",
                "timestamp":
                    time.time(),
            },
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
            "dashboard_sessions":
                len(
                    self._dashboard_sessions
                ),
            "admin_count":
                len(
                    self.admin_ids
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_LIVE_DASHBOARD = (
    LiveOperationalDashboard
)
