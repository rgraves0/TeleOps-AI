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


class WorkflowState(
    str,
    Enum,
):
    RUNNING = "running"
    PAUSED = "paused"
    HALTED = "halted"
    CANCELLED = "cancelled"


class ApprovalStatus(
    str,
    Enum,
):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"


@dataclass(slots=True)
class WorkflowSession:
    workflow_id: str
    agent_id: str
    state: WorkflowState
    current_task: str
    created_at: float
    updated_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ApprovalRequest:
    approval_id: str
    requester_id: str
    workflow_id: str
    operation_name: str
    risk_level: str
    status: ApprovalStatus
    created_at: float
    expires_at: float
    permissions: List[str]
    roles: List[str]
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class TelegramConsoleRBAC:
    """
    Telegram admin/superuser RBAC validator.
    """

    REQUIRED_PERMISSION = (
        "console.workflow.manage"
    )

    REQUIRED_APPROVAL_PERMISSION = (
        "console.approval.manage"
    )

    SYSTEM_ROLES = {
        "admin",
        "superuser",
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

        required_permission = (
            self.REQUIRED_APPROVAL_PERMISSION
            if "approval" in task_type
            else self.REQUIRED_PERMISSION
        )

        if (
            required_permission
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


class SQLiteWorkflowStore:
    """
    SQLite WAL workflow persistence.
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

    async def save_workflow(
        self,
        workflow: WorkflowSession,
    ) -> None:

        await asyncio.to_thread(
            self._save_workflow,
            workflow,
        )

    async def load_workflow(
        self,
        workflow_id: str,
    ) -> Optional[
        WorkflowSession
    ]:

        row = await asyncio.to_thread(
            self._load_workflow,
            workflow_id,
        )

        if not row:
            return None

        return WorkflowSession(
            workflow_id=row[0],
            agent_id=row[1],
            state=WorkflowState(
                row[2]
            ),
            current_task=row[3],
            created_at=row[4],
            updated_at=row[5],
            metadata=json.loads(
                row[6]
            ),
        )

    async def save_approval(
        self,
        approval: ApprovalRequest,
    ) -> None:

        await asyncio.to_thread(
            self._save_approval,
            approval,
        )

    async def load_approval(
        self,
        approval_id: str,
    ) -> Optional[
        ApprovalRequest
    ]:

        row = await asyncio.to_thread(
            self._load_approval,
            approval_id,
        )

        if not row:
            return None

        return ApprovalRequest(
            approval_id=row[0],
            requester_id=row[1],
            workflow_id=row[2],
            operation_name=row[3],
            risk_level=row[4],
            status=ApprovalStatus(
                row[5]
            ),
            created_at=row[6],
            expires_at=row[7],
            permissions=json.loads(
                row[8]
            ),
            roles=json.loads(
                row[9]
            ),
            metadata=json.loads(
                row[10]
            ),
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
            CREATE TABLE IF NOT EXISTS workflow_sessions (
                workflow_id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                state TEXT NOT NULL,
                current_task TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS approval_requests (
                approval_id TEXT PRIMARY KEY,
                requester_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                operation_name TEXT NOT NULL,
                risk_level TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                permissions TEXT NOT NULL,
                roles TEXT NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

    def _save_workflow(
        self,
        workflow: WorkflowSession,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO workflow_sessions (
                workflow_id,
                agent_id,
                state,
                current_task,
                created_at,
                updated_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                workflow.workflow_id,
                workflow.agent_id,
                workflow.state.value,
                workflow.current_task,
                workflow.created_at,
                workflow.updated_at,
                json.dumps(
                    workflow.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _load_workflow(
        self,
        workflow_id: str,
    ) -> Optional[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                workflow_id,
                agent_id,
                state,
                current_task,
                created_at,
                updated_at,
                metadata
            FROM workflow_sessions
            WHERE workflow_id = ?
            LIMIT 1
            """,
            (workflow_id,),
        )

        return cursor.fetchone()

    def _save_approval(
        self,
        approval: ApprovalRequest,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO approval_requests (
                approval_id,
                requester_id,
                workflow_id,
                operation_name,
                risk_level,
                status,
                created_at,
                expires_at,
                permissions,
                roles,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                approval.approval_id,
                approval.requester_id,
                approval.workflow_id,
                approval.operation_name,
                approval.risk_level,
                approval.status.value,
                approval.created_at,
                approval.expires_at,
                json.dumps(
                    approval.permissions
                ),
                json.dumps(
                    approval.roles
                ),
                json.dumps(
                    approval.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _load_approval(
        self,
        approval_id: str,
    ) -> Optional[Any]:

        cursor = self._connection.execute(
            """
            SELECT
                approval_id,
                requester_id,
                workflow_id,
                operation_name,
                risk_level,
                status,
                created_at,
                expires_at,
                permissions,
                roles,
                metadata
            FROM approval_requests
            WHERE approval_id = ?
            LIMIT 1
            """,
            (approval_id,),
        )

        return cursor.fetchone()


class WorkflowActionDispatcher:
    """
    Pause/resume/halt workflow dispatcher.
    """

    def __init__(
        self,
        *,
        store: SQLiteWorkflowStore,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:

        self.store = store
        self.message_bus = (
            message_bus
        )

    async def dispatch(
        self,
        *,
        workflow_id: str,
        action: str,
    ) -> WorkflowSession:

        workflow = (
            await self.store.load_workflow(
                workflow_id
            )
        )

        if not workflow:
            raise ValueError(
                "Workflow not found"
            )

        if action == "pause":
            workflow.state = (
                WorkflowState.PAUSED
            )

        elif action == "resume":
            workflow.state = (
                WorkflowState.RUNNING
            )

        elif action in (
            "halt",
            "cancel",
        ):
            workflow.state = (
                WorkflowState.HALTED
            )

        else:
            raise ValueError(
                "Unsupported workflow action"
            )

        workflow.updated_at = (
            time.time()
        )

        await self.store.save_workflow(
            workflow
        )

        await self._emit(
            "workflow.action",
            {
                "workflow_id":
                    workflow.workflow_id,
                "action":
                    action,
                "state":
                    workflow.state.value,
            },
        )

        return workflow

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


class InteractiveApprovalManager:
    """
    Human-in-the-loop approval runtime.
    """

    APPROVAL_TIMEOUT = 300

    def __init__(
        self,
        *,
        store: SQLiteWorkflowStore,
        message_bus: Optional[
            MessageBus
        ] = None,
    ) -> None:

        self.store = store
        self.message_bus = (
            message_bus
        )

        self._pending_tasks: Dict[
            str,
            asyncio.Task,
        ] = {}

    async def create_request(
        self,
        *,
        requester_id: str,
        workflow_id: str,
        operation_name: str,
        risk_level: str,
        permissions: Set[str],
        roles: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> ApprovalRequest:

        approval = ApprovalRequest(
            approval_id=
                secrets.token_hex(16),
            requester_id=
                requester_id,
            workflow_id=
                workflow_id,
            operation_name=
                operation_name,
            risk_level=
                risk_level,
            status=
                ApprovalStatus.PENDING,
            created_at=
                time.time(),
            expires_at=
                time.time()
                + self.APPROVAL_TIMEOUT,
            permissions=list(
                permissions
            ),
            roles=list(
                roles
            ),
            metadata=
                metadata or {},
        )

        await self.store.save_approval(
            approval
        )

        await self._emit(
            "approval.requested",
            {
                "approval_id":
                    approval.approval_id,
                "workflow_id":
                    workflow_id,
                "operation":
                    operation_name,
                "risk":
                    risk_level,
                "inline_keyboard": {
                    "buttons": [
                        {
                            "text":
                                "✅ Approve",
                            "callback_data":
                                f"approve:{approval.approval_id}",
                        },
                        {
                            "text":
                                "❌ Deny",
                            "callback_data":
                                f"deny:{approval.approval_id}",
                        },
                    ]
                },
            },
        )

        timeout_task = (
            asyncio.create_task(
                self._timeout_guard(
                    approval.approval_id
                )
            )
        )

        self._pending_tasks[
            approval.approval_id
        ] = timeout_task

        return approval

    async def approve(
        self,
        approval_id: str,
    ) -> ApprovalRequest:

        approval = (
            await self.store.load_approval(
                approval_id
            )
        )

        if not approval:
            raise ValueError(
                "Approval request not found"
            )

        approval.status = (
            ApprovalStatus.APPROVED
        )

        await self.store.save_approval(
            approval
        )

        await self._cancel_timeout(
            approval_id
        )

        await self._emit(
            "approval.approved",
            {
                "approval_id":
                    approval_id,
                "workflow_id":
                    approval.workflow_id,
            },
        )

        return approval

    async def deny(
        self,
        approval_id: str,
    ) -> ApprovalRequest:

        approval = (
            await self.store.load_approval(
                approval_id
            )
        )

        if not approval:
            raise ValueError(
                "Approval request not found"
            )

        approval.status = (
            ApprovalStatus.DENIED
        )

        await self.store.save_approval(
            approval
        )

        await self._cancel_timeout(
            approval_id
        )

        await self._emit(
            "approval.denied",
            {
                "approval_id":
                    approval_id,
                "workflow_id":
                    approval.workflow_id,
            },
        )

        return approval

    async def _timeout_guard(
        self,
        approval_id: str,
    ) -> None:

        try:
            await asyncio.sleep(
                self.APPROVAL_TIMEOUT
            )

            approval = (
                await self.store.load_approval(
                    approval_id
                )
            )

            if not approval:
                return

            if (
                approval.status
                != ApprovalStatus.PENDING
            ):
                return

            approval.status = (
                ApprovalStatus.EXPIRED
            )

            await self.store.save_approval(
                approval
            )

            await self._emit(
                "approval.expired",
                {
                    "approval_id":
                        approval_id,
                    "workflow_id":
                        approval.workflow_id,
                    "safety_state":
                        True,
                },
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.error(
                traceback.format_exc()
            )

    async def _cancel_timeout(
        self,
        approval_id: str,
    ) -> None:

        task = self._pending_tasks.pop(
            approval_id,
            None,
        )

        if task:
            task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

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


class TelegramCallbackQueryHandler:
    """
    Inline callback query processor.
    """

    def __init__(
        self,
        *,
        workflow_dispatcher:
            WorkflowActionDispatcher,
        approval_manager:
            InteractiveApprovalManager,
    ) -> None:

        self.workflow_dispatcher = (
            workflow_dispatcher
        )

        self.approval_manager = (
            approval_manager
        )

    async def process_callback(
        self,
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

        action, target = parts

        if action in (
            "pause",
            "resume",
            "halt",
            "cancel",
        ):
            workflow = (
                await self.workflow_dispatcher.dispatch(
                    workflow_id=target,
                    action=action,
                )
            )

            return {
                "type":
                    "workflow",
                "workflow_id":
                    workflow.workflow_id,
                "state":
                    workflow.state.value,
            }

        if action == "approve":
            approval = (
                await self.approval_manager.approve(
                    target
                )
            )

            return {
                "type":
                    "approval",
                "approval_id":
                    approval.approval_id,
                "status":
                    approval.status.value,
            }

        if action == "deny":
            approval = (
                await self.approval_manager.deny(
                    target
                )
            )

            return {
                "type":
                    "approval",
                "approval_id":
                    approval.approval_id,
                "status":
                    approval.status.value,
            }

        raise ValueError(
            "Unsupported callback action"
        )


class WorkflowControlsRuntime:
    """
    Async-first Telegram workflow controls runtime.

    Features:
    - Interactive Telegram workflow controls
    - Inline callback handlers
    - Pause/resume/halt orchestration
    - Human approval system
    - Auto-reject timeout guardrails
    - SQLite WAL persistence
    - Default Deny RBAC security
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
            "./data/workflow_controls.db"
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
            TelegramConsoleRBAC(
                router=router,
                admin_ids=
                    self.admin_ids,
            )
        )

        self._store = (
            SQLiteWorkflowStore(
                database_path=
                    database_path
            )
        )

        self.workflow_dispatcher = (
            WorkflowActionDispatcher(
                store=self._store,
                message_bus=
                    message_bus,
            )
        )

        self.approval_manager = (
            InteractiveApprovalManager(
                store=self._store,
                message_bus=
                    message_bus,
            )
        )

        self.callback_handler = (
            TelegramCallbackQueryHandler(
                workflow_dispatcher=
                    self.workflow_dispatcher,
                approval_manager=
                    self.approval_manager,
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
            "Starting WorkflowControlsRuntime"
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
            "Stopping WorkflowControlsRuntime"
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

    async def register_workflow(
        self,
        *,
        workflow_id: str,
        agent_id: str,
        current_task: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> WorkflowSession:

        workflow = WorkflowSession(
            workflow_id=
                workflow_id,
            agent_id=
                agent_id,
            state=
                WorkflowState.RUNNING,
            current_task=
                current_task,
            created_at=
                time.time(),
            updated_at=
                time.time(),
            metadata=
                metadata or {},
        )

        await self._store.save_workflow(
            workflow
        )

        return workflow

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
            "admin_count":
                len(
                    self.admin_ids
                ),
            "timestamp":
                time.time(),
        }


DEFAULT_WORKFLOW_CONTROLS = (
    WorkflowControlsRuntime
)
