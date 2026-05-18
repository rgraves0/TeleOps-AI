from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from src.db.database import (
    DatabaseManager,
)
from src.db.models import (
    AuditLogModel,
    ProviderStateModel,
    TaskModel,
    WorkflowModel,
    WorkflowRunModel,
)

logger = logging.getLogger(__name__)


# =========================================================
# BASE REPOSITORY
# =========================================================


class BaseRepository:

    def __init__(
        self,
        db: DatabaseManager,
    ) -> None:

        self.db = db


# =========================================================
# WORKFLOW REPOSITORY
# =========================================================


class WorkflowRepository(
    BaseRepository
):

    async def create_workflow(
        self,
        workflow: WorkflowModel,
    ) -> bool:

        query = """

        INSERT INTO workflows (

            workflow_id,
            name,
            status,
            total_steps,
            completed_steps,
            metadata,
            created_at,
            updated_at

        )

        VALUES (?, ?, ?, ?, ?, ?, ?, ?)

        """

        try:

            await self.db.execute(

                query,

                (

                    workflow.workflow_id,

                    workflow.name,

                    workflow.status,

                    workflow.total_steps,

                    workflow.completed_steps,

                    json.dumps(
                        workflow.metadata
                    ),

                    workflow.created_at,

                    workflow.updated_at,
                ),
            )

            return True

        except Exception:

            logger.exception(
                "Create workflow failed"
            )

            return False

    # =====================================================
    # GET WORKFLOW
    # =====================================================

    async def get_workflow(
        self,
        workflow_id: str,
    ) -> dict | None:

        return await (
            self.db.fetch_one(

                """

                SELECT *
                FROM workflows
                WHERE workflow_id = ?

                """,

                (workflow_id,),
            )
        )

    # =====================================================
    # UPDATE STATUS
    # =====================================================

    async def update_status(
        self,
        workflow_id: str,
        status: str,
        completed_steps: int,
    ) -> bool:

        try:

            await self.db.execute(

                """

                UPDATE workflows

                SET

                    status = ?,
                    completed_steps = ?,
                    updated_at = ?

                WHERE workflow_id = ?

                """,

                (

                    status,

                    completed_steps,

                    datetime.utcnow()
                    .isoformat(),

                    workflow_id,
                ),
            )

            return True

        except Exception:

            logger.exception(
                "Workflow update failed"
            )

            return False

    # =====================================================
    # LIST WORKFLOWS
    # =====================================================

    async def list_workflows(
        self,
        limit: int = 50,
    ) -> list[dict]:

        return await (
            self.db.fetch_all(

                """

                SELECT *
                FROM workflows

                ORDER BY created_at DESC

                LIMIT ?

                """,

                (limit,),
            )
        )


# =========================================================
# WORKFLOW RUN REPOSITORY
# =========================================================


class WorkflowRunRepository(
    BaseRepository
):

    async def create_run(
        self,
        run: WorkflowRunModel,
    ) -> bool:

        try:

            await self.db.execute(

                """

                INSERT INTO workflow_runs (

                    run_id,
                    workflow_id,
                    success,
                    failed_step,
                    execution_time_ms,
                    error_message,
                    created_at,
                    updated_at

                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?)

                """,

                (

                    run.run_id,

                    run.workflow_id,

                    int(run.success),

                    run.failed_step,

                    run.execution_time_ms,

                    run.error_message,

                    run.created_at,

                    run.updated_at,
                ),
            )

            return True

        except Exception:

            logger.exception(
                "Workflow run insert failed"
            )

            return False

    # =====================================================
    # GET RUNS
    # =====================================================

    async def get_runs(
        self,
        workflow_id: str,
    ) -> list[dict]:

        return await (
            self.db.fetch_all(

                """

                SELECT *
                FROM workflow_runs

                WHERE workflow_id = ?

                ORDER BY created_at DESC

                """,

                (workflow_id,),
            )
        )


# =========================================================
# TASK REPOSITORY
# =========================================================


class TaskRepository(
    BaseRepository
):

    async def create_task(
        self,
        task: TaskModel,
    ) -> bool:

        try:

            await self.db.execute(

                """

                INSERT INTO tasks (

                    task_id,
                    task_name,
                    status,
                    priority,
                    assigned_agent,
                    payload,
                    result,
                    created_at,
                    updated_at

                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

                """,

                (

                    task.task_id,

                    task.task_name,

                    task.status,

                    task.priority,

                    task.assigned_agent,

                    json.dumps(
                        task.payload
                    ),

                    json.dumps(
                        task.result
                    ),

                    task.created_at,

                    task.updated_at,
                ),
            )

            return True

        except Exception:

            logger.exception(
                "Task insert failed"
            )

            return False

    # =====================================================
    # UPDATE TASK
    # =====================================================

    async def update_task(
        self,
        task_id: str,
        status: str,
        result: dict,
    ) -> bool:

        try:

            await self.db.execute(

                """

                UPDATE tasks

                SET

                    status = ?,
                    result = ?,
                    updated_at = ?

                WHERE task_id = ?

                """,

                (

                    status,

                    json.dumps(
                        result
                    ),

                    datetime.utcnow()
                    .isoformat(),

                    task_id,
                ),
            )

            return True

        except Exception:

            logger.exception(
                "Task update failed"
            )

            return False

    # =====================================================
    # GET TASK
    # =====================================================

    async def get_task(
        self,
        task_id: str,
    ) -> dict | None:

        return await (
            self.db.fetch_one(

                """

                SELECT *
                FROM tasks
                WHERE task_id = ?

                """,

                (task_id,),
            )
        )

    # =====================================================
    # LIST TASKS
    # =====================================================

    async def list_tasks(
        self,
        limit: int = 100,
    ) -> list[dict]:

        return await (
            self.db.fetch_all(

                """

                SELECT *
                FROM tasks

                ORDER BY created_at DESC

                LIMIT ?

                """,

                (limit,),
            )
        )


# =========================================================
# PROVIDER STATE REPOSITORY
# =========================================================


class ProviderStateRepository(
    BaseRepository
):

    async def upsert_provider_state(
        self,
        provider: (
            ProviderStateModel
        ),
    ) -> bool:

        try:

            await self.db.execute(

                """

                INSERT OR REPLACE
                INTO provider_states (

                    provider_name,
                    api_key_hash,
                    status,
                    cooldown_until,
                    last_error,
                    total_requests,
                    failed_requests,
                    created_at,
                    updated_at

                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

                """,

                (

                    provider.provider_name,

                    provider.api_key_hash,

                    provider.status,

                    provider.cooldown_until,

                    provider.last_error,

                    provider.total_requests,

                    provider.failed_requests,

                    provider.created_at,

                    provider.updated_at,
                ),
            )

            return True

        except Exception:

            logger.exception(
                "Provider state upsert failed"
            )

            return False

    # =====================================================
    # GET PROVIDER
    # =====================================================

    async def get_provider(
        self,
        provider_name: str,
    ) -> dict | None:

        return await (
            self.db.fetch_one(

                """

                SELECT *
                FROM provider_states
                WHERE provider_name = ?

                """,

                (provider_name,),
            )
        )


# =========================================================
# AUDIT LOG REPOSITORY
# =========================================================


class AuditLogRepository(
    BaseRepository
):

    async def log(
        self,
        audit: AuditLogModel,
    ) -> bool:

        try:

            await self.db.execute(

                """

                INSERT INTO audit_logs (

                    log_id,
                    event_type,
                    actor_id,
                    action,
                    resource,
                    success,
                    details,
                    created_at,
                    updated_at

                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

                """,

                (

                    audit.log_id,

                    audit.event_type,

                    audit.actor_id,

                    audit.action,

                    audit.resource,

                    int(audit.success),

                    json.dumps(
                        audit.details
                    ),

                    audit.created_at,

                    audit.updated_at,
                ),
            )

            return True

        except Exception:

            logger.exception(
                "Audit log insert failed"
            )

            return False

    # =====================================================
    # GET LOGS
    # =====================================================

    async def get_logs(
        self,
        limit: int = 100,
    ) -> list[dict]:

        return await (
            self.db.fetch_all(

                """

                SELECT *
                FROM audit_logs

                ORDER BY created_at DESC

                LIMIT ?

                """,

                (limit,),
            )
        )
