from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from datetime import datetime
from datetime import timedelta
from typing import Awaitable
from typing import Callable

from src.db.database import (
    DatabaseManager,
)
from src.scheduler.task_models import (
    ScheduledTask,
    ScheduledTaskStatus,
    ScheduleType,
    SCHEDULED_TASK_SCHEMA,
    SCHEDULED_TASK_INDEXES,
    TaskExecutionResult,
)

logger = logging.getLogger(__name__)


# =========================================================
# AUTONOMOUS SCHEDULER
# =========================================================


class AutonomousScheduler:

    def __init__(
        self,
        db: DatabaseManager,
        max_concurrent_tasks: int = 5,
        poll_interval: int = 2,
    ) -> None:

        self.db = db

        self.max_concurrent_tasks = (
            max_concurrent_tasks
        )

        self.poll_interval = (
            poll_interval
        )

        self.running = False

        self.task_handlers: dict[
            str,
            Callable[..., Awaitable]
        ] = {}

        self.active_tasks: set[
            asyncio.Task
        ] = set()

        self.semaphore = (
            asyncio.Semaphore(
                max_concurrent_tasks
            )
        )

        self.shutdown_event = (
            asyncio.Event()
        )

        logger.info(
            "AutonomousScheduler initialized"
        )

    # =====================================================
    # INITIALIZE
    # =====================================================

    async def initialize(
        self,
    ) -> None:

        await self.db.execute(
            SCHEDULED_TASK_SCHEMA
        )

        for index_sql in (
            SCHEDULED_TASK_INDEXES
        ):

            await self.db.execute(
                index_sql
            )

        logger.info(
            "Scheduler schema initialized"
        )

    # =====================================================
    # REGISTER HANDLER
    # =====================================================

    def register_handler(
        self,
        task_name: str,
        handler: Callable[
            ...,
            Awaitable,
        ],
    ) -> None:

        self.task_handlers[
            task_name
        ] = handler

        logger.info(
            "Task handler registered=%s",
            task_name,
        )

    # =====================================================
    # ADD TASK
    # =====================================================

    async def add_task(
        self,
        task: ScheduledTask,
    ) -> bool:

        try:

            await self.db.execute(

                """

                INSERT OR REPLACE
                INTO scheduled_tasks (

                    task_id,
                    name,
                    schedule_type,
                    priority,
                    status,
                    interval_seconds,
                    cron_expression,
                    next_run_at,
                    last_run_at,
                    max_retries,
                    retry_count,
                    enabled,
                    persistent,
                    timeout_seconds,
                    metadata,
                    created_at,
                    updated_at

                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                """,

                (

                    task.task_id,

                    task.name,

                    task.schedule_type.value,

                    task.priority.value,

                    task.status.value,

                    task.interval_seconds,

                    task.cron_expression,

                    task.next_run_at,

                    task.last_run_at,

                    task.max_retries,

                    task.retry_count,

                    int(task.enabled),

                    int(task.persistent),

                    task.timeout_seconds,

                    json.dumps(
                        task.metadata
                    ),

                    task.created_at,

                    task.updated_at,
                ),
            )

            return True

        except Exception:

            logger.exception(
                "Add scheduled task failed"
            )

            return False

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if self.running:
            return

        self.running = True

        logger.info(
            "AutonomousScheduler started"
        )

        asyncio.create_task(
            self._scheduler_loop()
        )

    # =====================================================
    # SCHEDULER LOOP
    # =====================================================

    async def _scheduler_loop(
        self,
    ) -> None:

        while (
            self.running
            and not self.shutdown_event.is_set()
        ):

            try:

                await self._dispatch_tasks()

            except Exception:

                logger.exception(
                    "Scheduler loop error"
                )

            await asyncio.sleep(
                self.poll_interval
            )

    # =====================================================
    # DISPATCH TASKS
    # =====================================================

    async def _dispatch_tasks(
        self,
    ) -> None:

        now = (
            datetime.utcnow()
            .isoformat()
        )

        rows = await (
            self.db.fetch_all(

                """

                SELECT *

                FROM scheduled_tasks

                WHERE enabled = 1
                AND next_run_at <= ?

                ORDER BY

                    CASE priority

                        WHEN 'critical' THEN 1
                        WHEN 'high' THEN 2
                        WHEN 'normal' THEN 3
                        ELSE 4

                    END

                LIMIT ?

                """,

                (
                    now,
                    self.max_concurrent_tasks,
                ),
            )
        )

        for row in rows:

            task = asyncio.create_task(
                self._execute_task(
                    row
                )
            )

            self.active_tasks.add(
                task
            )

            task.add_done_callback(
                self.active_tasks.discard
            )

    # =====================================================
    # EXECUTE TASK
    # =====================================================

    async def _execute_task(
        self,
        row: dict,
    ) -> None:

        async with self.semaphore:

            task_name = row["name"]

            handler = (
                self.task_handlers.get(
                    task_name
                )
            )

            if not handler:

                logger.warning(
                    "Missing task handler=%s",
                    task_name,
                )

                return

            task_id = row["task_id"]

            start = time.perf_counter()

            started_at = (
                datetime.utcnow()
                .isoformat()
            )

            try:

                await self._update_status(

                    task_id,

                    ScheduledTaskStatus.RUNNING,
                )

                await asyncio.wait_for(

                    handler(
                        row
                    ),

                    timeout=row[
                        "timeout_seconds"
                    ],
                )

                finished_at = (
                    datetime.utcnow()
                    .isoformat()
                )

                duration_ms = round(

                    (
                        time.perf_counter()
                        - start
                    )
                    * 1000,

                    2,
                )

                result = (
                    TaskExecutionResult(

                        task_id=task_id,

                        success=True,

                        duration_ms=
                        duration_ms,

                        started_at=
                        started_at,

                        finished_at=
                        finished_at,
                    )
                )

                logger.info(
                    "Task executed=%s duration=%sms",
                    task_name,
                    duration_ms,
                )

                await self._reschedule(
                    row
                )

            except Exception as exc:

                logger.exception(
                    "Task execution failed=%s",
                    task_name,
                )

                await self._handle_failure(
                    row,
                    str(exc),
                )

    # =====================================================
    # RESCHEDULE
    # =====================================================

    async def _reschedule(
        self,
        row: dict,
    ) -> None:

        schedule_type = (
            row["schedule_type"]
        )

        if (
            schedule_type
            == ScheduleType.ONCE.value
        ):

            await self._update_status(

                row["task_id"],

                ScheduledTaskStatus.COMPLETED,
            )

            return

        next_run = (
            datetime.utcnow()
        )

        if (
            schedule_type
            == ScheduleType.INTERVAL.value
        ):

            seconds = (
                row[
                    "interval_seconds"
                ]
                or 60
            )

            next_run += timedelta(
                seconds=seconds
            )

        else:

            next_run += timedelta(
                minutes=5
            )

        await self.db.execute(

            """

            UPDATE scheduled_tasks

            SET

                next_run_at = ?,

                last_run_at = ?,

                status = ?

            WHERE task_id = ?

            """,

            (

                next_run.isoformat(),

                datetime.utcnow()
                .isoformat(),

                ScheduledTaskStatus.PENDING.value,

                row["task_id"],
            ),
        )

    # =====================================================
    # HANDLE FAILURE
    # =====================================================

    async def _handle_failure(
        self,
        row: dict,
        error: str,
    ) -> None:

        retry_count = (
            row["retry_count"]
            + 1
        )

        max_retries = (
            row["max_retries"]
        )

        if retry_count > max_retries:

            await self._update_status(

                row["task_id"],

                ScheduledTaskStatus.FAILED,
            )

            return

        backoff = min(
            retry_count * 30,
            300,
        )

        next_run = (

            datetime.utcnow()

            + timedelta(
                seconds=backoff
            )

        ).isoformat()

        await self.db.execute(

            """

            UPDATE scheduled_tasks

            SET

                retry_count = ?,

                next_run_at = ?,

                status = ?

            WHERE task_id = ?

            """,

            (

                retry_count,

                next_run,

                ScheduledTaskStatus.PENDING.value,

                row["task_id"],
            ),
        )

        logger.warning(
            "Task retry scheduled=%s retry=%s",
            row["name"],
            retry_count,
        )

    # =====================================================
    # UPDATE STATUS
    # =====================================================

    async def _update_status(
        self,
        task_id: str,
        status: (
            ScheduledTaskStatus
        ),
    ) -> None:

        await self.db.execute(

            """

            UPDATE scheduled_tasks

            SET

                status = ?,

                updated_at = ?

            WHERE task_id = ?

            """,

            (

                status.value,

                datetime.utcnow()
                .isoformat(),

                task_id,
            ),
        )

    # =====================================================
    # SHUTDOWN
    # =====================================================

    async def shutdown(
        self,
    ) -> None:

        logger.info(
            "Scheduler shutdown started"
        )

        self.running = False

        self.shutdown_event.set()

        if self.active_tasks:

            await asyncio.gather(

                *self.active_tasks,

                return_exceptions=True,
            )

        logger.info(
            "Scheduler shutdown completed"
        )

    # =====================================================
    # TASK STATS
    # =====================================================

    async def stats(
        self,
    ) -> dict:

        rows = await (
            self.db.fetch_all(

                """

                SELECT status,
                       COUNT(*) as total

                FROM scheduled_tasks

                GROUP BY status

                """
            )
        )

        summary = {

            row["status"]:
            row["total"]

            for row in rows
        }

        return {

            "running":
            self.running,

            "active_tasks":
            len(
                self.active_tasks
            ),

            "max_concurrent":
            self.max_concurrent_tasks,

            "tasks":
            summary,
        }
