from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from typing import Awaitable
from typing import Callable

from src.core.events import (
    EventBus,
)

logger = logging.getLogger(__name__)


# =========================================================
# WORKFLOW STEP
# =========================================================


@dataclass
class WorkflowStep:

    step_id: str

    name: str

    handler: Callable[
        [dict],
        Awaitable[Any],
    ]

    payload: dict = field(
        default_factory=dict
    )

    retry_count: int = 2

    timeout_seconds: int = 60


# =========================================================
# WORKFLOW
# =========================================================


@dataclass
class Workflow:

    workflow_id: str

    name: str

    steps: list[
        WorkflowStep
    ]

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )


# =========================================================
# EXECUTION RESULT
# =========================================================


@dataclass
class WorkflowResult:

    workflow_id: str

    success: bool

    completed_steps: int

    failed_step: str | None

    results: list[Any]

    started_at: str

    finished_at: str


# =========================================================
# WORKFLOW EXECUTOR
# =========================================================


class WorkflowExecutor:

    def __init__(
        self,
        event_bus: EventBus,
        max_concurrent_workflows: int = 2,
    ) -> None:

        self.event_bus = (
            event_bus
        )

        self.semaphore = (
            asyncio.Semaphore(
                max_concurrent_workflows
            )
        )

        self.active_workflows: dict[
            str,
            Workflow
        ] = {}

        logger.info(
            "WorkflowExecutor initialized"
        )

    # =====================================================
    # EXECUTE WORKFLOW
    # =====================================================

    async def execute(
        self,
        workflow: Workflow,
    ) -> WorkflowResult:

        async with self.semaphore:

            self.active_workflows[
                workflow.workflow_id
            ] = workflow

            logger.info(
                "Starting workflow=%s",
                workflow.name,
            )

            started_at = (
                datetime.utcnow()
                .isoformat()
            )

            results = []

            completed_steps = 0

            failed_step = None

            success = True

            try:

                for step in workflow.steps:

                    result = await (
                        self._execute_step(
                            step
                        )
                    )

                    results.append(
                        result
                    )

                    completed_steps += 1

                await (
                    self.event_bus.emit(
                        "workflow.completed",
                        {

                            "workflow_id":
                            workflow.workflow_id,

                            "workflow_name":
                            workflow.name,

                            "completed_steps":
                            completed_steps,
                        },
                    )
                )

            except Exception as exc:

                success = False

                failed_step = (
                    step.name
                )

                logger.exception(
                    "Workflow failed=%s",
                    workflow.name,
                )

                await (
                    self.event_bus.emit(
                        "workflow.failed",
                        {

                            "workflow_id":
                            workflow.workflow_id,

                            "workflow_name":
                            workflow.name,

                            "failed_step":
                            failed_step,

                            "error":
                            str(exc),
                        },
                    )
                )

            finally:

                self.active_workflows.pop(
                    workflow.workflow_id,
                    None,
                )

            finished_at = (
                datetime.utcnow()
                .isoformat()
            )

            return WorkflowResult(

                workflow_id=(
                    workflow.workflow_id
                ),

                success=success,

                completed_steps=(
                    completed_steps
                ),

                failed_step=(
                    failed_step
                ),

                results=results,

                started_at=(
                    started_at
                ),

                finished_at=(
                    finished_at
                ),
            )

    # =====================================================
    # EXECUTE STEP
    # =====================================================

    async def _execute_step(
        self,
        step: WorkflowStep,
    ) -> Any:

        logger.info(
            "Executing step=%s",
            step.name,
        )

        last_exception = None

        for attempt in range(
            step.retry_count + 1
        ):

            try:

                result = await (
                    asyncio.wait_for(
                        step.handler(
                            step.payload
                        ),
                        timeout=(
                            step.timeout_seconds
                        ),
                    )
                )

                await (
                    self.event_bus.emit(
                        "workflow.step.completed",
                        {

                            "step_id":
                            step.step_id,

                            "step_name":
                            step.name,
                        },
                    )
                )

                return result

            except Exception as exc:

                last_exception = exc

                logger.warning(
                    (
                        "Step failed "
                        "step=%s "
                        "attempt=%s"
                    ),
                    step.name,
                    attempt + 1,
                )

                await asyncio.sleep(1)

        await (
            self.event_bus.emit(
                "workflow.step.failed",
                {

                    "step_id":
                    step.step_id,

                    "step_name":
                    step.name,

                    "error":
                    str(last_exception),
                },
            )
        )

        raise last_exception

    # =====================================================
    # CREATE WORKFLOW
    # =====================================================

    def create_workflow(
        self,
        name: str,
        steps: list[
            WorkflowStep
        ],
    ) -> Workflow:

        return Workflow(

            workflow_id=str(
                uuid.uuid4()
            ),

            name=name,

            steps=steps,
        )

    # =====================================================
    # STATS
    # =====================================================

    def stats(
        self,
    ) -> dict:

        return {

            "active_workflows":
            len(
                self.active_workflows
            ),

            "workflow_ids":
            list(
                self.active_workflows
                .keys()
            ),
        }
