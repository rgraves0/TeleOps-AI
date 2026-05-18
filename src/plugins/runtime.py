from __future__ import annotations

import asyncio
import logging
import traceback
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.plugins.base import (
    ToolContext,
    ToolResult,
)
from src.plugins.registry import (
    ToolRegistry,
)

logger = logging.getLogger(__name__)


# =========================================================
# EXECUTION POLICY
# =========================================================


@dataclass
class ExecutionPolicy:

    max_concurrent_tools: int = 2

    default_timeout: int = 30

    allow_background_execution: bool = False

    max_payload_size: int = 100_000


# =========================================================
# RUNTIME METRICS
# =========================================================


@dataclass
class RuntimeMetrics:

    total_executions: int = 0

    successful_executions: int = 0

    failed_executions: int = 0

    timeout_executions: int = 0

    permission_denied: int = 0


# =========================================================
# PLUGIN RUNTIME
# =========================================================


class PluginRuntime:

    def __init__(
        self,
        registry: ToolRegistry,
        policy: (
            ExecutionPolicy
            | None
        ) = None,
    ) -> None:

        self.registry = registry

        self.policy = (
            policy
            or ExecutionPolicy()
        )

        self.metrics = (
            RuntimeMetrics()
        )

        self.semaphore = (
            asyncio.Semaphore(
                self.policy
                .max_concurrent_tools
            )
        )

        self.running_tasks: dict[
            str,
            asyncio.Task
        ] = {}

        logger.info(
            "PluginRuntime initialized"
        )

    # =====================================================
    # EXECUTE TOOL
    # =====================================================

    async def execute_tool(
        self,
        tool_name: str,
        payload: dict,
        context: ToolContext,
        timeout: int | None = None,
    ) -> ToolResult:

        async with self.semaphore:

            self.metrics.total_executions += 1

            try:

                # =========================================
                # PAYLOAD SIZE CHECK
                # =========================================

                payload_size = len(
                    str(payload)
                )

                if (
                    payload_size
                    > self.policy
                    .max_payload_size
                ):

                    self.metrics.failed_executions += 1

                    return ToolResult(

                        success=False,

                        error=(
                            "Payload too large"
                        ),
                    )

                # =========================================
                # TOOL EXISTENCE
                # =========================================

                tool = (
                    self.registry
                    .get_tool(
                        tool_name
                    )
                )

                if tool is None:

                    self.metrics.failed_executions += 1

                    return ToolResult(

                        success=False,

                        error=(
                            f"Tool not found: "
                            f"{tool_name}"
                        ),
                    )

                # =========================================
                # PERMISSION CHECK
                # =========================================

                if not tool.has_permission(
                    context
                ):

                    self.metrics.permission_denied += 1

                    return ToolResult(

                        success=False,

                        error=(
                            "Permission denied"
                        ),
                    )

                # =========================================
                # EXECUTION
                # =========================================

                execution_timeout = (
                    timeout
                    or tool.timeout_seconds
                    or self.policy
                    .default_timeout
                )

                result = await (
                    asyncio.wait_for(

                        self.registry
                        .execute_tool(
                            tool_name,
                            payload,
                            context,
                        ),

                        timeout=(
                            execution_timeout
                        ),
                    )
                )

                if result.success:

                    self.metrics.successful_executions += 1

                else:

                    self.metrics.failed_executions += 1

                return result

            except asyncio.TimeoutError:

                self.metrics.timeout_executions += 1

                logger.warning(
                    "Tool timeout=%s",
                    tool_name,
                )

                return ToolResult(

                    success=False,

                    error="Execution timeout",
                )

            except Exception as exc:

                self.metrics.failed_executions += 1

                logger.exception(
                    "Runtime execution failed"
                )

                return ToolResult(

                    success=False,

                    error=str(exc),
                )

    # =====================================================
    # BACKGROUND EXECUTION
    # =====================================================

    async def execute_background(
        self,
        execution_id: str,
        tool_name: str,
        payload: dict,
        context: ToolContext,
    ) -> bool:

        if (
            not self.policy
            .allow_background_execution
        ):

            return False

        async def runner():

            try:

                await self.execute_tool(
                    tool_name,
                    payload,
                    context,
                )

            except Exception:

                logger.exception(
                    "Background execution failed"
                )

            finally:

                self.running_tasks.pop(
                    execution_id,
                    None,
                )

        task = asyncio.create_task(
            runner()
        )

        self.running_tasks[
            execution_id
        ] = task

        return True

    # =====================================================
    # CANCEL EXECUTION
    # =====================================================

    async def cancel_execution(
        self,
        execution_id: str,
    ) -> bool:

        task = self.running_tasks.get(
            execution_id
        )

        if not task:
            return False

        task.cancel()

        self.running_tasks.pop(
            execution_id,
            None,
        )

        logger.warning(
            "Execution cancelled=%s",
            execution_id,
        )

        return True

    # =====================================================
    # HEALTHCHECK
    # =====================================================

    async def healthcheck(
        self,
    ) -> dict:

        registry_health = await (
            self.registry
            .healthcheck()
        )

        return {

            "healthy":
            True,

            "running_tasks":
            len(
                self.running_tasks
            ),

            "tool_health":
            registry_health,

            "metrics":
            self.stats(),
        }

    # =====================================================
    # STATS
    # =====================================================

    def stats(
        self,
    ) -> dict:

        return {

            "total_executions":
            self.metrics
            .total_executions,

            "successful_executions":
            self.metrics
            .successful_executions,

            "failed_executions":
            self.metrics
            .failed_executions,

            "timeout_executions":
            self.metrics
            .timeout_executions,

            "permission_denied":
            self.metrics
            .permission_denied,

            "running_tasks":
            len(
                self.running_tasks
            ),
        }
