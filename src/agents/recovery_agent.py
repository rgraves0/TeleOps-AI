from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import defaultdict
from enum import Enum
from typing import Any, Dict, List, Optional

from app.core.base_agent import BaseAgent
from app.core.message_bus import MessageBus


logger = logging.getLogger(__name__)


class RecoveryMode(str, Enum):
    NORMAL = "normal"
    DEGRADED = "degraded"
    FAILOVER = "failover"
    EMERGENCY = "emergency"


class RecoveryAgent(BaseAgent):
    """
    Autonomous recovery & self-healing subsystem.

    Responsibilities:
    - Restart failed agents
    - Trigger degradation mode
    - Perform emergency failover
    - Coordinate safe fallback routing
    - Prevent restart storms
    """

    MAX_RESTART_ATTEMPTS = 3
    RESTART_WINDOW_SECONDS = 300
    ALERT_CONSUMER_CONCURRENCY = 2

    def __init__(
        self,
        message_bus: MessageBus,
        *,
        agent_registry: Optional[Dict[str, Any]] = None,
        workflow_router: Optional[Any] = None,
        agent_id: str = "recovery-agent",
    ) -> None:
        super().__init__(agent_id=agent_id)

        self.message_bus = message_bus
        self.agent_registry = agent_registry or {}
        self.workflow_router = workflow_router

        self._running = False
        self._tasks: List[asyncio.Task] = []

        self._restart_history: Dict[str, List[float]] = defaultdict(list)

        self._recovery_mode = RecoveryMode.NORMAL

        self._alert_queue: asyncio.Queue = asyncio.Queue(maxsize=200)

    async def start(self) -> None:
        logger.info("Starting RecoveryAgent")

        self._running = True

        await self.message_bus.subscribe(
            "monitor.alert",
            self._handle_monitor_alert,
        )

        for _ in range(self.ALERT_CONSUMER_CONCURRENCY):
            self._tasks.append(
                asyncio.create_task(self._alert_worker())
            )

        self._tasks.append(
            asyncio.create_task(self._maintenance_loop())
        )

        logger.info("RecoveryAgent started")

    async def stop(self) -> None:
        logger.info("Stopping RecoveryAgent")

        self._running = False

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._tasks.clear()

        logger.info("RecoveryAgent stopped")

    async def _handle_monitor_alert(
        self,
        payload: Dict[str, Any],
    ) -> None:
        try:
            self._alert_queue.put_nowait(payload)
        except asyncio.QueueFull:
            logger.error(
                "Recovery queue full. Dropping alert: %s",
                payload,
            )

    async def _alert_worker(self) -> None:
        while self._running:
            try:
                alert = await self._alert_queue.get()

                await self._process_alert(alert)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Recovery alert worker failure")

    async def _process_alert(
        self,
        alert: Dict[str, Any],
    ) -> None:
        alert_type = alert.get("type")
        severity = alert.get("severity")
        reason = alert.get("reason")

        logger.warning(
            "Recovery processing alert | type=%s severity=%s reason=%s",
            alert_type,
            severity,
            reason,
        )

        if alert_type == "agent_alert":
            await self._handle_agent_alert(alert)

        elif alert_type == "system_alert":
            await self._handle_system_alert(alert)

    async def _handle_agent_alert(
        self,
        alert: Dict[str, Any],
    ) -> None:
        agent_id = alert.get("agent_id")
        severity = alert.get("severity")

        if not agent_id:
            return

        if severity == "dead":
            await self._restart_agent(agent_id)

        elif severity == "warning":
            await self._soft_recover_agent(agent_id)

    async def _handle_system_alert(
        self,
        alert: Dict[str, Any],
    ) -> None:
        severity = alert.get("severity")
        reason = alert.get("reason")

        if severity == "critical":
            await self._enter_degradation_mode(reason)

        if reason in {
            "memory_critical",
            "memory_spike_detected",
        }:
            await self._perform_memory_pressure_recovery()

        if reason in {
            "event_loop_stall",
        }:
            await self._perform_emergency_failover()

    async def _restart_agent(
        self,
        agent_id: str,
    ) -> None:
        if not self._can_restart(agent_id):
            logger.critical(
                "Restart threshold exceeded for agent=%s",
                agent_id,
            )

            await self._enter_emergency_mode(
                f"restart_threshold_exceeded:{agent_id}"
            )

            return

        agent = self.agent_registry.get(agent_id)

        if not agent:
            logger.error(
                "Agent not found in registry: %s",
                agent_id,
            )
            return

        logger.warning(
            "Attempting autonomous restart for agent=%s",
            agent_id,
        )

        try:
            if hasattr(agent, "stop"):
                await agent.stop()

            await asyncio.sleep(2)

            if hasattr(agent, "start"):
                await agent.start()

            self._restart_history[agent_id].append(time.time())

            await self.message_bus.publish(
                "recovery.agent_restarted",
                {
                    "agent_id": agent_id,
                    "timestamp": time.time(),
                },
            )

            logger.info(
                "Agent restart successful: %s",
                agent_id,
            )

        except Exception:
            logger.exception(
                "Autonomous restart failed for agent=%s",
                agent_id,
            )

            await self._failover_workflows(agent_id)

    async def _soft_recover_agent(
        self,
        agent_id: str,
    ) -> None:
        logger.info(
            "Attempting soft recovery for agent=%s",
            agent_id,
        )

        await self.message_bus.publish(
            "recovery.soft_recovery",
            {
                "agent_id": agent_id,
                "timestamp": time.time(),
            },
        )

    async def _failover_workflows(
        self,
        agent_id: str,
    ) -> None:
        logger.warning(
            "Executing workflow failover for agent=%s",
            agent_id,
        )

        if self.workflow_router:
            try:
                await self.workflow_router.enable_safe_mode(
                    failed_agent=agent_id
                )
            except Exception:
                logger.exception(
                    "Workflow router failover failure"
                )

        await self.message_bus.publish(
            "recovery.workflow_failover",
            {
                "failed_agent": agent_id,
                "timestamp": time.time(),
            },
        )

    async def _enter_degradation_mode(
        self,
        reason: str,
    ) -> None:
        if self._recovery_mode == RecoveryMode.DEGRADED:
            return

        logger.warning(
            "Entering degradation mode | reason=%s",
            reason,
        )

        self._recovery_mode = RecoveryMode.DEGRADED

        await self.message_bus.publish(
            "system.degradation_mode",
            {
                "reason": reason,
                "timestamp": time.time(),
            },
        )

        await self._apply_resource_restrictions()

    async def _enter_emergency_mode(
        self,
        reason: str,
    ) -> None:
        logger.critical(
            "Entering emergency mode | reason=%s",
            reason,
        )

        self._recovery_mode = RecoveryMode.EMERGENCY

        await self.message_bus.publish(
            "system.emergency_mode",
            {
                "reason": reason,
                "timestamp": time.time(),
            },
        )

        await self._perform_emergency_failover()

    async def _perform_memory_pressure_recovery(self) -> None:
        logger.warning(
            "Performing memory pressure recovery"
        )

        await self.message_bus.publish(
            "system.memory_cleanup",
            {
                "timestamp": time.time(),
            },
        )

        await asyncio.sleep(1)

    async def _perform_emergency_failover(self) -> None:
        logger.critical(
            "Performing emergency failover"
        )

        self._recovery_mode = RecoveryMode.FAILOVER

        await self.message_bus.publish(
            "system.failover",
            {
                "timestamp": time.time(),
                "mode": self._recovery_mode.value,
            },
        )

        if self.workflow_router:
            try:
                await self.workflow_router.route_to_fallback()
            except Exception:
                logger.exception(
                    "Emergency fallback routing failed"
                )

    async def _apply_resource_restrictions(self) -> None:
        logger.info(
            "Applying low-resource degradation policies"
        )

        await self.message_bus.publish(
            "system.resource_restrictions",
            {
                "disable_non_critical_tasks": True,
                "reduce_parallelism": True,
                "enable_backpressure": True,
                "timestamp": time.time(),
            },
        )

    async def _maintenance_loop(self) -> None:
        while self._running:
            try:
                self._cleanup_restart_history()
            except Exception:
                logger.exception(
                    "Recovery maintenance loop failure"
                )

            await asyncio.sleep(60)

    def _cleanup_restart_history(self) -> None:
        cutoff = time.time() - self.RESTART_WINDOW_SECONDS

        for agent_id, entries in list(
            self._restart_history.items()
        ):
            self._restart_history[agent_id] = [
                ts for ts in entries if ts >= cutoff
            ]

    def _can_restart(
        self,
        agent_id: str,
    ) -> bool:
        self._cleanup_restart_history()

        restart_count = len(
            self._restart_history.get(agent_id, [])
        )

        return restart_count < self.MAX_RESTART_ATTEMPTS

    @property
    def recovery_mode(self) -> str:
        return self._recovery_mode.value
