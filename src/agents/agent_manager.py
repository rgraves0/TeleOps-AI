from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from src.agents.base_agent import (
    AgentHealth,
    AgentStatus,
    BaseAgent,
)

logger = logging.getLogger(__name__)


# =========================================================
# AGENT MANAGER
# =========================================================


class AgentManager:

    def __init__(
        self,
        max_concurrent_agents: int = 5,
        monitor_interval: int = 15,
    ) -> None:

        self.max_concurrent_agents = (
            max_concurrent_agents
        )

        self.monitor_interval = (
            monitor_interval
        )

        self.agents: dict[
            str,
            BaseAgent
        ] = {}

        self.running = False

        self.monitor_task: (
            asyncio.Task | None
        ) = None

        self.shutdown_event = (
            asyncio.Event()
        )

        self.semaphore = (
            asyncio.Semaphore(
                max_concurrent_agents
            )
        )

        logger.info(
            "AgentManager initialized"
        )

    # =====================================================
    # REGISTER AGENT
    # =====================================================

    async def register_agent(
        self,
        agent: BaseAgent,
    ) -> None:

        self.agents[
            agent.agent_name
        ] = agent

        logger.info(
            "Agent registered=%s",
            agent.agent_name,
        )

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if self.running:
            return

        self.running = True

        self.shutdown_event.clear()

        for agent in (
            self.agents.values()
        ):

            async with self.semaphore:

                await agent.start()

                await asyncio.sleep(
                    0.05
                )

        self.monitor_task = (
            asyncio.create_task(
                self._monitor_loop()
            )
        )

        logger.info(
            "AgentManager started"
        )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        if not self.running:
            return

        self.running = False

        self.shutdown_event.set()

        if self.monitor_task:

            await asyncio.gather(

                self.monitor_task,

                return_exceptions=True,
            )

        stop_tasks = [

            asyncio.create_task(
                agent.stop()
            )

            for agent
            in self.agents.values()
        ]

        if stop_tasks:

            await asyncio.gather(

                *stop_tasks,

                return_exceptions=True,
            )

        logger.info(
            "AgentManager stopped"
        )

    # =====================================================
    # MONITOR LOOP
    # =====================================================

    async def _monitor_loop(
        self,
    ) -> None:

        while (

            self.running

            and not (
                self.shutdown_event
                .is_set()
            )
        ):

            try:

                await self._monitor_agents()

            except Exception:

                logger.exception(
                    "Agent monitor failed"
                )

            await asyncio.sleep(
                self.monitor_interval
            )

    # =====================================================
    # MONITOR AGENTS
    # =====================================================

    async def _monitor_agents(
        self,
    ) -> None:

        for agent in (
            self.agents.values()
        ):

            try:

                healthy = await (
                    agent.is_healthy()
                )

                if not healthy:

                    logger.warning(
                        "Unhealthy agent=%s",
                        agent.agent_name,
                    )

                    await self._recover_agent(
                        agent
                    )

            except Exception:

                logger.exception(
                    "Agent health check failed=%s",
                    agent.agent_name,
                )

    # =====================================================
    # RECOVER AGENT
    # =====================================================

    async def _recover_agent(
        self,
        agent: BaseAgent,
    ) -> None:

        logger.warning(
            "Recovering agent=%s",
            agent.agent_name,
        )

        try:

            await agent.stop()

            await asyncio.sleep(
                1
            )

            await agent.start()

            logger.info(
                "Agent recovered=%s",
                agent.agent_name,
            )

        except Exception:

            logger.exception(
                "Agent recovery failed=%s",
                agent.agent_name,
            )

    # =====================================================
    # GET AGENT
    # =====================================================

    async def get_agent(
        self,
        agent_name: str,
    ) -> (
        BaseAgent
        | None
    ):

        return self.agents.get(
            agent_name
        )

    # =====================================================
    # AGENT HEALTH
    # =====================================================

    async def agent_health(
        self,
        agent_name: str,
    ) -> (
        AgentHealth
        | None
    ):

        agent = (
            self.agents.get(
                agent_name
            )
        )

        if not agent:
            return None

        return await (
            agent.health()
        )

    # =====================================================
    # ALL HEALTH
    # =====================================================

    async def all_health(
        self,
    ) -> dict:

        result = {}

        for name, agent in (
            self.agents.items()
        ):

            try:

                health = await (
                    agent.health()
                )

                result[name] = (
                    health.__dict__
                )

            except Exception as exc:

                result[name] = {

                    "status":
                    "failed",

                    "error":
                    str(exc),
                }

        return result

    # =====================================================
    # RUNNING AGENTS
    # =====================================================

    async def running_agents(
        self,
    ) -> list[str]:

        result = []

        for name, agent in (
            self.agents.items()
        ):

            if agent.running:

                result.append(
                    name
                )

        return result

    # =====================================================
    # FAILED AGENTS
    # =====================================================

    async def failed_agents(
        self,
    ) -> list[str]:

        result = []

        for name, agent in (
            self.agents.items()
        ):

            if (
                agent.status
                == AgentStatus.FAILED
            ):

                result.append(
                    name
                )

        return result

    # =====================================================
    # STATS
    # =====================================================

    async def stats(
        self,
    ) -> dict:

        total = len(
            self.agents
        )

        running = len(
            await self.running_agents()
        )

        failed = len(
            await self.failed_agents()
        )

        return {

            "running":
            self.running,

            "total_agents":
            total,

            "running_agents":
            running,

            "failed_agents":
            failed,

            "max_concurrent":
            self.max_concurrent_agents,
        }
