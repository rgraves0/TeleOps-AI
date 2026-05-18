from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.memory.operational_memory import (
    OperationalMemory,
)
from src.monitoring.metrics import (
    MetricsCollector,
)
from src.scheduler.recovery_engine import (
    RecoveryEngine,
)

logger = logging.getLogger(__name__)


# =========================================================
# SYSTEM PRESSURE
# =========================================================


@dataclass
class SystemPressure:

    cpu_percent: float

    memory_percent: float

    queue_size: int

    active_tasks: int

    pressure_score: float


# =========================================================
# ADAPTIVE LIMITS
# =========================================================


@dataclass
class AdaptiveLimits:

    max_concurrency: int

    queue_limit: int

    request_delay_ms: int

    degraded_mode: bool


# =========================================================
# PROVIDER LOAD
# =========================================================


@dataclass
class ProviderLoad:

    provider_name: str

    reliability: float

    cooldown_active: bool

    latency_ms: float

    score: float


# =========================================================
# ADAPTIVE ENGINE
# =========================================================


class AdaptiveEngine:

    def __init__(
        self,
        metrics: MetricsCollector,
        operational_memory: (
            OperationalMemory
        ),
        recovery_engine: (
            RecoveryEngine
        ),
        max_history: int = 100,
    ) -> None:

        self.metrics = metrics

        self.operational_memory = (
            operational_memory
        )

        self.recovery_engine = (
            recovery_engine
        )

        self.max_history = (
            max_history
        )

        self.pressure_history = deque(
            maxlen=max_history
        )

        self.current_limits = (
            AdaptiveLimits(

                max_concurrency=5,

                queue_limit=50,

                request_delay_ms=0,

                degraded_mode=False,
            )
        )

        self.active_tasks = 0

        self.queue_size = 0

        self.last_adjustment = 0.0

        logger.info(
            "AdaptiveEngine initialized"
        )

    # =====================================================
    # COLLECT PRESSURE
    # =====================================================

    async def collect_pressure(
        self,
    ) -> SystemPressure:

        stats = await (
            self.metrics.snapshot()
        )

        cpu = float(
            stats.get(
                "cpu_percent",
                0,
            )
        )

        memory = float(
            stats.get(
                "memory_percent",
                0,
            )
        )

        queue = self.queue_size

        active = self.active_tasks

        pressure_score = (
            self._pressure_score(

                cpu=cpu,

                memory=memory,

                queue=queue,

                active=active,
            )
        )

        pressure = SystemPressure(

            cpu_percent=cpu,

            memory_percent=
            memory,

            queue_size=queue,

            active_tasks=
            active,

            pressure_score=
            pressure_score,
        )

        self.pressure_history.append(
            pressure
        )

        return pressure

    # =====================================================
    # PRESSURE SCORE
    # =====================================================

    def _pressure_score(
        self,
        cpu: float,
        memory: float,
        queue: int,
        active: int,
    ) -> float:

        cpu_weight = (
            cpu * 0.35
        )

        memory_weight = (
            memory * 0.45
        )

        queue_weight = min(
            queue * 1.5,
            100,
        )

        active_weight = min(
            active * 5,
            100,
        )

        total = (

            cpu_weight

            + memory_weight

            + queue_weight

            + active_weight
        )

        return round(
            min(total / 4, 100),
            2,
        )

    # =====================================================
    # ADAPTIVE LIMITS
    # =====================================================

    async def adaptive_limits(
        self,
    ) -> AdaptiveLimits:

        pressure = await (
            self.collect_pressure()
        )

        # =============================================
        # HIGH PRESSURE
        # =============================================

        if (
            pressure.pressure_score
            >= 85
        ):

            limits = (
                AdaptiveLimits(

                    max_concurrency=1,

                    queue_limit=10,

                    request_delay_ms=
                    2000,

                    degraded_mode=True,
                )
            )

        # =============================================
        # MEDIUM PRESSURE
        # =============================================

        elif (
            pressure.pressure_score
            >= 65
        ):

            limits = (
                AdaptiveLimits(

                    max_concurrency=2,

                    queue_limit=20,

                    request_delay_ms=
                    1000,

                    degraded_mode=False,
                )
            )

        # =============================================
        # NORMAL
        # =============================================

        elif (
            pressure.pressure_score
            >= 40
        ):

            limits = (
                AdaptiveLimits(

                    max_concurrency=4,

                    queue_limit=40,

                    request_delay_ms=
                    300,

                    degraded_mode=False,
                )
            )

        # =============================================
        # LOW PRESSURE
        # =============================================

        else:

            limits = (
                AdaptiveLimits(

                    max_concurrency=6,

                    queue_limit=80,

                    request_delay_ms=0,

                    degraded_mode=False,
                )
            )

        self.current_limits = (
            limits
        )

        return limits

    # =====================================================
    # THROTTLE DELAY
    # =====================================================

    async def throttle_delay(
        self,
    ) -> None:

        limits = await (
            self.adaptive_limits()
        )

        delay_ms = (
            limits.request_delay_ms
        )

        if delay_ms > 0:

            await asyncio.sleep(
                delay_ms / 1000
            )

    # =====================================================
    # QUEUE ACCEPTANCE
    # =====================================================

    async def accept_task(
        self,
    ) -> bool:

        limits = await (
            self.adaptive_limits()
        )

        return (
            self.queue_size
            < limits.queue_limit
        )

    # =====================================================
    # TASK START
    # =====================================================

    async def task_started(
        self,
    ) -> None:

        self.active_tasks += 1

    # =====================================================
    # TASK COMPLETE
    # =====================================================

    async def task_completed(
        self,
    ) -> None:

        self.active_tasks = max(

            0,

            self.active_tasks - 1,
        )

    # =====================================================
    # QUEUE UPDATE
    # =====================================================

    async def update_queue_size(
        self,
        size: int,
    ) -> None:

        self.queue_size = max(
            0,
            size,
        )

    # =====================================================
    # BEST PROVIDER
    # =====================================================

    async def best_provider(
        self,
    ) -> str | None:

        insights = await (

            self.operational_memory
            .provider_insights()
        )

        providers = (
            insights.get(
                "providers",
                [],
            )
        )

        ranked = []

        for provider in providers:

            provider_name = (
                provider.get(
                    "provider"
                )
            )

            cooldown = not (

                self.recovery_engine
                .provider_available(
                    provider_name
                )
            )

            reliability = float(

                provider.get(
                    "reliability",
                    0,
                )
            )

            latency = float(

                provider.get(
                    "latency",
                    0,
                )
            )

            score = reliability

            if cooldown:

                score -= 1.0

            score -= (
                latency / 10000
            )

            ranked.append(

                ProviderLoad(

                    provider_name=
                    provider_name,

                    reliability=
                    reliability,

                    cooldown_active=
                    cooldown,

                    latency_ms=
                    latency,

                    score=score,
                )
            )

        if not ranked:

            return None

        ranked.sort(

            key=lambda item:
            item.score,

            reverse=True,
        )

        return (
            ranked[0]
            .provider_name
        )

    # =====================================================
    # PREDICTIVE COOLDOWN
    # =====================================================

    async def predictive_cooldown_avoidance(
        self,
    ) -> list[str]:

        insights = await (

            self.operational_memory
            .provider_insights()
        )

        risky = []

        for provider in (
            insights.get(
                "providers",
                [],
            )
        ):

            reliability = float(

                provider.get(
                    "reliability",
                    1,
                )
            )

            failures = int(

                provider.get(
                    "failures",
                    0,
                )
            )

            if (

                reliability < 0.45

                or failures >= 10
            ):

                risky.append(

                    provider[
                        "provider"
                    ]
                )

        return risky

    # =====================================================
    # WORKLOAD TREND
    # =====================================================

    async def workload_trend(
        self,
    ) -> dict:

        if (
            len(
                self.pressure_history
            )
            < 5
        ):

            return {

                "trend":
                "unknown",

                "average":
                0,
            }

        scores = [

            item.pressure_score

            for item
            in self.pressure_history
        ]

        average = round(
            statistics.mean(scores),
            2,
        )

        latest = scores[-1]

        if latest > average + 10:

            trend = "increasing"

        elif latest < average - 10:

            trend = "decreasing"

        else:

            trend = "stable"

        return {

            "trend":
            trend,

            "average":
            average,

            "latest":
            latest,
        }

    # =====================================================
    # AUTO OPTIMIZE
    # =====================================================

    async def auto_optimize(
        self,
    ) -> dict:

        pressure = await (
            self.collect_pressure()
        )

        limits = await (
            self.adaptive_limits()
        )

        risky = await (
            self
            .predictive_cooldown_avoidance()
        )

        trend = await (
            self.workload_trend()
        )

        optimized = {

            "pressure":
            pressure.pressure_score,

            "limits":
            {

                "concurrency":
                limits
                .max_concurrency,

                "queue_limit":
                limits
                .queue_limit,

                "delay_ms":
                limits
                .request_delay_ms,
            },

            "risky_providers":
            risky,

            "trend":
            trend,
        }

        logger.info(
            "Adaptive optimization applied=%s",
            optimized,
        )

        return optimized

    # =====================================================
    # HEALTH
    # =====================================================

    async def health(
        self,
    ) -> dict:

        pressure = await (
            self.collect_pressure()
        )

        trend = await (
            self.workload_trend()
        )

        return {

            "pressure_score":
            pressure.pressure_score,

            "cpu":
            pressure.cpu_percent,

            "memory":
            pressure.memory_percent,

            "queue":
            pressure.queue_size,

            "active_tasks":
            pressure.active_tasks,

            "limits":
            self.current_limits
            .__dict__,

            "trend":
            trend,
        }
