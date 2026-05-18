from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import mean
from typing import Any

from src.memory.models import (
    BaseMemoryModel,
    MemoryType,
)
from src.memory.store import (
    MemoryStore,
)

logger = logging.getLogger(__name__)


# =========================================================
# PROVIDER FAILURE PROFILE
# =========================================================


@dataclass
class ProviderFailureProfile:

    provider_name: str

    total_requests: int = 0

    successful_requests: int = 0

    failed_requests: int = 0

    timeout_failures: int = 0

    cooldown_events: int = 0

    average_latency_ms: float = 0.0

    reliability_score: float = 1.0

    last_failure_at: (
        str | None
    ) = None


# =========================================================
# WORKFLOW OPTIMIZATION PROFILE
# =========================================================


@dataclass
class WorkflowOptimizationProfile:

    workflow_name: str

    total_runs: int = 0

    successful_runs: int = 0

    failed_runs: int = 0

    average_runtime_ms: float = 0.0

    optimization_score: float = 1.0

    last_failure_reason: (
        str | None
    ) = None


# =========================================================
# OPERATIONAL MEMORY
# =========================================================


class OperationalMemory:

    def __init__(
        self,
        store: MemoryStore,
    ) -> None:

        self.store = store

        self.provider_profiles: dict[
            str,
            ProviderFailureProfile
        ] = {}

        self.workflow_profiles: dict[
            str,
            WorkflowOptimizationProfile
        ] = {}

        logger.info(
            "OperationalMemory initialized"
        )

    # =====================================================
    # RECORD PROVIDER SUCCESS
    # =====================================================

    async def record_provider_success(
        self,
        provider_name: str,
        latency_ms: float,
    ) -> None:

        profile = (
            self._provider_profile(
                provider_name
            )
        )

        profile.total_requests += 1

        profile.successful_requests += 1

        profile.average_latency_ms = (
            self._rolling_average(

                current=
                profile.average_latency_ms,

                new_value=
                latency_ms,
            )
        )

        profile.reliability_score = (
            self._provider_score(
                profile
            )
        )

        await self._store_provider_event(

            provider_name=
            provider_name,

            event_type=
            "success",

            metadata={

                "latency_ms":
                latency_ms,

                "reliability":
                profile
                .reliability_score,
            },
        )

    # =====================================================
    # RECORD PROVIDER FAILURE
    # =====================================================

    async def record_provider_failure(
        self,
        provider_name: str,
        reason: str,
        timeout: bool = False,
    ) -> None:

        profile = (
            self._provider_profile(
                provider_name
            )
        )

        profile.total_requests += 1

        profile.failed_requests += 1

        profile.last_failure_at = (
            datetime.utcnow()
            .isoformat()
        )

        if timeout:

            profile.timeout_failures += 1

        profile.reliability_score = (
            self._provider_score(
                profile
            )
        )

        await self._store_provider_event(

            provider_name=
            provider_name,

            event_type=
            "failure",

            metadata={

                "reason":
                reason,

                "timeout":
                timeout,

                "reliability":
                profile
                .reliability_score,
            },
        )

    # =====================================================
    # RECORD COOLDOWN
    # =====================================================

    async def record_cooldown(
        self,
        provider_name: str,
        reason: str,
    ) -> None:

        profile = (
            self._provider_profile(
                provider_name
            )
        )

        profile.cooldown_events += 1

        profile.reliability_score = (
            self._provider_score(
                profile
            )
        )

        await self._store_provider_event(

            provider_name=
            provider_name,

            event_type=
            "cooldown",

            metadata={

                "reason":
                reason,

                "cooldowns":
                profile
                .cooldown_events,
            },
        )

    # =====================================================
    # RECORD WORKFLOW EXECUTION
    # =====================================================

    async def record_workflow_execution(
        self,
        workflow_name: str,
        runtime_ms: float,
        success: bool,
        failure_reason: (
            str | None
        ) = None,
    ) -> None:

        profile = (
            self._workflow_profile(
                workflow_name
            )
        )

        profile.total_runs += 1

        profile.average_runtime_ms = (
            self._rolling_average(

                current=
                profile.average_runtime_ms,

                new_value=
                runtime_ms,
            )
        )

        if success:

            profile.successful_runs += 1

        else:

            profile.failed_runs += 1

            profile.last_failure_reason = (
                failure_reason
            )

        profile.optimization_score = (
            self._workflow_score(
                profile
            )
        )

        await self._store_workflow_event(

            workflow_name=
            workflow_name,

            success=
            success,

            runtime_ms=
            runtime_ms,

            failure_reason=
            failure_reason,
        )

    # =====================================================
    # PROVIDER PROFILE
    # =====================================================

    def _provider_profile(
        self,
        provider_name: str,
    ) -> ProviderFailureProfile:

        if (
            provider_name
            not in
            self.provider_profiles
        ):

            self.provider_profiles[
                provider_name
            ] = (
                ProviderFailureProfile(
                    provider_name=
                    provider_name
                )
            )

        return (
            self.provider_profiles[
                provider_name
            ]
        )

    # =====================================================
    # WORKFLOW PROFILE
    # =====================================================

    def _workflow_profile(
        self,
        workflow_name: str,
    ) -> WorkflowOptimizationProfile:

        if (
            workflow_name
            not in
            self.workflow_profiles
        ):

            self.workflow_profiles[
                workflow_name
            ] = (
                WorkflowOptimizationProfile(
                    workflow_name=
                    workflow_name
                )
            )

        return (
            self.workflow_profiles[
                workflow_name
            ]
        )

    # =====================================================
    # PROVIDER SCORE
    # =====================================================

    def _provider_score(
        self,
        profile: (
            ProviderFailureProfile
        ),
    ) -> float:

        if (
            profile.total_requests
            == 0
        ):

            return 1.0

        success_ratio = (

            profile.successful_requests

            / profile.total_requests
        )

        timeout_penalty = (
            profile.timeout_failures
            * 0.03
        )

        cooldown_penalty = (
            profile.cooldown_events
            * 0.02
        )

        score = (

            success_ratio

            - timeout_penalty

            - cooldown_penalty
        )

        return round(
            max(score, 0.0),
            3,
        )

    # =====================================================
    # WORKFLOW SCORE
    # =====================================================

    def _workflow_score(
        self,
        profile: (
            WorkflowOptimizationProfile
        ),
    ) -> float:

        if (
            profile.total_runs
            == 0
        ):

            return 1.0

        success_ratio = (

            profile.successful_runs

            / profile.total_runs
        )

        runtime_penalty = min(

            profile.average_runtime_ms
            / 100000,

            0.25,
        )

        score = (
            success_ratio
            - runtime_penalty
        )

        return round(
            max(score, 0.0),
            3,
        )

    # =====================================================
    # ROLLING AVERAGE
    # =====================================================

    def _rolling_average(
        self,
        current: float,
        new_value: float,
    ) -> float:

        if current == 0:

            return round(
                new_value,
                2,
            )

        return round(

            (
                current
                + new_value
            )
            / 2,

            2,
        )

    # =====================================================
    # STORE PROVIDER EVENT
    # =====================================================

    async def _store_provider_event(
        self,
        provider_name: str,
        event_type: str,
        metadata: dict[
            str,
            Any
        ],
    ) -> None:

        content = (

            f"Provider={provider_name} "

            f"Event={event_type} "

            f"Metadata={json.dumps(metadata)}"
        )

        memory = (
            BaseMemoryModel(

                memory_type=
                MemoryType.OPERATIONAL,

                content=content,

                tags=[

                    "provider",
                    provider_name,
                    event_type,
                ],

                metadata=metadata,

                importance_score=0.8,
            )
        )

        await (
            self.store.store_memory(
                memory
            )
        )

    # =====================================================
    # STORE WORKFLOW EVENT
    # =====================================================

    async def _store_workflow_event(
        self,
        workflow_name: str,
        success: bool,
        runtime_ms: float,
        failure_reason: (
            str | None
        ),
    ) -> None:

        metadata = {

            "workflow":
            workflow_name,

            "success":
            success,

            "runtime_ms":
            runtime_ms,

            "failure_reason":
            failure_reason,
        }

        content = (

            f"Workflow={workflow_name} "

            f"Success={success} "

            f"Runtime={runtime_ms}"
        )

        memory = (
            BaseMemoryModel(

                memory_type=
                MemoryType.OPERATIONAL,

                content=content,

                tags=[

                    "workflow",
                    workflow_name,
                ],

                metadata=metadata,

                importance_score=0.7,
            )
        )

        await (
            self.store.store_memory(
                memory
            )
        )

    # =====================================================
    # BEST PROVIDER
    # =====================================================

    async def best_provider(
        self,
    ) -> str | None:

        if (
            not self.provider_profiles
        ):

            return None

        ranked = sorted(

            self.provider_profiles.values(),

            key=lambda item:
            item.reliability_score,

            reverse=True,
        )

        return (
            ranked[0]
            .provider_name
        )

    # =====================================================
    # WORKFLOW INSIGHTS
    # =====================================================

    async def workflow_insights(
        self,
    ) -> dict:

        top = sorted(

            self.workflow_profiles
            .values(),

            key=lambda item:
            item.optimization_score,

            reverse=True,
        )

        return {

            "workflow_count":
            len(
                self.workflow_profiles
            ),

            "top_workflows":

            [

                {

                    "workflow":
                    item.workflow_name,

                    "score":
                    item.optimization_score,

                    "runtime":
                    item.average_runtime_ms,
                }

                for item
                in top[:5]
            ],
        }

    # =====================================================
    # PROVIDER INSIGHTS
    # =====================================================

    async def provider_insights(
        self,
    ) -> dict:

        ranked = sorted(

            self.provider_profiles
            .values(),

            key=lambda item:
            item.reliability_score,

            reverse=True,
        )

        return {

            "providers":

            [

                {

                    "provider":
                    item.provider_name,

                    "reliability":
                    item.reliability_score,

                    "failures":
                    item.failed_requests,

                    "cooldowns":
                    item.cooldown_events,
                }

                for item
                in ranked
            ]
        }

    # =====================================================
    # LOW VALUE CLEANUP
    # =====================================================

    async def cleanup_low_value_memories(
        self,
        threshold: float = 0.2,
    ) -> int:

        rows = await (
            self.store.db.fetch_all(

                """

                SELECT memory_id

                FROM memory_store

                WHERE importance_score <= ?

                LIMIT 100

                """,

                (threshold,),
            )
        )

        deleted = 0

        for row in rows:

            success = await (
                self.store.delete_memory(
                    row[
                        "memory_id"
                    ]
                )
            )

            if success:

                deleted += 1

        if deleted > 0:

            logger.info(
                "Low value memories removed=%s",
                deleted,
            )

        return deleted
