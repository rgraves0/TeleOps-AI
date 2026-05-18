from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime
from datetime import timedelta
from typing import Any

from src.db.database import (
    DatabaseManager,
)
from src.memory.operational_memory import (
    OperationalMemory,
)
from src.memory.store import (
    MemoryStore,
)
from src.monitoring.metrics import (
    MetricsCollector,
)
from src.scheduler.autonomous_scheduler import (
    AutonomousScheduler,
)

logger = logging.getLogger(__name__)


# =========================================================
# MAINTENANCE JOB RESULT
# =========================================================


class MaintenanceResult(dict):

    pass


# =========================================================
# MAINTENANCE JOBS
# =========================================================


class MaintenanceJobs:

    def __init__(
        self,
        db: DatabaseManager,
        memory_store: MemoryStore,
        operational_memory: (
            OperationalMemory
        ),
        scheduler: (
            AutonomousScheduler
        ),
        metrics: (
            MetricsCollector
            | None
        ) = None,
    ) -> None:

        self.db = db

        self.memory_store = (
            memory_store
        )

        self.operational_memory = (
            operational_memory
        )

        self.scheduler = scheduler

        self.metrics = metrics

        logger.info(
            "MaintenanceJobs initialized"
        )

    # =====================================================
    # REGISTER JOBS
    # =====================================================

    async def register_jobs(
        self,
    ) -> None:

        self.scheduler.register_handler(

            "memory_cleanup",

            self.memory_cleanup_job,
        )

        self.scheduler.register_handler(

            "ttl_cleanup",

            self.ttl_cleanup_job,
        )

        self.scheduler.register_handler(

            "provider_health_refresh",

            self.provider_health_refresh_job,
        )

        self.scheduler.register_handler(

            "database_optimize",

            self.database_optimization_job,
        )

        self.scheduler.register_handler(

            "database_compact",

            self.database_compaction_job,
        )

        self.scheduler.register_handler(

            "workflow_cleanup",

            self.workflow_cleanup_job,
        )

        logger.info(
            "Maintenance jobs registered"
        )

    # =====================================================
    # MEMORY CLEANUP
    # =====================================================

    async def memory_cleanup_job(
        self,
        *_,
        **__,
    ) -> MaintenanceResult:

        started = datetime.utcnow()

        removed = await (

            self.operational_memory
            .cleanup_low_value_memories(
                threshold=0.2
            )
        )

        duration = (
            datetime.utcnow()
            - started
        ).total_seconds()

        logger.info(
            "Memory cleanup completed removed=%s",
            removed,
        )

        return MaintenanceResult(

            success=True,

            removed=removed,

            duration=duration,
        )

    # =====================================================
    # TTL CLEANUP
    # =====================================================

    async def ttl_cleanup_job(
        self,
        *_,
        **__,
    ) -> MaintenanceResult:

        now = (
            datetime.utcnow()
            .isoformat()
        )

        deleted = 0

        try:

            rows = await (
                self.db.fetch_all(

                    """

                    SELECT memory_id

                    FROM memory_store

                    WHERE expires_at IS NOT NULL
                    AND expires_at <= ?

                    LIMIT 500

                    """,

                    (now,),
                )
            )

            for row in rows:

                success = await (
                    self.memory_store
                    .delete_memory(

                        row[
                            "memory_id"
                        ]
                    )
                )

                if success:

                    deleted += 1

            logger.info(
                "TTL cleanup completed deleted=%s",
                deleted,
            )

            return MaintenanceResult(

                success=True,

                deleted=deleted,
            )

        except Exception as exc:

            logger.exception(
                "TTL cleanup failed"
            )

            return MaintenanceResult(

                success=False,

                error=str(exc),
            )

    # =====================================================
    # PROVIDER HEALTH REFRESH
    # =====================================================

    async def provider_health_refresh_job(
        self,
        *_,
        **__,
    ) -> MaintenanceResult:

        try:

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

            unhealthy = []

            for provider in providers:

                reliability = (
                    provider.get(
                        "reliability",
                        1.0,
                    )
                )

                if reliability < 0.5:

                    unhealthy.append(
                        provider[
                            "provider"
                        ]
                    )

            logger.info(
                "Provider refresh completed unhealthy=%s",
                len(unhealthy),
            )

            return MaintenanceResult(

                success=True,

                unhealthy=
                unhealthy,
            )

        except Exception as exc:

            logger.exception(
                "Provider refresh failed"
            )

            return MaintenanceResult(

                success=False,

                error=str(exc),
            )

    # =====================================================
    # DATABASE OPTIMIZATION
    # =====================================================

    async def database_optimization_job(
        self,
        *_,
        **__,
    ) -> MaintenanceResult:

        try:

            await self.db.execute(
                "PRAGMA optimize"
            )

            await self.db.execute(
                "ANALYZE"
            )

            logger.info(
                "Database optimization completed"
            )

            return MaintenanceResult(
                success=True
            )

        except Exception as exc:

            logger.exception(
                "Database optimization failed"
            )

            return MaintenanceResult(

                success=False,

                error=str(exc),
            )

    # =====================================================
    # DATABASE COMPACTION
    # =====================================================

    async def database_compaction_job(
        self,
        *_,
        **__,
    ) -> MaintenanceResult:

        try:

            await self.db.execute(
                "VACUUM"
            )

            logger.info(
                "Database compaction completed"
            )

            return MaintenanceResult(
                success=True
            )

        except Exception as exc:

            logger.exception(
                "Database compaction failed"
            )

            return MaintenanceResult(

                success=False,

                error=str(exc),
            )

    # =====================================================
    # WORKFLOW CLEANUP
    # =====================================================

    async def workflow_cleanup_job(
        self,
        *_,
        **__,
    ) -> MaintenanceResult:

        try:

            cutoff = (

                datetime.utcnow()

                - timedelta(
                    days=7
                )

            ).isoformat()

            await self.db.execute(

                """

                DELETE FROM workflow_history

                WHERE created_at <= ?

                AND status IN (
                    'completed',
                    'failed'
                )

                """,

                (cutoff,),
            )

            logger.info(
                "Workflow cleanup completed"
            )

            return MaintenanceResult(
                success=True
            )

        except Exception as exc:

            logger.exception(
                "Workflow cleanup failed"
            )

            return MaintenanceResult(

                success=False,

                error=str(exc),
            )

    # =====================================================
    # RESOURCE PRESSURE CHECK
    # =====================================================

    async def resource_pressure_job(
        self,
    ) -> MaintenanceResult:

        if not self.metrics:

            return MaintenanceResult(
                success=False,
                reason="metrics disabled",
            )

        try:

            stats = (
                await self.metrics
                .snapshot()
            )

            ram = stats.get(
                "memory_percent",
                0,
            )

            cpu = stats.get(
                "cpu_percent",
                0,
            )

            pressure = (

                ram >= 85

                or cpu >= 90
            )

            if pressure:

                logger.warning(
                    "Resource pressure detected cpu=%s ram=%s",
                    cpu,
                    ram,
                )

            return MaintenanceResult(

                success=True,

                pressure=pressure,

                cpu=cpu,

                ram=ram,
            )

        except Exception as exc:

            logger.exception(
                "Resource pressure check failed"
            )

            return MaintenanceResult(

                success=False,

                error=str(exc),
            )

    # =====================================================
    # STALE TASK CLEANUP
    # =====================================================

    async def stale_task_cleanup_job(
        self,
    ) -> MaintenanceResult:

        try:

            cutoff = (

                datetime.utcnow()

                - timedelta(
                    hours=6
                )

            ).isoformat()

            await self.db.execute(

                """

                UPDATE scheduled_tasks

                SET status = 'failed'

                WHERE status = 'running'
                AND updated_at <= ?

                """,

                (cutoff,),
            )

            logger.info(
                "Stale tasks cleanup completed"
            )

            return MaintenanceResult(
                success=True
            )

        except Exception as exc:

            logger.exception(
                "Stale cleanup failed"
            )

            return MaintenanceResult(

                success=False,

                error=str(exc),
            )

    # =====================================================
    # RUN ALL MAINTENANCE
    # =====================================================

    async def run_all(
        self,
    ) -> dict:

        results = {}

        jobs = [

            (
                "memory_cleanup",
                self.memory_cleanup_job,
            ),

            (
                "ttl_cleanup",
                self.ttl_cleanup_job,
            ),

            (
                "provider_refresh",
                self.provider_health_refresh_job,
            ),

            (
                "database_optimize",
                self.database_optimization_job,
            ),

            (
                "workflow_cleanup",
                self.workflow_cleanup_job,
            ),
        ]

        for name, job in jobs:

            try:

                results[name] = (
                    await job()
                )

            except Exception as exc:

                logger.exception(
                    "Maintenance job failed=%s",
                    name,
                )

                results[name] = {

                    "success":
                    False,

                    "error":
                    str(exc),
                }

            await asyncio.sleep(
                0.2
            )

        return results
