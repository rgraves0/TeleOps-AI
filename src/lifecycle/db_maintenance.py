from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sqlite3
import time
import traceback
from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RetentionRule:
    table_name: str
    timestamp_column: str
    retention_seconds: int
    batch_size: int = 500
    additional_where: str = ""


@dataclass(slots=True)
class MaintenanceStats:
    vacuum_runs: int = 0
    analyze_runs: int = 0
    purged_rows: int = 0
    last_vacuum_at: float = 0.0
    last_analyze_at: float = 0.0
    last_purge_at: float = 0.0
    wal_truncations: int = 0
    errors: int = 0
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class MaintenancePermissionError(
    Exception
):
    pass


class SQLiteMaintenanceController:
    """
    Async SQLite maintenance runtime.

    Features:
    - WAL monitoring
    - VACUUM orchestration
    - ANALYZE optimization
    - Retention-based purging
    - Async batch deletion
    - Default Deny RBAC
    """

    DEFAULT_VACUUM_INTERVAL = (
        86400
    )  # 24h

    DEFAULT_ANALYZE_INTERVAL = (
        43200
    )  # 12h

    DEFAULT_MONITOR_INTERVAL = 300

    DEFAULT_WAL_LIMIT_MB = 64

    def __init__(
        self,
        *,
        database_path: str,
        admin_roles: Set[str],
        admin_permissions: Set[str],
        wal_limit_mb: int = (
            DEFAULT_WAL_LIMIT_MB
        ),
        vacuum_interval: int = (
            DEFAULT_VACUUM_INTERVAL
        ),
        analyze_interval: int = (
            DEFAULT_ANALYZE_INTERVAL
        ),
        monitor_interval: int = (
            DEFAULT_MONITOR_INTERVAL
        ),
    ) -> None:

        self.database_path = Path(
            database_path
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.admin_roles = (
            admin_roles
        )

        self.admin_permissions = (
            admin_permissions
        )

        self.wal_limit_bytes = (
            wal_limit_mb
            * 1024
            * 1024
        )

        self.vacuum_interval = (
            vacuum_interval
        )

        self.analyze_interval = (
            analyze_interval
        )

        self.monitor_interval = (
            monitor_interval
        )

        self.retention_rules: List[
            RetentionRule
        ] = []

        self.stats = (
            MaintenanceStats()
        )

        self._running = False

        self._connection: Optional[
            sqlite3.Connection
        ] = None

        self._maintenance_task: Optional[
            asyncio.Task
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
            self._configure_sqlite
        )

    def _configure_sqlite(
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
            "PRAGMA cache_size=-1500;"
        )

        self._connection.execute(
            "PRAGMA busy_timeout=5000;"
        )

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting SQLiteMaintenanceController"
        )

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
            "Stopping SQLiteMaintenanceController"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def validate_admin(
        self,
        *,
        permissions: Set[str],
        roles: Set[str],
    ) -> bool:

        if (
            "system.db.maintenance"
            not in permissions
        ):
            return False

        if (
            "superuser"
            not in roles
            and "admin"
            not in roles
        ):
            return False

        return True

    async def register_retention_rule(
        self,
        *,
        rule: RetentionRule,
        permissions: Set[str],
        roles: Set[str],
    ) -> None:

        authorized = (
            await self.validate_admin(
                permissions=
                    permissions,
                roles=roles,
            )
        )

        if not authorized:
            raise MaintenancePermissionError(
                "Retention rule registration denied"
            )

        self.retention_rules.append(
            rule
        )

    async def trigger_vacuum(
        self,
        *,
        permissions: Set[str],
        roles: Set[str],
    ) -> None:

        authorized = (
            await self.validate_admin(
                permissions=
                    permissions,
                roles=roles,
            )
        )

        if not authorized:
            raise MaintenancePermissionError(
                "VACUUM execution denied"
            )

        await asyncio.to_thread(
            self._vacuum_sync
        )

    def _vacuum_sync(
        self,
    ) -> None:

        logger.info(
            "Running SQLite VACUUM"
        )

        self._connection.execute(
            "VACUUM;"
        )

        self.stats.vacuum_runs += 1

        self.stats.last_vacuum_at = (
            time.time()
        )

    async def trigger_analyze(
        self,
        *,
        permissions: Set[str],
        roles: Set[str],
    ) -> None:

        authorized = (
            await self.validate_admin(
                permissions=
                    permissions,
                roles=roles,
            )
        )

        if not authorized:
            raise MaintenancePermissionError(
                "ANALYZE execution denied"
            )

        await asyncio.to_thread(
            self._analyze_sync
        )

    def _analyze_sync(
        self,
    ) -> None:

        logger.info(
            "Running SQLite ANALYZE"
        )

        self._connection.execute(
            "ANALYZE;"
        )

        self.stats.analyze_runs += 1

        self.stats.last_analyze_at = (
            time.time()
        )

    async def purge_expired_data(
        self,
    ) -> None:

        now = int(
            time.time()
        )

        for rule in (
            self.retention_rules
        ):
            try:
                cutoff = (
                    now
                    - rule.retention_seconds
                )

                deleted = (
                    await asyncio.to_thread(
                        self._purge_rule_sync,
                        rule,
                        cutoff,
                    )
                )

                self.stats.purged_rows += (
                    deleted
                )

                self.stats.last_purge_at = (
                    time.time()
                )

            except Exception:
                self.stats.errors += 1

                logger.error(
                    traceback.format_exc()
                )

    def _purge_rule_sync(
        self,
        rule: RetentionRule,
        cutoff: int,
    ) -> int:

        total_deleted = 0

        while True:

            sql = f"""
            DELETE FROM {rule.table_name}
            WHERE {rule.timestamp_column} < ?
            """

            if (
                rule.additional_where
            ):
                sql += (
                    f" AND {rule.additional_where}"
                )

            sql += (
                f" LIMIT {rule.batch_size}"
            )

            cursor = (
                self._connection.execute(
                    sql,
                    (cutoff,),
                )
            )

            deleted = (
                cursor.rowcount
            )

            if deleted <= 0:
                break

            total_deleted += deleted

        return total_deleted

    async def wal_size_bytes(
        self,
    ) -> int:

        wal_path = Path(
            f"{self.database_path}-wal"
        )

        if not wal_path.exists():
            return 0

        return wal_path.stat().st_size

    async def checkpoint_wal(
        self,
    ) -> None:

        await asyncio.to_thread(
            self._checkpoint_sync
        )

    def _checkpoint_sync(
        self,
    ) -> None:

        self._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

        self.stats.wal_truncations += 1

    async def _maintenance_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await self._run_maintenance_cycle()

                await asyncio.sleep(
                    self.monitor_interval
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                self.stats.errors += 1

                logger.error(
                    traceback.format_exc()
                )

    async def _run_maintenance_cycle(
        self,
    ) -> None:

        now = time.time()

        wal_size = (
            await self.wal_size_bytes()
        )

        if (
            wal_size
            >= self.wal_limit_bytes
        ):
            logger.warning(
                "WAL size threshold exceeded"
            )

            await self.checkpoint_wal()

        if (
            now
            - self.stats.last_vacuum_at
            >= self.vacuum_interval
        ):
            await asyncio.to_thread(
                self._vacuum_sync
            )

        if (
            now
            - self.stats.last_analyze_at
            >= self.analyze_interval
        ):
            await asyncio.to_thread(
                self._analyze_sync
            )

        await self.purge_expired_data()

    async def runtime_state(
        self,
    ) -> Dict[str, Any]:

        wal_size = (
            await self.wal_size_bytes()
        )

        return {
            "running":
                self._running,
            "database":
                str(
                    self.database_path
                ),
            "wal_size_mb":
                round(
                    wal_size
                    / 1024
                    / 1024,
                    2,
                ),
            "retention_rules":
                len(
                    self.retention_rules
                ),
            "vacuum_runs":
                self.stats.vacuum_runs,
            "analyze_runs":
                self.stats.analyze_runs,
            "purged_rows":
                self.stats.purged_rows,
            "errors":
                self.stats.errors,
            "timestamp":
                time.time(),
        }

    async def healthcheck(
        self,
    ) -> Dict[str, Any]:

        try:
            await asyncio.to_thread(
                self._connection.execute,
                "SELECT 1;",
            )

            return {
                "healthy": True,
                "database":
                    str(
                        self.database_path
                    ),
                "timestamp":
                    time.time(),
            }

        except Exception as exc:
            return {
                "healthy": False,
                "error":
                    str(exc),
                "timestamp":
                    time.time(),
            }


DEFAULT_DB_MAINTENANCE = (
    SQLiteMaintenanceController
)
