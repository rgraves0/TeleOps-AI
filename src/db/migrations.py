from __future__ import annotations

import logging
from dataclasses import dataclass

from src.db.database import (
    DatabaseManager,
)
from src.db.models import (
    SCHEMA_DEFINITIONS,
)

logger = logging.getLogger(__name__)


# =========================================================
# MIGRATION
# =========================================================


@dataclass
class Migration:

    version: int

    name: str

    sql: str


# =========================================================
# MIGRATIONS
# =========================================================


MIGRATIONS = [

    Migration(

        version=1,

        name="create_workflows",

        sql=SCHEMA_DEFINITIONS[
            "workflows"
        ],
    ),

    Migration(

        version=2,

        name="create_workflow_runs",

        sql=SCHEMA_DEFINITIONS[
            "workflow_runs"
        ],
    ),

    Migration(

        version=3,

        name="create_tasks",

        sql=SCHEMA_DEFINITIONS[
            "tasks"
        ],
    ),

    Migration(

        version=4,

        name="create_provider_states",

        sql=SCHEMA_DEFINITIONS[
            "provider_states"
        ],
    ),

    Migration(

        version=5,

        name="create_audit_logs",

        sql=SCHEMA_DEFINITIONS[
            "audit_logs"
        ],
    ),

    Migration(

        version=6,

        name="create_memories",

        sql=SCHEMA_DEFINITIONS[
            "memories"
        ],
    ),
]


# =========================================================
# MIGRATION MANAGER
# =========================================================


class MigrationManager:

    def __init__(
        self,
        db: DatabaseManager,
    ) -> None:

        self.db = db

    # =====================================================
    # INIT TABLE
    # =====================================================

    async def initialize(
        self,
    ) -> None:

        await self.db.execute(

            """

            CREATE TABLE IF NOT EXISTS schema_migrations (

                version INTEGER PRIMARY KEY,

                name TEXT NOT NULL,

                applied_at TEXT DEFAULT CURRENT_TIMESTAMP

            )

            """
        )

        logger.info(
            "Migration table initialized"
        )

    # =====================================================
    # APPLIED VERSIONS
    # =====================================================

    async def applied_versions(
        self,
    ) -> set[int]:

        rows = await (
            self.db.fetch_all(

                """

                SELECT version
                FROM schema_migrations

                """
            )
        )

        return {

            row["version"]
            for row in rows
        }

    # =====================================================
    # RUN MIGRATIONS
    # =====================================================

    async def migrate(
        self,
    ) -> None:

        await self.initialize()

        applied = await (
            self.applied_versions()
        )

        for migration in (
            MIGRATIONS
        ):

            if (
                migration.version
                in applied
            ):

                continue

            logger.info(
                "Applying migration=%s",
                migration.name,
            )

            try:

                async with (
                    self.db.transaction()
                ):

                    await self.db.execute(
                        migration.sql
                    )

                    await self.db.execute(

                        """

                        INSERT INTO
                        schema_migrations (

                            version,
                            name

                        )

                        VALUES (?, ?)

                        """,

                        (

                            migration.version,

                            migration.name,
                        ),
                    )

                logger.info(
                    "Migration applied=%s",
                    migration.name,
                )

            except Exception:

                logger.exception(
                    "Migration failed=%s",
                    migration.name,
                )

                raise

    # =====================================================
    # CURRENT VERSION
    # =====================================================

    async def current_version(
        self,
    ) -> int:

        row = await (
            self.db.fetch_one(

                """

                SELECT MAX(version)
                as version

                FROM schema_migrations

                """
            )
        )

        if not row:
            return 0

        return row.get(
            "version",
            0,
        ) or 0
