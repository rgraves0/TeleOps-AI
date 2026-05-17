from __future__ import annotations

import json
import logging
import os

import aiosqlite

from app.database.models import (
    CREATE_USERS_TABLE,
    CREATE_ROLES_TABLE,
    CREATE_INBOXES_TABLE,
    CREATE_REMINDERS_TABLE,
    CREATE_AUDIT_LOGS_TABLE,
    CREATE_USER_INBOXES_TABLE,
    CREATE_INDEXES,
    DEFAULT_ROLES,
)

logger = logging.getLogger(__name__)

DATABASE_PATH = os.getenv(
    "DATABASE_PATH",
    "data/teleops.db"
)

_database_connection = None


async def init_db() -> None:
    global _database_connection

    database_directory = os.path.dirname(
        DATABASE_PATH
    )

    if database_directory:
        os.makedirs(
            database_directory,
            exist_ok=True
        )

    if _database_connection is None:
        _database_connection = (
            await aiosqlite.connect(
                DATABASE_PATH
            )
        )

        _database_connection.row_factory = (
            aiosqlite.Row
        )

        # =====================================================
        # SQLITE SETTINGS
        # =====================================================

        await _database_connection.execute(
            "PRAGMA journal_mode=WAL;"
        )

        await _database_connection.execute(
            "PRAGMA foreign_keys=ON;"
        )

        # =====================================================
        # CREATE CORE TABLES
        # =====================================================

        await _database_connection.execute(
            CREATE_ROLES_TABLE
        )

        await _database_connection.execute(
            CREATE_USERS_TABLE
        )

        await _database_connection.execute(
            CREATE_INBOXES_TABLE
        )

        await _database_connection.execute(
            CREATE_REMINDERS_TABLE
        )

        await _database_connection.execute(
            CREATE_AUDIT_LOGS_TABLE
        )

        await _database_connection.execute(
            CREATE_USER_INBOXES_TABLE
        )

        # =====================================================
        # CREATE INDEXES
        # =====================================================

        for index_query in CREATE_INDEXES:
            await _database_connection.execute(
                index_query
            )

        # =====================================================
        # INSERT DEFAULT ROLES
        # =====================================================

        for role in DEFAULT_ROLES:
            await _database_connection.execute(
                """
                INSERT OR IGNORE INTO roles (
                    name,
                    description,
                    permissions
                )
                VALUES (?, ?, ?)
                """,
                (
                    role["name"],
                    role["description"],
                    json.dumps(
                        role["permissions"]
                    )
                )
            )

        await _database_connection.commit()

        logger.info(
            "Database initialized "
            "path=%s",
            DATABASE_PATH
        )


async def get_db():
    global _database_connection

    if _database_connection is None:
        await init_db()

    return _database_connection


async def get_database_connection():
    return await get_db()


async def close_database() -> None:
    global _database_connection

    if _database_connection is not None:
        await _database_connection.close()

        _database_connection = None

        logger.info(
            "Database connection closed"
        )
