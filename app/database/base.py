from __future__ import annotations

import logging
import os

import aiosqlite

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

        await _database_connection.execute(
            "PRAGMA journal_mode=WAL;"
        )

        await _database_connection.execute(
            "PRAGMA foreign_keys=ON;"
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
