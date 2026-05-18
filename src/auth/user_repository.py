from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)


@dataclass
class User:

    id: int | None

    telegram_id: int

    username: str | None

    display_name: str | None

    role: str

    is_active: bool

    created_at: str

    updated_at: str


class UserRepository:

    def __init__(
        self,
        database_path: str,
    ) -> None:

        self.database_path = (
            database_path
        )

    # =====================================================
    # DATABASE
    # =====================================================

    async def initialize(
        self,
    ) -> None:

        async with aiosqlite.connect(
            self.database_path
        ) as db:

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    display_name TEXT,
                    role TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            await db.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_users_telegram_id
                ON users(telegram_id)
                """
            )

            await db.commit()

            logger.info(
                "UserRepository initialized"
            )

    # =====================================================
    # CREATE
    # =====================================================

    async def create_user(
        self,
        telegram_id: int,
        username: str | None,
        display_name: str | None,
        role: str = "user",
    ) -> User:

        now = (
            datetime.utcnow()
            .isoformat()
        )

        async with aiosqlite.connect(
            self.database_path
        ) as db:

            cursor = await db.execute(
                """
                INSERT INTO users (
                    telegram_id,
                    username,
                    display_name,
                    role,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    username,
                    display_name,
                    role,
                    1,
                    now,
                    now,
                ),
            )

            await db.commit()

            user_id = (
                cursor.lastrowid
            )

            logger.info(
                "Created user telegram_id=%s",
                telegram_id,
            )

            return User(
                id=user_id,
                telegram_id=telegram_id,
                username=username,
                display_name=display_name,
                role=role,
                is_active=True,
                created_at=now,
                updated_at=now,
            )

    # =====================================================
    # READ
    # =====================================================

    async def get_user_by_id(
        self,
        user_id: int,
    ) -> User | None:

        async with aiosqlite.connect(
            self.database_path
        ) as db:

            cursor = await db.execute(
                """
                SELECT
                    id,
                    telegram_id,
                    username,
                    display_name,
                    role,
                    is_active,
                    created_at,
                    updated_at
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            )

            row = await cursor.fetchone()

            if row is None:
                return None

            return self._row_to_user(
                row
            )

    async def get_user_by_telegram_id(
        self,
        telegram_id: int,
    ) -> User | None:

        async with aiosqlite.connect(
            self.database_path
        ) as db:

            cursor = await db.execute(
                """
                SELECT
                    id,
                    telegram_id,
                    username,
                    display_name,
                    role,
                    is_active,
                    created_at,
                    updated_at
                FROM users
                WHERE telegram_id = ?
                """,
                (telegram_id,),
            )

            row = await cursor.fetchone()

            if row is None:
                return None

            return self._row_to_user(
                row
            )

    async def list_users(
        self,
    ) -> list[User]:

        async with aiosqlite.connect(
            self.database_path
        ) as db:

            cursor = await db.execute(
                """
                SELECT
                    id,
                    telegram_id,
                    username,
                    display_name,
                    role,
                    is_active,
                    created_at,
                    updated_at
                FROM users
                ORDER BY created_at ASC
                """
            )

            rows = await cursor.fetchall()

            return [
                self._row_to_user(row)
                for row in rows
            ]

    # =====================================================
    # UPDATE
    # =====================================================

    async def update_role(
        self,
        telegram_id: int,
        role: str,
    ) -> None:

        now = (
            datetime.utcnow()
            .isoformat()
        )

        async with aiosqlite.connect(
            self.database_path
        ) as db:

            await db.execute(
                """
                UPDATE users
                SET role = ?,
                    updated_at = ?
                WHERE telegram_id = ?
                """,
                (
                    role,
                    now,
                    telegram_id,
                ),
            )

            await db.commit()

            logger.info(
                "Updated role telegram_id=%s role=%s",
                telegram_id,
                role,
            )

    async def set_active_status(
        self,
        telegram_id: int,
        is_active: bool,
    ) -> None:

        now = (
            datetime.utcnow()
            .isoformat()
        )

        async with aiosqlite.connect(
            self.database_path
        ) as db:

            await db.execute(
                """
                UPDATE users
                SET is_active = ?,
                    updated_at = ?
                WHERE telegram_id = ?
                """,
                (
                    int(is_active),
                    now,
                    telegram_id,
                ),
            )

            await db.commit()

    # =====================================================
    # DELETE
    # =====================================================

    async def delete_user(
        self,
        telegram_id: int,
    ) -> None:

        async with aiosqlite.connect(
            self.database_path
        ) as db:

            await db.execute(
                """
                DELETE FROM users
                WHERE telegram_id = ?
                """,
                (telegram_id,),
            )

            await db.commit()

            logger.warning(
                "Deleted user telegram_id=%s",
                telegram_id,
            )

    # =====================================================
    # INTERNAL
    # =====================================================

    def _row_to_user(
        self,
        row: tuple[Any, ...],
    ) -> User:

        return User(
            id=row[0],
            telegram_id=row[1],
            username=row[2],
            display_name=row[3],
            role=row[4],
            is_active=bool(row[5]),
            created_at=row[6],
            updated_at=row[7],
        )
