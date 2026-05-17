from __future__ import annotations

import logging
from typing import Any

import aiosqlite

from app.database.base import (
    get_database_connection,
)

logger = logging.getLogger(__name__)


CREATE_CHAT_MEMORY_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS chat_memory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_user_id INTEGER NOT NULL,
    role TEXT NOT NULL CHECK (
        role IN ('user', 'assistant')
    ),
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


CREATE_CHAT_MEMORY_INDEX_QUERY = """
CREATE INDEX IF NOT EXISTS idx_chat_memory_user_id
ON chat_memory (telegram_user_id);
"""


class ChatMemoryRepository:
    async def initialize_table(
        self
    ) -> None:
        async with (
            get_database_connection()
            as connection
        ):
            await connection.execute(
                CREATE_CHAT_MEMORY_TABLE_QUERY
            )

            await connection.execute(
                CREATE_CHAT_MEMORY_INDEX_QUERY
            )

            await connection.commit()

        logger.info(
            "chat_memory table initialized"
        )

    async def store_message(
        self,
        telegram_user_id: int,
        role: str,
        content: str
    ) -> int:
        normalized_role = (
            role.strip().lower()
        )

        if normalized_role not in (
            "user",
            "assistant"
        ):
            raise ValueError(
                "role must be either "
                "'user' or 'assistant'"
            )

        cleaned_content = (
            content.strip()
        )

        if not cleaned_content:
            raise ValueError(
                "content cannot be empty"
            )

        query = """
        INSERT INTO chat_memory (
            telegram_user_id,
            role,
            content
        )
        VALUES (?, ?, ?)
        """

        async with (
            get_database_connection()
            as connection
        ):
            cursor = await connection.execute(
                query,
                (
                    telegram_user_id,
                    normalized_role,
                    cleaned_content
                )
            )

            await connection.commit()

            inserted_id = (
                cursor.lastrowid
            )

        logger.info(
            "Stored chat memory | "
            "telegram_user_id=%s "
            "role=%s "
            "message_id=%s",
            telegram_user_id,
            normalized_role,
            inserted_id
        )

        return int(inserted_id)

    async def get_recent_history(
        self,
        telegram_user_id: int,
        limit: int = 20
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            limit = 20

        query = """
        SELECT
            id,
            telegram_user_id,
            role,
            content,
            created_at
        FROM chat_memory
        WHERE telegram_user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """

        async with (
            get_database_connection()
            as connection
        ):
            connection.row_factory = (
                aiosqlite.Row
            )

            cursor = await connection.execute(
                query,
                (
                    telegram_user_id,
                    limit
                )
            )

            rows = await cursor.fetchall()

        history = [
            {
                "id": row["id"],
                "telegram_user_id": (
                    row[
                        "telegram_user_id"
                    ]
                ),
                "role": row["role"],
                "content": (
                    row["content"]
                ),
                "created_at": (
                    row["created_at"]
                )
            }
            for row in reversed(rows)
        ]

        logger.info(
            "Loaded chat memory | "
            "telegram_user_id=%s "
            "messages=%s",
            telegram_user_id,
            len(history)
        )

        return history

    async def clear_history(
        self,
        telegram_user_id: int
    ) -> int:
        query = """
        DELETE FROM chat_memory
        WHERE telegram_user_id = ?
        """

        async with (
            get_database_connection()
            as connection
        ):
            cursor = await connection.execute(
                query,
                (
                    telegram_user_id,
                )
            )

            await connection.commit()

            deleted_count = (
                cursor.rowcount
            )

        logger.info(
            "Cleared chat memory | "
            "telegram_user_id=%s "
            "deleted=%s",
            telegram_user_id,
            deleted_count
        )

        return int(deleted_count)


chat_memory_repository = (
    ChatMemoryRepository()
)
