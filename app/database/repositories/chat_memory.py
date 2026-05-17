from __future__ import annotations

import logging
from typing import Any

from app.database.base import (
    get_db,
)

logger = logging.getLogger(__name__)


class ChatMemoryRepository:
    async def initialize_table(
        self
    ) -> None:
        db = await get_db()

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_chat_memory_telegram_user_id
            ON chat_memory (
                telegram_user_id
            )
            """
        )

        await db.commit()

        logger.info(
            "chat_memory table initialized"
        )

    async def store_message(
        self,
        telegram_user_id: int,
        role: str,
        content: str
    ) -> None:
        if not content.strip():
            return

        db = await get_db()

        await db.execute(
            """
            INSERT INTO chat_memory (
                telegram_user_id,
                role,
                content
            )
            VALUES (?, ?, ?)
            """,
            (
                telegram_user_id,
                role,
                content.strip()
            )
        )

        await db.commit()

        logger.debug(
            "Stored chat memory "
            "telegram_user_id=%s role=%s",
            telegram_user_id,
            role
        )

    async def get_recent_history(
        self,
        telegram_user_id: int,
        limit: int = 20
    ) -> list[dict[str, Any]]:
        db = await get_db()

        cursor = await db.execute(
            """
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
            """,
            (
                telegram_user_id,
                limit
            )
        )

        rows = await cursor.fetchall()

        await cursor.close()

        history = []

        for row in reversed(rows):
            history.append(
                {
                    "id": row["id"],
                    "telegram_user_id": row[
                        "telegram_user_id"
                    ],
                    "role": row["role"],
                    "content": row["content"],
                    "created_at": row[
                        "created_at"
                    ]
                }
            )

        logger.debug(
            "Loaded memory history "
            "telegram_user_id=%s count=%s",
            telegram_user_id,
            len(history)
        )

        return history

    async def clear_history(
        self,
        telegram_user_id: int
    ) -> None:
        db = await get_db()

        await db.execute(
            """
            DELETE FROM chat_memory
            WHERE telegram_user_id = ?
            """,
            (
                telegram_user_id,
            )
        )

        await db.commit()

        logger.info(
            "Cleared chat memory "
            "telegram_user_id=%s",
            telegram_user_id
        )


chat_memory_repository = (
    ChatMemoryRepository()
)
