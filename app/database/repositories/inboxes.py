from __future__ import annotations

from typing import Any

from app.database.base import db


class InboxRepository:
    async def create_inbox(
        self,
        name: str,
        inbox_type: str,
        owner_user_id: int | None = None,
        telegram_chat_id: int | None = None
    ) -> int:
        if inbox_type not in (
            "private",
            "shared"
        ):
            raise ValueError(
                "Invalid inbox type"
            )

        connection = await db.get_connection()

        cursor = await connection.execute(
            """
            INSERT INTO inboxes (
                name,
                type,
                owner_user_id,
                telegram_chat_id
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                name,
                inbox_type,
                owner_user_id,
                telegram_chat_id
            )
        )

        await connection.commit()

        return cursor.lastrowid

    async def assign_user_to_inbox(
        self,
        user_id: int,
        inbox_id: int
    ) -> bool:
        connection = await db.get_connection()

        await connection.execute(
            """
            INSERT OR IGNORE INTO user_inboxes (
                user_id,
                inbox_id
            )
            VALUES (?, ?)
            """,
            (
                user_id,
                inbox_id
            )
        )

        await connection.commit()

        return True

    async def remove_user_from_inbox(
        self,
        user_id: int,
        inbox_id: int
    ) -> bool:
        connection = await db.get_connection()

        await connection.execute(
            """
            DELETE FROM user_inboxes
            WHERE user_id = ?
            AND inbox_id = ?
            """,
            (
                user_id,
                inbox_id
            )
        )

        await connection.commit()

        return True

    async def get_inbox_by_id(
        self,
        inbox_id: int
    ) -> dict[str, Any] | None:
        row = await db.fetch_one(
            """
            SELECT *
            FROM inboxes
            WHERE id = ?
            """,
            (inbox_id,)
        )

        if row is None:
            return None

        return dict(row)

    async def list_user_inboxes(
        self,
        user_id: int
    ) -> list[dict[str, Any]]:
        rows = await db.fetch_all(
            """
            SELECT
                inboxes.id,
                inboxes.name,
                inboxes.type,
                inboxes.owner_user_id,
                inboxes.telegram_chat_id,
                inboxes.created_at
            FROM inboxes
            INNER JOIN user_inboxes
                ON inboxes.id = user_inboxes.inbox_id
            WHERE user_inboxes.user_id = ?
            ORDER BY inboxes.id ASC
            """,
            (user_id,)
        )

        return [
            dict(row)
            for row in rows
        ]

    async def user_has_access(
        self,
        user_id: int,
        inbox_id: int
    ) -> bool:
        inbox = await self.get_inbox_by_id(
            inbox_id
        )

        if inbox is None:
            return False

        if (
            inbox["type"] == "private"
            and inbox["owner_user_id"]
            == user_id
        ):
            return True

        row = await db.fetch_one(
            """
            SELECT id
            FROM user_inboxes
            WHERE user_id = ?
            AND inbox_id = ?
            """,
            (
                user_id,
                inbox_id
            )
        )

        return row is not None

    async def delete_inbox(
        self,
        inbox_id: int
    ) -> bool:
        connection = await db.get_connection()

        await connection.execute(
            """
            DELETE FROM user_inboxes
            WHERE inbox_id = ?
            """,
            (inbox_id,)
        )

        await connection.execute(
            """
            DELETE FROM inboxes
            WHERE id = ?
            """,
            (inbox_id,)
        )

        await connection.commit()

        return True

    async def get_private_inbox(
        self,
        owner_user_id: int
    ) -> dict[str, Any] | None:
        row = await db.fetch_one(
            """
            SELECT *
            FROM inboxes
            WHERE owner_user_id = ?
            AND type = 'private'
            LIMIT 1
            """,
            (owner_user_id,)
        )

        if row is None:
            return None

        return dict(row)
