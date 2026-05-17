from __future__ import annotations

from typing import Any
from app.database.base import get_db

class UserRepository:
    async def create_user(
        self,
        telegram_id: int,
        username: str | None,
        full_name: str,
        role_name: str = "user"
    ) -> int:
        db = await get_db()
        cursor = await db.execute(
            "SELECT id FROM roles WHERE name = ?", (role_name,)
        )
        role = await cursor.fetchone()
        if role is None:
            await cursor.close()
            raise ValueError(f"Role '{role_name}' does not exist")

        role_id = role["id"]
        cursor = await db.execute(
            """
            INSERT INTO users (telegram_id, username, full_name, role_id)
            VALUES (?, ?, ?, ?)
            """,
            (telegram_id, username, full_name, role_id)
        )
        await db.commit()
        return cursor.lastrowid

    async def get_by_id(self, user_id: int) -> dict[str, Any] | None:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT users.id, users.telegram_id, users.username, users.full_name,
                   users.is_active, users.is_banned, users.created_at, roles.name AS role_name
            FROM users
            INNER JOIN roles ON users.role_id = roles.id
            WHERE users.id = ?
            """,
            (user_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def get_by_telegram_id(self, telegram_id: int) -> dict[str, Any] | None:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT users.id, users.telegram_id, users.username, users.full_name,
                   users.is_active, users.is_banned, users.created_at, roles.name AS role_name
            FROM users
            INNER JOIN roles ON users.role_id = roles.id
            WHERE users.telegram_id = ?
            """,
            (telegram_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def assign_role(self, user_id: int, role_name: str) -> bool:
        db = await get_db()
        cursor = await db.execute(
            "SELECT id FROM roles WHERE name = ?", (role_name,)
        )
        role = await cursor.fetchone()
        if role is None:
            await cursor.close()
            raise ValueError(f"Role '{role_name}' does not exist")

        await db.execute(
            """
            UPDATE users
            SET role_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (role["id"], user_id)
        )
        await db.commit()
        return True

    async def list_users(self) -> list[dict[str, Any]]:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT users.id, users.telegram_id, users.username, users.full_name,
                   users.is_active, users.is_banned, users.created_at, roles.name AS role_name
            FROM users
            INNER JOIN roles ON users.role_id = roles.id
            ORDER BY users.id ASC
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def deactivate_user(self, user_id: int) -> bool:
        db = await get_db()
        await db.execute(
            """
            UPDATE users
            SET is_active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (user_id,)
        )
        await db.commit()
        return True

    async def ban_user(self, user_id: int) -> bool:
        db = await get_db()
        await db.execute(
            """
            UPDATE users
            SET is_banned = 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (user_id,)
        )
        await db.commit()
        return True

    async def user_exists(self, telegram_id: int) -> bool:
        db = await get_db()
        cursor = await db.execute(
            "SELECT id FROM users WHERE telegram_id = ?", (telegram_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row is not None
