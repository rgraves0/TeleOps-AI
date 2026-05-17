from __future__ import annotations

from datetime import datetime
from typing import Any
from app.database.base import get_db

class ReminderRepository:
    async def create_reminder(
        self,
        user_id: int,
        title: str,
        description: str | None,
        remind_at: datetime
    ) -> int:
        db = await get_db()
        cursor = await db.execute(
            """
            INSERT INTO reminders (user_id, title, description, remind_at)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, title, description, remind_at.isoformat())
        )
        await db.commit()
        return cursor.lastrowid

    async def get_by_id(self, reminder_id: int) -> dict[str, Any] | None:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT reminders.id, reminders.user_id, reminders.title, reminders.description,
                   reminders.remind_at, reminders.is_sent, reminders.created_at,
                   users.telegram_id, users.full_name
            FROM reminders
            INNER JOIN users ON reminders.user_id = users.id
            WHERE reminders.id = ?
            """,
            (reminder_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def list_user_reminders(self, user_id: int) -> list[dict[str, Any]]:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT id, user_id, title, description, remind_at, is_sent, created_at
            FROM reminders
            WHERE user_id = ?
            ORDER BY remind_at ASC
            """,
            (user_id,)
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def update_reminder(
        self,
        reminder_id: int,
        title: str,
        description: str | None,
        remind_at: datetime
    ) -> bool:
        db = await get_db()
        await db.execute(
            """
            UPDATE reminders
            SET title = ?, description = ?, remind_at = ?
            WHERE id = ?
            """,
            (title, description, remind_at.isoformat(), reminder_id)
        )
        await db.commit()
        return True

    async def delete_reminder(self, reminder_id: int) -> bool:
        db = await get_db()
        await db.execute("DELETE FROM reminders WHERE id = ?", (reminder_id,))
        await db.commit()
        return True

    async def mark_as_sent(self, reminder_id: int) -> bool:
        db = await get_db()
        await db.execute("UPDATE reminders SET is_sent = 1 WHERE id = ?", (reminder_id,))
        await db.commit()
        return True

    async def get_due_reminders(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        db = await get_db()
        cursor = await db.execute(
            """
            SELECT reminders.id, reminders.user_id, reminders.title, reminders.description,
                   reminders.remind_at, users.telegram_id
            FROM reminders
            INNER JOIN users ON reminders.user_id = users.id
            WHERE remind_at BETWEEN ? AND ?
            ORDER BY remind_at ASC
            """,
            (start_time.isoformat(), end_time.isoformat())
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def get_user_by_telegram_id(self, telegram_id: int) -> dict[str, Any] | None:
        db = await get_db()
        cursor = await db.execute(
            "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None
