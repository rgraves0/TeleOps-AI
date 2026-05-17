from __future__ import annotations

from typing import Any

from app.database.base import db


class RcloneMetaRepository:
    async def initialize_table(
        self
    ) -> None:
        connection = await db.get_connection()

        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS rclone_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                remote_name TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,

                file_size INTEGER,
                mime_type TEXT,

                created_at TIMESTAMP
                    DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        await connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_rclone_file_name
            ON rclone_files(file_name)
            """
        )

        await connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_rclone_remote_name
            ON rclone_files(remote_name)
            """
        )

        await connection.commit()

    async def add_file_metadata(
        self,
        remote_name: str,
        file_name: str,
        file_path: str,
        file_size: int | None = None,
        mime_type: str | None = None
    ) -> int:
        connection = await db.get_connection()

        cursor = await connection.execute(
            """
            INSERT INTO rclone_files (
                remote_name,
                file_name,
                file_path,
                file_size,
                mime_type
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                remote_name,
                file_name,
                file_path,
                file_size,
                mime_type
            )
        )

        await connection.commit()

        return cursor.lastrowid

    async def search_files(
        self,
        keyword: str,
        limit: int = 20
    ) -> list[dict[str, Any]]:
        rows = await db.fetch_all(
            """
            SELECT
                id,
                remote_name,
                file_name,
                file_path,
                file_size,
                mime_type,
                created_at
            FROM rclone_files
            WHERE
                file_name LIKE ?
                OR file_path LIKE ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (
                f"%{keyword}%",
                f"%{keyword}%",
                limit
            )
        )

        return [
            dict(row)
            for row in rows
        ]

    async def list_remote_files(
        self,
        remote_name: str
    ) -> list[dict[str, Any]]:
        rows = await db.fetch_all(
            """
            SELECT
                id,
                remote_name,
                file_name,
                file_path,
                file_size,
                mime_type,
                created_at
            FROM rclone_files
            WHERE remote_name = ?
            ORDER BY file_name ASC
            """,
            (remote_name,)
        )

        return [
            dict(row)
            for row in rows
        ]

    async def delete_remote_files(
        self,
        remote_name: str
    ) -> bool:
        connection = await db.get_connection()

        await connection.execute(
            """
            DELETE FROM rclone_files
            WHERE remote_name = ?
            """,
            (remote_name,)
        )

        await connection.commit()

        return True
