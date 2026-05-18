from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


# =========================================================
# MEMORY ENTRY
# =========================================================


@dataclass
class MemoryEntry:

    memory_id: str

    agent_id: str

    content: str

    embedding: list[
        float
    ] | None = None

    metadata: dict = field(
        default_factory=dict
    )

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )


# =========================================================
# TASK MEMORY STORE
# =========================================================


class PersistentTaskMemory:

    def __init__(
        self,
        database_path: str,
    ) -> None:

        self.database_path = (
            Path(database_path)
        )

        self.connection = (
            sqlite3.connect(
                self.database_path
            )
        )

        self.initialize()

    # =====================================================
    # INITIALIZE
    # =====================================================

    def initialize(
        self,
    ) -> None:

        cursor = (
            self.connection.cursor()
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS
            task_memory (
                memory_id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_agent_memory
            ON task_memory(agent_id)
            """
        )

        self.connection.commit()

        logger.info(
            "PersistentTaskMemory initialized"
        )

    # =====================================================
    # STORE MEMORY
    # =====================================================

    def store_memory(
        self,
        agent_id: str,
        content: str,
        embedding: list[
            float
        ] | None = None,
        metadata: dict | None = None,
    ) -> MemoryEntry:

        entry = MemoryEntry(
            memory_id=str(
                uuid.uuid4()
            ),
            agent_id=agent_id,
            content=content,
            embedding=embedding,
            metadata=(
                metadata or {}
            ),
        )

        cursor = (
            self.connection.cursor()
        )

        cursor.execute(
            """
            INSERT INTO task_memory (
                memory_id,
                agent_id,
                content,
                embedding,
                metadata,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                entry.memory_id,
                entry.agent_id,
                entry.content,
                json.dumps(
                    entry.embedding
                ),
                json.dumps(
                    entry.metadata
                ),
                entry.created_at,
            ),
        )

        self.connection.commit()

        logger.info(
            "Stored memory=%s",
            entry.memory_id,
        )

        return entry

    # =====================================================
    # GET AGENT MEMORIES
    # =====================================================

    def get_agent_memories(
        self,
        agent_id: str,
        limit: int = 50,
    ) -> list[MemoryEntry]:

        cursor = (
            self.connection.cursor()
        )

        cursor.execute(
            """
            SELECT
                memory_id,
                agent_id,
                content,
                embedding,
                metadata,
                created_at
            FROM task_memory
            WHERE agent_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (
                agent_id,
                limit,
            ),
        )

        rows = (
            cursor.fetchall()
        )

        memories = []

        for row in rows:

            memories.append(
                MemoryEntry(
                    memory_id=row[0],
                    agent_id=row[1],
                    content=row[2],
                    embedding=(
                        json.loads(
                            row[3]
                        )
                        if row[3]
                        else None
                    ),
                    metadata=(
                        json.loads(
                            row[4]
                        )
                        if row[4]
                        else {}
                    ),
                    created_at=row[5],
                )
            )

        return memories

    # =====================================================
    # SEARCH MEMORY
    # =====================================================

    def search_memory(
        self,
        agent_id: str,
        keyword: str,
    ) -> list[MemoryEntry]:

        cursor = (
            self.connection.cursor()
        )

        cursor.execute(
            """
            SELECT
                memory_id,
                agent_id,
                content,
                embedding,
                metadata,
                created_at
            FROM task_memory
            WHERE agent_id = ?
            AND content LIKE ?
            ORDER BY created_at DESC
            """,
            (
                agent_id,
                f"%{keyword}%",
            ),
        )

        rows = (
            cursor.fetchall()
        )

        results = []

        for row in rows:

            results.append(
                MemoryEntry(
                    memory_id=row[0],
                    agent_id=row[1],
                    content=row[2],
                    embedding=(
                        json.loads(
                            row[3]
                        )
                        if row[3]
                        else None
                    ),
                    metadata=(
                        json.loads(
                            row[4]
                        )
                        if row[4]
                        else {}
                    ),
                    created_at=row[5],
                )
            )

        return results

    # =====================================================
    # DELETE MEMORY
    # =====================================================

    def delete_memory(
        self,
        memory_id: str,
    ) -> None:

        cursor = (
            self.connection.cursor()
        )

        cursor.execute(
            """
            DELETE FROM task_memory
            WHERE memory_id = ?
            """,
            (memory_id,),
        )

        self.connection.commit()

        logger.warning(
            "Deleted memory=%s",
            memory_id,
        )

    # =====================================================
    # CLOSE
    # =====================================================

    def close(
        self,
    ) -> None:

        self.connection.close()

        logger.info(
            "Memory database closed"
        )
