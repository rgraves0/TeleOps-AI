from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any
from uuid import uuid4


# =========================================================
# MEMORY TYPES
# =========================================================


class MemoryType:

    CONVERSATION = (
        "conversation"
    )

    WORKFLOW = (
        "workflow"
    )

    TASK = (
        "task"
    )

    OPERATIONAL = (
        "operational"
    )

    SUMMARY = (
        "summary"
    )


# =========================================================
# BASE MEMORY MODEL
# =========================================================


@dataclass
class BaseMemoryModel:

    memory_id: str = field(
        default_factory=lambda:
        str(uuid4())
    )

    memory_type: str = (
        MemoryType.CONVERSATION
    )

    content: str = ""

    embedding_ref: (
        str | None
    ) = None

    tags: list[str] = field(
        default_factory=list
    )

    metadata: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )

    importance_score: float = 0.5

    access_count: int = 0

    expires_at: (
        str | None
    ) = None

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )

    updated_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )


# =========================================================
# MEMORY QUERY RESULT
# =========================================================


@dataclass
class MemoryQueryResult:

    memory_id: str

    memory_type: str

    content: str

    score: float

    metadata: dict[
        str,
        Any
    ]


# =========================================================
# MEMORY INDEX
# =========================================================


@dataclass
class MemoryIndex:

    memory_id: str

    keyword: str

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )


# =========================================================
# MEMORY SCHEMA
# =========================================================


MEMORY_SCHEMA = """

CREATE TABLE IF NOT EXISTS memory_store (

    memory_id TEXT PRIMARY KEY,

    memory_type TEXT NOT NULL,

    content TEXT NOT NULL,

    embedding_ref TEXT,

    tags TEXT,

    metadata TEXT,

    importance_score REAL DEFAULT 0.5,

    access_count INTEGER DEFAULT 0,

    expires_at TEXT,

    created_at TEXT NOT NULL,

    updated_at TEXT NOT NULL
)

"""


# =========================================================
# MEMORY INDEX SCHEMA
# =========================================================


MEMORY_INDEX_SCHEMA = """

CREATE TABLE IF NOT EXISTS memory_index (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    memory_id TEXT NOT NULL,

    keyword TEXT NOT NULL,

    created_at TEXT NOT NULL
)

"""


# =========================================================
# MEMORY INDEXES
# =========================================================


MEMORY_INDEXES = [

    """

    CREATE INDEX IF NOT EXISTS
    idx_memory_type
    ON memory_store(memory_type)

    """,

    """

    CREATE INDEX IF NOT EXISTS
    idx_memory_created
    ON memory_store(created_at)

    """,

    """

    CREATE INDEX IF NOT EXISTS
    idx_memory_expiry
    ON memory_store(expires_at)

    """,

    """

    CREATE INDEX IF NOT EXISTS
    idx_memory_keyword
    ON memory_index(keyword)

    """,
]
