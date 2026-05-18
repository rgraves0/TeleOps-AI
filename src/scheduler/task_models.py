from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4


# =========================================================
# TASK PRIORITY
# =========================================================


class TaskPriority(str, Enum):

    LOW = "low"

    NORMAL = "normal"

    HIGH = "high"

    CRITICAL = "critical"


# =========================================================
# TASK STATUS
# =========================================================


class ScheduledTaskStatus(str, Enum):

    PENDING = "pending"

    RUNNING = "running"

    COMPLETED = "completed"

    FAILED = "failed"

    CANCELLED = "cancelled"


# =========================================================
# SCHEDULE TYPES
# =========================================================


class ScheduleType(str, Enum):

    INTERVAL = "interval"

    CRON = "cron"

    ONCE = "once"


# =========================================================
# SCHEDULED TASK
# =========================================================


@dataclass
class ScheduledTask:

    task_id: str = field(
        default_factory=lambda:
        str(uuid4())
    )

    name: str = ""

    schedule_type: (
        ScheduleType
    ) = ScheduleType.INTERVAL

    priority: (
        TaskPriority
    ) = TaskPriority.NORMAL

    status: (
        ScheduledTaskStatus
    ) = ScheduledTaskStatus.PENDING

    interval_seconds: (
        int | None
    ) = None

    cron_expression: (
        str | None
    ) = None

    next_run_at: (
        str | None
    ) = None

    last_run_at: (
        str | None
    ) = None

    max_retries: int = 3

    retry_count: int = 0

    enabled: bool = True

    persistent: bool = True

    timeout_seconds: int = 60

    metadata: dict[
        str,
        Any
    ] = field(
        default_factory=dict
    )

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
# TASK EXECUTION RESULT
# =========================================================


@dataclass
class TaskExecutionResult:

    task_id: str

    success: bool

    duration_ms: float

    started_at: str

    finished_at: str

    error: (
        str | None
    ) = None


# =========================================================
# TASK SCHEMA
# =========================================================


SCHEDULED_TASK_SCHEMA = """

CREATE TABLE IF NOT EXISTS scheduled_tasks (

    task_id TEXT PRIMARY KEY,

    name TEXT NOT NULL,

    schedule_type TEXT NOT NULL,

    priority TEXT NOT NULL,

    status TEXT NOT NULL,

    interval_seconds INTEGER,

    cron_expression TEXT,

    next_run_at TEXT,

    last_run_at TEXT,

    max_retries INTEGER DEFAULT 3,

    retry_count INTEGER DEFAULT 0,

    enabled INTEGER DEFAULT 1,

    persistent INTEGER DEFAULT 1,

    timeout_seconds INTEGER DEFAULT 60,

    metadata TEXT,

    created_at TEXT NOT NULL,

    updated_at TEXT NOT NULL
)

"""


# =========================================================
# INDEXES
# =========================================================


SCHEDULED_TASK_INDEXES = [

    """

    CREATE INDEX IF NOT EXISTS
    idx_scheduled_next_run
    ON scheduled_tasks(next_run_at)

    """,

    """

    CREATE INDEX IF NOT EXISTS
    idx_scheduled_status
    ON scheduled_tasks(status)

    """,

    """

    CREATE INDEX IF NOT EXISTS
    idx_scheduled_priority
    ON scheduled_tasks(priority)

    """,
]
