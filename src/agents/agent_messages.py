from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any


# =========================================================
# MESSAGE TYPE
# =========================================================


class MessageType(str, Enum):

    EVENT = "event"

    COMMAND = "command"

    REQUEST = "request"

    RESPONSE = "response"

    ERROR = "error"

    SYSTEM = "system"


# =========================================================
# MESSAGE PRIORITY
# =========================================================


class MessagePriority(int, Enum):

    CRITICAL = 0

    HIGH = 1

    NORMAL = 2

    LOW = 3


# =========================================================
# MESSAGE STATUS
# =========================================================


class MessageStatus(str, Enum):

    PENDING = "pending"

    PROCESSING = "processing"

    COMPLETED = "completed"

    FAILED = "failed"

    EXPIRED = "expired"


# =========================================================
# AGENT MESSAGE
# =========================================================


@dataclass(slots=True)
class AgentMessage:

    message_type: MessageType

    sender: str

    recipient: str | None

    topic: str

    payload: dict[str, Any]

    priority: MessagePriority = (
        MessagePriority.NORMAL
    )

    correlation_id: str = field(
        default_factory=lambda:
        str(uuid.uuid4())
    )

    reply_to: str | None = None

    ttl_seconds: int = 60

    retries: int = 0

    max_retries: int = 3

    created_at: float = field(
        default_factory=time.time
    )

    status: MessageStatus = (
        MessageStatus.PENDING
    )

    metadata: dict[str, Any] = field(
        default_factory=dict
    )

    # =====================================================
    # EXPIRED
    # =====================================================

    def expired(
        self,
    ) -> bool:

        return (
            time.time()
            >= (
                self.created_at
                + self.ttl_seconds
            )
        )

    # =====================================================
    # CAN RETRY
    # =====================================================

    def can_retry(
        self,
    ) -> bool:

        return (
            self.retries
            < self.max_retries
        )

    # =====================================================
    # MARK RETRY
    # =====================================================

    def mark_retry(
        self,
    ) -> None:

        self.retries += 1

    # =====================================================
    # SERIALIZE
    # =====================================================

    def to_dict(
        self,
    ) -> dict[str, Any]:

        return {

            "message_type":
            self.message_type.value,

            "sender":
            self.sender,

            "recipient":
            self.recipient,

            "topic":
            self.topic,

            "payload":
            self.payload,

            "priority":
            int(self.priority),

            "correlation_id":
            self.correlation_id,

            "reply_to":
            self.reply_to,

            "ttl_seconds":
            self.ttl_seconds,

            "retries":
            self.retries,

            "max_retries":
            self.max_retries,

            "created_at":
            self.created_at,

            "status":
            self.status.value,

            "metadata":
            self.metadata,
        }


# =========================================================
# REQUEST TRACKER
# =========================================================


@dataclass(slots=True)
class PendingRequest:

    correlation_id: str

    created_at: float

    timeout_seconds: int

    future: Any

    requester: str

    topic: str

    # =====================================================
    # EXPIRED
    # =====================================================

    def expired(
        self,
    ) -> bool:

        return (
            time.time()
            >= (
                self.created_at
                + self.timeout_seconds
            )
        )


# =========================================================
# DEAD LETTER ENTRY
# =========================================================


@dataclass(slots=True)
class DeadLetterMessage:

    message: AgentMessage

    reason: str

    failed_at: float = field(
        default_factory=time.time
    )


# =========================================================
# SUBSCRIPTION
# =========================================================


@dataclass(slots=True)
class Subscription:

    agent_name: str

    topic: str

    priority_only: (
        MessagePriority
        | None
    ) = None
