from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.core.events import (
    Event,
    EventBus,
)
from src.ai.summarizer import (
    EmailSummarizer,
)

logger = logging.getLogger(__name__)


# =========================================================
# EVENT NAMES
# =========================================================


MAIL_RECEIVED_EVENT = (
    "mail.received"
)

MAIL_SUMMARIZED_EVENT = (
    "mail.summarized"
)

MAIL_FAILED_EVENT = (
    "mail.failed"
)

MAIL_SENT_EVENT = (
    "mail.sent"
)


# =========================================================
# MAIL EVENT PAYLOADS
# =========================================================


@dataclass
class MailReceivedPayload:

    uid: str

    sender: str

    recipients: list[str]

    subject: str

    body: str

    received_at: str

    attachments: list[
        dict[str, Any]
    ]


@dataclass
class MailSummaryPayload:

    uid: str

    subject: str

    sender: str

    summary: str

    importance: str

    action_required: bool

    summarized_at: str


@dataclass
class MailFailurePayload:

    operation: str

    reason: str

    timestamp: str


# =========================================================
# MAIL EVENT PUBLISHER
# =========================================================


class MailEventPublisher:

    def __init__(
        self,
        event_bus: EventBus,
    ) -> None:

        self.event_bus = (
            event_bus
        )

    # =====================================================
    # MAIL RECEIVED
    # =====================================================

    async def publish_mail_received(
        self,
        payload: (
            MailReceivedPayload
        ),
    ) -> Event:

        return await (
            self.event_bus.emit(
                MAIL_RECEIVED_EVENT,
                payload.__dict__,
            )
        )

    # =====================================================
    # MAIL SUMMARIZED
    # =====================================================

    async def publish_mail_summarized(
        self,
        payload: (
            MailSummaryPayload
        ),
    ) -> Event:

        return await (
            self.event_bus.emit(
                MAIL_SUMMARIZED_EVENT,
                payload.__dict__,
            )
        )

    # =====================================================
    # MAIL FAILURE
    # =====================================================

    async def publish_mail_failed(
        self,
        payload: (
            MailFailurePayload
        ),
    ) -> Event:

        return await (
            self.event_bus.emit(
                MAIL_FAILED_EVENT,
                payload.__dict__,
            )
        )

    # =====================================================
    # MAIL SENT
    # =====================================================

    async def publish_mail_sent(
        self,
        payload: dict,
    ) -> Event:

        return await (
            self.event_bus.emit(
                MAIL_SENT_EVENT,
                payload,
            )
        )


# =========================================================
# MAIL EVENT CONSUMER
# =========================================================


class MailEventConsumer:

    def __init__(
        self,
        event_bus: EventBus,
        summarizer: (
            EmailSummarizer
        ),
    ) -> None:

        self.event_bus = (
            event_bus
        )

        self.summarizer = (
            summarizer
        )

        logger.info(
            "MailEventConsumer initialized"
        )

    # =====================================================
    # REGISTER
    # =====================================================

    def register(
        self,
    ) -> None:

        self.event_bus.subscribe(
            MAIL_RECEIVED_EVENT,
            self.handle_mail_received,
        )

        self.event_bus.subscribe(
            MAIL_FAILED_EVENT,
            self.handle_mail_failure,
        )

        self.event_bus.subscribe(
            MAIL_SENT_EVENT,
            self.handle_mail_sent,
        )

        logger.info(
            "Mail event handlers registered"
        )

    # =====================================================
    # HANDLE MAIL RECEIVED
    # =====================================================

    async def handle_mail_received(
        self,
        event: Event,
    ) -> None:

        logger.info(
            "Processing mail event"
        )

        payload = (
            event.payload
        )

        try:

            summary = await (
                self.summarizer
                .summarize_email(
                    subject=payload[
                        "subject"
                    ],
                    sender=payload[
                        "sender"
                    ],
                    body=payload[
                        "body"
                    ],
                )
            )

            summarized_payload = (
                MailSummaryPayload(

                    uid=payload[
                        "uid"
                    ],

                    subject=payload[
                        "subject"
                    ],

                    sender=payload[
                        "sender"
                    ],

                    summary=(
                        summary.summary
                    ),

                    importance=(
                        summary.importance
                    ),

                    action_required=(
                        summary.action_required
                    ),

                    summarized_at=(
                        datetime.utcnow()
                        .isoformat()
                    ),
                )
            )

            await (
                self.event_bus.emit(
                    MAIL_SUMMARIZED_EVENT,
                    summarized_payload
                    .__dict__,
                )
            )

            logger.info(
                "Mail summarized"
            )

        except Exception as exc:

            logger.exception(
                "Mail processing failed"
            )

            await (
                self.event_bus.emit(
                    MAIL_FAILED_EVENT,
                    {

                        "operation":
                        "summarize",

                        "reason":
                        str(exc),

                        "timestamp":
                        datetime.utcnow()
                        .isoformat(),
                    },
                )
            )

    # =====================================================
    # HANDLE FAILURE
    # =====================================================

    async def handle_mail_failure(
        self,
        event: Event,
    ) -> None:

        logger.error(
            "Mail failure=%s",
            event.payload,
        )

    # =====================================================
    # HANDLE SENT
    # =====================================================

    async def handle_mail_sent(
        self,
        event: Event,
    ) -> None:

        logger.info(
            "Mail sent event=%s",
            event.payload,
        )
