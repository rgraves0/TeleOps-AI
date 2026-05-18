from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.core.events import (
    Event,
    EventBus,
)

logger = logging.getLogger(__name__)


# =========================================================
# SYSTEM EVENT NAMES
# =========================================================


SYSTEM_STARTUP_EVENT = (
    "system.startup"
)

SYSTEM_SHUTDOWN_EVENT = (
    "system.shutdown"
)

SYSTEM_WARNING_EVENT = (
    "system.warning"
)

SYSTEM_ERROR_EVENT = (
    "system.error"
)

RESOURCE_CRITICAL_EVENT = (
    "resource.critical"
)

PROVIDER_FAILED_EVENT = (
    "provider.failed"
)

TASK_COMPLETED_EVENT = (
    "task.completed"
)

TASK_FAILED_EVENT = (
    "task.failed"
)


# =========================================================
# PAYLOADS
# =========================================================


@dataclass
class SystemWarningPayload:

    message: str

    timestamp: str

    metadata: dict[
        str,
        Any
    ]


@dataclass
class SystemErrorPayload:

    error: str

    source: str

    timestamp: str

    metadata: dict[
        str,
        Any
    ]


@dataclass
class ResourceCriticalPayload:

    resource_type: str

    value: float

    threshold: float

    timestamp: str


# =========================================================
# SYSTEM EVENT PUBLISHER
# =========================================================


class SystemEventPublisher:

    def __init__(
        self,
        event_bus: EventBus,
    ) -> None:

        self.event_bus = (
            event_bus
        )

    # =====================================================
    # STARTUP
    # =====================================================

    async def publish_startup(
        self,
    ) -> Event:

        return await (
            self.event_bus.emit(
                SYSTEM_STARTUP_EVENT,
                {

                    "timestamp":
                    datetime.utcnow()
                    .isoformat(),
                },
            )
        )

    # =====================================================
    # SHUTDOWN
    # =====================================================

    async def publish_shutdown(
        self,
    ) -> Event:

        return await (
            self.event_bus.emit(
                SYSTEM_SHUTDOWN_EVENT,
                {

                    "timestamp":
                    datetime.utcnow()
                    .isoformat(),
                },
            )
        )

    # =====================================================
    # WARNING
    # =====================================================

    async def publish_warning(
        self,
        payload: (
            SystemWarningPayload
        ),
    ) -> Event:

        return await (
            self.event_bus.emit(
                SYSTEM_WARNING_EVENT,
                payload.__dict__,
            )
        )

    # =====================================================
    # ERROR
    # =====================================================

    async def publish_error(
        self,
        payload: (
            SystemErrorPayload
        ),
    ) -> Event:

        return await (
            self.event_bus.emit(
                SYSTEM_ERROR_EVENT,
                payload.__dict__,
            )
        )

    # =====================================================
    # RESOURCE CRITICAL
    # =====================================================

    async def publish_resource_critical(
        self,
        payload: (
            ResourceCriticalPayload
        ),
    ) -> Event:

        return await (
            self.event_bus.emit(
                RESOURCE_CRITICAL_EVENT,
                payload.__dict__,
            )
        )


# =========================================================
# SYSTEM EVENT CONSUMER
# =========================================================


class SystemEventConsumer:

    def __init__(
        self,
        event_bus: EventBus,
    ) -> None:

        self.event_bus = (
            event_bus
        )

        logger.info(
            "SystemEventConsumer initialized"
        )

    # =====================================================
    # REGISTER
    # =====================================================

    def register(
        self,
    ) -> None:

        self.event_bus.subscribe(
            SYSTEM_STARTUP_EVENT,
            self.handle_startup,
        )

        self.event_bus.subscribe(
            SYSTEM_SHUTDOWN_EVENT,
            self.handle_shutdown,
        )

        self.event_bus.subscribe(
            SYSTEM_WARNING_EVENT,
            self.handle_warning,
        )

        self.event_bus.subscribe(
            SYSTEM_ERROR_EVENT,
            self.handle_error,
        )

        self.event_bus.subscribe(
            RESOURCE_CRITICAL_EVENT,
            self.handle_resource_critical,
        )

        self.event_bus.subscribe(
            TASK_FAILED_EVENT,
            self.handle_task_failed,
        )

        logger.info(
            "System event handlers registered"
        )

    # =====================================================
    # STARTUP
    # =====================================================

    async def handle_startup(
        self,
        event: Event,
    ) -> None:

        logger.info(
            "System startup complete"
        )

    # =====================================================
    # SHUTDOWN
    # =====================================================

    async def handle_shutdown(
        self,
        event: Event,
    ) -> None:

        logger.warning(
            "System shutting down"
        )

    # =====================================================
    # WARNING
    # =====================================================

    async def handle_warning(
        self,
        event: Event,
    ) -> None:

        logger.warning(
            "System warning=%s",
            event.payload,
        )

    # =====================================================
    # ERROR
    # =====================================================

    async def handle_error(
        self,
        event: Event,
    ) -> None:

        logger.error(
            "System error=%s",
            event.payload,
        )

    # =====================================================
    # RESOURCE CRITICAL
    # =====================================================

    async def handle_resource_critical(
        self,
        event: Event,
    ) -> None:

        logger.critical(
            "Critical resource=%s",
            event.payload,
        )

    # =====================================================
    # TASK FAILED
    # =====================================================

    async def handle_task_failed(
        self,
        event: Event,
    ) -> None:

        logger.error(
            "Task failed=%s",
            event.payload,
        )
