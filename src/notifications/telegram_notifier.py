from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime

import httpx

from src.core.events import (
    Event,
    EventBus,
)

logger = logging.getLogger(__name__)


# =========================================================
# TELEGRAM NOTIFIER
# =========================================================


class TelegramNotifier:

    def __init__(
        self,
        bot_token: str,
        admin_chat_ids: list[int],
        event_bus: EventBus,
        queue_limit: int = 100,
    ) -> None:

        self.bot_token = (
            bot_token
        )

        self.admin_chat_ids = (
            admin_chat_ids
        )

        self.event_bus = (
            event_bus
        )

        self.queue: asyncio.Queue = (
            asyncio.Queue(
                maxsize=queue_limit
            )
        )

        self.running = False

        self.worker_task = None

        self.http_client = (
            httpx.AsyncClient(
                timeout=15.0
            )
        )

        logger.info(
            "TelegramNotifier initialized"
        )

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if self.running:
            return

        self.running = True

        self.worker_task = (
            asyncio.create_task(
                self._worker_loop()
            )
        )

        self._register_handlers()

        logger.info(
            "TelegramNotifier started"
        )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        self.running = False

        if self.worker_task:

            self.worker_task.cancel()

            try:

                await (
                    self.worker_task
                )

            except asyncio.CancelledError:
                pass

        await self.http_client.aclose()

        logger.warning(
            "TelegramNotifier stopped"
        )

    # =====================================================
    # REGISTER
    # =====================================================

    def _register_handlers(
        self,
    ) -> None:

        events = [

            "workflow.failed",

            "resource.critical",

            "system.error",

            "mail.failed",

            "provider.failed",
        ]

        for event_name in events:

            self.event_bus.subscribe(
                event_name,
                self.handle_notification,
            )

    # =====================================================
    # HANDLE EVENT
    # =====================================================

    async def handle_notification(
        self,
        event: Event,
    ) -> None:

        try:

            message = (
                self._format_event(
                    event
                )
            )

            await self.queue.put(
                message
            )

        except asyncio.QueueFull:

            logger.error(
                "Notification queue full"
            )

    # =====================================================
    # WORKER LOOP
    # =====================================================

    async def _worker_loop(
        self,
    ) -> None:

        while self.running:

            try:

                message = (
                    await self.queue.get()
                )

                await self._broadcast(
                    message
                )

            except asyncio.CancelledError:

                break

            except Exception:

                logger.exception(
                    "Notification worker failed"
                )

    # =====================================================
    # BROADCAST
    # =====================================================

    async def _broadcast(
        self,
        message: str,
    ) -> None:

        tasks = [

            self._send_message(
                chat_id,
                message,
            )

            for chat_id
            in self.admin_chat_ids
        ]

        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    # =====================================================
    # SEND MESSAGE
    # =====================================================

    async def _send_message(
        self,
        chat_id: int,
        text: str,
    ) -> None:

        url = (
            f"https://api.telegram.org/bot"
            f"{self.bot_token}"
            f"/sendMessage"
        )

        payload = {

            "chat_id":
            chat_id,

            "text":
            text,
        }

        try:

            response = await (
                self.http_client.post(
                    url,
                    json=payload,
                )
            )

            if (
                response.status_code
                != 200
            ):

                logger.error(
                    (
                        "Telegram send failed "
                        "status=%s"
                    ),
                    response.status_code,
                )

        except Exception:

            logger.exception(
                "Telegram API failed"
            )

    # =====================================================
    # FORMAT EVENT
    # =====================================================

    def _format_event(
        self,
        event: Event,
    ) -> str:

        payload = (
            event.payload
        )

        lines = [

            "⚠️ TeleOps Alert",

            "",

            f"Event: {event.name}",

            f"Time: {datetime.utcnow().isoformat()}",
        ]

        for key, value in (
            payload.items()
        ):

            lines.append(
                f"{key}: {value}"
            )

        return "\n".join(
            lines
        )

    # =====================================================
    # MANUAL NOTIFY
    # =====================================================

    async def notify(
        self,
        message: str,
    ) -> None:

        await self.queue.put(
            message
        )
