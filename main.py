from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress

from dotenv import load_dotenv

from app.core.scheduler import (
    scheduler_manager,
)
from app.database.base import (
    db,
    init_db,
)
from app.database.repositories.rclone_meta import (
    RcloneMetaRepository,
)
from app.interfaces.telegram.bot import (
    TelegramBot,
)
from app.plugins.loader import (
    plugin_loader,
)
from app.services.reminder_service import (
    ReminderService,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    )
)

logger = logging.getLogger(__name__)


class TeleOpsApplication:
    def __init__(self):
        self.bot = TelegramBot()

        self.reminder_service = (
            ReminderService()
        )

        self.rclone_repository = (
            RcloneMetaRepository()
        )

        self.shutdown_event = (
            asyncio.Event()
        )

    async def initialize(self) -> None:
        logger.info(
            "Initializing database..."
        )

        await init_db()

        await self.rclone_repository.initialize_table()

        logger.info(
            "Database initialized"
        )

        logger.info(
            "Loading plugins..."
        )

        plugin_loader.load_all_plugins()

        plugins = (
            plugin_loader.list_plugins()
        )

        for plugin in plugins:
            logger.info(
                "Plugin: %s | enabled=%s",
                plugin["name"],
                plugin["enabled"]
            )

        logger.info(
            "Starting scheduler..."
        )

        scheduler_manager.attach_application(
            self.bot.application
        )

        scheduler_manager.start()

        logger.info(
            "Restoring reminder jobs..."
        )

        await (
            self.reminder_service
            .restore_pending_reminders()
        )

        logger.info(
            "System initialization completed"
        )

    async def start_bot(self) -> None:
        logger.info(
            "Starting Telegram bot..."
        )

        self.bot.setup()

        application = (
            self.bot.application
        )

        await application.initialize()

        await application.start()

        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES
        )

        logger.info(
            "Telegram bot polling started"
        )

    async def stop_bot(self) -> None:
        application = (
            self.bot.application
        )

        logger.info(
            "Stopping Telegram bot..."
        )

        with suppress(Exception):
            await application.updater.stop()

        with suppress(Exception):
            await application.stop()

        with suppress(Exception):
            await application.shutdown()

    async def shutdown(self) -> None:
        logger.info(
            "Graceful shutdown started..."
        )

        await self.stop_bot()

        await scheduler_manager.shutdown()

        await db.disconnect()

        logger.info(
            "Database disconnected"
        )

        logger.info(
            "Shutdown complete"
        )

    def _register_signal_handlers(
        self
    ) -> None:
        loop = asyncio.get_running_loop()

        for sig in (
            signal.SIGINT,
            signal.SIGTERM
        ):
            loop.add_signal_handler(
                sig,
                self.shutdown_event.set
            )

    async def run(self) -> None:
        self._register_signal_handlers()

        await self.initialize()

        await self.start_bot()

        logger.info(
            "TeleOps-AI is running"
        )

        await self.shutdown_event.wait()

        await self.shutdown()


async def main() -> None:
    application = (
        TeleOpsApplication()
    )

    try:
        await application.run()

    except Exception:
        logger.exception(
            "Fatal application error"
        )

        await application.shutdown()


if __name__ == "__main__":
    from telegram import Update

    asyncio.run(main())
