from __future__ import annotations

import asyncio
import logging
import signal

from dotenv import (
    load_dotenv,
)

from app.core.scheduler import (
    scheduler_service,
)
from app.database.base import (
    close_database,
    init_db,
)
from app.database.repositories.chat_memory import (
    chat_memory_repository,
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
    reminder_service,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(name)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
)

logger = logging.getLogger(__name__)


class TeleOpsApplication:
    def __init__(self) -> None:
        self.bot = TelegramBot()

        self.running = False

        self.shutdown_event = (
            asyncio.Event()
        )

        self.rclone_repository = (
            RcloneMetaRepository()
        )

    async def initialize(
        self
    ) -> None:
        logger.info(
            "Initializing database..."
        )

        await init_db()

        logger.info(
            "Initializing chat memory..."
        )

        await (
            chat_memory_repository
            .initialize_table()
        )

        logger.info(
            "Initializing RClone metadata..."
        )

        await (
            self.rclone_repository
            .initialize_table()
        )

        logger.info(
            "Loading plugins..."
        )

        await (
            plugin_loader
            .load_all_plugins()
        )

        plugins = (
            plugin_loader
            .list_plugins()
        )

        for plugin in plugins:
            logger.info(
                "Plugin loaded | "
                "name=%s | enabled=%s",
                plugin.get("name"),
                plugin.get("enabled")
            )

        logger.info(
            "Attaching Telegram "
            "application to scheduler..."
        )

        await (
            scheduler_service
            .attach_application(
                self.bot.application
            )
        )

        logger.info(
            "Starting scheduler..."
        )

        await (
            scheduler_service.start()
        )

        logger.info(
            "Restoring reminder jobs..."
        )

        await (
            reminder_service
            .restore_jobs()
        )

        logger.info(
            "Initialization completed"
        )

    async def start_bot(
        self
    ) -> None:
        logger.info(
            "Starting Telegram bot..."
        )

        await self.bot.run()

    async def shutdown(
        self
    ) -> None:
        if not self.running:
            return

        logger.info(
            "Shutdown sequence started..."
        )

        self.running = False

        try:
            logger.info(
                "Stopping Telegram bot..."
            )

            await self.bot.shutdown()

        except Exception:
            logger.exception(
                "Telegram bot shutdown failed"
            )

        try:
            logger.info(
                "Stopping scheduler..."
            )

            await (
                scheduler_service
                .shutdown()
            )

        except Exception:
            logger.exception(
                "Scheduler shutdown failed"
            )

        try:
            logger.info(
                "Closing database..."
            )

            await close_database()

        except Exception:
            logger.exception(
                "Database shutdown failed"
            )

        self.shutdown_event.set()

        logger.info(
            "TeleOps-AI shutdown completed"
        )

    async def run(
        self
    ) -> None:
        try:
            self.running = True

            await self.initialize()

            logger.info(
                "TeleOps-AI is operational"
            )

            await self.start_bot()

            await (
                self.shutdown_event.wait()
            )

        except asyncio.CancelledError:
            logger.info(
                "Application cancelled"
            )

        except Exception:
            logger.exception(
                "Fatal application error"
            )

        finally:
            await self.shutdown()


async def main() -> None:
    application = (
        TeleOpsApplication()
    )

    loop = (
        asyncio.get_running_loop()
    )

    def handle_shutdown_signal():
        logger.info(
            "Shutdown signal received"
        )

        asyncio.create_task(
            application.shutdown()
        )

    for shutdown_signal in (
        signal.SIGINT,
        signal.SIGTERM
    ):
        try:
            loop.add_signal_handler(
                shutdown_signal,
                handle_shutdown_signal
            )

        except NotImplementedError:
            logger.warning(
                "Signal handlers are "
                "not supported on "
                "this platform"
            )

    await application.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info(
            "Application interrupted"
        )
