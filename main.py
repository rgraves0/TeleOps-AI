from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress
from dotenv import load_dotenv

from app.core.scheduler import scheduler_service
from app.database.base import close_database, init_db
from app.database.repositories.chat_memory import chat_memory_repository
from app.database.repositories.rclone_meta import RcloneMetaRepository
from app.interfaces.telegram.bot import TelegramBot
from app.plugins.loader import plugin_loader
# =====================================================================
# FIXED: Imported 'ReminderService' class instead of lowercase 'reminder_service'
# =====================================================================
from app.services.reminder_service import ReminderService

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

class TeleOpsApplication:
    def __init__(self) -> None:
        self.bot = TelegramBot()
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.rclone_repository = RcloneMetaRepository()
        # =====================================================================
        # FIXED: Instantiated the ReminderService class correctly
        # =====================================================================
        self.reminder_service = ReminderService()

    async def initialize(self) -> None:
        logger.info("Initializing database...")
        await init_db()

        logger.info("Initializing chat memory tables...")
        await chat_memory_repository.initialize_table()

        logger.info("Initializing RClone metadata tables...")
        await self.rclone_repository.initialize_table()

        logger.info("Loading plugins...")
        # =====================================================================
        # FIXED: Removed 'await' because load_all_plugins() is a synchronous function
        # =====================================================================
        plugin_loader.load_all_plugins()

        logger.info("Attaching Telegram application to scheduler...")
        await scheduler_service.attach_application(self.bot.application)

        logger.info("Starting scheduler...")
        await scheduler_service.start()

        logger.info("Restoring scheduled reminders...")
        # =====================================================================
        # FIXED: Called restore_jobs via self.reminder_service instance
        # =====================================================================
        await self.reminder_service.restore_jobs()

        logger.info("Core initialization completed")

    async def start_bot(self) -> None:
        logger.info("Starting Telegram bot...")
        await self.bot.run()

    async def shutdown(self) -> None:
        if not self.running:
            return

        logger.info("Shutdown sequence started...")
        self.running = False

        try:
            logger.info("Stopping Telegram bot...")
            await self.bot.shutdown()
        except Exception:
            logger.exception("Telegram bot shutdown failed")

        try:
            logger.info("Stopping scheduler...")
            await scheduler_service.shutdown()
        except Exception:
            logger.exception("Scheduler shutdown failed")

        try:
            logger.info("Closing database...")
            await close_database()
        except Exception:
            logger.exception("Database shutdown failed")

        self.shutdown_event.set()
        logger.info("TeleOps-AI shutdown completed")

    async def run(self) -> None:
        try:
            self.running = True
            await self.initialize()
            logger.info("TeleOps-AI is fully operational")
            await self.start_bot()
            await self.shutdown_event.wait()
        except asyncio.CancelledError:
            logger.info("Application cancelled")
        except Exception:
            logger.exception("Fatal application error")
        finally:
            await self.shutdown()

async def main() -> None:
    application = TeleOpsApplication()
    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("Shutdown signal received")
        asyncio.create_task(application.shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")

    await application.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application interrupted")
