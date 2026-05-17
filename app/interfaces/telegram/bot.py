from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.database.base import (
    db,
)
from app.interfaces.telegram.handlers import (
    register_handlers,
)
from app.interfaces.telegram.middleware import (
    auth_middleware,
)

load_dotenv()

logging.basicConfig(
    format=(
        "%(asctime)s | "
        "%(name)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
    level=logging.INFO
)

logger = logging.getLogger(__name__)


class TelegramBot:
    def __init__(self):
        self.token = os.getenv(
            "TELEGRAM_BOT_TOKEN"
        )

        if not self.token:
            raise RuntimeError(
                "TELEGRAM_BOT_TOKEN is missing"
            )

        self.application = (
            Application.builder()
            .token(self.token)
            .build()
        )

    async def startup(self) -> None:
        logger.info(
            "Telegram bot startup completed"
        )

    async def shutdown(self) -> None:
        logger.info(
            "Closing database connection..."
        )

        await db.disconnect()

        logger.info(
            "Telegram bot shutdown completed"
        )

    async def global_error_handler(
        self,
        update: object,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        logger.exception(
            "Unhandled Telegram error",
            exc_info=context.error
        )

        if isinstance(update, Update):
            if update.effective_message:
                try:
                    await update.effective_message.reply_text(
                        "❌ Internal server error"
                    )

                except Exception:
                    logger.exception(
                        "Failed to send "
                        "error message"
                    )

    def setup(self) -> None:
        self.application.add_handler(
            MessageHandler(
                filters.ALL,
                auth_middleware
            ),
            group=-1
        )

        register_handlers(
            self.application
        )

        self.application.add_error_handler(
            self.global_error_handler
        )

        logger.info(
            "Telegram handlers registered"
        )

    async def run(self) -> None:
        await self.startup()

        self.setup()

        logger.info(
            "Initializing Telegram application..."
        )

        await self.application.initialize()

        logger.info(
            "Starting Telegram application..."
        )

        await self.application.start()

        if self.application.updater is None:
            raise RuntimeError(
                "Telegram updater is unavailable"
            )

        logger.info(
            "Starting Telegram polling..."
        )

        await self.application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False
        )

        logger.info(
            "Telegram bot is running"
        )
