from __future__ import annotations

import logging
import os

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationHandlerStop,
    ContextTypes,
)

from app.services.auth_service import (
    AuthService,
)

load_dotenv()

logger = logging.getLogger(__name__)

auth_service = AuthService()

OWNER_IDS = {
    int(user_id.strip())
    for user_id in os.getenv(
        "TELEGRAM_ADMIN_IDS",
        ""
    ).split(",")
    if user_id.strip().isdigit()
}

DEFAULT_DENY = (
    os.getenv(
        "DEFAULT_DENY",
        "true"
    ).lower() == "true"
)


async def auth_middleware(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user is None:
        raise ApplicationHandlerStop

    telegram_user = update.effective_user

    try:
        user = (
            await auth_service
            .authenticate_telegram_user(
                telegram_user
            )
        )

        context.user_data["user"] = user
        context.user_data["role"] = (
            user["role_name"]
        )

        telegram_id = telegram_user.id

        if (
            DEFAULT_DENY
            and telegram_id not in OWNER_IDS
        ):
            if update.effective_message:
                await update.effective_message.reply_text(
                    "❌ Access Denied"
                )

            logger.warning(
                "Default deny blocked user: %s",
                telegram_id
            )

            raise ApplicationHandlerStop

    except Exception as exc:
        logger.exception(
            "Authentication middleware error: %s",
            exc
        )

        if update.effective_message:
            await update.effective_message.reply_text(
                "❌ Access Denied"
            )

        raise ApplicationHandlerStop
