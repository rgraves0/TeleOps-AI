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
    ).strip().lower()
    == "true"
)


async def auth_middleware(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user is None:
        logger.warning(
            "Blocked update without effective_user"
        )

        raise ApplicationHandlerStop

    telegram_user = (
        update.effective_user
    )

    telegram_id = telegram_user.id

    try:
        user = (
            await auth_service
            .authenticate_telegram_user(
                telegram_user
            )
        )

    except Exception as exc:
        logger.exception(
            "Authentication failed for "
            "telegram_id=%s: %s",
            telegram_id,
            exc
        )

        if update.effective_message:
            await update.effective_message.reply_text(
                "❌ Authentication Failed"
            )

        raise ApplicationHandlerStop

    if DEFAULT_DENY:
        if telegram_id not in OWNER_IDS:
            logger.warning(
                "Default deny blocked "
                "telegram_id=%s",
                telegram_id
            )

            if update.effective_message:
                await update.effective_message.reply_text(
                    "❌ Access Denied"
                )

            raise ApplicationHandlerStop

    context.user_data["user"] = user

    context.user_data["user_id"] = (
        user["id"]
    )

    context.user_data["telegram_id"] = (
        user["telegram_id"]
    )

    context.user_data["role"] = (
        user["role_name"]
    )

    context.user_data["full_name"] = (
        user["full_name"]
    )

    context.user_data["username"] = (
        user["username"]
    )

    logger.info(
        "Authenticated telegram_id=%s "
        "role=%s",
        telegram_id,
        user["role_name"]
    )
