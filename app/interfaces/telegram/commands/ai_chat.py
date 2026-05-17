from __future__ import annotations

import logging

from telegram import (
    Update,
)
from telegram.constants import (
    ChatAction,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.services.ai_service import (
    AIService,
)

logger = logging.getLogger(__name__)

ai_service = AIService()


async def ai_mode_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    context.user_data["ai_mode"] = True

    await update.message.reply_text(
        (
            "🤖 AI Chat Mode Enabled\n\n"
            "You can now chat naturally with the AI assistant.\n"
            "Tools, reminders, search, and actions are available automatically.\n\n"
            "Use /exitai to leave AI mode."
        )
    )


async def exit_ai_mode_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    context.user_data["ai_mode"] = False

    await update.message.reply_text(
        (
            "✅ AI Chat Mode Disabled\n\n"
            "You are now back in normal bot command mode."
        )
    )


async def ai_chat_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.message:
        return

    ai_mode = context.user_data.get(
        "ai_mode",
        False
    )

    if not ai_mode:
        return

    user_message = (
        update.message.text or ""
    ).strip()

    if not user_message:
        return

    telegram_user = update.effective_user

    if telegram_user is None:
        return

    telegram_user_id = (
        telegram_user.id
    )

    processing_message = (
        await update.message.reply_text(
            "🧠 Processing..."
        )
    )

    try:
        await context.bot.send_chat_action(
            chat_id=update.effective_chat.id,
            action=ChatAction.TYPING
        )

        result = await (
            ai_service.process_user_message(
                telegram_user_id=telegram_user_id,
                message=user_message
            )
        )

        response_text = ""

        if isinstance(result, dict):
            response_text = (
                result.get("response")
                or result.get("message")
                or result.get("summary")
                or "⚠️ Empty AI response"
            )

        else:
            response_text = str(result)

        if not response_text.strip():
            response_text = (
                "⚠️ AI returned an empty response."
            )

        await processing_message.edit_text(
            response_text[:4096]
        )

    except Exception as exc:
        logger.exception(
            "AI chat handler failed: %s",
            exc
        )

        await processing_message.edit_text(
            (
                "❌ AI request failed.\n"
                "Please try again later."
            )
        )


def register_ai_chat_handlers(
    application: Application
) -> None:
    application.add_handler(
        CommandHandler(
            "ai",
            ai_mode_command
        )
    )

    application.add_handler(
        CommandHandler(
            "exitai",
            exit_ai_mode_command
        )
    )

    application.add_handler(
        MessageHandler(
            (
                filters.TEXT
                & ~filters.COMMAND
            ),
            ai_chat_handler
        ),
        group=0
    )
