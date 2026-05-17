from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import (
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


async def ai_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    context.user_data[
        "ai_chat_mode"
    ] = True

    await update.effective_message.reply_text(
        (
            "🤖 AI Chat Mode Enabled\n\n"
            "Send messages directly."
        )
    )


async def ai_chat_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not context.user_data.get(
        "ai_chat_mode"
    ):
        return

    if not update.effective_message:
        return

    if not update.effective_user:
        return

    text = (
        update.effective_message.text
        .strip()
    )

    if not text:
        return

    processing_message = (
        await update.effective_message.reply_text(
            "🧠 Processing..."
        )
    )

    try:
        result = (
            await ai_service
            .process_user_message(
                telegram_user_id=(
                    update.effective_user.id
                ),
                user_message=text
            )
        )

        if result["type"] == "chat":
            response_text = (
                result["summary"]
            )

        else:
            intent_data = (
                result["intent_data"]
            )

            response_text = (
                "🧠 Intent Analysis\n\n"
                f"Intent: "
                f"{intent_data.get('intent')}\n"
                f"Confidence: "
                f"{intent_data.get('confidence')}\n"
                f"Summary: "
                f"{intent_data.get('summary')}"
            )

        await processing_message.edit_text(
            response_text
        )

    except Exception as exc:
        logger.exception(
            "AI chat handler failed: %s",
            exc
        )

        await processing_message.edit_text(
            "❌ AI processing failed"
        )


async def exit_ai_chat_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    context.user_data[
        "ai_chat_mode"
    ] = False

    await update.effective_message.reply_text(
        "🚪 AI Chat Mode Disabled"
    )


def register_ai_chat_handlers(
    application
) -> None:
    application.add_handler(
        CommandHandler(
            "ai",
            ai_command
        )
    )

    application.add_handler(
        CommandHandler(
            "exitai",
            exit_ai_chat_command
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            ai_chat_handler
        )
    )
