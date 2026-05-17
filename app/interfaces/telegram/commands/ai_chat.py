from __future__ import annotations

import logging

from telegram import Update
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
            "Send messages directly to chat with AI."
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
    )

    if text is None:
        return

    user_message = text.strip()

    if not user_message:
        return

    telegram_user_id = (
        update.effective_user.id
    )

    processing_message = (
        await update.effective_message.reply_text(
            "🧠 Processing..."
        )
    )

    try:
        result = (
            await ai_service
            .process_user_message(
                telegram_user_id=telegram_user_id,
                user_message=user_message
            )
        )

        result_type = result.get(
            "type",
            "unknown"
        )

        if result_type == "chat":
            response_text = result.get(
                "summary"
            )

            if not response_text:
                response_text = result.get(
                    "response",
                    "No response generated."
                )

        else:
            intent_data = result.get(
                "intent_data",
                {}
            )

            detected_intent = (
                intent_data.get(
                    "intent",
                    "unknown"
                )
            )

            confidence = (
                intent_data.get(
                    "confidence",
                    0.0
                )
            )

            summary = (
                intent_data.get(
                    "summary",
                    "No summary available."
                )
            )

            language = (
                intent_data.get(
                    "language",
                    "unknown"
                )
            )

            action_required = (
                intent_data.get(
                    "action_required",
                    False
                )
            )

            response_text = (
                "🧠 Intent Analysis\n\n"
                f"📌 Intent: {detected_intent}\n"
                f"🌐 Language: {language}\n"
                f"📊 Confidence: {confidence}\n"
                f"⚡ Action Required: {action_required}\n\n"
                f"📝 Summary:\n{summary}"
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
    application: Application
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
