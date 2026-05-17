from __future__ import annotations

import asyncio
import contextlib
import html
import logging

from telegram import (
    Update,
)
from telegram.constants import (
    ChatAction,
    ParseMode,
)
from telegram.ext import (
    ContextTypes,
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
    context.user_data["ai_mode"] = True

    message = (
        "🤖 <b>AI Chat Mode Enabled</b>\n\n"
        "You can now chat naturally with TeleOps-AI.\n\n"
        "Examples:\n"
        "• Search latest AI news\n"
        "• Check unread emails\n"
        "• Find backup.zip\n"
        "• What's the weather in Bangkok?\n\n"
        "Use /exitai to leave AI mode."
    )

    await update.message.reply_text(
        text=message,
        parse_mode=ParseMode.HTML
    )


async def exit_ai_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    context.user_data["ai_mode"] = False

    await update.message.reply_text(
        text=(
            "✅ <b>AI Chat Mode Disabled</b>\n\n"
            "You are now back in normal command mode."
        ),
        parse_mode=ParseMode.HTML
    )


async def clear_memory_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    telegram_user_id = (
        update.effective_user.id
    )

    await ai_service.clear_memory(
        telegram_user_id
    )

    await update.message.reply_text(
        text=(
            "🧠 <b>Conversation memory cleared.</b>"
        ),
        parse_mode=ParseMode.HTML
    )


async def typing_indicator_loop(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    stop_event: asyncio.Event
) -> None:
    try:
        while not stop_event.is_set():
            await context.bot.send_chat_action(
                chat_id=chat_id,
                action=ChatAction.TYPING
            )

            await asyncio.sleep(4)

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        logger.exception(
            "Typing indicator failed: %s",
            exc
        )


async def update_processing_message(
    processing_message,
    text: str
) -> None:
    try:
        await processing_message.edit_text(
            text=text,
            parse_mode=ParseMode.HTML
        )

    except Exception as exc:
        logger.debug(
            "Processing message update skipped: %s",
            exc
        )


def detect_processing_stage(
    user_message: str
) -> str:
    lowered = user_message.lower()

    if any(
        keyword in lowered
        for keyword in [
            "email",
            "mail",
            "inbox"
        ]
    ):
        return (
            "📧 <b>Fetching unread emails...</b>"
        )

    if any(
        keyword in lowered
        for keyword in [
            "weather",
            "temperature",
            "rain"
        ]
    ):
        return (
            "🌦 <b>Checking weather data...</b>"
        )

    if any(
        keyword in lowered
        for keyword in [
            "search",
            "news",
            "google",
            "internet"
        ]
    ):
        return (
            "🔎 <b>Searching the web...</b>"
        )

    if any(
        keyword in lowered
        for keyword in [
            ".zip",
            ".pdf",
            ".doc",
            ".docx",
            "find",
            "storage",
            "backup"
        ]
    ):
        return (
            "🗂 <b>Searching cloud storage...</b>"
        )

    if any(
        keyword in lowered
        for keyword in [
            "system",
            "status",
            "cpu",
            "ram"
        ]
    ):
        return (
            "🖥 <b>Collecting system status...</b>"
        )

    return (
        "🤖 <b>Thinking...</b>"
    )


def format_ai_response(
    response_text: str
) -> str:
    cleaned = response_text.strip()

    cleaned = html.escape(cleaned)

    cleaned = cleaned.replace(
        "\n",
        "<br>"
    )

    return cleaned


async def ai_chat_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if (
        update.message is None
        or update.effective_user is None
    ):
        return

    telegram_user_id = (
        update.effective_user.id
    )

    user_message = (
        update.message.text or ""
    ).strip()

    if not user_message:
        return

    ai_mode = context.user_data.get(
        "ai_mode",
        False
    )

    if not ai_mode:
        return

    chat_id = (
        update.effective_chat.id
    )

    processing_message = (
        await update.message.reply_text(
            text=(
                "🤖 <b>Processing request...</b>"
            ),
            parse_mode=ParseMode.HTML
        )
    )

    processing_stage = (
        detect_processing_stage(
            user_message
        )
    )

    await update_processing_message(
        processing_message,
        processing_stage
    )

    stop_event = asyncio.Event()

    typing_task = asyncio.create_task(
        typing_indicator_loop(
            context=context,
            chat_id=chat_id,
            stop_event=stop_event
        )
    )

    try:
        result = await (
            ai_service.process_user_message(
                telegram_user_id=(
                    telegram_user_id
                ),
                message=user_message
            )
        )

        response_text = (
            result.get(
                "response",
                "No response generated."
            )
        )

        formatted_response = (
            format_ai_response(
                response_text
            )
        )

        response_type = (
            result.get(
                "type",
                "chat"
            )
        )

        header = ""

        if response_type == "workflow":
            header = (
                "⚡ <b>Workflow Completed</b>\n\n"
            )

        elif response_type == "tool":
            header = (
                "🛠 <b>Task Completed</b>\n\n"
            )

        elif response_type == "error":
            header = (
                "⚠️ <b>Processing Issue</b>\n\n"
            )

        final_message = (
            f"{header}"
            f"{formatted_response}"
        )

        await processing_message.edit_text(
            text=final_message,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )

    except Exception as exc:
        logger.exception(
            "AI chat handler failed: %s",
            exc
        )

        await processing_message.edit_text(
            text=(
                "⚠️ <b>Something went wrong.</b>"
                "<br><br>"
                "Please try again."
            ),
            parse_mode=ParseMode.HTML
        )

    finally:
        stop_event.set()

        typing_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):
            await typing_task
