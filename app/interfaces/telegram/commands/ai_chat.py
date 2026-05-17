from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from datetime import datetime

import pytz
from dotenv import load_dotenv

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)

from telegram.constants import (
    ChatAction,
    ParseMode,
)

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.database.repositories.users import (
    UserRepository,
)

from app.services.ai_service import (
    AIService,
)

from app.services.reminder_service import (
    ReminderService,
)

load_dotenv()

logger = logging.getLogger(__name__)

TIMEZONE = os.getenv(
    "TIMEZONE",
    "Asia/Bangkok"
)

timezone = pytz.timezone(
    TIMEZONE
)

user_repository = UserRepository()

reminder_service = ReminderService()

ai_service = AIService()


# =========================================================
# AI MODE COMMANDS
# =========================================================

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
        "• What's the weather in Tokyo?\n"
        "• YGN to BKK flight schedule\n\n"
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


# =========================================================
# AI HELPERS
# =========================================================

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
            "internet",
            "flight",
            "schedule"
        ]
    ):

        return (
            "🌐 <b>Searching the web...</b>"
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
            "☁️ <b>Searching cloud storage...</b>"
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

    return "🤔 <b>Thinking...</b>"


def format_ai_response(
    response_text: str
) -> str:

    if not response_text:

        return "No response generated."

    cleaned = str(
        response_text
    ).strip()

    # =====================================================
    # ESCAPE TELEGRAM HTML SPECIAL CHARACTERS
    # =====================================================

    cleaned = (
        cleaned
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

    # =====================================================
    # NORMALIZE NEWLINES
    # =====================================================

    cleaned = cleaned.replace(
        "\r\n",
        "\n"
    )

    cleaned = cleaned.replace(
        "\r",
        "\n"
    )

    # =====================================================
    # REMOVE UNSUPPORTED HTML TAGS
    # =====================================================

    cleaned = cleaned.replace(
        "<br>",
        "\n"
    )

    cleaned = cleaned.replace(
        "<br/>",
        "\n"
    )

    cleaned = cleaned.replace(
        "<br />",
        "\n"
    )

    # =====================================================
    # TELEGRAM MESSAGE LIMIT SAFETY
    # =====================================================

    MAX_TELEGRAM_LENGTH = 3500

    if len(cleaned) > MAX_TELEGRAM_LENGTH:

        cleaned = (
            cleaned[
                :MAX_TELEGRAM_LENGTH
            ]
            + "\n\n..."
        )

    return cleaned


# =========================================================
# AI CHAT CORE
# =========================================================

async def ai_chat_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    if update.message is None:
        return

    if update.effective_user is None:
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

    chat_id = update.effective_chat.id

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

        result = (
            await ai_service.process_user_message(
                telegram_user_id=telegram_user_id,
                message=user_message
            )
        )

        response_text = result.get(
            "response",
            "No response generated."
        )

        formatted_response = (
            format_ai_response(
                response_text
            )
        )

        response_type = result.get(
            "type",
            "chat"
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
            f"{header}{formatted_response}"
        )

        try:

            await processing_message.edit_text(
                text=final_message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )

        except Exception:

            logger.exception(
                "Telegram HTML formatting failed"
            )

            safe_text = (
                final_message
                .replace("<b>", "")
                .replace("</b>", "")
            )

            safe_text = (
                safe_text
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&amp;", "&")
            )

            await processing_message.edit_text(
                text=safe_text[:3500],
                disable_web_page_preview=True
            )

        return

    except Exception as exc:

        logger.exception(
            "AI chat handler failed: %s",
            exc
        )

        try:

            await processing_message.edit_text(
                text=(
                    "⚠️ <b>Something went wrong.</b>\n\n"
                    "Please try again."
                ),
                parse_mode=ParseMode.HTML
            )

        except Exception:

            await processing_message.edit_text(
                text=(
                    "⚠️ Something went wrong.\n\n"
                    "Please try again."
                )
            )

    finally:

        stop_event.set()

        typing_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):

            await typing_task


# =========================================================
# CALENDAR COMMANDS
# =========================================================

async def calendar_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    if not update.effective_message:
        return

    keyboard = [
        [
            InlineKeyboardButton(
                text="➕ Add Event",
                callback_data="calendar_add"
            )
        ],
        [
            InlineKeyboardButton(
                text="📅 List Events",
                callback_data="calendar_list"
            )
        ]
    ]

    await update.effective_message.reply_text(
        text="📅 Calendar Menu",
        reply_markup=InlineKeyboardMarkup(
            keyboard
        )
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
            exit_ai_command
        )
    )

    application.add_handler(
        CommandHandler(
            "clear",
            clear_memory_command
        )
    )

    application.add_handler(
        CommandHandler(
            "calendar",
            calendar_command
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            ai_chat_handler
        ),
        group=0
    )
