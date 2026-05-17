from __future__ import annotations

import os
from datetime import datetime

import pytz
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
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
from app.services.reminder_service import (
    ReminderService,
)

load_dotenv()

TIMEZONE = os.getenv(
    "TIMEZONE",
    "Asia/Bangkok"
)

timezone = pytz.timezone(
    TIMEZONE
)

user_repository = UserRepository()
reminder_service = ReminderService()


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
        text="📆 Calendar Menu",
        reply_markup=InlineKeyboardMarkup(
            keyboard
        )
    )


async def calendar_callback_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query

    if query is None:
        return

    await query.answer()

    telegram_user = update.effective_user

    if telegram_user is None:
        return

    user = (
        await user_repository
        .get_by_telegram_id(
            telegram_user.id
        )
    )

    if user is None:
        await query.edit_message_text(
            "❌ User not found"
        )

        return

    if query.data == "calendar_add":
        context.user_data[
            "calendar_create_mode"
        ] = True

        await query.edit_message_text(
            (
                "📝 Send event using format:\n\n"
                "title | YYYY-MM-DD HH:MM | description\n\n"
                f"Timezone: {TIMEZONE}"
            )
        )

        return

    if query.data == "calendar_list":
        reminders = (
            await reminder_service
            .list_user_reminders(
                user["id"]
            )
        )

        if not reminders:
            await query.edit_message_text(
                "📭 No events found"
            )

            return

        keyboard = []

        lines = []

        for reminder in reminders:
            remind_at = (
                reminder["remind_at"]
            )

            lines.append(
                (
                    f"📌 {reminder['title']}\n"
                    f"⏰ {remind_at}"
                )
            )

            keyboard.append([
                InlineKeyboardButton(
                    text=(
                        f"🗑 Delete "
                        f"{reminder['id']}"
                    ),
                    callback_data=(
                        f"delete_event_"
                        f"{reminder['id']}"
                    )
                )
            ])

        await query.edit_message_text(
            text="\n\n".join(lines),
            reply_markup=InlineKeyboardMarkup(
                keyboard
            )
        )


async def create_event_message_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not context.user_data.get(
        "calendar_create_mode"
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

    telegram_user = (
        update.effective_user
    )

    user = (
        await user_repository
        .get_by_telegram_id(
            telegram_user.id
        )
    )

    if user is None:
        await update.effective_message.reply_text(
            "❌ User not found"
        )

        return

    parts = [
        part.strip()
        for part in user_message.split("|")
    ]

    if len(parts) < 2:
        await update.effective_message.reply_text(
            (
                "❌ Invalid format\n\n"
                "Example:\n"
                "Meeting | 2026-05-18 14:00 | Team sync"
            )
        )

        return

    title = parts[0]

    description = ""

    if len(parts) >= 3:
        description = parts[2]

    try:
        naive_datetime = datetime.strptime(
            parts[1],
            "%Y-%m-%d %H:%M"
        )

        localized_datetime = (
            timezone.localize(
                naive_datetime
            )
        )

    except ValueError:
        await update.effective_message.reply_text(
            (
                "❌ Invalid date/time format\n\n"
                "Use:\n"
                "YYYY-MM-DD HH:MM"
            )
        )

        return

    current_time = datetime.now(
        timezone
    )

    if localized_datetime <= current_time:
        await update.effective_message.reply_text(
            (
                "❌ Event time must be "
                "in the future"
            )
        )

        return

    reminder_id = (
        await reminder_service
        .create_reminder(
            user_id=user["id"],
            title=title,
            description=description,
            remind_at=localized_datetime
        )
    )

    context.user_data[
        "calendar_create_mode"
    ] = False

    formatted_time = (
        localized_datetime.strftime(
            "%Y-%m-%d %H:%M %Z"
        )
    )

    await update.effective_message.reply_text(
        (
            "✅ Event created successfully\n\n"
            f"🆔 ID: {reminder_id}\n"
            f"📌 Title: {title}\n"
            f"⏰ Time: {formatted_time}\n"
            f"📝 Description: "
            f"{description or 'None'}"
        )
    )


async def delete_event_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query

    if query is None:
        return

    await query.answer()

    reminder_id = int(
        query.data.split("_")[-1]
    )

    await reminder_service.delete_reminder(
        reminder_id
    )

    await query.edit_message_text(
        (
            "🗑 Event deleted successfully\n\n"
            f"Event ID: {reminder_id}"
        )
    )


def register_calendar_handlers(
    application: Application
) -> None:
    application.add_handler(
        CommandHandler(
            "calendar",
            calendar_command
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            calendar_callback_handler,
            pattern=r"^calendar_"
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            delete_event_callback,
            pattern=r"^delete_event_"
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            create_event_message_handler
        )
    )
