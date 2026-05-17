from __future__ import annotations

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
)

from app.core.permissions import (
    role_required,
)
from app.database.repositories.users import (
    UserRepository,
)
from app.services.auth_service import (
    AuthService,
)

user_repository = UserRepository()
auth_service = AuthService()


@role_required(["owner", "admin"])
async def admin_panel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    users = (
        await user_repository.list_users()
    )

    if not users:
        await update.effective_message.reply_text(
            "No users found."
        )

        return

    keyboard = []

    for user in users:
        keyboard.append([
            InlineKeyboardButton(
                text=(
                    f"{user['full_name']} "
                    f"({user['role_name']})"
                ),
                callback_data=(
                    f"admin_user_"
                    f"{user['id']}"
                )
            )
        ])

    await update.effective_message.reply_text(
        "🛡 Admin Control Panel",
        reply_markup=InlineKeyboardMarkup(
            keyboard
        )
    )


@role_required(["owner", "admin"])
async def admin_user_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query

    if query is None:
        return

    await query.answer()

    user_id = int(
        query.data.split("_")[-1]
    )

    user = await user_repository.get_by_id(
        user_id
    )

    if user is None:
        await query.edit_message_text(
            "❌ User not found"
        )

        return

    keyboard = [
        [
            InlineKeyboardButton(
                text="Promote Admin",
                callback_data=(
                    f"promote_"
                    f"{user_id}"
                )
            )
        ],
        [
            InlineKeyboardButton(
                text="Ban User",
                callback_data=(
                    f"ban_"
                    f"{user_id}"
                )
            )
        ]
    ]

    username = (
        f"@{user['username']}"
        if user["username"]
        else "No username"
    )

    message = (
        "👤 User Information\n\n"
        f"Name: {user['full_name']}\n"
        f"Username: {username}\n"
        f"Role: {user['role_name']}\n"
        f"Telegram ID: {user['telegram_id']}"
    )

    await query.edit_message_text(
        text=message,
        reply_markup=InlineKeyboardMarkup(
            keyboard
        )
    )


@role_required(["owner"])
async def promote_user_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query

    if query is None:
        return

    await query.answer()

    user_id = int(
        query.data.split("_")[-1]
    )

    user = await user_repository.get_by_id(
        user_id
    )

    if user is None:
        await query.edit_message_text(
            "❌ User not found"
        )

        return

    await auth_service.assign_role(
        telegram_id=user["telegram_id"],
        role_name="admin"
    )

    updated_user = (
        await user_repository.get_by_id(
            user_id
        )
    )

    role_name = "admin"

    if updated_user:
        role_name = (
            updated_user["role_name"]
        )

    await query.edit_message_text(
        (
            "✅ User promoted successfully\n\n"
            f"👤 Name: {user['full_name']}\n"
            f"🛡 New Role: {role_name}"
        )
    )


@role_required(["owner", "admin"])
async def ban_user_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query

    if query is None:
        return

    await query.answer()

    user_id = int(
        query.data.split("_")[-1]
    )

    user = await user_repository.get_by_id(
        user_id
    )

    if user is None:
        await query.edit_message_text(
            "❌ User not found"
        )

        return

    await user_repository.ban_user(
        user_id
    )

    await query.edit_message_text(
        (
            "🚫 User banned successfully\n\n"
            f"👤 {user['full_name']}"
        )
    )


def register_admin_handlers(
    application: Application
) -> None:
    application.add_handler(
        CommandHandler(
            "admin",
            admin_panel_command
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            admin_user_callback,
            pattern=r"^admin_user_"
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            promote_user_callback,
            pattern=r"^promote_"
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            ban_user_callback,
            pattern=r"^ban_"
        )
    )
