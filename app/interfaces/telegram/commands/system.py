from __future__ import annotations

import platform
import sqlite3
import time

import psutil
from telegram import Update
from telegram.ext import ContextTypes

from app.core.scheduler import (
    scheduler_manager,
)
from app.database.base import (
    DATABASE_PATH,
)
from app.plugins.loader import (
    plugin_loader,
)

START_TIME = time.time()


async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    user = context.user_data.get(
        "user",
        {}
    )

    full_name = user.get(
        "full_name",
        "Unknown"
    )

    role_name = user.get(
        "role_name",
        "unknown"
    )

    message = (
        "🤖 TeleOps-AI Online\n\n"
        f"👤 User: {full_name}\n"
        f"🛡 Role: {role_name}\n\n"
        "Use /help to see commands."
    )

    await update.effective_message.reply_text(
        message
    )


async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    help_text = (
        "📘 TeleOps-AI Commands\n\n"
        "/start - Start bot\n"
        "/help - Show help\n"
        "/status - System status\n"
        "/calendar - Open calendar menu\n"
        "/events - List reminders/events\n"
        "/ai - AI assistant mode\n"
        "/clear - Clear AI memory\n"
        "/whoami - Show account info\n\n"
        "Admin Commands:\n"
        "/admin - Admin panel"
    )

    await update.effective_message.reply_text(
        help_text
    )


async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.effective_message:
        return

    cpu_usage = psutil.cpu_percent()

    ram = psutil.virtual_memory()

    ram_used_mb = (
        ram.used / 1024 / 1024
    )

    ram_total_mb = (
        ram.total / 1024 / 1024
    )

    uptime_seconds = int(
        time.time() - START_TIME
    )

    db_state = await check_database()

    loaded_plugins = (
        plugin_loader.list_plugins()
    )

    plugin_lines = []

    for plugin in loaded_plugins:
        state = (
            "✅"
            if plugin["enabled"]
            else "❌"
        )

        plugin_lines.append(
            f"{state} {plugin['name']}"
        )

    plugin_text = "\n".join(
        plugin_lines
    )

    if not plugin_text:
        plugin_text = "No plugins loaded"

    scheduler_jobs = len(
        scheduler_manager.list_jobs()
    )

    message = (
        "📊 TeleOps-AI Status\n\n"
        f"🖥 Platform: {platform.system()}\n"
        f"🐍 Python: {platform.python_version()}\n"
        f"⚡ CPU Usage: {cpu_usage}%\n"
        f"🧠 RAM Usage: "
        f"{ram_used_mb:.2f} MB / "
        f"{ram_total_mb:.2f} MB\n"
        f"⏱ Uptime: {uptime_seconds}s\n"
        f"🗄 Database: {db_state}\n"
        f"📅 Scheduled Jobs: {scheduler_jobs}\n\n"
        f"🔌 Plugins:\n{plugin_text}"
    )

    await update.effective_message.reply_text(
        message
    )


async def check_database() -> str:
    try:
        connection = sqlite3.connect(
            DATABASE_PATH
        )

        connection.execute(
            "SELECT 1"
        )

        connection.close()

        return "Connected"

    except Exception:
        return "Disconnected"
