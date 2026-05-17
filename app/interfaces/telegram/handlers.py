from __future__ import annotations

from telegram.ext import (
    Application,
    CommandHandler,
)

from app.interfaces.telegram.commands.admin import (
    register_admin_handlers,
)
from app.interfaces.telegram.commands.ai_chat import (
    register_ai_chat_handlers,
)
from app.interfaces.telegram.commands.calendar import (
    register_calendar_handlers,
)
from app.interfaces.telegram.commands.system import (
    help_command,
    start_command,
    status_command,
)


def register_handlers(
    application: Application
) -> None:
    application.add_handler(
        CommandHandler(
            "start",
            start_command
        )
    )

    application.add_handler(
        CommandHandler(
            "help",
            help_command
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status_command
        )
    )

    register_ai_chat_handlers(
        application
    )

    register_calendar_handlers(
        application
    )

    register_admin_handlers(
        application
    )
