from __future__ import annotations

import logging
from datetime import datetime

from telegram import Update
from telegram.ext import (
    CommandHandler,
    ContextTypes,
)

from src.core.config import (
    AppConfig,
)
from src.interfaces.telegram.admin_commands import (
    AdminAccessManager,
)
from src.db.repositories import (
    ProviderStateRepository,
)
from src.plugins.runtime import (
    PluginRuntime,
)

logger = logging.getLogger(__name__)


# =========================================================
# PROVIDER COMMANDS
# =========================================================


class ProviderCommands:

    def __init__(
        self,
        config: AppConfig,
        provider_repo: (
            ProviderStateRepository
        ),
        runtime: PluginRuntime,
    ) -> None:

        self.config = config

        self.provider_repo = (
            provider_repo
        )

        self.runtime = runtime

        self.access = (
            AdminAccessManager(
                config
            )
        )

        logger.info(
            "ProviderCommands initialized"
        )

    # =====================================================
    # HANDLERS
    # =====================================================

    def handlers(
        self,
    ) -> list[CommandHandler]:

        return [

            CommandHandler(
                "providers",
                self.providers,
            ),

            CommandHandler(
                "workers",
                self.workers,
            ),

            CommandHandler(
                "cooldowns",
                self.cooldowns,
            ),
        ]

    # =====================================================
    # PROVIDERS
    # =====================================================

    async def providers(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):
            return

        provider_names = [

            "openrouter",

            "groq",
        ]

        lines = [

            "🧠 Providers",

            "",
        ]

        for provider_name in (
            provider_names
        ):

            provider = await (
                self.provider_repo
                .get_provider(
                    provider_name
                )
            )

            if not provider:

                lines.extend(

                    [

                        (
                            f"• {provider_name}"
                        ),

                        "  Status: unknown",

                        "",
                    ]
                )

                continue

            lines.extend(

                [

                    (
                        f"• {provider_name}"
                    ),

                    (
                        f"  Status: "
                        f"{provider['status']}"
                    ),

                    (
                        f"  Requests: "
                        f"{provider['total_requests']}"
                    ),

                    (
                        f"  Failed: "
                        f"{provider['failed_requests']}"
                    ),

                    "",
                ]
            )

        await (
            update.effective_message
            .reply_text(
                "\n".join(lines)
            )
        )

    # =====================================================
    # WORKERS
    # =====================================================

    async def workers(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):
            return

        runtime_stats = (
            self.runtime
            .stats()
        )

        lines = [

            "👷 Workers",

            "",

            (
                f"Executions: "
                f"{runtime_stats['total_executions']}"
            ),

            (
                f"Success: "
                f"{runtime_stats['successful_executions']}"
            ),

            (
                f"Failed: "
                f"{runtime_stats['failed_executions']}"
            ),

            (
                f"Running: "
                f"{runtime_stats['running_tasks']}"
            ),
        ]

        await (
            update.effective_message
            .reply_text(
                "\n".join(lines)
            )
        )

    # =====================================================
    # COOLDOWNS
    # =====================================================

    async def cooldowns(
        self,
        update: Update,
        context: (
            ContextTypes.DEFAULT_TYPE
        ),
    ) -> None:

        if not await (
            self.access.require_admin(
                update
            )
        ):
            return

        provider_names = [

            "openrouter",

            "groq",
        ]

        lines = [

            "⏳ Provider Cooldowns",

            "",
        ]

        for provider_name in (
            provider_names
        ):

            provider = await (
                self.provider_repo
                .get_provider(
                    provider_name
                )
            )

            if not provider:

                continue

            cooldown = (
                provider.get(
                    "cooldown_until"
                )
            )

            if not cooldown:

                cooldown = (
                    "No cooldown"
                )

            lines.extend(

                [

                    (
                        f"• {provider_name}"
                    ),

                    (
                        f"  Cooldown: "
                        f"{cooldown}"
                    ),

                    "",
                ]
            )

        await (
            update.effective_message
            .reply_text(
                "\n".join(lines)
            )
        )
