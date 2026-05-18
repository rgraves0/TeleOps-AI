from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

from src.plugins.base import (
    BasePlugin,
    BaseTool,
    ToolContext,
    ToolResult,
)

logger = logging.getLogger(__name__)


# =========================================================
# TOOL REGISTRY
# =========================================================


class ToolRegistry:

    def __init__(
        self,
    ) -> None:

        self.plugins: dict[
            str,
            BasePlugin
        ] = {}

        self.tools: dict[
            str,
            BaseTool
        ] = {}

        logger.info(
            "ToolRegistry initialized"
        )

    # =====================================================
    # REGISTER PLUGIN
    # =====================================================

    async def register_plugin(
        self,
        plugin: BasePlugin,
    ) -> None:

        if (
            plugin.name
            in self.plugins
        ):

            raise RuntimeError(
                (
                    "Plugin already exists: "
                    f"{plugin.name}"
                )
            )

        await plugin.startup()

        self.plugins[
            plugin.name
        ] = plugin

        for tool in plugin.tools():

            await self.register_tool(
                tool
            )

        logger.info(
            "Registered plugin=%s",
            plugin.name,
        )

    # =====================================================
    # REGISTER TOOL
    # =====================================================

    async def register_tool(
        self,
        tool: BaseTool,
    ) -> None:

        if (
            tool.name
            in self.tools
        ):

            raise RuntimeError(
                (
                    "Tool already exists: "
                    f"{tool.name}"
                )
            )

        self.tools[
            tool.name
        ] = tool

        logger.info(
            "Registered tool=%s",
            tool.name,
        )

    # =====================================================
    # REMOVE PLUGIN
    # =====================================================

    async def remove_plugin(
        self,
        plugin_name: str,
    ) -> None:

        plugin = (
            self.plugins.get(
                plugin_name
            )
        )

        if not plugin:
            return

        await plugin.shutdown()

        for tool in plugin.tools():

            self.tools.pop(
                tool.name,
                None,
            )

        self.plugins.pop(
            plugin_name,
            None,
        )

        logger.warning(
            "Removed plugin=%s",
            plugin_name,
        )

    # =====================================================
    # REMOVE TOOL
    # =====================================================

    async def remove_tool(
        self,
        tool_name: str,
    ) -> None:

        self.tools.pop(
            tool_name,
            None,
        )

        logger.warning(
            "Removed tool=%s",
            tool_name,
        )

    # =====================================================
    # GET TOOL
    # =====================================================

    def get_tool(
        self,
        tool_name: str,
    ) -> BaseTool | None:

        return self.tools.get(
            tool_name
        )

    # =====================================================
    # GET PLUGIN
    # =====================================================

    def get_plugin(
        self,
        plugin_name: str,
    ) -> BasePlugin | None:

        return self.plugins.get(
            plugin_name
        )

    # =====================================================
    # EXECUTE TOOL
    # =====================================================

    async def execute_tool(
        self,
        tool_name: str,
        payload: dict,
        context: ToolContext,
    ) -> ToolResult:

        tool = self.get_tool(
            tool_name
        )

        if tool is None:

            return ToolResult(

                success=False,

                error=(
                    f"Tool not found: "
                    f"{tool_name}"
                ),
            )

        if not tool.enabled:

            return ToolResult(

                success=False,

                error=(
                    f"Tool disabled: "
                    f"{tool_name}"
                ),
            )

        if not tool.has_permission(
            context
        ):

            return ToolResult(

                success=False,

                error=(
                    "Permission denied"
                ),
            )

        return await (
            tool.safe_execute(
                payload,
                context,
            )
        )

    # =====================================================
    # HEALTHCHECK
    # =====================================================

    async def healthcheck(
        self,
    ) -> dict:

        results = {}

        for name, tool in (
            self.tools.items()
        ):

            try:

                healthy = await (
                    tool.healthcheck()
                )

                results[name] = (
                    healthy
                )

            except Exception:

                logger.exception(
                    "Healthcheck failed=%s",
                    name,
                )

                results[name] = False

        return results

    # =====================================================
    # LIST TOOLS
    # =====================================================

    def list_tools(
        self,
    ) -> list[dict]:

        return [

            tool.info()

            for tool
            in self.tools.values()
        ]

    # =====================================================
    # LIST PLUGINS
    # =====================================================

    def list_plugins(
        self,
    ) -> list[dict]:

        return [

            plugin.info()

            for plugin
            in self.plugins.values()
        ]

    # =====================================================
    # STATS
    # =====================================================

    def stats(
        self,
    ) -> dict:

        return {

            "plugin_count":
            len(
                self.plugins
            ),

            "tool_count":
            len(
                self.tools
            ),

            "plugins":
            list(
                self.plugins.keys()
            ),

            "tools":
            list(
                self.tools.keys()
            ),
        }


# =========================================================
# GLOBAL REGISTRY
# =========================================================


tool_registry = (
    ToolRegistry()
)
