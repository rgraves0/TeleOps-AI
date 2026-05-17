from __future__ import annotations

import importlib
import logging
from pathlib import Path
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)


class PluginError(Exception):
    pass


class PluginLoader:
    def __init__(
        self,
        plugins_directory: str = "app/plugins"
    ):
        self.plugins_directory = Path(
            plugins_directory
        )

        self.loaded_plugins: dict[
            str,
            ModuleType
        ] = {}

        self.enabled_plugins: set[str] = set()

    def discover_plugins(
        self
    ) -> list[str]:
        plugins: list[str] = []

        if not self.plugins_directory.exists():
            return plugins

        for item in self.plugins_directory.iterdir():
            if not item.is_dir():
                continue

            if item.name.startswith("_"):
                continue

            plugin_file = (
                item / "plugin.py"
            )

            if plugin_file.exists():
                plugins.append(item.name)

        return sorted(plugins)

    def load_plugin(
        self,
        plugin_name: str
    ) -> bool:
        if plugin_name in self.loaded_plugins:
            return True

        module_path = (
            f"app.plugins."
            f"{plugin_name}.plugin"
        )

        try:
            module = importlib.import_module(
                module_path
            )

            self.loaded_plugins[
                plugin_name
            ] = module

            self.enabled_plugins.add(
                plugin_name
            )

            logger.info(
                "Loaded plugin: %s",
                plugin_name
            )

            return True

        except Exception as exc:
            logger.exception(
                "Failed to load plugin %s: %s",
                plugin_name,
                exc
            )

            raise PluginError(
                f"Plugin load failed: "
                f"{plugin_name}"
            ) from exc

    def load_all_plugins(
        self
    ) -> None:
        plugins = (
            self.discover_plugins()
        )

        for plugin_name in plugins:
            try:
                self.load_plugin(
                    plugin_name
                )

            except PluginError:
                continue

    def unload_plugin(
        self,
        plugin_name: str
    ) -> bool:
        if plugin_name not in self.loaded_plugins:
            return False

        del self.loaded_plugins[
            plugin_name
        ]

        self.enabled_plugins.discard(
            plugin_name
        )

        logger.info(
            "Unloaded plugin: %s",
            plugin_name
        )

        return True

    def enable_plugin(
        self,
        plugin_name: str
    ) -> bool:
        if plugin_name not in self.loaded_plugins:
            self.load_plugin(
                plugin_name
            )

        self.enabled_plugins.add(
            plugin_name
        )

        logger.info(
            "Enabled plugin: %s",
            plugin_name
        )

        return True

    def disable_plugin(
        self,
        plugin_name: str
    ) -> bool:
        if plugin_name not in self.loaded_plugins:
            return False

        self.enabled_plugins.discard(
            plugin_name
        )

        logger.info(
            "Disabled plugin: %s",
            plugin_name
        )

        return True

    def is_enabled(
        self,
        plugin_name: str
    ) -> bool:
        return (
            plugin_name
            in self.enabled_plugins
        )

    def get_plugin(
        self,
        plugin_name: str
    ) -> Any:
        if not self.is_enabled(
            plugin_name
        ):
            raise PluginError(
                f"Plugin disabled: "
                f"{plugin_name}"
            )

        plugin = self.loaded_plugins.get(
            plugin_name
        )

        if plugin is None:
            raise PluginError(
                f"Plugin not loaded: "
                f"{plugin_name}"
            )

        return plugin

    def list_plugins(
        self
    ) -> list[dict[str, Any]]:
        discovered = (
            self.discover_plugins()
        )

        results = []

        for plugin_name in discovered:
            results.append({
                "name": plugin_name,
                "loaded": (
                    plugin_name
                    in self.loaded_plugins
                ),
                "enabled": (
                    plugin_name
                    in self.enabled_plugins
                )
            })

        return results


plugin_loader = PluginLoader()
