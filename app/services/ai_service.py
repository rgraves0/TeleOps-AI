from __future__ import annotations

import json
import logging
import os
import platform
from collections import defaultdict
from datetime import datetime
from typing import Any

import psutil

from app.ai.prompts import (
    INTENT_PARSER_PROMPT,
    SYSTEM_PROMPT,
)
from app.ai.provider import (
    AIProvider,
    AIProviderException,
)
from app.plugins.loader import (
    plugin_loader,
)

logger = logging.getLogger(__name__)


SUMMARY_PROMPT = """
You are TeleOps-AI, a Telegram-native AI assistant.

Your job is to convert raw tool outputs into natural conversational replies.

Rules:
- Never expose raw JSON.
- Never expose internal intents, confidence values, or system reasoning.
- Keep responses concise and human-friendly.
- Reply in the SAME language used by the user.
- If the user used Burmese, reply naturally in Burmese.
- If the user used English, reply naturally in English.
- Summarize tool outputs clearly.
- If tools fail, explain the failure politely and naturally.
- Avoid robotic wording.
"""


class AIService:
    def __init__(self):
        self.provider = AIProvider()

        self.memory: dict[
            int,
            list[dict[str, str]]
        ] = defaultdict(list)

        self.max_memory_messages = 12

        self.tool_keywords = {
            "weather": [
                "weather",
                "temperature",
                "rain",
                "forecast",
                "climate",
                "ရာသီဥတု",
                "မိုးလေဝသ"
            ],
            "web_search": [
                "search",
                "google",
                "find",
                "lookup",
                "news",
                "latest",
                "ရှာ",
                "သတင်း"
            ],
            "system_status": [
                "status",
                "system",
                "cpu",
                "ram",
                "memory",
                "uptime",
                "health"
            ],
            "calendar_add": [
                "remind",
                "reminder",
                "schedule",
                "calendar",
                "meeting",
                "alarm",
                "သတိပေး",
                "အချိန်ဇယား"
            ]
        }

        self.intent_dispatcher = {
            "web_search": (
                self.handle_web_search
            ),
            "weather": (
                self.handle_weather
            ),
            "system_status": (
                self.handle_system_status
            ),
            "calendar_add": (
                self.handle_calendar_action
            ),
            "reminder": (
                self.handle_calendar_action
            )
        }

    async def process_user_message(
        self,
        telegram_user_id: int,
        message: str
    ) -> dict[str, Any]:
        try:
            route_type = (
                self.detect_route_type(
                    message
                )
            )

            logger.info(
                "AI route type=%s "
                "user_id=%s",
                route_type,
                telegram_user_id
            )

            if route_type == "chat":
                response = await (
                    self.handle_chat(
                        telegram_user_id,
                        message
                    )
                )

                return {
                    "type": "chat",
                    "response": response
                }

            intent_result = await (
                self.parse_intent(
                    message
                )
            )

            tool_execution = await (
                self.dispatch_tool(
                    intent_result,
                    message
                )
            )

            raw_output = (
                tool_execution.get(
                    "raw_output",
                    ""
                )
            )

            tool_error = (
                tool_execution.get(
                    "error",
                    False
                )
            )

            summarized_response = (
                await self.summarize_tool_output(
                    original_user_message=message,
                    intent_result=intent_result,
                    raw_output=raw_output,
                    tool_error=tool_error
                )
            )

            return {
                "type": "tool",
                "response": summarized_response,
                "intent_data": intent_result
            }

        except Exception as exc:
            logger.exception(
                "AIService process failed: %s",
                exc
            )

            fallback_response = (
                await self.generate_friendly_error(
                    user_message=message
                )
            )

            return {
                "type": "error",
                "response": fallback_response
            }

    def detect_route_type(
        self,
        message: str
    ) -> str:
        lowered = message.lower()

        for keywords in (
            self.tool_keywords.values()
        ):
            for keyword in keywords:
                if keyword in lowered:
                    return "tool"

        return "chat"

    async def handle_chat(
        self,
        telegram_user_id: int,
        message: str
    ) -> str:
        memory_context = (
            self.get_memory_context(
                telegram_user_id
            )
        )

        messages = [
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            }
        ]

        messages.extend(
            memory_context
        )

        messages.append(
            {
                "role": "user",
                "content": message
            }
        )

        response = await (
            self.provider.generate_response(
                messages=messages
            )
        )

        self.append_memory(
            telegram_user_id,
            "user",
            message
        )

        self.append_memory(
            telegram_user_id,
            "assistant",
            response
        )

        return response

    async def parse_intent(
        self,
        message: str
    ) -> dict[str, Any]:
        messages = [
            {
                "role": "system",
                "content": (
                    INTENT_PARSER_PROMPT
                )
            },
            {
                "role": "user",
                "content": message
            }
        ]

        raw_response = await (
            self.provider.generate_response(
                messages=messages,
                temperature=0.2
            )
        )

        logger.info(
            "Intent parser raw response=%s",
            raw_response
        )

        try:
            parsed = json.loads(
                raw_response
            )

            if not isinstance(
                parsed,
                dict
            ):
                raise ValueError(
                    "Intent response "
                    "must be dict"
                )

            return parsed

        except Exception:
            logger.exception(
                "Failed to parse intent JSON"
            )

            return {
                "intent": "chat",
                "confidence": 0.0,
                "summary": message,
                "action_required": False,
                "entities": {}
            }

    async def dispatch_tool(
        self,
        intent_data: dict[str, Any],
        original_message: str
    ) -> dict[str, Any]:
        intent = (
            intent_data.get(
                "intent",
                "chat"
            )
        )

        logger.info(
            "Dispatching tool "
            "intent=%s",
            intent
        )

        handler = (
            self.intent_dispatcher.get(
                intent
            )
        )

        if handler is None:
            fallback_response = (
                await self.handle_chat_fallback(
                    original_message
                )
            )

            return {
                "intent": intent,
                "raw_output": (
                    fallback_response
                ),
                "error": False
            }

        try:
            result = await handler(
                intent_data,
                original_message
            )

            return {
                "intent": intent,
                "raw_output": result,
                "error": False
            }

        except Exception as exc:
            logger.exception(
                "Tool dispatch failed: %s",
                exc
            )

            return {
                "intent": intent,
                "raw_output": str(exc),
                "error": True
            }

    async def handle_weather(
        self,
        intent_data: dict[str, Any],
        original_message: str
    ) -> str:
        plugin = (
            plugin_loader.get_plugin(
                "weather"
            )
        )

        if plugin is None:
            raise RuntimeError(
                "Weather service "
                "is unavailable."
            )

        entities = (
            intent_data.get(
                "entities",
                {}
            )
        )

        city = (
            entities.get("city")
            or entities.get("location")
            or original_message
        )

        logger.info(
            "Executing weather "
            "plugin city=%s",
            city
        )

        result = await plugin.get_weather(
            city
        )

        return str(result)

    async def handle_web_search(
        self,
        intent_data: dict[str, Any],
        original_message: str
    ) -> str:
        plugin = (
            plugin_loader.get_plugin(
                "websearch"
            )
        )

        if plugin is None:
            raise RuntimeError(
                "Web search service "
                "is unavailable."
            )

        entities = (
            intent_data.get(
                "entities",
                {}
            )
        )

        query = (
            entities.get("query")
            or original_message
        )

        logger.info(
            "Executing web search "
            "query=%s",
            query
        )

        results = await plugin.search(
            query=query
        )

        return str(results)

    async def handle_system_status(
        self,
        intent_data: dict[str, Any],
        original_message: str
    ) -> str:
        memory = (
            psutil.virtual_memory()
        )

        cpu_usage = (
            psutil.cpu_percent(
                interval=1
            )
        )

        disk = psutil.disk_usage(
            "/"
        )

        boot_time = datetime.fromtimestamp(
            psutil.boot_time()
        )

        plugins = (
            plugin_loader.list_plugins()
        )

        plugin_names = [
            plugin["name"]
            for plugin in plugins
            if plugin["enabled"]
        ]

        result = (
            f"System Status\n\n"
            f"Platform: "
            f"{platform.system()} "
            f"{platform.release()}\n"
            f"CPU Usage: "
            f"{cpu_usage}%\n"
            f"RAM Usage: "
            f"{memory.percent}%\n"
            f"RAM Available: "
            f"{round(memory.available / 1024 / 1024)} MB\n"
            f"Disk Usage: "
            f"{disk.percent}%\n"
            f"Python Version: "
            f"{platform.python_version()}\n"
            f"Boot Time: "
            f"{boot_time}\n"
            f"Loaded Plugins: "
            f"{', '.join(plugin_names)}\n"
            f"AI Provider: "
            f"{os.getenv('AI_PROVIDER')}"
        )

        return result

    async def handle_calendar_action(
        self,
        intent_data: dict[str, Any],
        original_message: str
    ) -> str:
        return (
            "Reminder and calendar "
            "workflow routing is ready."
        )

    async def summarize_tool_output(
        self,
        original_user_message: str,
        intent_result: dict[str, Any],
        raw_output: str,
        tool_error: bool = False
    ) -> str:
        intent = (
            intent_result.get(
                "intent",
                "unknown"
            )
        )

        if tool_error:
            instruction = (
                "The tool execution failed. "
                "Explain the failure politely "
                "without exposing raw system "
                "errors."
            )

        else:
            instruction = (
                "Summarize the tool result "
                "naturally and conversationally."
            )

        summary_prompt = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": (
                    f"Original User Message:\n"
                    f"{original_user_message}\n\n"
                    f"Intent:\n"
                    f"{intent}\n\n"
                    f"Instruction:\n"
                    f"{instruction}\n\n"
                    f"Raw Tool Result:\n"
                    f"{raw_output}"
                )
            }
        ]

        try:
            response = await (
                self.provider.generate_response(
                    messages=summary_prompt,
                    temperature=0.4
                )
            )

            return response

        except Exception as exc:
            logger.exception(
                "Tool summarization failed: %s",
                exc
            )

            if tool_error:
                return (
                    "⚠️ Sorry, I couldn't "
                    "complete that request "
                    "right now."
                )

            return (
                "⚠️ I received the result, "
                "but couldn't summarize it "
                "properly."
            )

    async def generate_friendly_error(
        self,
        user_message: str
    ) -> str:
        prompt = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": (
                    "Generate a polite AI "
                    "assistant error reply "
                    "for the following user "
                    f"message:\n{user_message}"
                )
            }
        ]

        try:
            return await (
                self.provider.generate_response(
                    messages=prompt,
                    temperature=0.3
                )
            )

        except Exception:
            return (
                "⚠️ Sorry, something went "
                "wrong while processing "
                "your request."
            )

    async def handle_chat_fallback(
        self,
        message: str
    ) -> str:
        messages = [
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            },
            {
                "role": "user",
                "content": message
            }
        ]

        return await (
            self.provider.generate_response(
                messages=messages
            )
        )

    def append_memory(
        self,
        telegram_user_id: int,
        role: str,
        content: str
    ) -> None:
        self.memory[
            telegram_user_id
        ].append(
            {
                "role": role,
                "content": content
            }
        )

        if (
            len(
                self.memory[
                    telegram_user_id
                ]
            )
            > self.max_memory_messages
        ):
            self.memory[
                telegram_user_id
            ] = (
                self.memory[
                    telegram_user_id
                ][
                    -self.max_memory_messages:
                ]
            )

    def get_memory_context(
        self,
        telegram_user_id: int
    ) -> list[dict[str, str]]:
        return list(
            self.memory.get(
                telegram_user_id,
                []
            )
        )

    async def clear_memory(
        self,
        telegram_user_id: int
    ) -> None:
        if (
            telegram_user_id
            in self.memory
        ):
            del self.memory[
                telegram_user_id
            ]

    async def health_check(
        self
    ) -> bool:
        try:
            response = await (
                self.provider.generate_response(
                    messages=[
                        {
                            "role": "user",
                            "content": "ping"
                        }
                    ]
                )
            )

            return bool(response)

        except AIProviderException:
            logger.exception(
                "AI provider health "
                "check failed"
            )

            return False
