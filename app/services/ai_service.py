from __future__ import annotations

import gc
import json
import logging
import platform
from datetime import datetime
from typing import Any

import psutil

from app.ai.prompts import SYSTEM_PROMPT
from app.ai.provider import (
    AIProvider,
    AIProviderException,
)
from app.database.repositories.chat_memory import (
    chat_memory_repository,
)
from app.database.repositories.rclone_meta import (
    RcloneMetaRepository,
)
from app.plugins.loader import (
    plugin_loader,
)
from app.services.inbox_service import (
    InboxService,
)
from app.services.plugin_service import (
    PluginService,
)

logger = logging.getLogger(__name__)

plugin_service = PluginService()


# =========================================================
# WORKFLOW ROUTER
# =========================================================

WORKFLOW_ROUTER_PROMPT = """
You are TeleOps-AI Autonomous Workflow Router.

Your job:
- Analyze user requests
- Decide whether tools are required
- Build lightweight workflows
- Avoid unnecessary tool usage
- Optimize for low-memory environments

AVAILABLE TOOLS:

1. web_search
Purpose:
- Search latest information
- Search internet data
- Search news

JSON:
{
  "tool": "web_search",
  "query": "latest AI news"
}

2. weather
Purpose:
- Check weather
- Forecast
- Temperature

JSON:
{
  "tool": "weather",
  "city": "Bangkok"
}

3. system_status
Purpose:
- CPU usage
- RAM usage
- System health

JSON:
{
  "tool": "system_status"
}

4. email_summary
Purpose:
- Fetch unread emails
- Summarize inbox

JSON:
{
  "tool": "email_summary"
}

5. rclone_search
Purpose:
- Search indexed cloud storage metadata
- Locate files

JSON:
{
  "tool": "rclone_search",
  "keyword": "backup.zip"
}

WORKFLOW FORMAT:

{
  "workflow": [
    {
      "step": 1,
      "type": "tool",
      "tool": "weather",
      "city": "Bangkok"
    },
    {
      "step": 2,
      "type": "summarize"
    }
  ]
}

CASUAL CHAT FORMAT:

{
  "workflow": [
    {
      "step": 1,
      "type": "chat"
    }
  ]
}

LANGUAGE RULES:
- Preserve the user's original language
- English input => English workflow
- Burmese input => Burmese workflow
- Never force Burmese unnecessarily

RULES:
- Return ONLY valid JSON
- Never return markdown
- Never explain reasoning
"""


# =========================================================
# SUMMARY PROMPTS
# =========================================================

SUMMARY_PROMPT = """
You are TeleOps-AI.

Convert raw tool outputs into clean conversational replies.

LANGUAGE RULES:
- Detect the user's language automatically.
- English input => English output.
- Burmese input => Burmese Unicode output.
- Never translate unless requested.
- Never mix Burmese and English unnecessarily.
- Never use Zawgyi encoding.
- Keep Burmese Unicode clean.

GENERAL RULES:
- Be concise
- Be human-friendly
- Never expose raw JSON
- Never expose raw database rows
- Never expose secrets or tokens
"""


EMAIL_SUMMARY_PROMPT = """
You are TeleOps-AI Email Assistant.

Summarize unread emails clearly.

Mention:
- Sender
- Subject
- Important information

LANGUAGE RULES:
- Match the user's language automatically.
- English user => English reply.
- Burmese user => Burmese Unicode reply.

RULES:
- Keep concise
- Never expose raw HTML
- Never expose MIME structures
- Reply naturally
"""


RCLONE_SUMMARY_PROMPT = """
You are TeleOps-AI Storage Assistant.

Convert storage search results into clean conversational summaries.

Mention:
- Remote name
- File name
- Folder path
- Approximate file size

LANGUAGE RULES:
- Match the user's language automatically.
- English user => English reply.
- Burmese user => Burmese Unicode reply.

RULES:
- Organize clearly
- Never expose raw SQL rows
- Reply naturally
"""


# =========================================================
# AI SERVICE
# =========================================================

class AIService:

    def __init__(self):

        self.provider = AIProvider()

        self.max_history_messages = 20

        self.max_email_body_chars = 2000

        self.rclone_repository = (
            RcloneMetaRepository()
        )

        self.inbox_service = InboxService()

    async def process_user_message(
        self,
        telegram_user_id: int,
        message: str
    ) -> dict[str, Any]:

        memory_context = []

        workflow = {}

        workflow_context = ""

        executed_steps = []

        try:

            memory_context = (
                await self.get_memory_context(
                    telegram_user_id
                )
            )

            workflow = await (
                self.generate_workflow(
                    message=message,
                    memory_context=memory_context
                )
            )

            workflow_steps = (
                workflow.get(
                    "workflow",
                    []
                )
            )

            if not workflow_steps:

                response = await (
                    self.generate_chat_response(
                        memory_context=memory_context,
                        message=message
                    )
                )

                await self.store_conversation(
                    telegram_user_id,
                    message,
                    response
                )

                return {
                    "type": "chat",
                    "response": response
                }

            first_step = workflow_steps[0]

            if (
                first_step.get("type")
                == "chat"
            ):

                response = await (
                    self.generate_chat_response(
                        memory_context=memory_context,
                        message=message
                    )
                )

                await self.store_conversation(
                    telegram_user_id,
                    message,
                    response
                )

                return {
                    "type": "chat",
                    "response": response
                }

            for step in workflow_steps:

                try:

                    result = await (
                        self.execute_workflow_step(
                            telegram_user_id=telegram_user_id,
                            step=step,
                            workflow_context=workflow_context
                        )
                    )

                    if result:
                        workflow_context += (
                            "\n\n"
                            f"{result}"
                        )

                    executed_steps.append(
                        {
                            "step": step.get("step"),
                            "type": step.get("type")
                        }
                    )

                except Exception as exc:

                    logger.exception(
                        "Workflow step failed: %s",
                        exc
                    )

                    workflow_context += (
                        "\n\n"
                        "One workflow step failed "
                        "but execution continued safely."
                    )

            final_response = await (
                self.generate_final_response(
                    original_message=message,
                    workflow_context=workflow_context
                )
            )

            await self.store_conversation(
                telegram_user_id,
                message,
                final_response
            )

            return {
                "type": "workflow",
                "response": final_response,
                "steps": executed_steps
            }

        except Exception as exc:

            logger.exception(
                "AIService failed: %s",
                exc
            )

            fallback = await (
                self.generate_friendly_error(
                    message
                )
            )

            await self.store_conversation(
                telegram_user_id,
                message,
                fallback
            )

            return {
                "type": "error",
                "response": fallback
            }

        finally:

            del memory_context
            del workflow
            del workflow_context
            del executed_steps

            gc.collect()

    async def generate_workflow(
        self,
        message: str,
        memory_context: list[
            dict[str, str]
        ]
    ) -> dict[str, Any]:

        router_messages = [
            {
                "role": "system",
                "content": WORKFLOW_ROUTER_PROMPT
            }
        ]

        router_messages.extend(
            memory_context[-10:]
        )

        router_messages.append(
            {
                "role": "user",
                "content": message
            }
        )

        raw_response = await (
            self.provider.generate_response(
                messages=router_messages,
                temperature=0.1
            )
        )

        logger.info(
            "Workflow router response=%s",
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
                    "Workflow must be dict"
                )

            return parsed

        except Exception:

            logger.exception(
                "Workflow parse failed"
            )

            return {
                "workflow": [
                    {
                        "step": 1,
                        "type": "chat"
                    }
                ]
            }

    async def execute_workflow_step(
        self,
        telegram_user_id: int,
        step: dict[str, Any],
        workflow_context: str
    ) -> str:

        step_type = (
            step.get("type")
        )

        if step_type == "tool":
            return await (
                self.execute_tool(
                    telegram_user_id=telegram_user_id,
                    step=step
                )
            )

        if step_type == "summarize":
            return await (
                self.summarize_context(
                    workflow_context
                )
            )

        return ""

    async def execute_tool(
        self,
        telegram_user_id: int,
        step: dict[str, Any]
    ) -> str:

        tool_name = (
            step.get("tool")
        )

        if tool_name == "web_search":
            return await (
                self.execute_web_search(
                    step
                )
            )

        if tool_name == "weather":
            return await (
                self.execute_weather(
                    step
                )
            )

        if tool_name == "system_status":
            return await (
                self.execute_system_status()
            )

        if tool_name in (
            "email_summary",
            "mail_check"
        ):
            return await (
                self.execute_email_summary(
                    telegram_user_id
                )
            )

        if tool_name in (
            "rclone_search",
            "storage_search"
        ):
            return await (
                self.execute_rclone_search(
                    step
                )
            )

        return f"Unknown tool: {tool_name}"

    async def execute_web_search(
        self,
        step: dict[str, Any]
    ) -> str:

        query = (
            step.get(
                "query",
                ""
            )
        )

        if not query:
            return "Search query missing."

        plugin = (
            plugin_loader.get_plugin(
                "websearch"
            )
        )

        if plugin is None:
            return "Web search plugin not available."

        result = await plugin.search(
            query=query
        )

        return str(result)

    async def execute_weather(
        self,
        step: dict[str, Any]
    ) -> str:

        city = (
            step.get(
                "city",
                ""
            )
        )

        if not city:
            return "City parameter missing."

        plugin = (
            plugin_loader.get_plugin(
                "weather"
            )
        )

        if plugin is None:
            return "Weather plugin not available."

        result = await (
            plugin.get_weather(
                city
            )
        )

        return str(result)

    async def execute_system_status(
        self
    ) -> str:

        memory = (
            psutil.virtual_memory()
        )

        cpu = psutil.cpu_percent(
            interval=1
        )

        disk = psutil.disk_usage(
            "/"
        )

        plugins = (
            plugin_loader.list_plugins()
        )

        enabled_plugins = [
            plugin["name"]
            for plugin in plugins
            if plugin["enabled"]
        ]

        return (
            f"System Status\n\n"
            f"Platform: {platform.system()} {platform.release()}\n"
            f"CPU Usage: {cpu}%\n"
            f"RAM Usage: {memory.percent}%\n"
            f"Disk Usage: {disk.percent}%\n"
            f"Python: {platform.python_version()}\n"
            f"Plugins: {', '.join(enabled_plugins)}\n"
            f"Time: {datetime.utcnow()}"
        )

    async def execute_email_summary(
        self,
        telegram_user_id: int
    ) -> str:

        try:

            settings = await (
                plugin_service.load_mail_settings(
                    telegram_user_id
                )
            )

            if not settings:
                return (
                    "Mail settings are not configured."
                )

            emails = await (
                self.inbox_service.fetch_emails(
                    telegram_user_id=telegram_user_id,
                    inbox_id=settings.get("inbox_id"),
                    host=settings.get("host"),
                    email=settings.get("email"),
                    password=settings.get("password"),
                    unread_only=True,
                    limit=10
                )
            )

            if not emails:
                return (
                    "No unread emails were found."
                )

            raw_email_text = ""

            for index, item in enumerate(
                emails,
                start=1
            ):

                sender = (
                    item.get(
                        "from",
                        "Unknown"
                    )
                )

                subject = (
                    item.get(
                        "subject",
                        "No Subject"
                    )
                )

                body = (
                    item.get(
                        "body",
                        ""
                    )
                )

                body = body[
                    :self.max_email_body_chars
                ]

                raw_email_text += (
                    f"\n\n"
                    f"Email {index}\n"
                    f"From: {sender}\n"
                    f"Subject: {subject}\n"
                    f"Body: {body}"
                )

            summary_messages = [
                {
                    "role": "system",
                    "content": EMAIL_SUMMARY_PROMPT
                },
                {
                    "role": "user",
                    "content": raw_email_text
                }
            ]

            summarized = await (
                self.provider.generate_response(
                    messages=summary_messages,
                    temperature=0.4
                )
            )

            return summarized

        except Exception as exc:

            logger.exception(
                "Email summary failed: %s",
                exc
            )

            return (
                "Unable to fetch emails right now."
            )

    async def execute_rclone_search(
        self,
        step: dict[str, Any]
    ) -> str:

        try:

            keyword = (
                step.get(
                    "keyword",
                    ""
                )
            )

            if not keyword:
                return (
                    "Storage search keyword missing."
                )

            results = await (
                self.rclone_repository.search_files(
                    keyword=keyword
                )
            )

            if not results:
                return (
                    "No matching files were found."
                )

            raw_result_text = ""

            for index, item in enumerate(
                results,
                start=1
            ):

                raw_result_text += (
                    f"\n\n"
                    f"Result {index}\n"
                    f"Remote: {item.get('remote_name')}\n"
                    f"File: {item.get('file_name')}\n"
                    f"Path: {item.get('file_path')}\n"
                    f"Size: {item.get('file_size')}"
                )

            messages = [
                {
                    "role": "system",
                    "content": RCLONE_SUMMARY_PROMPT
                },
                {
                    "role": "user",
                    "content": raw_result_text
                }
            ]

            summarized = await (
                self.provider.generate_response(
                    messages=messages,
                    temperature=0.3
                )
            )

            return summarized

        except Exception as exc:

            logger.exception(
                "Rclone search failed: %s",
                exc
            )

            return (
                "Unable to search cloud storage right now."
            )

    async def summarize_context(
        self,
        workflow_context: str
    ) -> str:

        messages = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": workflow_context
            }
        ]

        return await (
            self.provider.generate_response(
                messages=messages,
                temperature=0.4
            )
        )

    async def generate_chat_response(
        self,
        memory_context: list[
            dict[str, str]
        ],
        message: str
    ) -> str:

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

        return await (
            self.provider.generate_response(
                messages=messages,
                temperature=0.7
            )
        )

    async def generate_final_response(
        self,
        original_message: str,
        workflow_context: str
    ) -> str:

        messages = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": f'''
Original User Message:
{original_message}

IMPORTANT:
- Reply in the SAME language as the user.
- English input => English output.
- Burmese input => Burmese Unicode output.

Workflow Results:
{workflow_context}
'''
            }
        ]

        return await (
            self.provider.generate_response(
                messages=messages,
                temperature=0.4
            )
        )

    async def get_memory_context(
        self,
        telegram_user_id: int
    ) -> list[dict[str, str]]:

        history = await (
            chat_memory_repository
            .get_recent_history(
                telegram_user_id=telegram_user_id,
                limit=self.max_history_messages
            )
        )

        context_messages = []

        for item in history:

            content = (
                item.get(
                    "content",
                    ""
                ).strip()
            )

            if not content:
                continue

            context_messages.append(
                {
                    "role": item.get(
                        "role",
                        "user"
                    ),
                    "content": content
                }
            )

        return context_messages

    async def store_conversation(
        self,
        telegram_user_id: int,
        user_message: str,
        assistant_message: str
    ) -> None:

        try:

            await (
                chat_memory_repository.store_message(
                    telegram_user_id=telegram_user_id,
                    role="user",
                    content=user_message
                )
            )

            await (
                chat_memory_repository.store_message(
                    telegram_user_id=telegram_user_id,
                    role="assistant",
                    content=assistant_message
                )
            )

        except Exception:

            logger.exception(
                "Failed to store conversation"
            )

    async def clear_memory(
        self,
        telegram_user_id: int
    ) -> None:

        try:

            await (
                chat_memory_repository.clear_history(
                    telegram_user_id
                )
            )

        except Exception:

            logger.exception(
                "Failed to clear memory"
            )

    async def generate_friendly_error(
        self,
        user_message: str
    ) -> str:

        try:

            messages = [
                {
                    "role": "system",
                    "content": SUMMARY_PROMPT
                },
                {
                    "role": "user",
                    "content": (
                        "Generate a friendly "
                        "assistant error reply "
                        f"for:\n{user_message}"
                    )
                }
            ]

            return await (
                self.provider.generate_response(
                    messages=messages,
                    temperature=0.3
                )
            )

        except Exception:

            return (
                "⚠️ Sorry, something went wrong "
                "while processing your request."
            )

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
                "AI provider health check failed"
            )

            return False
