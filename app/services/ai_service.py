from __future__ import annotations

import json
import logging
import os
import platform
from datetime import datetime
from typing import Any

import psutil

from app.ai.prompts import (
    SYSTEM_PROMPT,
)
from app.ai.provider import (
    AIProvider,
    AIProviderException,
)
from app.database.repositories.chat_memory import (
    chat_memory_repository,
)
from app.plugins.loader import (
    plugin_loader,
)
from app.services.inbox_service import (
    inbox_service,
)
from app.services.plugin_service import (
    plugin_service,
)

logger = logging.getLogger(__name__)


AGENT_WORKFLOW_PROMPT = """
You are TeleOps-AI Autonomous Agent Planner.

Your responsibilities:
- Analyze user requests
- Decide whether tools are needed
- Build multi-step workflows
- Chain tools and AI tasks together

AVAILABLE TOOLS:

1. web_search
Purpose:
- Search latest news
- Search internet information
- Search current events

JSON:
{
  "tool": "web_search",
  "query": "search query"
}

2. weather
Purpose:
- Weather forecast
- Rain
- Climate
- Temperature

JSON:
{
  "tool": "weather",
  "city": "city name"
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
- Check unread emails
- Summarize emails
- Detect important emails

JSON:
{
  "tool": "email_summary"
}

AVAILABLE AI TASKS:

1. summarize
2. translate
3. explain

RULES:
- Return ONLY valid JSON
- Never return markdown
- Never explain reasoning

WORKFLOW FORMAT:

{
  "workflow": [
    {
      "step": 1,
      "type": "tool",
      "tool": "web_search",
      "query": "latest AI news"
    },
    {
      "step": 2,
      "type": "ai_task",
      "task": "summarize",
      "language": "burmese"
    }
  ]
}

If casual conversation only:

{
  "workflow": [
    {
      "step": 1,
      "type": "chat"
    }
  ]
}
"""


SUMMARY_PROMPT = """
You are TeleOps-AI.

Convert raw outputs into human-friendly conversational replies.

Rules:
- Never expose raw JSON
- Never expose internal system logic
- Keep replies concise
- Reply in same language as user
- Burmese => Burmese response
- English => English response
- Sound natural and human
"""


EMAIL_SUMMARY_PROMPT = """
You are TeleOps-AI Email Intelligence Assistant.

Your job:
- Read raw email contents
- Summarize unread emails clearly
- Mention:
  - sender
  - subject
  - key important information

Rules:
- Keep summary concise
- Group similar emails
- Highlight urgent emails
- Reply in same language as user
- Never expose raw MIME or HTML
- Make summaries conversational
"""


class AIService:
    def __init__(self):
        self.provider = AIProvider()

        self.max_memory_messages = 20

    async def process_user_message(
        self,
        telegram_user_id: int,
        message: str
    ) -> dict[str, Any]:
        try:
            memory_context = (
                await self.build_memory_context(
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

            logger.info(
                "Workflow generated "
                "steps=%s",
                len(workflow_steps)
            )

            if not workflow_steps:
                response = await (
                    self.handle_chat(
                        telegram_user_id,
                        message,
                        memory_context
                    )
                )

                await (
                    self.store_conversation_pair(
                        telegram_user_id,
                        message,
                        response
                    )
                )

                return {
                    "type": "chat",
                    "response": response
                }

            first_step = (
                workflow_steps[0]
            )

            if (
                first_step.get(
                    "type"
                )
                == "chat"
            ):
                response = await (
                    self.handle_chat(
                        telegram_user_id,
                        message,
                        memory_context
                    )
                )

                await (
                    self.store_conversation_pair(
                        telegram_user_id,
                        message,
                        response
                    )
                )

                return {
                    "type": "chat",
                    "response": response
                }

            workflow_context = ""

            executed_steps = []

            for step in workflow_steps:
                step_type = (
                    step.get(
                        "type"
                    )
                )

                logger.info(
                    "Executing workflow "
                    "step=%s type=%s",
                    step.get("step"),
                    step_type
                )

                try:
                    if step_type == "tool":
                        result = await (
                            self.execute_tool_step(
                                telegram_user_id=(
                                    telegram_user_id
                                ),
                                step=step
                            )
                        )

                        workflow_context += (
                            "\n\n"
                            f"[Tool Result]\n"
                            f"{result}"
                        )

                        executed_steps.append(
                            {
                                "step": (
                                    step.get(
                                        "step"
                                    )
                                ),
                                "type": "tool",
                                "tool": (
                                    step.get(
                                        "tool"
                                    )
                                ),
                                "result": result
                            }
                        )

                    elif (
                        step_type
                        == "ai_task"
                    ):
                        result = await (
                            self.execute_ai_task(
                                original_message=message,
                                workflow_context=(
                                    workflow_context
                                ),
                                task_step=step
                            )
                        )

                        workflow_context += (
                            "\n\n"
                            f"[AI Task Result]\n"
                            f"{result}"
                        )

                        executed_steps.append(
                            {
                                "step": (
                                    step.get(
                                        "step"
                                    )
                                ),
                                "type": "ai_task",
                                "result": result
                            }
                        )

                except Exception as exc:
                    logger.exception(
                        "Workflow step "
                        "execution failed: %s",
                        exc
                    )

                    workflow_context += (
                        "\n\n"
                        "[Step Failed]\n"
                        "A workflow step failed "
                        "but execution continued."
                    )

            final_response = await (
                self.generate_final_response(
                    original_user_message=message,
                    workflow_context=(
                        workflow_context
                    )
                )
            )

            await (
                self.store_conversation_pair(
                    telegram_user_id,
                    message,
                    final_response
                )
            )

            return {
                "type": "workflow",
                "response": final_response,
                "steps": executed_steps
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

            await (
                self.store_conversation_pair(
                    telegram_user_id,
                    message,
                    fallback_response
                )
            )

            return {
                "type": "error",
                "response": fallback_response
            }

    async def generate_workflow(
        self,
        message: str,
        memory_context: list[
            dict[str, str]
        ]
    ) -> dict[str, Any]:
        messages = [
            {
                "role": "system",
                "content": (
                    AGENT_WORKFLOW_PROMPT
                )
            }
        ]

        messages.extend(
            memory_context[-10:]
        )

        messages.append(
            {
                "role": "user",
                "content": message
            }
        )

        raw_response = await (
            self.provider.generate_response(
                messages=messages,
                temperature=0.1
            )
        )

        logger.info(
            "Workflow planner "
            "response=%s",
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
                    "Workflow response "
                    "must be dict"
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

    async def build_memory_context(
        self,
        telegram_user_id: int
    ) -> list[dict[str, str]]:
        history = await (
            chat_memory_repository
            .get_recent_history(
                telegram_user_id=(
                    telegram_user_id
                ),
                limit=(
                    self.max_memory_messages
                )
            )
        )

        messages = []

        for item in history:
            content = item.get(
                "content",
                ""
            )

            if not content:
                continue

            messages.append(
                {
                    "role": item.get(
                        "role",
                        "user"
                    ),
                    "content": content
                }
            )

        return messages

    async def handle_chat(
        self,
        telegram_user_id: int,
        message: str,
        memory_context: list[
            dict[str, str]
        ]
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

        response = await (
            self.provider.generate_response(
                messages=messages,
                temperature=0.7
            )
        )

        return response

    async def execute_tool_step(
        self,
        telegram_user_id: int,
        step: dict[str, Any]
    ) -> str:
        tool_name = (
            step.get(
                "tool"
            )
        )

        logger.info(
            "Executing tool=%s",
            tool_name
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
            "mail_check",
            "email_summary"
        ):
            return await (
                self.execute_email_summary(
                    telegram_user_id
                )
            )

        return (
            "Unknown tool "
            f"{tool_name}"
        )

    async def execute_email_summary(
        self,
        telegram_user_id: int
    ) -> str:
        try:
            logger.info(
                "Loading mail settings "
                "telegram_user_id=%s",
                telegram_user_id
            )

            mail_settings = await (
                plugin_service
                .load_mail_settings(
                    telegram_user_id
                )
            )

            if not mail_settings:
                return (
                    "Mail settings are "
                    "not configured."
                )

            inbox_id = (
                mail_settings.get(
                    "inbox_id"
                )
            )

            host = (
                mail_settings.get(
                    "host"
                )
            )

            email_address = (
                mail_settings.get(
                    "email"
                )
            )

            password = (
                mail_settings.get(
                    "password"
                )
            )

            if not all([
                inbox_id,
                host,
                email_address,
                password
            ]):
                return (
                    "Incomplete mail "
                    "credentials detected."
                )

            logger.info(
                "Fetching unread emails "
                "telegram_user_id=%s",
                telegram_user_id
            )

            emails = await (
                inbox_service.fetch_emails(
                    telegram_user_id=(
                        telegram_user_id
                    ),
                    inbox_id=inbox_id,
                    host=host,
                    email=email_address,
                    password=password,
                    unread_only=True,
                    limit=10
                )
            )

            if not emails:
                return (
                    "No unread emails "
                    "were found."
                )

            email_text = ""

            for index, email_data in enumerate(
                emails,
                start=1
            ):
                sender = (
                    email_data.get(
                        "from",
                        "Unknown Sender"
                    )
                )

                subject = (
                    email_data.get(
                        "subject",
                        "No Subject"
                    )
                )

                body = (
                    email_data.get(
                        "body",
                        ""
                    )
                )

                email_text += (
                    f"\n\n"
                    f"Email {index}\n"
                    f"From: {sender}\n"
                    f"Subject: {subject}\n"
                    f"Body: {body[:2500]}"
                )

            summary_messages = [
                {
                    "role": "system",
                    "content": (
                        EMAIL_SUMMARY_PROMPT
                    )
                },
                {
                    "role": "user",
                    "content": email_text
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
                "Unable to check emails "
                "right now."
            )

    async def execute_ai_task(
        self,
        original_message: str,
        workflow_context: str,
        task_step: dict[str, Any]
    ) -> str:
        task_name = (
            task_step.get(
                "task",
                "summarize"
            )
        )

        language = (
            task_step.get(
                "language",
                "same"
            )
        )

        logger.info(
            "Executing AI task=%s",
            task_name
        )

        task_prompt = [
            {
                "role": "system",
                "content": (
                    SUMMARY_PROMPT
                )
            },
            {
                "role": "user",
                "content": (
                    f"Original User Request:\n"
                    f"{original_message}\n\n"
                    f"Task Type:\n"
                    f"{task_name}\n\n"
                    f"Language:\n"
                    f"{language}\n\n"
                    f"Workflow Context:\n"
                    f"{workflow_context}"
                )
            }
        ]

        result = await (
            self.provider.generate_response(
                messages=task_prompt,
                temperature=0.4
            )
        )

        return result

    async def execute_web_search(
        self,
        step: dict[str, Any]
    ) -> str:
        plugin = (
            plugin_loader.get_plugin(
                "websearch"
            )
        )

        if plugin is None:
            return (
                "Web search plugin "
                "is unavailable."
            )

        query = (
            step.get(
                "query",
                ""
            )
        )

        if not query:
            return (
                "Search query "
                "was empty."
            )

        result = await plugin.search(
            query=query
        )

        return str(result)

    async def execute_weather(
        self,
        step: dict[str, Any]
    ) -> str:
        plugin = (
            plugin_loader.get_plugin(
                "weather"
            )
        )

        if plugin is None:
            return (
                "Weather plugin "
                "is unavailable."
            )

        city = (
            step.get(
                "city",
                ""
            )
        )

        if not city:
            return (
                "City parameter "
                "was empty."
            )

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

        return (
            f"System Status\n\n"
            f"Platform: "
            f"{platform.system()} "
            f"{platform.release()}\n"
            f"CPU Usage: "
            f"{cpu_usage}%\n"
            f"RAM Usage: "
            f"{memory.percent}%\n"
            f"Available RAM: "
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

    async def generate_final_response(
        self,
        original_user_message: str,
        workflow_context: str
    ) -> str:
        messages = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": (
                    f"Original User Request:\n"
                    f"{original_user_message}\n\n"
                    f"Workflow Results:\n"
                    f"{workflow_context}\n\n"
                    f"Generate final "
                    f"human-friendly reply."
                )
            }
        ]

        try:
            response = await (
                self.provider.generate_response(
                    messages=messages,
                    temperature=0.5
                )
            )

            return response

        except Exception as exc:
            logger.exception(
                "Final response failed: %s",
                exc
            )

            return (
                "⚠️ I completed part "
                "of the workflow, but "
                "couldn't generate the "
                "final response."
            )

    async def store_conversation_pair(
        self,
        telegram_user_id: int,
        user_message: str,
        assistant_message: str
    ) -> None:
        try:
            await (
                chat_memory_repository
                .store_message(
                    telegram_user_id=(
                        telegram_user_id
                    ),
                    role="user",
                    content=user_message
                )
            )

            await (
                chat_memory_repository
                .store_message(
                    telegram_user_id=(
                        telegram_user_id
                    ),
                    role="assistant",
                    content=assistant_message
                )
            )

        except Exception:
            logger.exception(
                "Failed to store "
                "chat memory"
            )

    async def clear_memory(
        self,
        telegram_user_id: int
    ) -> None:
        try:
            await (
                chat_memory_repository
                .clear_history(
                    telegram_user_id
                )
            )

            logger.info(
                "Memory cleared "
                "telegram_user_id=%s",
                telegram_user_id
            )

        except Exception:
            logger.exception(
                "Failed to clear memory"
            )

    async def generate_friendly_error(
        self,
        user_message: str
    ) -> str:
        messages = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": (
                    "Generate a friendly "
                    "AI assistant error "
                    f"reply for:\n"
                    f"{user_message}"
                )
            }
        ]

        try:
            return await (
                self.provider.generate_response(
                    messages=messages,
                    temperature=0.3
                )
            )

        except Exception:
            return (
                "⚠️ Sorry, something "
                "went wrong while "
                "processing your request."
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
                "AI provider health "
                "check failed"
            )

            return False
