from __future__ import annotations

import json
from collections import defaultdict

from app.ai.prompts import (
    INTENT_PARSER_PROMPT,
    SUMMARY_PROMPT,
    SYSTEM_PROMPT,
)
from app.ai.provider import (
    AIProvider,
    AIProviderException,
)


class AIServiceException(Exception):
    pass


class AIService:
    def __init__(self):
        self.provider = AIProvider()

        self.memory: dict[
            int,
            list[dict[str, str]]
        ] = defaultdict(list)

        self.max_memory_messages = 20

    async def chat(
        self,
        telegram_user_id: int,
        user_message: str
    ) -> str:
        if not user_message.strip():
            raise AIServiceException(
                "User message is empty"
            )

        history = self.memory[
            telegram_user_id
        ]

        messages = [
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            }
        ]

        messages.extend(history)

        messages.append({
            "role": "user",
            "content": user_message
        })

        response = (
            await self.provider
            .generate_response(messages)
        )

        history.append({
            "role": "user",
            "content": user_message
        })

        history.append({
            "role": "assistant",
            "content": response
        })

        self._trim_memory(
            telegram_user_id
        )

        return response

    async def parse_intent(
        self,
        user_message: str
    ) -> dict:
        messages = [
            {
                "role": "system",
                "content": (
                    INTENT_PARSER_PROMPT
                )
            },
            {
                "role": "user",
                "content": user_message
            }
        ]

        response = (
            await self.provider
            .generate_response(
                messages=messages,
                temperature=0.1,
                max_tokens=500
            )
        )

        try:
            parsed = json.loads(response)

            return parsed

        except json.JSONDecodeError as exc:
            raise AIServiceException(
                "Failed to parse intent JSON"
            ) from exc

    async def summarize(
        self,
        text: str,
        language_hint: str | None = None
    ) -> str:
        prompt = text

        if language_hint:
            prompt = (
                f"Language: {language_hint}\n\n"
                f"{text}"
            )

        messages = [
            {
                "role": "system",
                "content": SUMMARY_PROMPT
            },
            {
                "role": "user",
                "content": prompt
            }
        ]

        return (
            await self.provider
            .generate_response(
                messages=messages,
                temperature=0.3,
                max_tokens=300
            )
        )

    async def process_user_message(
        self,
        telegram_user_id: int,
        user_message: str
    ) -> dict:
        intent_result = (
            await self.parse_intent(
                user_message
            )
        )

        intent = intent_result.get(
            "intent",
            "unknown"
        )

        if intent == "ai_chat":
            response = await self.chat(
                telegram_user_id,
                user_message
            )

            summary = await self.summarize(
                response
            )

            return {
                "type": "chat",
                "intent": intent,
                "response": response,
                "summary": summary,
                "intent_data": intent_result
            }

        return {
            "type": "action",
            "intent": intent,
            "intent_data": intent_result
        }

    def clear_memory(
        self,
        telegram_user_id: int
    ) -> None:
        if telegram_user_id in self.memory:
            del self.memory[
                telegram_user_id
            ]

    def get_memory(
        self,
        telegram_user_id: int
    ) -> list[dict[str, str]]:
        return self.memory.get(
            telegram_user_id,
            []
        )

    def _trim_memory(
        self,
        telegram_user_id: int
    ) -> None:
        history = self.memory[
            telegram_user_id
        ]

        if len(history) > (
            self.max_memory_messages * 2
        ):
            self.memory[
                telegram_user_id
            ] = history[
                -self.max_memory_messages:
            ]

    async def health_check(self) -> bool:
        try:
            response = await self.provider.generate_response(
                messages=[
                    {
                        "role": "user",
                        "content": "ping"
                    }
                ],
                temperature=0.0,
                max_tokens=10
            )

            return bool(response)

        except AIProviderException:
            return False
