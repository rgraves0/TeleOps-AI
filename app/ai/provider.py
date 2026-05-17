from __future__ import annotations

import logging
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class AIProviderError(Exception):
    pass


class AIProvider:
    def __init__(self):
        self.provider = os.getenv(
            "AI_PROVIDER",
            "groq"
        ).lower()

        self.timeout = int(
            os.getenv(
                "AI_TIMEOUT_SECONDS",
                "60"
            )
        )

        self.temperature = float(
            os.getenv(
                "AI_TEMPERATURE",
                "0.7"
            )
        )

        self.max_tokens = int(
            os.getenv(
                "AI_MAX_TOKENS",
                "2048"
            )
        )

    async def chat_completion(
        self,
        messages: list[dict]
    ) -> str:
        if self.provider == "groq":
            return await self._groq_chat(
                messages
            )

        if self.provider == "gemini":
            return await self._gemini_chat(
                messages
            )

        if self.provider == "openai":
            return await self._openai_chat(
                messages
            )

        if self.provider == "openrouter":
            return await self._openrouter_chat(
                messages
            )

        raise AIProviderError(
            f"Unsupported provider: "
            f"{self.provider}"
        )

    async def _groq_chat(
        self,
        messages: list[dict]
    ) -> str:
        api_key = os.getenv(
            "GROQ_API_KEY"
        )

        model = os.getenv(
            "GROQ_MODEL",
            "llama-3.1-70b-versatile"
        )

        if not api_key:
            raise AIProviderError(
                "Missing GROQ_API_KEY"
            )

        url = (
            "https://api.groq.com/"
            "openai/v1/chat/completions"
        )

        headers = {
            "Authorization": (
                f"Bearer {api_key}"
            ),
            "Content-Type": (
                "application/json"
            )
        }

        payload = {
            "model": model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens
        }

        logger.info(
            "Sending Groq request..."
        )

        async with httpx.AsyncClient(
            timeout=self.timeout
        ) as client:
            response = await client.post(
                url,
                headers=headers,
                json=payload
            )

        logger.info(
            "Groq response status=%s",
            response.status_code
        )

        if response.status_code >= 400:
            raise AIProviderError(
                response.text
            )

        data = response.json()

        try:
            return data["choices"][0][
                "message"
            ]["content"]

        except Exception as exc:
            logger.exception(
                "Invalid Groq response"
            )

            raise AIProviderError(
                "Failed to parse "
                "Groq response"
            ) from exc

    async def _gemini_chat(
        self,
        messages: list[dict]
    ) -> str:
        api_key = os.getenv(
            "GEMINI_API_KEY"
        )

        model = os.getenv(
            "GEMINI_MODEL",
            "gemini-1.5-flash"
        )

        if not api_key:
            raise AIProviderError(
                "Missing GEMINI_API_KEY"
            )

        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{model}:generateContent"
            f"?key={api_key}"
        )

        prompt = "\n".join(
            msg["content"]
            for msg in messages
        )

        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": prompt
                        }
                    ]
                }
            ]
        }

        logger.info(
            "Sending Gemini request..."
        )

        async with httpx.AsyncClient(
            timeout=self.timeout
        ) as client:
            response = await client.post(
                url,
                json=payload
            )

        logger.info(
            "Gemini response status=%s",
            response.status_code
        )

        if response.status_code >= 400:
            raise AIProviderError(
                response.text
            )

        data = response.json()

        try:
            return data["candidates"][0][
                "content"
            ]["parts"][0]["text"]

        except Exception as exc:
            logger.exception(
                "Invalid Gemini response"
            )

            raise AIProviderError(
                "Failed to parse "
                "Gemini response"
            ) from exc

    async def _openai_chat(
        self,
        messages: list[dict]
    ) -> str:
        raise AIProviderError(
            "OpenAI provider "
            "not implemented yet"
        )

    async def _openrouter_chat(
        self,
        messages: list[dict]
    ) -> str:
        raise AIProviderError(
            "OpenRouter provider "
            "not implemented yet"
        )
