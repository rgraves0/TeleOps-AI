from __future__ import annotations

import os
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()


class AIProviderException(Exception):
    pass


class AIProvider:
    def __init__(self):
        self.provider = os.getenv(
            "AI_PROVIDER",
            "groq"
        ).lower()

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

        self.timeout = int(
            os.getenv(
                "AI_TIMEOUT_SECONDS",
                "60"
            )
        )

        self.providers = {
            "openai": {
                "api_key": os.getenv(
                    "OPENAI_API_KEY"
                ),
                "model": os.getenv(
                    "OPENAI_MODEL",
                    "gpt-4o-mini"
                ),
                "base_url": "https://api.openai.com/v1/chat/completions"
            },

            "groq": {
                "api_key": os.getenv(
                    "GROQ_API_KEY"
                ),
                "model": os.getenv(
                    "GROQ_MODEL",
                    "llama-3.1-70b-versatile"
                ),
                "base_url": "https://api.groq.com/openai/v1/chat/completions"
            },

            "openrouter": {
                "api_key": os.getenv(
                    "OPENROUTER_API_KEY"
                ),
                "model": os.getenv(
                    "OPENROUTER_MODEL"
                ),
                "base_url": "https://openrouter.ai/api/v1/chat/completions"
            },

            "gemini": {
                "api_key": os.getenv(
                    "GEMINI_API_KEY"
                ),
                "model": os.getenv(
                    "GEMINI_MODEL",
                    "gemini-1.5-flash"
                ),
                "base_url": (
                    "https://generativelanguage.googleapis.com"
                    "/v1beta/models"
                )
            }
        }

    def _get_provider_config(self) -> dict:
        config = self.providers.get(
            self.provider
        )

        if config is None:
            raise AIProviderException(
                f"Unsupported provider: {self.provider}"
            )

        if not config["api_key"]:
            raise AIProviderException(
                f"Missing API key for provider: {self.provider}"
            )

        return config

    async def generate_response(
        self,
        messages: list[dict[str, str]],
        temperature: float | None = None,
        max_tokens: int | None = None
    ) -> str:
        config = self._get_provider_config()

        if self.provider == "gemini":
            return await self._call_gemini(
                config=config,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )

        return await self._call_openai_compatible(
            config=config,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens
        )

    async def _call_openai_compatible(
        self,
        config: dict,
        messages: list[dict[str, str]],
        temperature: float | None,
        max_tokens: int | None
    ) -> str:
        payload = {
            "model": config["model"],
            "messages": messages,
            "temperature": (
                temperature
                if temperature is not None
                else self.temperature
            ),
            "max_tokens": (
                max_tokens
                if max_tokens is not None
                else self.max_tokens
            )
        }

        headers = {
            "Authorization": (
                f"Bearer {config['api_key']}"
            ),
            "Content-Type": "application/json"
        }

        if self.provider == "openrouter":
            headers["HTTP-Referer"] = (
                "https://teleops-ai.local"
            )

            headers["X-Title"] = "TeleOps-AI"

        async with httpx.AsyncClient(
            timeout=self.timeout
        ) as client:
            response = await client.post(
                config["base_url"],
                headers=headers,
                json=payload
            )

        if response.status_code >= 400:
            raise AIProviderException(
                f"{self.provider} API error: "
                f"{response.status_code} "
                f"{response.text}"
            )

        data = response.json()

        try:
            return (
                data["choices"][0]
                ["message"]["content"]
                .strip()
            )

        except (
            KeyError,
            IndexError
        ) as exc:
            raise AIProviderException(
                "Invalid AI response format"
            ) from exc

    async def _call_gemini(
        self,
        config: dict,
        messages: list[dict[str, str]],
        temperature: float | None,
        max_tokens: int | None
    ) -> str:
        contents = []

        for message in messages:
            role = (
                "model"
                if message["role"] == "assistant"
                else "user"
            )

            contents.append({
                "role": role,
                "parts": [
                    {
                        "text": message["content"]
                    }
                ]
            })

        payload = {
            "contents": contents,
            "generationConfig": {
                "temperature": (
                    temperature
                    if temperature is not None
                    else self.temperature
                ),
                "maxOutputTokens": (
                    max_tokens
                    if max_tokens is not None
                    else self.max_tokens
                )
            }
        }

        endpoint = (
            f"{config['base_url']}/"
            f"{config['model']}:generateContent"
            f"?key={config['api_key']}"
        )

        async with httpx.AsyncClient(
            timeout=self.timeout
        ) as client:
            response = await client.post(
                endpoint,
                json=payload
            )

        if response.status_code >= 400:
            raise AIProviderException(
                f"Gemini API error: "
                f"{response.status_code} "
                f"{response.text}"
            )

        data = response.json()

        try:
            return (
                data["candidates"][0]
                ["content"]["parts"][0]["text"]
                .strip()
            )

        except (
            KeyError,
            IndexError
        ) as exc:
            raise AIProviderException(
                "Invalid Gemini response format"
            ) from exc
