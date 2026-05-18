from __future__ import annotations

import copy
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha1
from typing import Any, Dict, List, Optional, Set, Tuple

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)

from app.tools.native_selector import (
    FunctionCallResult,
)


logger = logging.getLogger(__name__)


class ProviderType(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GROQ = "groq"
    GOOGLE = "google"
    GENERIC = "generic"


class TranslationResult(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    DENIED = "denied"
    INVALID = "invalid"


@dataclass(slots=True)
class ProviderRequest:
    provider: ProviderType
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    tool_name: str
    parameters: Dict[str, Any]
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ProviderPayload:
    provider: str
    payload: Dict[str, Any]
    translated_at: float
    checksum: str
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ValidationResult:
    valid: bool
    reason: Optional[str] = None


class RuntimeSignatureValidator:
    """
    Lightweight runtime validator.

    Responsibilities:
    - Parameter type validation
    - Default deny enforcement
    - Provider schema validation
    - Signature verification
    """

    MAX_STRING_LENGTH = 8192

    def validate_parameters(
        self,
        *,
        schema: Dict[str, Any],
        parameters: Dict[str, Any],
    ) -> ValidationResult:
        properties = (
            schema.get(
                "parameters",
                {},
            ).get(
                "properties",
                {},
            )
        )

        required = (
            schema.get(
                "parameters",
                {},
            ).get(
                "required",
                [],
            )
        )

        for field in required:
            if field not in parameters:
                return ValidationResult(
                    valid=False,
                    reason=(
                        f"Missing required field: {field}"
                    ),
                )

        for key, value in (
            parameters.items()
        ):
            definition = properties.get(key)

            if not definition:
                return ValidationResult(
                    valid=False,
                    reason=(
                        f"Unexpected field: {key}"
                    ),
                )

            valid = self._validate_type(
                value,
                definition.get(
                    "type",
                    "string",
                ),
            )

            if not valid:
                return ValidationResult(
                    valid=False,
                    reason=(
                        f"Invalid type for: {key}"
                    ),
                )

        return ValidationResult(valid=True)

    def validate_provider_payload(
        self,
        provider: ProviderType,
        payload: Dict[str, Any],
    ) -> ValidationResult:
        try:
            if provider == ProviderType.OPENAI:
                if "type" not in payload:
                    return ValidationResult(
                        valid=False,
                        reason=(
                            "OpenAI payload missing type"
                        ),
                    )

            elif provider == (
                ProviderType.ANTHROPIC
            ):
                if "input_schema" not in payload:
                    return ValidationResult(
                        valid=False,
                        reason=(
                            "Anthropic payload missing input_schema"
                        ),
                    )

            elif provider == ProviderType.GOOGLE:
                if "function_declarations" not in payload:
                    return ValidationResult(
                        valid=False,
                        reason=(
                            "Google payload missing function_declarations"
                        ),
                    )

            return ValidationResult(valid=True)

        except Exception as exc:
            return ValidationResult(
                valid=False,
                reason=str(exc),
            )

    def _validate_type(
        self,
        value: Any,
        expected_type: str,
    ) -> bool:
        if expected_type == "string":
            return (
                isinstance(value, str)
                and len(value)
                <= self.MAX_STRING_LENGTH
            )

        if expected_type == "integer":
            return isinstance(value, int)

        if expected_type == "number":
            return isinstance(
                value,
                (
                    int,
                    float,
                ),
            )

        if expected_type == "boolean":
            return isinstance(value, bool)

        if expected_type == "array":
            return isinstance(value, list)

        if expected_type == "object":
            return isinstance(value, dict)

        return True


class SchemaTranslator:
    """
    Pure lightweight provider schema mapper.

    No heavy abstraction layers.
    """

    def to_openai(
        self,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": schema["name"],
                "description":
                    schema.get(
                        "description",
                        "",
                    ),
                "parameters":
                    schema.get(
                        "parameters",
                        {},
                    ),
            },
        }

    def to_anthropic(
        self,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "name": schema["name"],
            "description":
                schema.get(
                    "description",
                    "",
                ),
            "input_schema":
                schema.get(
                    "parameters",
                    {},
                ),
        }

    def to_groq(
        self,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": schema["name"],
                "description":
                    schema.get(
                        "description",
                        "",
                    ),
                "parameters":
                    schema.get(
                        "parameters",
                        {},
                    ),
            },
        }

    def to_google(
        self,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "function_declarations": [
                {
                    "name":
                        schema["name"],
                    "description":
                        schema.get(
                            "description",
                            "",
                        ),
                    "parameters":
                        schema.get(
                            "parameters",
                            {},
                        ),
                }
            ]
        }

    def normalize_payload(
        self,
        provider: ProviderType,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        if provider in {
            ProviderType.OPENAI,
            ProviderType.GROQ,
        }:
            function = payload.get(
                "function",
                {},
            )

            return {
                "name":
                    function.get(
                        "name"
                    ),
                "description":
                    function.get(
                        "description"
                    ),
                "parameters":
                    function.get(
                        "parameters"
                    ),
            }

        if provider == (
            ProviderType.ANTHROPIC
        ):
            return {
                "name":
                    payload.get(
                        "name"
                    ),
                "description":
                    payload.get(
                        "description"
                    ),
                "parameters":
                    payload.get(
                        "input_schema"
                    ),
            }

        if provider == ProviderType.GOOGLE:
            declarations = payload.get(
                "function_declarations",
                [],
            )

            if not declarations:
                return {}

            item = declarations[0]

            return {
                "name":
                    item.get(
                        "name"
                    ),
                "description":
                    item.get(
                        "description"
                    ),
                "parameters":
                    item.get(
                        "parameters"
                    ),
            }

        return payload


class FailoverAdapter:
    """
    Dynamic provider failover translator.
    """

    def __init__(
        self,
        translator: SchemaTranslator,
    ) -> None:
        self._translator = translator

    def translate_failover(
        self,
        *,
        source_provider: ProviderType,
        target_provider: ProviderType,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        normalized = (
            self._translator.normalize_payload(
                source_provider,
                payload,
            )
        )

        if (
            target_provider
            == ProviderType.OPENAI
        ):
            return self._translator.to_openai(
                normalized
            )

        if (
            target_provider
            == ProviderType.ANTHROPIC
        ):
            return (
                self._translator.to_anthropic(
                    normalized
                )
            )

        if (
            target_provider
            == ProviderType.GROQ
        ):
            return self._translator.to_groq(
                normalized
            )

        if (
            target_provider
            == ProviderType.GOOGLE
        ):
            return (
                self._translator.to_google(
                    normalized
                )
            )

        return normalized


class ProviderAwareBridge:
    """
    Provider-aware Toolchain Runtime.

    Features:
    - Provider schema translation
    - Runtime provider adaptation
    - Failover schema translation
    - RBAC enforcement
    - Default deny security
    - Lightweight normalized mapping
    - Async-safe provider switching
    """

    CACHE_LIMIT = 128

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

        self._translator = (
            SchemaTranslator()
        )

        self._validator = (
            RuntimeSignatureValidator()
        )

        self._failover_adapter = (
            FailoverAdapter(
                self._translator
            )
        )

        self._translation_cache: Dict[
            str,
            ProviderPayload,
        ] = {}

    async def translate_tool(
        self,
        *,
        provider: ProviderType,
        tool_schema: Dict[str, Any],
    ) -> ProviderPayload:
        cache_key = self._cache_key(
            provider.value,
            tool_schema,
        )

        cached = (
            self._translation_cache.get(
                cache_key
            )
        )

        if cached:
            return cached

        translated = (
            self._translate_schema(
                provider,
                tool_schema,
            )
        )

        validation = (
            self._validator.validate_provider_payload(
                provider,
                translated,
            )
        )

        if not validation.valid:
            raise ValueError(
                validation.reason
            )

        payload = ProviderPayload(
            provider=provider.value,
            payload=translated,
            translated_at=time.time(),
            checksum=self._checksum(
                translated
            ),
        )

        self._store_cache(
            cache_key,
            payload,
        )

        return payload

    async def validate_execution(
        self,
        *,
        request: ProviderRequest,
        schema: Dict[str, Any],
    ) -> ValidationResult:
        """
        Default deny + RBAC validation.
        """

        context = RouteContext(
            requester_id=(
                request.requester_id
            ),
            requester_roles=(
                request.requester_roles
            ),
            requester_permissions=(
                request.requester_permissions
            ),
            task_type=request.tool_name,
        )

        route = await self.router.route(
            task=request.tool_name,
            context=context,
        )

        if (
            route.decision
            != RouteDecision.ALLOWED
        ):
            return ValidationResult(
                valid=False,
                reason=(
                    "RBAC denied access"
                ),
            )

        return (
            self._validator.validate_parameters(
                schema=schema,
                parameters=request.parameters,
            )
        )

    async def failover_translate(
        self,
        *,
        source_provider: ProviderType,
        target_provider: ProviderType,
        payload: Dict[str, Any],
    ) -> ProviderPayload:
        translated = (
            self._failover_adapter.translate_failover(
                source_provider=
                    source_provider,
                target_provider=
                    target_provider,
                payload=payload,
            )
        )

        validation = (
            self._validator.validate_provider_payload(
                target_provider,
                translated,
            )
        )

        if not validation.valid:
            raise ValueError(
                validation.reason
            )

        return ProviderPayload(
            provider=target_provider.value,
            payload=translated,
            translated_at=time.time(),
            checksum=self._checksum(
                translated
            ),
            metadata={
                "failover_from":
                    source_provider.value,
            },
        )

    async def adapt_function_call(
        self,
        *,
        provider: ProviderType,
        result: FunctionCallResult,
    ) -> ProviderPayload:
        if not result.schema:
            raise ValueError(
                "Missing function schema"
            )

        translated = await self.translate_tool(
            provider=provider,
            tool_schema=result.schema,
        )

        payload = copy.deepcopy(
            translated.payload
        )

        if provider in {
            ProviderType.OPENAI,
            ProviderType.GROQ,
        }:
            payload["function_call"] = {
                "name":
                    result.tool_name,
                "arguments":
                    json.dumps(
                        result.parameters,
                        ensure_ascii=False,
                    ),
            }

        elif (
            provider
            == ProviderType.ANTHROPIC
        ):
            payload["tool_choice"] = {
                "name":
                    result.tool_name,
            }

            payload["input"] = (
                result.parameters
            )

        elif (
            provider
            == ProviderType.GOOGLE
        ):
            payload["tool_config"] = {
                "function_calling_config": {
                    "mode": "AUTO"
                }
            }

            payload["arguments"] = (
                result.parameters
            )

        return ProviderPayload(
            provider=provider.value,
            payload=payload,
            translated_at=time.time(),
            checksum=self._checksum(
                payload
            ),
        )

    def supported_providers(
        self,
    ) -> List[str]:
        return [
            provider.value
            for provider in ProviderType
        ]

    def translation_stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "cache_entries":
                len(
                    self._translation_cache
                ),
            "supported_providers":
                self.supported_providers(),
            "timestamp":
                time.time(),
        }

    def clear_cache(
        self,
    ) -> None:
        self._translation_cache.clear()

    def _translate_schema(
        self,
        provider: ProviderType,
        schema: Dict[str, Any],
    ) -> Dict[str, Any]:
        if provider == ProviderType.OPENAI:
            return self._translator.to_openai(
                schema
            )

        if provider == (
            ProviderType.ANTHROPIC
        ):
            return (
                self._translator.to_anthropic(
                    schema
                )
            )

        if provider == ProviderType.GROQ:
            return self._translator.to_groq(
                schema
            )

        if provider == ProviderType.GOOGLE:
            return (
                self._translator.to_google(
                    schema
                )
            )

        return schema

    def _cache_key(
        self,
        provider: str,
        schema: Dict[str, Any],
    ) -> str:
        raw = json.dumps(
            schema,
            sort_keys=True,
            ensure_ascii=False,
        )

        return sha1(
            (
                provider + raw
            ).encode("utf-8")
        ).hexdigest()

    def _checksum(
        self,
        payload: Dict[str, Any],
    ) -> str:
        raw = json.dumps(
            payload,
            sort_keys=True,
            ensure_ascii=False,
        )

        return sha1(
            raw.encode("utf-8")
        ).hexdigest()[:16]

    def _store_cache(
        self,
        key: str,
        payload: ProviderPayload,
    ) -> None:
        if (
            len(self._translation_cache)
            >= self.CACHE_LIMIT
        ):
            oldest = next(
                iter(
                    self._translation_cache
                )
            )

            self._translation_cache.pop(
                oldest,
                None,
            )

        self._translation_cache[key] = (
            payload
        )
