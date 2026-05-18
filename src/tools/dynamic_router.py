from __future__ import annotations

import asyncio
import inspect
import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple


logger = logging.getLogger(__name__)


class PermissionEffect(str, Enum):
    ALLOW = "allow"
    DENY = "deny"


class ToolScope(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    ADMIN = "admin"
    SYSTEM = "system"


class RouteDecision(str, Enum):
    ALLOWED = "allowed"
    DENIED = "denied"
    NOT_FOUND = "not_found"
    INVALID_SIGNATURE = "invalid_signature"
    INVALID_CONTEXT = "invalid_context"


@dataclass(slots=True)
class ToolMetadata:
    tool_name: str
    description: str
    handler: Callable[..., Any]
    capabilities: Set[str]
    required_roles: Set[str]
    required_permissions: Set[str]
    scope: ToolScope
    timeout_seconds: int = 30
    enabled: bool = True
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RouteContext:
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    task_type: str
    namespace: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RouteResult:
    decision: RouteDecision
    tool_name: Optional[str]
    reason: str
    execution_time_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class RBACPolicy:
    """
    Lightweight RBAC + Default Deny policy evaluator.
    Optimized for low-memory VPS environments.
    """

    def __init__(self) -> None:
        self._role_permissions: Dict[str, Set[str]] = {}

    def register_role(
        self,
        role: str,
        permissions: Set[str],
    ) -> None:
        self._role_permissions[role] = permissions

    def has_access(
        self,
        *,
        tool: ToolMetadata,
        context: RouteContext,
    ) -> bool:
        """
        Default Deny:
        - Explicit role match required if roles exist
        - Explicit permissions required if permissions exist
        """

        if not tool.enabled:
            return False

        if tool.required_roles:
            if not (
                tool.required_roles &
                context.requester_roles
            ):
                return False

        effective_permissions = set(
            context.requester_permissions
        )

        for role in context.requester_roles:
            effective_permissions.update(
                self._role_permissions.get(
                    role,
                    set(),
                )
            )

        if tool.required_permissions:
            if not (
                tool.required_permissions <=
                effective_permissions
            ):
                return False

        if tool.scope == ToolScope.ADMIN:
            return "admin" in context.requester_roles

        if tool.scope == ToolScope.SYSTEM:
            return "system" in context.requester_roles

        return True


class DynamicToolRouter:
    """
    Async-first Dynamic Tool Router.

    Features:
    - Dynamic runtime routing
    - Lightweight in-memory registry
    - Default-deny RBAC security
    - Signature validation
    - Capability-aware routing
    - Minimal RAM overhead
    - Async-safe execution
    """

    ROUTE_CACHE_LIMIT = 256

    def __init__(self) -> None:
        self._tools: Dict[str, ToolMetadata] = {}

        self._capability_index: Dict[
            str,
            Set[str],
        ] = {}

        self._route_cache: Dict[
            str,
            Tuple[str, float],
        ] = {}

        self._rbac = RBACPolicy()

        self._lock = asyncio.Lock()

    async def register_tool(
        self,
        *,
        tool_name: str,
        handler: Callable[..., Any],
        description: str,
        capabilities: Optional[Set[str]] = None,
        required_roles: Optional[Set[str]] = None,
        required_permissions: Optional[
            Set[str]
        ] = None,
        scope: ToolScope = ToolScope.INTERNAL,
        timeout_seconds: int = 30,
        tags: Optional[Set[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self._lock:
            if tool_name in self._tools:
                raise ValueError(
                    f"Tool already registered: {tool_name}"
                )

            self._validate_handler(handler)

            tool = ToolMetadata(
                tool_name=tool_name,
                description=description,
                handler=handler,
                capabilities=capabilities or set(),
                required_roles=required_roles or set(),
                required_permissions=(
                    required_permissions or set()
                ),
                scope=scope,
                timeout_seconds=timeout_seconds,
                tags=tags or set(),
                metadata=metadata or {},
            )

            self._tools[tool_name] = tool

            for capability in tool.capabilities:
                if capability not in (
                    self._capability_index
                ):
                    self._capability_index[
                        capability
                    ] = set()

                self._capability_index[
                    capability
                ].add(tool_name)

            logger.info(
                "Tool registered | name=%s",
                tool_name,
            )

    async def unregister_tool(
        self,
        tool_name: str,
    ) -> None:
        async with self._lock:
            tool = self._tools.pop(
                tool_name,
                None,
            )

            if not tool:
                return

            for capability in tool.capabilities:
                tools = self._capability_index.get(
                    capability
                )

                if tools:
                    tools.discard(tool_name)

            self._invalidate_route_cache()

            logger.info(
                "Tool unregistered | name=%s",
                tool_name,
            )

    async def route(
        self,
        *,
        task: str,
        context: RouteContext,
    ) -> RouteResult:
        start = time.perf_counter()

        normalized = self._normalize_task(task)

        try:
            tool = await self._resolve_tool(
                normalized
            )

            if not tool:
                return RouteResult(
                    decision=RouteDecision.NOT_FOUND,
                    tool_name=None,
                    reason="No matching tool found",
                    execution_time_ms=self._elapsed_ms(
                        start
                    ),
                )

            if not self._validate_context(
                context
            ):
                return RouteResult(
                    decision=(
                        RouteDecision.INVALID_CONTEXT
                    ),
                    tool_name=tool.tool_name,
                    reason="Invalid routing context",
                    execution_time_ms=self._elapsed_ms(
                        start
                    ),
                )

            allowed = self._rbac.has_access(
                tool=tool,
                context=context,
            )

            if not allowed:
                return RouteResult(
                    decision=RouteDecision.DENIED,
                    tool_name=tool.tool_name,
                    reason=(
                        "RBAC policy denied access"
                    ),
                    execution_time_ms=self._elapsed_ms(
                        start
                    ),
                )

            return RouteResult(
                decision=RouteDecision.ALLOWED,
                tool_name=tool.tool_name,
                reason="Route allowed",
                execution_time_ms=self._elapsed_ms(
                    start
                ),
                metadata={
                    "scope": tool.scope.value,
                    "capabilities": list(
                        tool.capabilities
                    ),
                },
            )

        except Exception as exc:
            logger.exception(
                "Routing failure | task=%s",
                task,
            )

            return RouteResult(
                decision=(
                    RouteDecision.INVALID_CONTEXT
                ),
                tool_name=None,
                reason=str(exc),
                execution_time_ms=self._elapsed_ms(
                    start
                ),
            )

    async def execute(
        self,
        *,
        task: str,
        context: RouteContext,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        route = await self.route(
            task=task,
            context=context,
        )

        if route.decision != RouteDecision.ALLOWED:
            raise PermissionError(
                route.reason
            )

        tool = self._tools.get(
            route.tool_name
        )

        if not tool:
            raise LookupError(
                "Tool disappeared during execution"
            )

        payload = payload or {}

        logger.info(
            "Executing tool | tool=%s requester=%s",
            tool.tool_name,
            context.requester_id,
        )

        return await self._execute_handler(
            tool,
            payload,
        )

    async def _execute_handler(
        self,
        tool: ToolMetadata,
        payload: Dict[str, Any],
    ) -> Any:
        try:
            result = tool.handler(**payload)

            if inspect.isawaitable(result):
                return await asyncio.wait_for(
                    result,
                    timeout=tool.timeout_seconds,
                )

            return result

        except asyncio.TimeoutError:
            logger.error(
                "Tool execution timeout | tool=%s",
                tool.tool_name,
            )

            raise TimeoutError(
                f"Tool timeout: {tool.tool_name}"
            )

    async def _resolve_tool(
        self,
        normalized_task: str,
    ) -> Optional[ToolMetadata]:
        cache_key = normalized_task

        cached = self._route_cache.get(
            cache_key
        )

        if cached:
            tool_name, _ = cached

            return self._tools.get(tool_name)

        capability_matches = (
            self._match_capabilities(
                normalized_task
            )
        )

        if capability_matches:
            tool_name = capability_matches[0]

            self._cache_route(
                cache_key,
                tool_name,
            )

            return self._tools.get(tool_name)

        direct_match = self._tools.get(
            normalized_task
        )

        if direct_match:
            self._cache_route(
                cache_key,
                direct_match.tool_name,
            )

            return direct_match

        return None

    def _match_capabilities(
        self,
        task: str,
    ) -> List[str]:
        matched_tools: List[str] = []

        keywords = set(
            re.findall(
                r"[a-zA-Z0-9_]+",
                task.lower(),
            )
        )

        for capability, tools in (
            self._capability_index.items()
        ):
            capability_keywords = set(
                capability.lower().split(".")
            )

            if capability_keywords & keywords:
                matched_tools.extend(
                    list(tools)
                )

        return matched_tools

    def _validate_handler(
        self,
        handler: Callable[..., Any],
    ) -> None:
        if not callable(handler):
            raise TypeError(
                "Tool handler must be callable"
            )

        signature = inspect.signature(
            handler
        )

        for param in signature.parameters.values():
            if (
                param.kind ==
                inspect.Parameter.VAR_KEYWORD
            ):
                return

        logger.warning(
            "Tool handler missing flexible kwargs signature"
        )

    def _validate_context(
        self,
        context: RouteContext,
    ) -> bool:
        if not context.requester_id:
            return False

        if not context.task_type:
            return False

        return True

    def _normalize_task(
        self,
        task: str,
    ) -> str:
        return (
            task.strip()
            .lower()
            .replace(" ", ".")
        )

    def _cache_route(
        self,
        task: str,
        tool_name: str,
    ) -> None:
        if (
            len(self._route_cache)
            >= self.ROUTE_CACHE_LIMIT
        ):
            oldest = next(
                iter(self._route_cache)
            )

            self._route_cache.pop(
                oldest,
                None,
            )

        self._route_cache[task] = (
            tool_name,
            time.time(),
        )

    def _invalidate_route_cache(
        self,
    ) -> None:
        self._route_cache.clear()

    def register_role_policy(
        self,
        role: str,
        permissions: Set[str],
    ) -> None:
        self._rbac.register_role(
            role,
            permissions,
        )

    def list_tools(
        self,
    ) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []

        for tool in self._tools.values():
            result.append(
                {
                    "tool_name":
                        tool.tool_name,
                    "scope":
                        tool.scope.value,
                    "enabled":
                        tool.enabled,
                    "capabilities":
                        list(tool.capabilities),
                    "required_roles":
                        list(
                            tool.required_roles
                        ),
                    "required_permissions":
                        list(
                            tool.required_permissions
                        ),
                    "tags":
                        list(tool.tags),
                }
            )

        return result

    def get_tool(
        self,
        tool_name: str,
    ) -> Optional[ToolMetadata]:
        return self._tools.get(
            tool_name
        )

    async def enable_tool(
        self,
        tool_name: str,
    ) -> bool:
        tool = self._tools.get(
            tool_name
        )

        if not tool:
            return False

        tool.enabled = True

        return True

    async def disable_tool(
        self,
        tool_name: str,
    ) -> bool:
        tool = self._tools.get(
            tool_name
        )

        if not tool:
            return False

        tool.enabled = False

        return True

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "registered_tools":
                len(self._tools),
            "capability_indexes":
                len(
                    self._capability_index
                ),
            "route_cache_size":
                len(self._route_cache),
            "timestamp":
                time.time(),
        }

    def _elapsed_ms(
        self,
        start: float,
    ) -> float:
        return round(
            (
                time.perf_counter() - start
            ) * 1000,
            2,
        )
