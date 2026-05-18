from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import (
    defaultdict,
    deque,
)
from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

from app.core.message_bus import (
    MessageBus,
)

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


class AbuseAction(
    str,
    Enum,
):
    ALLOW = "allow"
    THROTTLE = "throttle"
    BLOCK = "block"
    REVOKE = "revoke"


@dataclass(slots=True)
class ExecutionEvent:
    actor_id: str
    workflow_id: str
    tool_name: str
    timestamp: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class RateLimitResult:
    allowed: bool
    remaining_tokens: float
    retry_after: float
    action: AbuseAction
    reason: Optional[str] = None


@dataclass(slots=True)
class LoopAnalysisResult:
    loop_detected: bool
    repetitive_count: int
    action: AbuseAction
    reason: Optional[str] = None


class ExecutionLoopDetector:
    """
    Recursive/redundant execution detector.
    """

    HISTORY_LIMIT = 32

    def __init__(
        self,
        *,
        max_loop_threshold: int = 6,
    ) -> None:
        self.max_loop_threshold = max(
            2,
            max_loop_threshold,
        )

        self._histories: Dict[
            str,
            Deque[str],
        ] = defaultdict(
            lambda: deque(
                maxlen=self.HISTORY_LIMIT
            )
        )

    async def track(
        self,
        *,
        workflow_id: str,
        tool_name: str,
    ) -> LoopAnalysisResult:

        history = self._histories[
            workflow_id
        ]

        history.append(
            tool_name
        )

        repetitive_count = (
            self._count_repetitions(
                history
            )
        )

        if (
            repetitive_count
            >= self.max_loop_threshold
        ):
            return LoopAnalysisResult(
                loop_detected=True,
                repetitive_count=
                    repetitive_count,
                action=
                    AbuseAction.REVOKE,
                reason=
                    "Recursive execution loop detected",
            )

        if (
            repetitive_count
            >= (
                self.max_loop_threshold
                - 2
            )
        ):
            return LoopAnalysisResult(
                loop_detected=True,
                repetitive_count=
                    repetitive_count,
                action=
                    AbuseAction.THROTTLE,
                reason=
                    "Potential recursive execution pattern",
            )

        return LoopAnalysisResult(
            loop_detected=False,
            repetitive_count=
                repetitive_count,
            action=
                AbuseAction.ALLOW,
        )

    def clear_workflow(
        self,
        workflow_id: str,
    ) -> None:
        self._histories.pop(
            workflow_id,
            None,
        )

    def _count_repetitions(
        self,
        history: Deque[str],
    ) -> int:
        if not history:
            return 0

        latest = history[-1]

        count = 0

        for tool in reversed(
            history
        ):
            if tool == latest:
                count += 1
            else:
                break

        return count

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "tracked_workflows":
                len(
                    self._histories
                ),
            "threshold":
                self.max_loop_threshold,
        }


class TokenBucket:
    """
    Lightweight token bucket limiter.
    """

    def __init__(
        self,
        *,
        capacity: int,
        refill_rate: float,
    ) -> None:
        self.capacity = float(
            capacity
        )

        self.tokens = float(
            capacity
        )

        self.refill_rate = (
            refill_rate
        )

        self.last_refill = (
            time.monotonic()
        )

    def consume(
        self,
        amount: float = 1.0,
    ) -> RateLimitResult:
        self._refill()

        if self.tokens >= amount:
            self.tokens -= amount

            return RateLimitResult(
                allowed=True,
                remaining_tokens=
                    self.tokens,
                retry_after=0.0,
                action=
                    AbuseAction.ALLOW,
            )

        required = (
            amount - self.tokens
        )

        retry_after = (
            required
            / self.refill_rate
        )

        action = (
            AbuseAction.THROTTLE
        )

        if retry_after > 15:
            action = (
                AbuseAction.BLOCK
            )

        return RateLimitResult(
            allowed=False,
            remaining_tokens=
                self.tokens,
            retry_after=
                retry_after,
            action=action,
            reason=
                "Rate limit exceeded",
        )

    def _refill(
        self,
    ) -> None:
        now = time.monotonic()

        elapsed = (
            now - self.last_refill
        )

        refill = (
            elapsed
            * self.refill_rate
        )

        if refill <= 0:
            return

        self.tokens = min(
            self.capacity,
            self.tokens + refill,
        )

        self.last_refill = now


class DynamicRateLimiter:
    """
    Dynamic multi-identity rate limiter.
    """

    DEFAULT_CAPACITY = 30
    DEFAULT_REFILL_RATE = 1.5

    CLEANUP_TTL = 3600

    def __init__(
        self,
    ) -> None:
        self._buckets: Dict[
            str,
            TokenBucket,
        ] = {}

        self._last_seen: Dict[
            str,
            float,
        ] = {}

    async def evaluate(
        self,
        *,
        identity: str,
        cost: float = 1.0,
        capacity: Optional[
            int
        ] = None,
        refill_rate: Optional[
            float
        ] = None,
    ) -> RateLimitResult:

        bucket = (
            self._buckets.get(
                identity
            )
        )

        if not bucket:
            bucket = TokenBucket(
                capacity=
                    capacity
                    or self.DEFAULT_CAPACITY,
                refill_rate=
                    refill_rate
                    or self.DEFAULT_REFILL_RATE,
            )

            self._buckets[
                identity
            ] = bucket

        self._last_seen[
            identity
        ] = time.time()

        return bucket.consume(
            amount=cost
        )

    async def cleanup(
        self,
    ) -> None:
        now = time.time()

        expired: List[str] = []

        for identity, last_seen in (
            self._last_seen.items()
        ):
            if (
                now - last_seen
                > self.CLEANUP_TTL
            ):
                expired.append(
                    identity
                )

        for identity in expired:
            self._buckets.pop(
                identity,
                None,
            )

            self._last_seen.pop(
                identity,
                None,
            )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "tracked_buckets":
                len(
                    self._buckets
                ),
        }


class SecurityRevocationManager:
    """
    Execution token revocation manager.
    """

    REVOCATION_TTL = 1800

    def __init__(
        self,
    ) -> None:
        self._revoked: Dict[
            str,
            float,
        ] = {}

    async def revoke(
        self,
        actor_id: str,
    ) -> None:
        self._revoked[
            actor_id
        ] = time.time()

    async def is_revoked(
        self,
        actor_id: str,
    ) -> bool:
        revoked_at = (
            self._revoked.get(
                actor_id
            )
        )

        if not revoked_at:
            return False

        if (
            time.time()
            - revoked_at
            > self.REVOCATION_TTL
        ):
            self._revoked.pop(
                actor_id,
                None,
            )

            return False

        return True

    async def cleanup(
        self,
    ) -> None:
        now = time.time()

        expired = [
            actor
            for actor, ts
            in self._revoked.items()
            if (
                now - ts
                > self.REVOCATION_TTL
            )
        ]

        for actor in expired:
            self._revoked.pop(
                actor,
                None,
            )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "revoked_actors":
                len(
                    self._revoked
                ),
        }


class AbuseProtectionRBAC:
    """
    Default Deny RBAC validator.
    """

    REQUIRED_PERMISSION = (
        "execution.runtime"
    )

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> bool:

        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        context = RouteContext(
            requester_id=
                requester_id,
            requester_roles=
                roles,
            requester_permissions=
                permissions,
            task_type=
                "execution.runtime",
            metadata=metadata or {},
        )

        route = await self.router.route(
            task=
                "execution.runtime",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class AntiLoopAbusePreventer:
    """
    Async-first Anti-loop & Abuse Prevention Engine.

    Features:
    - Recursive execution detection
    - Token bucket throttling
    - Dynamic abuse prevention
    - Execution revocation
    - Security alert broadcasting
    - Default Deny enforcement
    - Low memory overhead
    """

    MAINTENANCE_INTERVAL = 600

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        message_bus: Optional[
            MessageBus
        ] = None,
        loop_threshold: int = 6,
    ) -> None:

        self.router = router

        self.message_bus = (
            message_bus
        )

        self._rbac = (
            AbuseProtectionRBAC(
                router
            )
        )

        self._loop_detector = (
            ExecutionLoopDetector(
                max_loop_threshold=
                    loop_threshold
            )
        )

        self._rate_limiter = (
            DynamicRateLimiter()
        )

        self._revocation = (
            SecurityRevocationManager()
        )

        self._running = False

        self._maintenance_task: Optional[
            asyncio.Task
        ] = None

        self._blocked_events = 0

        self._throttled_events = 0

        self._revoked_events = 0

    async def start(
        self,
    ) -> None:
        logger.info(
            "Starting AntiLoopAbusePreventer"
        )

        self._running = True

        self._maintenance_task = (
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(
        self,
    ) -> None:
        logger.info(
            "Stopping AntiLoopAbusePreventer"
        )

        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._maintenance_task

    async def validate_execution(
        self,
        *,
        requester_id: str,
        permissions: Set[str],
        roles: Set[str],
        workflow_id: str,
        tool_name: str,
        identity: str,
        cost: float = 1.0,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> Tuple[
        bool,
        str,
    ]:
        """
        Main protection gateway.
        """

        allowed = (
            await self._rbac.validate(
                requester_id=
                    requester_id,
                permissions=
                    permissions,
                roles=roles,
                metadata=
                    metadata,
            )
        )

        if not allowed:
            return (
                False,
                "RBAC denied",
            )

        revoked = (
            await self._revocation.is_revoked(
                requester_id
            )
        )

        if revoked:
            return (
                False,
                "Execution token revoked",
            )

        rate_result = (
            await self._rate_limiter.evaluate(
                identity=
                    identity,
                cost=cost,
            )
        )

        if not rate_result.allowed:
            if (
                rate_result.action
                == AbuseAction.BLOCK
            ):
                self._blocked_events += 1

                await self._emit_alert(
                    requester_id=
                        requester_id,
                    event_type=
                        "rate_limit_block",
                    details={
                        "identity":
                            identity,
                        "retry_after":
                            rate_result.retry_after,
                    },
                )

                return (
                    False,
                    "Rate limit blocked",
                )

            self._throttled_events += 1

            return (
                False,
                (
                    "Rate limited "
                    f"(retry in {round(rate_result.retry_after, 2)}s)"
                ),
            )

        loop_result = (
            await self._loop_detector.track(
                workflow_id=
                    workflow_id,
                tool_name=
                    tool_name,
            )
        )

        if (
            loop_result.action
            == AbuseAction.REVOKE
        ):
            self._revoked_events += 1

            await self._revocation.revoke(
                requester_id
            )

            await self._emit_alert(
                requester_id=
                    requester_id,
                event_type=
                    "execution_loop_detected",
                details={
                    "workflow_id":
                        workflow_id,
                    "tool_name":
                        tool_name,
                    "repetitions":
                        loop_result.repetitive_count,
                },
            )

            return (
                False,
                "Execution loop revoked",
            )

        if (
            loop_result.action
            == AbuseAction.THROTTLE
        ):
            self._throttled_events += 1

            return (
                False,
                "Potential recursive loop throttled",
            )

        return (
            True,
            "Execution allowed",
        )

    async def reset_workflow(
        self,
        workflow_id: str,
    ) -> None:
        self._loop_detector.clear_workflow(
            workflow_id
        )

    async def _emit_alert(
        self,
        *,
        requester_id: str,
        event_type: str,
        details: Dict[str, Any],
    ) -> None:
        if not self.message_bus:
            return

        payload = {
            "type":
                event_type,
            "requester_id":
                requester_id,
            "details":
                details,
            "timestamp":
                time.time(),
        }

        await self.message_bus.publish(
            topic="security.alert",
            payload=payload,
        )

    async def _maintenance_loop(
        self,
    ) -> None:
        while self._running:
            try:
                await asyncio.sleep(
                    self.MAINTENANCE_INTERVAL
                )

                await self._rate_limiter.cleanup()

                await self._revocation.cleanup()

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Abuse prevention maintenance failure"
                )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "blocked_events":
                self._blocked_events,
            "throttled_events":
                self._throttled_events,
            "revoked_events":
                self._revoked_events,
            "loop_detector":
                self._loop_detector.stats(),
            "rate_limiter":
                self._rate_limiter.stats(),
            "revocations":
                self._revocation.stats(),
            "timestamp":
                time.time(),
        }


DEFAULT_ABUSE_PREVENTER = (
    AntiLoopAbusePreventer
)
