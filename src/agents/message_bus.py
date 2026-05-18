from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from contextlib import suppress
from typing import Any
from typing import Awaitable
from typing import Callable

from src.agents.agent_messages import (
    AgentMessage,
)
from src.agents.agent_messages import (
    DeadLetterMessage,
)
from src.agents.agent_messages import (
    MessagePriority,
)
from src.agents.agent_messages import (
    MessageStatus,
)
from src.agents.agent_messages import (
    MessageType,
)
from src.agents.agent_messages import (
    PendingRequest,
)

logger = logging.getLogger(__name__)


# =========================================================
# MESSAGE BUS
# =========================================================


class MessageBus:

    def __init__(
        self,
        queue_size: int = 1000,
        request_timeout: int = 30,
        cleanup_interval: int = 15,
    ) -> None:

        self.queue_size = queue_size

        self.request_timeout = (
            request_timeout
        )

        self.cleanup_interval = (
            cleanup_interval
        )

        self.running = False

        self.shutdown_event = (
            asyncio.Event()
        )

        self.subscribers: dict[
            str,
            list[
                Callable[
                    [AgentMessage],
                    Awaitable[None],
                ]
            ]
        ] = defaultdict(list)

        self.agent_queues: dict[
            str,
            asyncio.PriorityQueue
        ] = {}

        self.pending_requests: dict[
            str,
            PendingRequest
        ] = {}

        self.dead_letters: list[
            DeadLetterMessage
        ] = []

        self.workers: dict[
            str,
            asyncio.Task
        ] = {}

        self.cleanup_task: (
            asyncio.Task | None
        ) = None

        self.stats_data = {

            "published": 0,

            "processed": 0,

            "expired": 0,

            "dead_letters": 0,

            "retries": 0,
        }

        logger.info(
            "MessageBus initialized"
        )

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if self.running:
            return

        self.running = True

        self.shutdown_event.clear()

        self.cleanup_task = (
            asyncio.create_task(
                self._cleanup_loop()
            )
        )

        logger.info(
            "MessageBus started"
        )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        if not self.running:
            return

        self.running = False

        self.shutdown_event.set()

        if self.cleanup_task:

            self.cleanup_task.cancel()

            with suppress(
                asyncio.CancelledError
            ):

                await self.cleanup_task

        for task in (
            self.workers.values()
        ):

            task.cancel()

        for task in (
            self.workers.values()
        ):

            with suppress(
                asyncio.CancelledError
            ):

                await task

        logger.info(
            "MessageBus stopped"
        )

    # =====================================================
    # REGISTER AGENT
    # =====================================================

    async def register_agent(
        self,
        agent_name: str,
    ) -> None:

        if (
            agent_name
            in self.agent_queues
        ):

            return

        queue = asyncio.PriorityQueue(
            maxsize=self.queue_size
        )

        self.agent_queues[
            agent_name
        ] = queue

        self.workers[
            agent_name
        ] = asyncio.create_task(

            self._agent_worker(
                agent_name
            )
        )

        logger.info(
            "Agent queue registered=%s",
            agent_name,
        )

    # =====================================================
    # SUBSCRIBE
    # =====================================================

    async def subscribe(
        self,
        topic: str,
        handler: Callable[
            [AgentMessage],
            Awaitable[None],
        ],
    ) -> None:

        self.subscribers[
            topic
        ].append(handler)

    # =====================================================
    # PUBLISH
    # =====================================================

    async def publish(
        self,
        message: AgentMessage,
    ) -> bool:

        if not self.running:
            return False

        if message.expired():

            self.stats_data[
                "expired"
            ] += 1

            return False

        handlers = (
            self.subscribers.get(
                message.topic,
                [],
            )
        )

        for handler in handlers:

            asyncio.create_task(
                self._safe_handler(

                    handler,
                    message,
                )
            )

        if message.recipient:

            return await (
                self.send_direct(
                    message
                )
            )

        self.stats_data[
            "published"
        ] += 1

        return True

    # =====================================================
    # DIRECT SEND
    # =====================================================

    async def send_direct(
        self,
        message: AgentMessage,
    ) -> bool:

        recipient = (
            message.recipient
        )

        if not recipient:
            return False

        queue = (
            self.agent_queues.get(
                recipient
            )
        )

        if not queue:

            await self._dead_letter(

                message,

                "recipient_not_found",
            )

            return False

        if queue.full():

            await self._dead_letter(

                message,

                "queue_full",
            )

            return False

        await queue.put(

            (
                int(
                    message.priority
                ),

                time.time(),

                message,
            )
        )

        self.stats_data[
            "published"
        ] += 1

        return True

    # =====================================================
    # REQUEST
    # =====================================================

    async def request(
        self,
        message: AgentMessage,
        timeout: (
            int | None
        ) = None,
    ) -> Any:

        loop = (
            asyncio.get_running_loop()
        )

        future = loop.create_future()

        request = PendingRequest(

            correlation_id=
            message.correlation_id,

            created_at=
            time.time(),

            timeout_seconds=
            timeout
            or self.request_timeout,

            future=future,

            requester=
            message.sender,

            topic=
            message.topic,
        )

        self.pending_requests[
            message.correlation_id
        ] = request

        success = await (
            self.send_direct(
                message
            )
        )

        if not success:

            raise RuntimeError(
                "request failed"
            )

        return await asyncio.wait_for(

            future,

            timeout=(
                timeout
                or self.request_timeout
            ),
        )

    # =====================================================
    # RESPOND
    # =====================================================

    async def respond(
        self,
        correlation_id: str,
        payload: dict[str, Any],
    ) -> bool:

        request = (
            self.pending_requests.get(
                correlation_id
            )
        )

        if not request:
            return False

        if request.future.done():
            return False

        request.future.set_result(
            payload
        )

        self.pending_requests.pop(
            correlation_id,
            None,
        )

        return True

    # =====================================================
    # AGENT WORKER
    # =====================================================

    async def _agent_worker(
        self,
        agent_name: str,
    ) -> None:

        queue = (
            self.agent_queues[
                agent_name
            ]
        )

        while (

            self.running

            and not (
                self.shutdown_event
                .is_set()
            )
        ):

            try:

                _, _, message = (

                    await queue.get()
                )

                if message.expired():

                    self.stats_data[
                        "expired"
                    ] += 1

                    continue

                await self.publish(
                    message
                )

                self.stats_data[
                    "processed"
                ] += 1

            except asyncio.CancelledError:

                break

            except Exception:

                logger.exception(
                    "Agent worker failed=%s",
                    agent_name,
                )

                await asyncio.sleep(
                    0.1
                )

    # =====================================================
    # SAFE HANDLER
    # =====================================================

    async def _safe_handler(
        self,
        handler: Callable,
        message: AgentMessage,
    ) -> None:

        try:

            message.status = (
                MessageStatus
                .PROCESSING
            )

            await handler(message)

            message.status = (
                MessageStatus
                .COMPLETED
            )

        except Exception as exc:

            logger.exception(
                "Message handler failed"
            )

            message.status = (
                MessageStatus.FAILED
            )

            if message.can_retry():

                message.mark_retry()

                self.stats_data[
                    "retries"
                ] += 1

                await asyncio.sleep(
                    0.5
                )

                await self.publish(
                    message
                )

            else:

                await self._dead_letter(

                    message,

                    str(exc),
                )

    # =====================================================
    # DEAD LETTER
    # =====================================================

    async def _dead_letter(
        self,
        message: AgentMessage,
        reason: str,
    ) -> None:

        self.dead_letters.append(

            DeadLetterMessage(

                message=message,

                reason=reason,
            )
        )

        self.stats_data[
            "dead_letters"
        ] += 1

    # =====================================================
    # CLEANUP LOOP
    # =====================================================

    async def _cleanup_loop(
        self,
    ) -> None:

        while (

            self.running

            and not (
                self.shutdown_event
                .is_set()
            )
        ):

            try:

                expired = []

                for (
                    correlation_id,
                    request,
                ) in (
                    self.pending_requests
                    .items()
                ):

                    if request.expired():

                        expired.append(
                            correlation_id
                        )

                for item in expired:

                    request = (
                        self.pending_requests
                        .pop(
                            item,
                            None,
                        )
                    )

                    if (
                        request
                        and not (
                            request.future
                            .done()
                        )
                    ):

                        request.future.cancel()

                if (
                    len(
                        self.dead_letters
                    )
                    > 1000
                ):

                    self.dead_letters = (
                        self.dead_letters[
                            -500:
                        ]
                    )

            except Exception:

                logger.exception(
                    "Cleanup loop failed"
                )

            await asyncio.sleep(
                self.cleanup_interval
            )

    # =====================================================
    # BACKPRESSURE
    # =====================================================

    async def pressure(
        self,
    ) -> dict[str, Any]:

        queues = {}

        for name, queue in (
            self.agent_queues.items()
        ):

            queues[name] = {

                "size":
                queue.qsize(),

                "capacity":
                self.queue_size,

                "pressure":
                round(

                    (
                        queue.qsize()
                        / self.queue_size
                    )
                    * 100,

                    2,
                ),
            }

        return queues

    # =====================================================
    # STATS
    # =====================================================

    async def stats(
        self,
    ) -> dict[str, Any]:

        return {

            **self.stats_data,

            "agents":
            len(
                self.agent_queues
            ),

            "subscriptions":
            len(
                self.subscribers
            ),

            "pending_requests":
            len(
                self.pending_requests
            ),

            "dead_letter_size":
            len(
                self.dead_letters
            ),
        }
