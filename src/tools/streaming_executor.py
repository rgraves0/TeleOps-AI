from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha1
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Optional,
    Union,
)


logger = logging.getLogger(__name__)


class ExecutionState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    TIMEOUT = "timeout"


class ChunkType(str, Enum):
    DATA = "data"
    ERROR = "error"
    STATUS = "status"
    SUMMARY = "summary"
    COMPLETE = "complete"


@dataclass(slots=True)
class CancellationToken:
    cancelled: bool = False
    reason: Optional[str] = None

    def cancel(self, reason: str = "cancelled") -> None:
        self.cancelled = True
        self.reason = reason

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise asyncio.CancelledError(
                self.reason or "Execution cancelled"
            )


@dataclass(slots=True)
class ExecutionChunk:
    chunk_type: ChunkType
    execution_id: str
    data: Any
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ExecutionContext:
    execution_id: str
    tool_name: str
    timeout_seconds: int = 60
    max_chunk_size: int = 4096
    compress_context: bool = True
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class ErrorEnvelope:
    error_type: str
    message: str
    traceback_summary: str
    recoverable: bool
    timestamp: float


class ContextCompressor:
    """
    Lightweight context compression layer.

    Goals:
    - Minimize RAM footprint
    - Preserve operational summaries
    - Avoid storing full stream payloads
    """

    MAX_CONTEXT_ITEMS = 50
    MAX_STRING_LENGTH = 1200

    def __init__(self) -> None:
        self._compressed_context: Deque[str] = deque(
            maxlen=self.MAX_CONTEXT_ITEMS
        )

    def add(
        self,
        item: Any,
    ) -> None:
        try:
            normalized = self._normalize(item)

            if normalized:
                self._compressed_context.append(
                    normalized
                )

        except Exception:
            logger.exception(
                "Context compression failure"
            )

    def snapshot(self) -> List[str]:
        return list(self._compressed_context)

    def summarize(self) -> Dict[str, Any]:
        return {
            "items": len(
                self._compressed_context
            ),
            "checksum": self._checksum(),
            "latest": (
                self._compressed_context[-1]
                if self._compressed_context
                else None
            ),
        }

    def clear(self) -> None:
        self._compressed_context.clear()

    def _normalize(
        self,
        item: Any,
    ) -> str:
        if isinstance(item, bytes):
            item = item.decode(
                "utf-8",
                errors="ignore",
            )

        if not isinstance(item, str):
            item = json.dumps(
                item,
                ensure_ascii=False,
                default=str,
                separators=(",", ":"),
            )

        item = item.strip()

        if len(item) > self.MAX_STRING_LENGTH:
            item = (
                item[
                    : self.MAX_STRING_LENGTH
                ] + "...[truncated]"
            )

        return item

    def _checksum(self) -> str:
        raw = "".join(
            self._compressed_context
        )

        return sha1(
            raw.encode("utf-8")
        ).hexdigest()[:12]


class StreamingToolExecutor:
    """
    Async-first Streaming Tool Executor.

    Features:
    - Chunk-based streaming
    - Async generator execution
    - Cancellation-safe runtime
    - Timeout enforcement
    - Context compression
    - Graceful exception isolation
    - Low RAM footprint
    - Secure execution encapsulation
    """

    DEFAULT_STREAM_DELAY = 0
    MAX_BUFFERED_CHUNKS = 32

    def __init__(self) -> None:
        self._active_executions: Dict[
            str,
            asyncio.Task,
        ] = {}

        self._context_store: Dict[
            str,
            ContextCompressor,
        ] = {}

    async def execute(
        self,
        *,
        handler: Callable[..., Any],
        context: ExecutionContext,
        payload: Optional[Dict[str, Any]] = None,
        cancellation_token: Optional[
            CancellationToken
        ] = None,
    ) -> AsyncGenerator[ExecutionChunk, None]:
        payload = payload or {}

        cancellation_token = (
            cancellation_token
            or CancellationToken()
        )

        compressor = ContextCompressor()

        self._context_store[
            context.execution_id
        ] = compressor

        start_time = time.time()

        yield ExecutionChunk(
            chunk_type=ChunkType.STATUS,
            execution_id=context.execution_id,
            data="execution_started",
            created_at=time.time(),
            metadata={
                "tool": context.tool_name,
            },
        )

        try:
            async for chunk in self._execute_stream(
                handler=handler,
                context=context,
                payload=payload,
                cancellation_token=(
                    cancellation_token
                ),
                compressor=compressor,
            ):
                cancellation_token.raise_if_cancelled()

                compressor.add(chunk)

                yield ExecutionChunk(
                    chunk_type=ChunkType.DATA,
                    execution_id=(
                        context.execution_id
                    ),
                    data=chunk,
                    created_at=time.time(),
                )

            yield ExecutionChunk(
                chunk_type=ChunkType.SUMMARY,
                execution_id=context.execution_id,
                data=compressor.summarize(),
                created_at=time.time(),
                metadata={
                    "duration_seconds":
                        round(
                            time.time()
                            - start_time,
                            2,
                        ),
                },
            )

            yield ExecutionChunk(
                chunk_type=ChunkType.COMPLETE,
                execution_id=context.execution_id,
                data="execution_completed",
                created_at=time.time(),
            )

        except asyncio.TimeoutError:
            logger.warning(
                "Execution timeout | execution=%s",
                context.execution_id,
            )

            yield self._error_chunk(
                context.execution_id,
                ErrorEnvelope(
                    error_type="TimeoutError",
                    message=(
                        "Execution exceeded timeout"
                    ),
                    traceback_summary="",
                    recoverable=True,
                    timestamp=time.time(),
                ),
            )

        except asyncio.CancelledError:
            logger.warning(
                "Execution cancelled | execution=%s",
                context.execution_id,
            )

            yield ExecutionChunk(
                chunk_type=ChunkType.STATUS,
                execution_id=context.execution_id,
                data="execution_cancelled",
                created_at=time.time(),
                metadata={
                    "reason":
                        cancellation_token.reason
                },
            )

        except Exception as exc:
            logger.exception(
                "Execution failure | execution=%s",
                context.execution_id,
            )

            yield self._error_chunk(
                context.execution_id,
                self._wrap_exception(exc),
            )

        finally:
            compressor.clear()

            self._context_store.pop(
                context.execution_id,
                None,
            )

    async def _execute_stream(
        self,
        *,
        handler: Callable[..., Any],
        context: ExecutionContext,
        payload: Dict[str, Any],
        cancellation_token: CancellationToken,
        compressor: ContextCompressor,
    ) -> AsyncGenerator[Any, None]:
        cancellation_token.raise_if_cancelled()

        if inspect.isasyncgenfunction(
            handler
        ):
            async for item in self._iterate_asyncgen(
                handler,
                payload,
                context,
                cancellation_token,
            ):
                yield item

            return

        result = handler(**payload)

        if inspect.isawaitable(result):
            result = await asyncio.wait_for(
                result,
                timeout=context.timeout_seconds,
            )

        if inspect.isasyncgen(result):
            async for item in self._iterate_async_generator(
                result,
                cancellation_token,
            ):
                yield item

            return

        if self._is_iterable_stream(result):
            for item in result:
                cancellation_token.raise_if_cancelled()

                yield self._sanitize_chunk(
                    item,
                    context.max_chunk_size,
                )

            return

        yield self._sanitize_chunk(
            result,
            context.max_chunk_size,
        )

    async def _iterate_asyncgen(
        self,
        handler: Callable[..., Any],
        payload: Dict[str, Any],
        context: ExecutionContext,
        cancellation_token: CancellationToken,
    ) -> AsyncGenerator[Any, None]:
        generator = handler(**payload)

        try:
            async with asyncio.timeout(
                context.timeout_seconds
            ):
                async for item in generator:
                    cancellation_token.raise_if_cancelled()

                    yield self._sanitize_chunk(
                        item,
                        context.max_chunk_size,
                    )

        finally:
            with contextlib.suppress(
                Exception
            ):
                await generator.aclose()

    async def _iterate_async_generator(
        self,
        generator: AsyncIterator[Any],
        cancellation_token: CancellationToken,
    ) -> AsyncGenerator[Any, None]:
        try:
            async for item in generator:
                cancellation_token.raise_if_cancelled()

                yield item

        finally:
            with contextlib.suppress(
                Exception
            ):
                await generator.aclose()

    def _sanitize_chunk(
        self,
        item: Any,
        max_chunk_size: int,
    ) -> Any:
        """
        Prevent oversized chunks from exploding memory.
        """

        try:
            if isinstance(item, bytes):
                if len(item) > max_chunk_size:
                    return (
                        item[
                            :max_chunk_size
                        ] + b"..."
                    )

                return item

            if isinstance(item, str):
                if len(item) > max_chunk_size:
                    return (
                        item[
                            :max_chunk_size
                        ] + "...[cut]"
                    )

                return item

            serialized = json.dumps(
                item,
                ensure_ascii=False,
                default=str,
                separators=(",", ":"),
            )

            if len(serialized) > max_chunk_size:
                serialized = (
                    serialized[
                        :max_chunk_size
                    ] + "...[cut]"
                )

            return json.loads(serialized)

        except Exception:
            return str(item)[
                :max_chunk_size
            ]

    def _is_iterable_stream(
        self,
        value: Any,
    ) -> bool:
        if isinstance(
            value,
            (
                str,
                bytes,
                dict,
            ),
        ):
            return False

        return isinstance(
            value,
            Iterable,
        )

    def _wrap_exception(
        self,
        exc: Exception,
    ) -> ErrorEnvelope:
        tb = traceback.format_exc()

        summarized_tb = (
            tb[-1500:]
            if len(tb) > 1500
            else tb
        )

        return ErrorEnvelope(
            error_type=(
                exc.__class__.__name__
            ),
            message=str(exc),
            traceback_summary=summarized_tb,
            recoverable=not isinstance(
                exc,
                (
                    MemoryError,
                    SystemExit,
                    KeyboardInterrupt,
                ),
            ),
            timestamp=time.time(),
        )

    def _error_chunk(
        self,
        execution_id: str,
        envelope: ErrorEnvelope,
    ) -> ExecutionChunk:
        return ExecutionChunk(
            chunk_type=ChunkType.ERROR,
            execution_id=execution_id,
            data={
                "error_type":
                    envelope.error_type,
                "message":
                    envelope.message,
                "recoverable":
                    envelope.recoverable,
            },
            created_at=time.time(),
            metadata={
                "traceback":
                    envelope.traceback_summary,
                "timestamp":
                    envelope.timestamp,
            },
        )

    async def cancel_execution(
        self,
        execution_id: str,
    ) -> bool:
        task = self._active_executions.get(
            execution_id
        )

        if not task:
            return False

        task.cancel()

        return True

    def get_context_snapshot(
        self,
        execution_id: str,
    ) -> Optional[List[str]]:
        compressor = (
            self._context_store.get(
                execution_id
            )
        )

        if not compressor:
            return None

        return compressor.snapshot()

    def active_execution_count(
        self,
    ) -> int:
        return len(
            self._active_executions
        )

    async def stream_iterable(
        self,
        iterable: Iterable[Any],
        *,
        execution_id: str,
        chunk_size: int = 1,
        delay: float = 0,
    ) -> AsyncGenerator[
        ExecutionChunk,
        None,
    ]:
        """
        Lightweight utility helper for
        chunk-streaming iterables.
        """

        batch: List[Any] = []

        for item in iterable:
            batch.append(item)

            if len(batch) >= chunk_size:
                yield ExecutionChunk(
                    chunk_type=ChunkType.DATA,
                    execution_id=execution_id,
                    data=batch.copy(),
                    created_at=time.time(),
                )

                batch.clear()

                if delay > 0:
                    await asyncio.sleep(
                        delay
                    )

        if batch:
            yield ExecutionChunk(
                chunk_type=ChunkType.DATA,
                execution_id=execution_id,
                data=batch.copy(),
                created_at=time.time(),
            )

    async def stream_file_reader(
        self,
        *,
        file_path: str,
        execution_id: str,
        chunk_bytes: int = 4096,
        encoding: str = "utf-8",
    ) -> AsyncGenerator[
        ExecutionChunk,
        None,
    ]:
        """
        Memory-safe streaming file reader.
        """

        loop = asyncio.get_running_loop()

        try:
            with open(
                file_path,
                "rb",
            ) as file_handle:

                while True:
                    chunk = await loop.run_in_executor(
                        None,
                        file_handle.read,
                        chunk_bytes,
                    )

                    if not chunk:
                        break

                    yield ExecutionChunk(
                        chunk_type=ChunkType.DATA,
                        execution_id=execution_id,
                        data=chunk.decode(
                            encoding,
                            errors="ignore",
                        ),
                        created_at=time.time(),
                    )

        except Exception as exc:
            yield self._error_chunk(
                execution_id,
                self._wrap_exception(exc),
            )
