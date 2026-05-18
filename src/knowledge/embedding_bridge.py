from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import random
import sqlite3
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from app.tools.dynamic_router import (
    DynamicToolRouter,
    RouteContext,
    RouteDecision,
)


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class EmbeddingChunk:
    chunk_id: str
    document_id: str
    text: str
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class EmbeddingResult:
    chunk_id: str
    vector: List[float]
    dimensions: int
    provider: str
    model: str
    created_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class EmbeddingRequest:
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    provider: str
    model: str
    chunks: List[EmbeddingChunk]
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class RetryableEmbeddingError(
    Exception
):
    pass


class RateLimitError(
    RetryableEmbeddingError
):
    pass


class PermissionDeniedError(
    Exception
):
    pass


class ChunkRBACValidator:
    """
    Default Deny + RBAC enforcement.
    """

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate(
        self,
        *,
        requester_id: str,
        requester_roles: Set[str],
        requester_permissions: Set[str],
        document_id: str,
    ) -> bool:
        context = RouteContext(
            requester_id=requester_id,
            requester_roles=requester_roles,
            requester_permissions=(
                requester_permissions
            ),
            task_type="knowledge.embedding",
            metadata={
                "document_id":
                    document_id,
            },
        )

        route = await self.router.route(
            task="knowledge.embedding",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class SQLiteVectorStore:
    """
    Lightweight SQLite vector persistence.

    Stores vectors as compact BLOB arrays.
    """

    SQLITE_BUSY_TIMEOUT = 5000

    def __init__(
        self,
        *,
        database_path: str,
    ) -> None:
        self.database_path = (
            Path(database_path)
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(self) -> None:
        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure_database
        )

        await asyncio.to_thread(
            self._create_tables
        )

    async def close(self) -> None:
        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def store_embedding(
        self,
        result: EmbeddingResult,
    ) -> None:
        await asyncio.to_thread(
            self._insert_embedding,
            result,
        )

    async def fetch_embedding(
        self,
        chunk_id: str,
    ) -> Optional[EmbeddingResult]:
        row = await asyncio.to_thread(
            self._fetch_embedding_row,
            chunk_id,
        )

        if not row:
            return None

        return EmbeddingResult(
            chunk_id=row[0],
            vector=self._decode_vector(
                row[1]
            ),
            dimensions=row[2],
            provider=row[3],
            model=row[4],
            created_at=row[5],
            metadata=json.loads(
                row[6]
            ),
        )

    async def fetch_batch_without_vectors(
        self,
        *,
        limit: int = 32,
    ) -> List[Tuple[str, str, str]]:
        return await asyncio.to_thread(
            self._fetch_pending_chunks,
            limit,
        )

    def _configure_database(
        self,
    ) -> None:
        self._connection.execute(
            "PRAGMA journal_mode=WAL;"
        )

        self._connection.execute(
            "PRAGMA synchronous=NORMAL;"
        )

        self._connection.execute(
            "PRAGMA temp_store=MEMORY;"
        )

        self._connection.execute(
            "PRAGMA cache_size=-2000;"
        )

        self._connection.execute(
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS chunk_embeddings (
                chunk_id TEXT PRIMARY KEY,
                vector_blob BLOB NOT NULL,
                dimensions INTEGER NOT NULL,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                created_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_embeddings_provider
            ON chunk_embeddings(provider)
            """
        )

    def _insert_embedding(
        self,
        result: EmbeddingResult,
    ) -> None:
        vector_blob = (
            self._encode_vector(
                result.vector
            )
        )

        self._connection.execute(
            """
            INSERT OR REPLACE INTO chunk_embeddings (
                chunk_id,
                vector_blob,
                dimensions,
                provider,
                model,
                created_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.chunk_id,
                vector_blob,
                result.dimensions,
                result.provider,
                result.model,
                result.created_at,
                json.dumps(
                    result.metadata,
                    ensure_ascii=False,
                ),
            ),
        )

    def _fetch_embedding_row(
        self,
        chunk_id: str,
    ) -> Optional[Tuple]:
        cursor = self._connection.execute(
            """
            SELECT
                chunk_id,
                vector_blob,
                dimensions,
                provider,
                model,
                created_at,
                metadata
            FROM chunk_embeddings
            WHERE chunk_id = ?
            LIMIT 1
            """,
            (chunk_id,),
        )

        return cursor.fetchone()

    def _fetch_pending_chunks(
        self,
        limit: int,
    ) -> List[Tuple[str, str, str]]:
        cursor = self._connection.execute(
            """
            SELECT
                sc.chunk_id,
                sc.document_id,
                sc.text_content
            FROM semantic_chunks sc
            LEFT JOIN chunk_embeddings ce
            ON sc.chunk_id = ce.chunk_id
            WHERE ce.chunk_id IS NULL
            LIMIT ?
            """,
            (limit,),
        )

        return cursor.fetchall()

    def _encode_vector(
        self,
        vector: Sequence[float],
    ) -> bytes:
        return struct.pack(
            f"{len(vector)}f",
            *vector,
        )

    def _decode_vector(
        self,
        blob: bytes,
    ) -> List[float]:
        if not blob:
            return []

        dimensions = (
            len(blob) // 4
        )

        return list(
            struct.unpack(
                f"{dimensions}f",
                blob,
            )
        )


class BackoffRetryHandler:
    """
    Lightweight async retry/backoff.
    """

    DEFAULT_MAX_RETRIES = 5

    async def execute(
        self,
        *,
        operation: Callable[
            ...,
            Awaitable[Any],
        ],
        max_retries: int = (
            DEFAULT_MAX_RETRIES
        ),
        base_delay: float = 1.0,
    ) -> Any:
        last_error = None

        for attempt in range(
            max_retries
        ):
            try:
                return await operation()

            except (
                RetryableEmbeddingError,
                asyncio.TimeoutError,
            ) as exc:
                last_error = exc

                delay = (
                    base_delay
                    * (2**attempt)
                )

                jitter = random.uniform(
                    0.0,
                    0.25,
                )

                sleep_time = (
                    delay + jitter
                )

                logger.warning(
                    "Embedding retry | attempt=%s delay=%.2f",
                    attempt + 1,
                    sleep_time,
                )

                await asyncio.sleep(
                    sleep_time
                )

        raise last_error


class EmbeddingClientBridge:
    """
    Async embedding extraction bridge.

    Provider-agnostic lightweight runtime.
    """

    MAX_BATCH_SIZE = 16
    MAX_CONCURRENT_REQUESTS = 3

    def __init__(
        self,
        *,
        provider_manager: Any,
    ) -> None:
        self.provider_manager = (
            provider_manager
        )

        self._retry_handler = (
            BackoffRetryHandler()
        )

        self._semaphore = (
            asyncio.Semaphore(
                self.MAX_CONCURRENT_REQUESTS
            )
        )

    async def extract_embeddings(
        self,
        *,
        provider: str,
        model: str,
        chunks: List[EmbeddingChunk],
    ) -> AsyncGenerator[
        EmbeddingResult,
        None,
    ]:
        """
        Batch-stream embedding extraction.
        """

        batches = self._create_batches(
            chunks,
            self.MAX_BATCH_SIZE,
        )

        for batch in batches:
            async with self._semaphore:
                results = (
                    await self._retry_handler.execute(
                        operation=lambda:
                        self._request_batch(
                            provider=provider,
                            model=model,
                            batch=batch,
                        )
                    )
                )

                for item in results:
                    yield item

    async def _request_batch(
        self,
        *,
        provider: str,
        model: str,
        batch: List[EmbeddingChunk],
    ) -> List[EmbeddingResult]:
        """
        Provider manager bridge.

        Expected provider_manager API:
        await provider_manager.create_embeddings(...)
        """

        texts = [
            chunk.text
            for chunk in batch
        ]

        try:
            response = (
                await self.provider_manager.create_embeddings(
                    provider=provider,
                    model=model,
                    input_texts=texts,
                )
            )

        except Exception as exc:
            message = str(exc).lower()

            if (
                "rate"
                in message
                or "429"
                in message
            ):
                raise RateLimitError(
                    str(exc)
                ) from exc

            if (
                "timeout"
                in message
                or "connection"
                in message
            ):
                raise RetryableEmbeddingError(
                    str(exc)
                ) from exc

            raise

        vectors = (
            self._extract_vectors(
                response
            )
        )

        results: List[
            EmbeddingResult
        ] = []

        for chunk, vector in zip(
            batch,
            vectors,
        ):
            results.append(
                EmbeddingResult(
                    chunk_id=
                        chunk.chunk_id,
                    vector=vector,
                    dimensions=len(
                        vector
                    ),
                    provider=provider,
                    model=model,
                    created_at=time.time(),
                    metadata={
                        "document_id":
                            chunk.document_id,
                    },
                )
            )

        return results

    def _extract_vectors(
        self,
        response: Any,
    ) -> List[List[float]]:
        """
        Provider-normalized vector extraction.
        """

        if isinstance(
            response,
            dict,
        ):
            if "data" in response:
                return [
                    item["embedding"]
                    for item in response[
                        "data"
                    ]
                ]

            if "embeddings" in response:
                return response[
                    "embeddings"
                ]

        if isinstance(
            response,
            list,
        ):
            return response

        raise ValueError(
            "Unsupported embedding response"
        )

    def _create_batches(
        self,
        items: List[
            EmbeddingChunk
        ],
        batch_size: int,
    ) -> List[
        List[EmbeddingChunk]
    ]:
        return [
            items[
                idx:
                idx + batch_size
            ]
            for idx in range(
                0,
                len(items),
                batch_size,
            )
        ]


class LightweightEmbeddingBridge:
    """
    Production-safe embedding runtime.

    Features:
    - Async embedding extraction
    - SQLite WAL vector persistence
    - Batch-stream processing
    - Backoff retry handling
    - Failure isolation
    - RBAC enforcement
    - Low-memory execution
    """

    CLEANUP_INTERVAL = 3600

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        provider_manager: Any,
        database_path: str = (
            "./data/knowledge_vectors.db"
        ),
    ) -> None:
        self.router = router

        self._rbac = (
            ChunkRBACValidator(
                router
            )
        )

        self._vector_store = (
            SQLiteVectorStore(
                database_path=
                    database_path
            )
        )

        self._client_bridge = (
            EmbeddingClientBridge(
                provider_manager=
                    provider_manager
            )
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

    async def start(self) -> None:
        await self._vector_store.initialize()

        self._running = True

        self._tasks.append(
            asyncio.create_task(
                self._maintenance_loop()
            )
        )

    async def stop(self) -> None:
        self._running = False

        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await task

        self._tasks.clear()

        await self._vector_store.close()

    async def process_request(
        self,
        request: EmbeddingRequest,
    ) -> AsyncGenerator[
        EmbeddingResult,
        None,
    ]:
        """
        RBAC-aware embedding pipeline.
        """

        allowed_chunks: List[
            EmbeddingChunk
        ] = []

        for chunk in request.chunks:
            allowed = (
                await self._rbac.validate(
                    requester_id=
                        request.requester_id,
                    requester_roles=
                        request.requester_roles,
                    requester_permissions=
                        request.requester_permissions,
                    document_id=
                        chunk.document_id,
                )
            )

            if not allowed:
                logger.warning(
                    "Embedding RBAC denied | chunk=%s",
                    chunk.chunk_id,
                )

                continue

            allowed_chunks.append(
                chunk
            )

        async for result in (
            self._client_bridge.extract_embeddings(
                provider=
                    request.provider,
                model=
                    request.model,
                chunks=
                    allowed_chunks,
            )
        ):
            await self._vector_store.store_embedding(
                result
            )

            yield result

    async def auto_embed_pending_chunks(
        self,
        *,
        requester_id: str,
        requester_roles: Set[str],
        requester_permissions: Set[str],
        provider: str,
        model: str,
        batch_limit: int = 32,
    ) -> AsyncGenerator[
        EmbeddingResult,
        None,
    ]:
        """
        Auto-process chunks without vectors.
        """

        rows = (
            await self._vector_store.fetch_batch_without_vectors(
                limit=batch_limit
            )
        )

        chunks: List[
            EmbeddingChunk
        ] = []

        for row in rows:
            chunk_id = row[0]
            document_id = row[1]
            text = row[2]

            allowed = (
                await self._rbac.validate(
                    requester_id=
                        requester_id,
                    requester_roles=
                        requester_roles,
                    requester_permissions=
                        requester_permissions,
                    document_id=
                        document_id,
                )
            )

            if not allowed:
                continue

            chunks.append(
                EmbeddingChunk(
                    chunk_id=chunk_id,
                    document_id=document_id,
                    text=text,
                )
            )

        request = EmbeddingRequest(
            requester_id=requester_id,
            requester_roles=
                requester_roles,
            requester_permissions=
                requester_permissions,
            provider=provider,
            model=model,
            chunks=chunks,
        )

        async for result in (
            self.process_request(
                request
            )
        ):
            yield result

    async def fetch_vector(
        self,
        chunk_id: str,
    ) -> Optional[
        EmbeddingResult
    ]:
        return (
            await self._vector_store.fetch_embedding(
                chunk_id
            )
        )

    async def _maintenance_loop(
        self,
    ) -> None:
        while self._running:
            try:
                await asyncio.sleep(
                    self.CLEANUP_INTERVAL
                )

                await asyncio.to_thread(
                    self._wal_checkpoint
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Embedding maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self._vector_store._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "database":
                str(
                    self._vector_store.database_path
                ),
            "max_batch_size":
                self._client_bridge.MAX_BATCH_SIZE,
            "max_concurrency":
                self._client_bridge.MAX_CONCURRENT_REQUESTS,
            "timestamp":
                time.time(),
        }
