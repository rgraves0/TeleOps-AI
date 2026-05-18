from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import sqlite3
import struct
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
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
class SearchRequest:
    requester_id: str
    requester_roles: Set[str]
    requester_permissions: Set[str]
    query: str
    query_vector: Optional[
        List[float]
    ] = None
    limit: int = 10
    keyword_weight: float = 0.5
    semantic_weight: float = 0.5
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class SearchResult:
    chunk_id: str
    document_id: str
    text: str
    score: float
    keyword_score: float
    semantic_score: float
    rank: int
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class SearchRBACValidator:
    """
    Search-level RBAC validator.

    Enforces Default Deny policy.
    """

    def __init__(
        self,
        router: DynamicToolRouter,
    ) -> None:
        self.router = router

    async def validate_document(
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
            task_type="knowledge.search",
            metadata={
                "document_id":
                    document_id,
            },
        )

        route = await self.router.route(
            task="knowledge.search",
            context=context,
        )

        return (
            route.decision
            == RouteDecision.ALLOWED
        )


class SQLiteFTSBridge:
    """
    SQLite FTS5 keyword search bridge.
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

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(self) -> None:
        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
        )

        await asyncio.to_thread(
            self._configure_database
        )

        await asyncio.to_thread(
            self._initialize_fts
        )

    async def close(self) -> None:
        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def keyword_search(
        self,
        *,
        query: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._fts_search,
            query,
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

    def _initialize_fts(
        self,
    ) -> None:
        self._connection.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS semantic_chunks_fts
            USING fts5(
                chunk_id,
                document_id,
                text_content
            )
            """
        )

        self._connection.execute(
            """
            INSERT INTO semantic_chunks_fts (
                chunk_id,
                document_id,
                text_content
            )
            SELECT
                chunk_id,
                document_id,
                text_content
            FROM semantic_chunks
            WHERE chunk_id NOT IN (
                SELECT chunk_id
                FROM semantic_chunks_fts
            )
            """
        )

    def _fts_search(
        self,
        query: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        cursor = self._connection.execute(
            """
            SELECT
                chunk_id,
                document_id,
                text_content,
                bm25(semantic_chunks_fts)
            FROM semantic_chunks_fts
            WHERE semantic_chunks_fts MATCH ?
            ORDER BY bm25(semantic_chunks_fts)
            LIMIT ?
            """,
            (
                query,
                limit,
            ),
        )

        rows = cursor.fetchall()

        results = []

        for row in rows:
            results.append(
                {
                    "chunk_id":
                        row[0],
                    "document_id":
                        row[1],
                    "text":
                        row[2],
                    "score":
                        abs(
                            float(row[3])
                        ),
                }
            )

        return results


class SQLiteVectorBridge:
    """
    SQLite vector similarity bridge.
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

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(self) -> None:
        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
        )

        await asyncio.to_thread(
            self._configure_database
        )

    async def close(self) -> None:
        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    async def semantic_search(
        self,
        *,
        query_vector: List[float],
        limit: int,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self._semantic_similarity,
            query_vector,
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

    def _semantic_similarity(
        self,
        query_vector: List[float],
        limit: int,
    ) -> List[Dict[str, Any]]:
        cursor = self._connection.execute(
            """
            SELECT
                ce.chunk_id,
                sc.document_id,
                sc.text_content,
                ce.vector_blob
            FROM chunk_embeddings ce
            INNER JOIN semantic_chunks sc
            ON ce.chunk_id = sc.chunk_id
            """
        )

        scored: List[
            Dict[str, Any]
        ] = []

        for row in cursor.fetchall():
            chunk_id = row[0]
            document_id = row[1]
            text = row[2]
            vector_blob = row[3]

            vector = (
                self._decode_vector(
                    vector_blob
                )
            )

            similarity = (
                self._cosine_similarity(
                    query_vector,
                    vector,
                )
            )

            scored.append(
                {
                    "chunk_id":
                        chunk_id,
                    "document_id":
                        document_id,
                    "text":
                        text,
                    "score":
                        similarity,
                }
            )

        scored.sort(
            key=lambda item:
            item["score"],
            reverse=True,
        )

        return scored[:limit]

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

    def _cosine_similarity(
        self,
        a: Sequence[float],
        b: Sequence[float],
    ) -> float:
        if (
            not a
            or not b
            or len(a) != len(b)
        ):
            return 0.0

        dot_product = sum(
            x * y
            for x, y in zip(a, b)
        )

        norm_a = math.sqrt(
            sum(x * x for x in a)
        )

        norm_b = math.sqrt(
            sum(y * y for y in b)
        )

        if (
            norm_a == 0
            or norm_b == 0
        ):
            return 0.0

        return dot_product / (
            norm_a * norm_b
        )


class ReciprocalRankFusion:
    """
    Lightweight Reciprocal Rank Fusion.

    Pure math/list implementation.
    """

    DEFAULT_K = 60

    def fuse(
        self,
        *,
        keyword_results: List[
            Dict[str, Any]
        ],
        semantic_results: List[
            Dict[str, Any]
        ],
        keyword_weight: float,
        semantic_weight: float,
    ) -> List[Dict[str, Any]]:
        scores: Dict[
            str,
            Dict[str, Any]
        ] = {}

        for rank, result in enumerate(
            keyword_results,
            start=1,
        ):
            chunk_id = (
                result["chunk_id"]
            )

            rrf_score = (
                keyword_weight
                * (
                    1.0
                    / (
                        self.DEFAULT_K
                        + rank
                    )
                )
            )

            scores.setdefault(
                chunk_id,
                {
                    **result,
                    "keyword_score":
                        result[
                            "score"
                        ],
                    "semantic_score":
                        0.0,
                    "rrf_score":
                        0.0,
                },
            )

            scores[chunk_id][
                "rrf_score"
            ] += rrf_score

        for rank, result in enumerate(
            semantic_results,
            start=1,
        ):
            chunk_id = (
                result["chunk_id"]
            )

            rrf_score = (
                semantic_weight
                * (
                    1.0
                    / (
                        self.DEFAULT_K
                        + rank
                    )
                )
            )

            scores.setdefault(
                chunk_id,
                {
                    **result,
                    "keyword_score":
                        0.0,
                    "semantic_score":
                        result[
                            "score"
                        ],
                    "rrf_score":
                        0.0,
                },
            )

            scores[chunk_id][
                "semantic_score"
            ] = result["score"]

            scores[chunk_id][
                "rrf_score"
            ] += rrf_score

        ranked = sorted(
            scores.values(),
            key=lambda item:
            item["rrf_score"],
            reverse=True,
        )

        return ranked


class HybridSearchEngine:
    """
    Async-first Hybrid Search Engine.

    Features:
    - SQLite FTS5 BM25 search
    - Semantic cosine similarity search
    - Reciprocal Rank Fusion
    - Concurrent retrieval
    - RBAC-aware filtering
    - Default Deny enforcement
    - Low-memory architecture
    """

    CLEANUP_INTERVAL = 3600

    def __init__(
        self,
        *,
        router: DynamicToolRouter,
        semantic_database_path: str = (
            "./data/semantic_index.db"
        ),
        vector_database_path: str = (
            "./data/knowledge_vectors.db"
        ),
    ) -> None:
        self.router = router

        self._rbac = (
            SearchRBACValidator(
                router
            )
        )

        self._fts = SQLiteFTSBridge(
            database_path=
                semantic_database_path
        )

        self._vectors = (
            SQLiteVectorBridge(
                database_path=
                    vector_database_path
            )
        )

        self._rrf = (
            ReciprocalRankFusion()
        )

        self._running = False

        self._tasks: List[
            asyncio.Task
        ] = []

        self._query_cache: deque[
            str
        ] = deque(maxlen=256)

    async def start(self) -> None:
        await self._fts.initialize()

        await self._vectors.initialize()

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

        await self._fts.close()

        await self._vectors.close()

    async def search(
        self,
        request: SearchRequest,
    ) -> List[SearchResult]:
        """
        Hybrid retrieval pipeline.
        """

        keyword_task = (
            asyncio.create_task(
                self._fts.keyword_search(
                    query=request.query,
                    limit=request.limit
                    * 3,
                )
            )
        )

        semantic_task = None

        if request.query_vector:
            semantic_task = (
                asyncio.create_task(
                    self._vectors.semantic_search(
                        query_vector=
                            request.query_vector,
                        limit=
                            request.limit
                            * 3,
                    )
                )
            )

        keyword_results = (
            await keyword_task
        )

        semantic_results = []

        if semantic_task:
            semantic_results = (
                await semantic_task
            )

        fused = self._rrf.fuse(
            keyword_results=
                keyword_results,
            semantic_results=
                semantic_results,
            keyword_weight=
                request.keyword_weight,
            semantic_weight=
                request.semantic_weight,
        )

        filtered: List[
            SearchResult
        ] = []

        for rank, item in enumerate(
            fused,
            start=1,
        ):
            allowed = (
                await self._rbac.validate_document(
                    requester_id=
                        request.requester_id,
                    requester_roles=
                        request.requester_roles,
                    requester_permissions=
                        request.requester_permissions,
                    document_id=
                        item[
                            "document_id"
                        ],
                )
            )

            if not allowed:
                continue

            filtered.append(
                SearchResult(
                    chunk_id=
                        item[
                            "chunk_id"
                        ],
                    document_id=
                        item[
                            "document_id"
                        ],
                    text=
                        item["text"],
                    score=round(
                        item[
                            "rrf_score"
                        ],
                        6,
                    ),
                    keyword_score=round(
                        item.get(
                            "keyword_score",
                            0.0,
                        ),
                        6,
                    ),
                    semantic_score=round(
                        item.get(
                            "semantic_score",
                            0.0,
                        ),
                        6,
                    ),
                    rank=rank,
                    metadata={
                        "source":
                            "hybrid_search",
                    },
                )
            )

            if (
                len(filtered)
                >= request.limit
            ):
                break

        self._query_cache.append(
            request.query
        )

        return filtered

    async def keyword_only_search(
        self,
        request: SearchRequest,
    ) -> List[SearchResult]:
        keyword_results = (
            await self._fts.keyword_search(
                query=request.query,
                limit=request.limit,
            )
        )

        return await self._filter_results(
            request=request,
            results=keyword_results,
            source="keyword",
        )

    async def semantic_only_search(
        self,
        request: SearchRequest,
    ) -> List[SearchResult]:
        if not request.query_vector:
            return []

        semantic_results = (
            await self._vectors.semantic_search(
                query_vector=
                    request.query_vector,
                limit=request.limit,
            )
        )

        return await self._filter_results(
            request=request,
            results=semantic_results,
            source="semantic",
        )

    async def _filter_results(
        self,
        *,
        request: SearchRequest,
        results: List[
            Dict[str, Any]
        ],
        source: str,
    ) -> List[SearchResult]:
        filtered: List[
            SearchResult
        ] = []

        for rank, item in enumerate(
            results,
            start=1,
        ):
            allowed = (
                await self._rbac.validate_document(
                    requester_id=
                        request.requester_id,
                    requester_roles=
                        request.requester_roles,
                    requester_permissions=
                        request.requester_permissions,
                    document_id=
                        item[
                            "document_id"
                        ],
                )
            )

            if not allowed:
                continue

            filtered.append(
                SearchResult(
                    chunk_id=
                        item[
                            "chunk_id"
                        ],
                    document_id=
                        item[
                            "document_id"
                        ],
                    text=
                        item["text"],
                    score=round(
                        item["score"],
                        6,
                    ),
                    keyword_score=
                        item[
                            "score"
                        ]
                        if source
                        == "keyword"
                        else 0.0,
                    semantic_score=
                        item[
                            "score"
                        ]
                        if source
                        == "semantic"
                        else 0.0,
                    rank=rank,
                    metadata={
                        "source":
                            source,
                    },
                )
            )

        return filtered

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
                    "Hybrid search maintenance failure"
                )

    def _wal_checkpoint(
        self,
    ) -> None:
        self._fts._connection.execute(
            "PRAGMA wal_checkpoint(TRUNCATE);"
        )

    def stats(
        self,
    ) -> Dict[str, Any]:
        return {
            "running":
                self._running,
            "query_cache":
                len(
                    self._query_cache
                ),
            "fts_database":
                str(
                    self._fts.database_path
                ),
            "vector_database":
                str(
                    self._vectors.database_path
                ),
            "timestamp":
                time.time(),
        }
