from __future__ import annotations

import asyncio
import logging
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.memory.models import (
    BaseMemoryModel,
    MemoryType,
)
from src.memory.store import (
    MemoryStore,
)

logger = logging.getLogger(__name__)


# =========================================================
# SUMMARY RESULT
# =========================================================


@dataclass
class SummaryResult:

    original_length: int

    summarized_length: int

    compression_ratio: float

    summary: str


# =========================================================
# MEMORY SUMMARIZER
# =========================================================


class MemorySummarizer:

    def __init__(
        self,
        store: MemoryStore,
    ) -> None:

        self.store = store

        self.stop_words = {

            "the",
            "and",
            "for",
            "with",
            "that",
            "this",
            "from",
            "have",
            "will",
            "about",
            "into",
            "your",
        }

        logger.info(
            "MemorySummarizer initialized"
        )

    # =====================================================
    # SUMMARIZE TEXT
    # =====================================================

    async def summarize_text(
        self,
        text: str,
        max_sentences: int = 5,
    ) -> SummaryResult:

        original_length = len(text)

        sentences = (
            self._split_sentences(
                text
            )
        )

        if len(sentences) <= (
            max_sentences
        ):

            return SummaryResult(

                original_length=
                original_length,

                summarized_length=
                original_length,

                compression_ratio=1.0,

                summary=text,
            )

        scores = (
            self._score_sentences(
                sentences
            )
        )

        ranked = sorted(

            scores.items(),

            key=lambda item:
            item[1],

            reverse=True,
        )

        selected_indexes = sorted(

            [

                idx

                for idx, _
                in ranked[
                    :max_sentences
                ]
            ]
        )

        selected = [

            sentences[idx]

            for idx
            in selected_indexes
        ]

        summary = " ".join(
            selected
        )

        summarized_length = len(
            summary
        )

        ratio = round(

            summarized_length
            / max(
                original_length,
                1,
            ),

            3,
        )

        return SummaryResult(

            original_length=
            original_length,

            summarized_length=
            summarized_length,

            compression_ratio=
            ratio,

            summary=summary,
        )

    # =====================================================
    # SPLIT SENTENCES
    # =====================================================

    def _split_sentences(
        self,
        text: str,
    ) -> list[str]:

        parts = re.split(

            r"(?<=[.!?])\s+",

            text,
        )

        return [

            part.strip()

            for part in parts

            if part.strip()
        ]

    # =====================================================
    # SCORE SENTENCES
    # =====================================================

    def _score_sentences(
        self,
        sentences: list[str],
    ) -> dict[int, float]:

        words = []

        for sentence in sentences:

            tokens = re.findall(
                r"\b[a-zA-Z0-9]+\b",
                sentence.lower(),
            )

            words.extend(

                [

                    token

                    for token in tokens

                    if token
                    not in self.stop_words
                ]
            )

        frequency = Counter(
            words
        )

        scores = {}

        for idx, sentence in enumerate(
            sentences
        ):

            tokens = re.findall(
                r"\b[a-zA-Z0-9]+\b",
                sentence.lower(),
            )

            score = sum(

                frequency.get(
                    token,
                    0,
                )

                for token in tokens
            )

            scores[idx] = score

        return scores

    # =====================================================
    # SUMMARIZE CONVERSATION
    # =====================================================

    async def summarize_conversation(
        self,
        messages: list[str],
        max_sentences: int = 8,
    ) -> SummaryResult:

        merged = "\n".join(
            messages
        )

        return await (
            self.summarize_text(

                merged,

                max_sentences=
                max_sentences,
            )
        )

    # =====================================================
    # SUMMARIZE WORKFLOW
    # =====================================================

    async def summarize_workflow(
        self,
        workflow_steps: list[
            dict
        ],
    ) -> SummaryResult:

        text_parts = []

        for step in workflow_steps:

            tool = step.get(
                "tool",
                "unknown",
            )

            status = step.get(
                "status",
                "unknown",
            )

            text_parts.append(

                f"Tool {tool} "
                f"executed with "
                f"status {status}."
            )

        combined = " ".join(
            text_parts
        )

        return await (
            self.summarize_text(
                combined,
                max_sentences=5,
            )
        )

    # =====================================================
    # STORE SUMMARY
    # =====================================================

    async def store_summary(
        self,
        summary: str,
        metadata: (
            dict[str, Any]
            | None
        ) = None,
    ) -> bool:

        memory = (
            BaseMemoryModel(

                memory_type=
                MemoryType.SUMMARY,

                content=summary,

                metadata=(
                    metadata
                    or {}
                ),
            )
        )

        return await (
            self.store.store_memory(
                memory
            )
        )

    # =====================================================
    # AUTO CLEANUP
    # =====================================================

    async def auto_summarize_large_memories(
        self,
        min_length: int = 4000,
    ) -> int:

        rows = await (
            self.store.db.fetch_all(

                """

                SELECT *

                FROM memory_store

                WHERE LENGTH(content) >= ?

                LIMIT 25

                """,

                (min_length,),
            )
        )

        summarized = 0

        for row in rows:

            result = await (
                self.summarize_text(

                    row["content"],

                    max_sentences=6,
                )
            )

            await (
                self.store.store_memory(

                    BaseMemoryModel(

                        memory_type=
                        MemoryType.SUMMARY,

                        content=
                        result.summary,

                        metadata={

                            "source_memory":
                            row[
                                "memory_id"
                            ],
                        },
                    )
                )
            )

            summarized += 1

        if summarized > 0:

            logger.info(
                "Auto summarized=%s",
                summarized,
            )

        return summarized
