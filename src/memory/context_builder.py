from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from src.memory.retriever import (
    MemoryRetriever,
    RetrievalResult,
)

logger = logging.getLogger(__name__)


# =========================================================
# CONTEXT WINDOW
# =========================================================


@dataclass
class ContextWindow:

    query: str

    compressed_context: str

    included_memories: int

    total_characters: int


# =========================================================
# CONTEXT BUILDER
# =========================================================


class ContextBuilder:

    def __init__(
        self,
        retriever: MemoryRetriever,
    ) -> None:

        self.retriever = retriever

        logger.info(
            "ContextBuilder initialized"
        )

    # =====================================================
    # BUILD CONTEXT
    # =====================================================

    async def build_context(
        self,
        query: str,
        limit: int = 5,
        max_chars: int = 4000,
    ) -> ContextWindow:

        memories = await (
            self.retriever.retrieve(

                query=query,

                limit=limit,
            )
        )

        sections = []

        current_size = 0

        included = 0

        for memory in memories:

            block = (
                self._format_memory(
                    memory
                )
            )

            block_size = len(block)

            if (
                current_size
                + block_size
                > max_chars
            ):

                break

            sections.append(
                block
            )

            current_size += (
                block_size
            )

            included += 1

        context = "\n\n".join(
            sections
        )

        return ContextWindow(

            query=query,

            compressed_context=context,

            included_memories=(
                included
            ),

            total_characters=(
                len(context)
            ),
        )

    # =====================================================
    # FORMAT MEMORY
    # =====================================================

    def _format_memory(
        self,
        memory: RetrievalResult,
    ) -> str:

        content = (
            self._compress_text(
                memory.content
            )
        )

        return "\n".join(

            [

                (
                    f"[{memory.memory_type}]"
                ),

                (
                    f"Score: "
                    f"{memory.relevance_score}"
                ),

                content,
            ]
        )

    # =====================================================
    # TEXT COMPRESSION
    # =====================================================

    def _compress_text(
        self,
        text: str,
        max_length: int = 700,
    ) -> str:

        text = " ".join(
            text.split()
        )

        if len(text) <= max_length:

            return text

        truncated = (
            text[:max_length]
            .rsplit(
                " ",
                1,
            )[0]
        )

        return (
            truncated
            + " ..."
        )

    # =====================================================
    # BUILD OPERATIONAL CONTEXT
    # =====================================================

    async def operational_context(
        self,
        query: str,
        max_chars: int = 2500,
    ) -> ContextWindow:

        memories = await (
            self.retriever
            .operational_memory(
                query=query,
                limit=5,
            )
        )

        sections = []

        total = 0

        count = 0

        for memory in memories:

            content = (
                self._compress_text(
                    memory.content,
                    max_length=400,
                )
            )

            section = "\n".join(

                [

                    "Operational Insight:",

                    content,
                ]
            )

            if (
                total
                + len(section)
                > max_chars
            ):

                break

            sections.append(
                section
            )

            total += len(section)

            count += 1

        final_context = (
            "\n\n".join(
                sections
            )
        )

        return ContextWindow(

            query=query,

            compressed_context=(
                final_context
            ),

            included_memories=(
                count
            ),

            total_characters=(
                len(final_context)
            ),
        )

    # =====================================================
    # WORKFLOW CONTEXT
    # =====================================================

    async def workflow_context(
        self,
        workflow_name: str,
        max_chars: int = 3000,
    ) -> ContextWindow:

        histories = await (
            self.retriever
            .workflow_history(
                workflow_name,
                limit=10,
            )
        )

        sections = []

        total = 0

        count = 0

        for history in histories:

            section = "\n".join(

                [

                    (
                        f"Workflow:"
                    ),

                    self._compress_text(
                        history.content,
                        max_length=500,
                    ),
                ]
            )

            if (
                total
                + len(section)
                > max_chars
            ):

                break

            sections.append(
                section
            )

            total += len(section)

            count += 1

        context = "\n\n".join(
            sections
        )

        return ContextWindow(

            query=workflow_name,

            compressed_context=context,

            included_memories=count,

            total_characters=len(
                context
            ),
        )

    # =====================================================
    # BUILD MINIMAL CONTEXT
    # =====================================================

    async def minimal_context(
        self,
        query: str,
    ) -> str:

        context = await (
            self.build_context(

                query=query,

                limit=3,

                max_chars=1200,
            )
        )

        return (
            context
            .compressed_context
        )
