from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


# =========================================================
# COMPRESSION RESULT
# =========================================================


@dataclass
class CompressionResult:

    original_tokens: int

    compressed_tokens: int

    compression_ratio: float

    compressed_text: str


# =========================================================
# TOKEN ESTIMATOR
# =========================================================


class TokenEstimator:

    @staticmethod
    def estimate_tokens(
        text: str,
    ) -> int:

        return max(
            1,
            len(text) // 4,
        )


# =========================================================
# CONTEXT COMPRESSOR
# =========================================================


class ContextCompressor:

    def __init__(
        self,
    ) -> None:

        logger.info(
            "ContextCompressor initialized"
        )

    # =====================================================
    # COMPRESS CONTEXT
    # =====================================================

    async def compress_context(
        self,
        text: str,
        target_tokens: int = 1200,
    ) -> CompressionResult:

        original_tokens = (
            TokenEstimator
            .estimate_tokens(
                text
            )
        )

        if (
            original_tokens
            <= target_tokens
        ):

            return CompressionResult(

                original_tokens=
                original_tokens,

                compressed_tokens=
                original_tokens,

                compression_ratio=1.0,

                compressed_text=text,
            )

        compressed = (
            self._compress(
                text,
                target_tokens,
            )
        )

        compressed_tokens = (
            TokenEstimator
            .estimate_tokens(
                compressed
            )
        )

        ratio = round(

            compressed_tokens
            / max(
                original_tokens,
                1,
            ),

            3,
        )

        return CompressionResult(

            original_tokens=
            original_tokens,

            compressed_tokens=
            compressed_tokens,

            compression_ratio=
            ratio,

            compressed_text=
            compressed,
        )

    # =====================================================
    # INTERNAL COMPRESS
    # =====================================================

    def _compress(
        self,
        text: str,
        target_tokens: int,
    ) -> str:

        paragraphs = [

            p.strip()

            for p in text.split(
                "\n"
            )

            if p.strip()
        ]

        selected = []

        current_tokens = 0

        for paragraph in paragraphs:

            paragraph = (
                self._cleanup_text(
                    paragraph
                )
            )

            tokens = (
                TokenEstimator
                .estimate_tokens(
                    paragraph
                )
            )

            if (

                current_tokens
                + tokens

                > target_tokens

            ):

                remaining = (
                    target_tokens
                    - current_tokens
                )

                if remaining > 50:

                    truncated = (
                        self._truncate(
                            paragraph,
                            remaining,
                        )
                    )

                    selected.append(
                        truncated
                    )

                break

            selected.append(
                paragraph
            )

            current_tokens += (
                tokens
            )

        return "\n".join(
            selected
        )

    # =====================================================
    # CLEANUP
    # =====================================================

    def _cleanup_text(
        self,
        text: str,
    ) -> str:

        text = re.sub(
            r"\s+",
            " ",
            text,
        )

        text = re.sub(
            r"\n+",
            "\n",
            text,
        )

        return text.strip()

    # =====================================================
    # TRUNCATE
    # =====================================================

    def _truncate(
        self,
        text: str,
        max_tokens: int,
    ) -> str:

        approx_chars = (
            max_tokens * 4
        )

        if len(text) <= approx_chars:

            return text

        shortened = (
            text[:approx_chars]
            .rsplit(
                " ",
                1,
            )[0]
        )

        return shortened + " ..."

    # =====================================================
    # COMPRESS WORKFLOW
    # =====================================================

    async def compress_workflow_history(
        self,
        workflow_history: list[
            dict
        ],
        target_tokens: int = 1000,
    ) -> CompressionResult:

        lines = []

        for item in workflow_history:

            line = (

                f"Workflow: "
                f"{item.get('workflow', 'unknown')} | "

                f"Status: "
                f"{item.get('status', 'unknown')} | "

                f"Steps: "
                f"{item.get('steps', 0)}"
            )

            lines.append(
                line
            )

        combined = "\n".join(
            lines
        )

        return await (
            self.compress_context(

                combined,

                target_tokens=
                target_tokens,
            )
        )

    # =====================================================
    # COMPRESS CONVERSATION
    # =====================================================

    async def compress_conversation(
        self,
        messages: list[str],
        target_tokens: int = 1500,
    ) -> CompressionResult:

        merged = "\n".join(
            messages
        )

        return await (
            self.compress_context(

                merged,

                target_tokens=
                target_tokens,
            )
        )

    # =====================================================
    # MINIMAL CONTEXT
    # =====================================================

    async def minimal_context(
        self,
        text: str,
    ) -> str:

        result = await (
            self.compress_context(

                text,

                target_tokens=500,
            )
        )

        return (
            result.compressed_text
        )
