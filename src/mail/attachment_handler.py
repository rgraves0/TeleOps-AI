from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from pathlib import Path

import aiofiles

logger = logging.getLogger(__name__)


class AttachmentHandler:

    def __init__(
        self,
        storage_directory: str,
        chunk_size: int = (
            1024 * 1024
        ),
    ) -> None:

        self.storage_directory = (
            Path(storage_directory)
        )

        self.chunk_size = (
            chunk_size
        )

        self.storage_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

    # =====================================================
    # SAVE ATTACHMENT
    # =====================================================

    async def save_attachment(
        self,
        filename: str,
        content: bytes,
    ) -> str:

        safe_filename = (
            self._sanitize_filename(
                filename
            )
        )

        file_path = (
            self.storage_directory
            / safe_filename
        )

        async with aiofiles.open(
            file_path,
            "wb",
        ) as file:

            for chunk_start in range(
                0,
                len(content),
                self.chunk_size,
            ):

                chunk = content[
                    chunk_start:
                    chunk_start
                    + self.chunk_size
                ]

                await file.write(
                    chunk
                )

        logger.info(
            "Saved attachment=%s",
            file_path,
        )

        return str(file_path)

    # =====================================================
    # STREAM READ
    # =====================================================

    async def stream_attachment(
        self,
        file_path: str,
    ):

        path = Path(file_path)

        async with aiofiles.open(
            path,
            "rb",
        ) as file:

            while True:

                chunk = await file.read(
                    self.chunk_size
                )

                if not chunk:
                    break

                yield chunk

    # =====================================================
    # DELETE
    # =====================================================

    async def delete_attachment(
        self,
        file_path: str,
    ) -> None:

        path = Path(file_path)

        if not path.exists():
            return

        await asyncio.to_thread(
            os.remove,
            path,
        )

        logger.info(
            "Deleted attachment=%s",
            file_path,
        )

    # =====================================================
    # CHECKSUM
    # =====================================================

    async def generate_checksum(
        self,
        file_path: str,
    ) -> str:

        sha256 = hashlib.sha256()

        async for chunk in (
            self.stream_attachment(
                file_path
            )
        ):

            sha256.update(chunk)

        return sha256.hexdigest()

    # =====================================================
    # INTERNAL
    # =====================================================

    def _sanitize_filename(
        self,
        filename: str,
    ) -> str:

        sanitized = (
            filename
            .replace("/", "_")
            .replace("\\", "_")
            .replace("..", "_")
            .strip()
        )

        return sanitized[:255]
