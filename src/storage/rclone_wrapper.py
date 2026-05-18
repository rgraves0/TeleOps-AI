from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path

from src.utils.error_handling import (
    ProviderUnavailableError,
)

logger = logging.getLogger(__name__)


@dataclass
class RcloneResult:

    success: bool

    command: list[str]

    stdout: str

    stderr: str

    return_code: int


class RcloneWrapper:

    def __init__(
        self,
        rclone_binary: str = "rclone",
        config_path: str | None = None,
        timeout: int = 3600,
    ) -> None:

        self.rclone_binary = (
            rclone_binary
        )

        self.config_path = (
            config_path
        )

        self.timeout = timeout

    # =====================================================
    # EXECUTE
    # =====================================================

    async def execute(
        self,
        args: list[str],
    ) -> RcloneResult:

        command = [
            self.rclone_binary
        ]

        if self.config_path:

            command.extend(
                [
                    "--config",
                    self.config_path,
                ]
            )

        command.extend(args)

        logger.info(
            "Executing rclone command=%s",
            command,
        )

        try:

            process = await (
                asyncio.create_subprocess_exec(
                    *command,
                    stdout=(
                        asyncio.subprocess.PIPE
                    ),
                    stderr=(
                        asyncio.subprocess.PIPE
                    ),
                )
            )

            stdout, stderr = (
                await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.timeout,
                )
            )

            stdout_text = (
                stdout.decode(
                    errors="ignore"
                )
            )

            stderr_text = (
                stderr.decode(
                    errors="ignore"
                )
            )

            success = (
                process.returncode == 0
            )

            if not success:

                logger.error(
                    "Rclone command failed "
                    "stderr=%s",
                    stderr_text,
                )

            return RcloneResult(
                success=success,
                command=command,
                stdout=stdout_text,
                stderr=stderr_text,
                return_code=(
                    process.returncode
                ),
            )

        except asyncio.TimeoutError:

            raise ProviderUnavailableError(
                "Rclone operation timeout"
            )

    # =====================================================
    # COPY
    # =====================================================

    async def copy(
        self,
        source: str,
        destination: str,
    ) -> RcloneResult:

        return await self.execute(
            [
                "copy",
                source,
                destination,
                "--progress",
            ]
        )

    # =====================================================
    # SYNC
    # =====================================================

    async def sync(
        self,
        source: str,
        destination: str,
    ) -> RcloneResult:

        return await self.execute(
            [
                "sync",
                source,
                destination,
                "--progress",
            ]
        )

    # =====================================================
    # MOVE
    # =====================================================

    async def move(
        self,
        source: str,
        destination: str,
    ) -> RcloneResult:

        return await self.execute(
            [
                "move",
                source,
                destination,
            ]
        )

    # =====================================================
    # DELETE
    # =====================================================

    async def delete(
        self,
        target: str,
    ) -> RcloneResult:

        return await self.execute(
            [
                "delete",
                target,
            ]
        )

    # =====================================================
    # LIST FILES
    # =====================================================

    async def list_files(
        self,
        remote: str,
    ) -> list[dict]:

        result = await self.execute(
            [
                "lsjson",
                remote,
            ]
        )

        if not result.success:

            raise ProviderUnavailableError(
                result.stderr
            )

        try:

            return json.loads(
                result.stdout
            )

        except Exception as exc:

            raise ProviderUnavailableError(
                str(exc)
            ) from exc

    # =====================================================
    # CREATE DIRECTORY
    # =====================================================

    async def mkdir(
        self,
        remote: str,
    ) -> RcloneResult:

        return await self.execute(
            [
                "mkdir",
                remote,
            ]
        )

    # =====================================================
    # FILE EXISTS
    # =====================================================

    async def exists(
        self,
        remote_path: str,
    ) -> bool:

        result = await self.execute(
            [
                "lsf",
                remote_path,
            ]
        )

        return result.success

    # =====================================================
    # GET SIZE
    # =====================================================

    async def size(
        self,
        remote: str,
    ) -> dict:

        result = await self.execute(
            [
                "size",
                remote,
                "--json",
            ]
        )

        if not result.success:

            raise ProviderUnavailableError(
                result.stderr
            )

        return json.loads(
            result.stdout
        )
