from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
import re
import ssl
import time
import traceback
import urllib.request

from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
)

logger = logging.getLogger(__name__)


class AuditExporterError(Exception):
    pass


class ExportPermissionDenied(
    AuditExporterError
):
    pass


@dataclass(slots=True)
class AuditEvent:
    event_type: str
    payload: Dict[str, Any]
    timestamp: float = field(
        default_factory=time.time
    )
    actor_id: Optional[str] = None
    severity: str = "INFO"


@dataclass(slots=True)
class SignedAuditRecord:
    sequence_id: int
    timestamp: float
    event_type: str
    payload: Dict[str, Any]
    signature: str
    previous_hash: str
    current_hash: str
    severity: str


class RBACGuard:
    """
    Default-deny RBAC enforcement.
    """

    REQUIRED_ROLE = "superuser"

    REQUIRED_PERMISSION = (
        "system.audit.export"
    )

    async def validate(
        self,
        *,
        roles: Set[str],
        permissions: Set[str],
    ) -> bool:

        if (
            self.REQUIRED_ROLE
            not in roles
        ):
            return False

        if (
            self.REQUIRED_PERMISSION
            not in permissions
        ):
            return False

        return True


class RedactionFilter:
    """
    Sensitive data auto-redactor.
    """

    SECRET_PATTERNS = [
        re.compile(
            r"sk-[a-zA-Z0-9]{20,}",
            re.IGNORECASE,
        ),
        re.compile(
            r"AIza[a-zA-Z0-9\-_]{20,}",
            re.IGNORECASE,
        ),
        re.compile(
            r"ghp_[a-zA-Z0-9]{20,}",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b\d{16}\b"
        ),
        re.compile(
            r"Bearer\s+[a-zA-Z0-9\.\-_]+",
            re.IGNORECASE,
        ),
    ]

    REDACTION_TEXT = (
        "[REDACTED]"
    )

    def sanitize(
        self,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:

        sanitized = {}

        for (
            key,
            value,
        ) in payload.items():

            if isinstance(
                value,
                dict,
            ):
                sanitized[
                    key
                ] = self.sanitize(
                    value
                )

            elif isinstance(
                value,
                list,
            ):
                sanitized[
                    key
                ] = [
                    self._sanitize_value(
                        item
                    )
                    for item in value
                ]

            else:
                sanitized[
                    key
                ] = self._sanitize_value(
                    value
                )

        return sanitized

    def _sanitize_value(
        self,
        value: Any,
    ) -> Any:

        if not isinstance(
            value,
            str,
        ):
            return value

        sanitized = value

        for pattern in (
            self.SECRET_PATTERNS
        ):
            sanitized = (
                pattern.sub(
                    self.REDACTION_TEXT,
                    sanitized,
                )
            )

        return sanitized


class HMACSigner:
    """
    Tamper-evident HMAC-SHA256 signer.
    """

    def __init__(
        self,
        *,
        signing_key: str,
    ) -> None:

        self.signing_key = (
            signing_key.encode(
                "utf-8"
            )
        )

    def sign(
        self,
        payload: bytes,
    ) -> str:

        return hmac.new(
            self.signing_key,
            payload,
            hashlib.sha256,
        ).hexdigest()

    def hash_record(
        self,
        payload: bytes,
    ) -> str:

        return hashlib.sha256(
            payload
        ).hexdigest()


class AppendOnlyFileManager:
    """
    Append-only JSONL archive.
    """

    def __init__(
        self,
        *,
        file_path: str,
    ) -> None:

        self.file_path = Path(
            file_path
        )

        self.file_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._lock = (
            asyncio.Lock()
        )

    async def append(
        self,
        record: SignedAuditRecord,
    ) -> None:

        async with self._lock:

            line = json.dumps(
                {
                    "sequence_id":
                        record.sequence_id,
                    "timestamp":
                        record.timestamp,
                    "event_type":
                        record.event_type,
                    "payload":
                        record.payload,
                    "signature":
                        record.signature,
                    "previous_hash":
                        record.previous_hash,
                    "current_hash":
                        record.current_hash,
                    "severity":
                        record.severity,
                },
                ensure_ascii=False,
            )

            await asyncio.to_thread(
                self._write_line,
                line,
            )

    def _write_line(
        self,
        line: str,
    ) -> None:

        with self.file_path.open(
            "a",
            encoding="utf-8",
        ) as handle:

            handle.write(
                line + "\n"
            )

            handle.flush()

            os.fsync(
                handle.fileno()
            )


class AsyncWebhookShipper:
    """
    Non-blocking webhook exporter.
    """

    def __init__(
        self,
        *,
        endpoint: Optional[
            str
        ] = None,
        timeout: int = 10,
    ) -> None:

        self.endpoint = endpoint
        self.timeout = timeout

        self.ssl_context = (
            ssl.create_default_context()
        )

    async def ship(
        self,
        records: List[
            SignedAuditRecord
        ],
    ) -> bool:

        if not self.endpoint:
            return True

        return await asyncio.to_thread(
            self._ship_sync,
            records,
        )

    def _ship_sync(
        self,
        records: List[
            SignedAuditRecord
        ],
    ) -> bool:

        try:

            payload = json.dumps(
                [
                    {
                        "sequence_id":
                            r.sequence_id,
                        "timestamp":
                            r.timestamp,
                        "event_type":
                            r.event_type,
                        "payload":
                            r.payload,
                        "signature":
                            r.signature,
                        "previous_hash":
                            r.previous_hash,
                        "current_hash":
                            r.current_hash,
                        "severity":
                            r.severity,
                    }
                    for r in records
                ]
            ).encode("utf-8")

            request = (
                urllib.request.Request(
                    url=self.endpoint,
                    method="POST",
                    data=payload,
                    headers={
                        "Content-Type":
                            "application/json"
                    },
                )
            )

            with urllib.request.urlopen(
                request,
                timeout=self.timeout,
                context=self.ssl_context,
            ) as response:

                return (
                    200
                    <= response.status
                    < 300
                )

        except Exception:
            logger.error(
                traceback.format_exc()
            )

            return False


class AuditExporter:
    """
    Production-safe async audit exporter.

    Features:
    - HMAC SHA256 signatures
    - Append-only JSONL archive
    - Webhook/syslog style export
    - Async batching
    - Auto-redaction
    - Hash chain tamper evidence
    - Default-deny RBAC
    """

    DEFAULT_BATCH_SIZE = 25
    DEFAULT_FLUSH_INTERVAL = 5

    def __init__(
        self,
        *,
        signing_key: str,
        archive_path: str,
        webhook_endpoint: Optional[
            str
        ] = None,
        batch_size: int = (
            DEFAULT_BATCH_SIZE
        ),
        flush_interval: int = (
            DEFAULT_FLUSH_INTERVAL
        ),
    ) -> None:

        self.signer = HMACSigner(
            signing_key=
                signing_key
        )

        self.redactor = (
            RedactionFilter()
        )

        self.guard = RBACGuard()

        self.file_manager = (
            AppendOnlyFileManager(
                file_path=
                    archive_path
            )
        )

        self.shipper = (
            AsyncWebhookShipper(
                endpoint=
                    webhook_endpoint
            )
        )

        self.batch_size = (
            batch_size
        )

        self.flush_interval = (
            flush_interval
        )

        self._queue: asyncio.Queue[
            AuditEvent
        ] = asyncio.Queue(
            maxsize=5000
        )

        self._running = False

        self._worker_task: Optional[
            asyncio.Task
        ] = None

        self._sequence = 0

        self._last_hash = (
            "GENESIS"
        )

        self.stats = {
            "exported": 0,
            "failed": 0,
            "redacted": 0,
        }

    async def start(
        self,
    ) -> None:

        if self._running:
            return

        logger.info(
            "Starting AuditExporter"
        )

        self._running = True

        self._worker_task = (
            asyncio.create_task(
                self._export_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping AuditExporter"
        )

        self._running = False

        if self._worker_task:
            self._worker_task.cancel()

            with contextlib.suppress(
                asyncio.CancelledError
            ):
                await self._worker_task

    async def enqueue(
        self,
        *,
        event: AuditEvent,
    ) -> None:

        if (
            not self._running
        ):
            raise AuditExporterError(
                "Exporter not started"
            )

        await self._queue.put(
            event
        )

    async def update_config(
        self,
        *,
        roles: Set[str],
        permissions: Set[str],
        batch_size: Optional[
            int
        ] = None,
        flush_interval: Optional[
            int
        ] = None,
    ) -> None:

        authorized = (
            await self.guard.validate(
                roles=roles,
                permissions=
                    permissions,
            )
        )

        if not authorized:
            raise ExportPermissionDenied(
                "Audit exporter config denied"
            )

        if batch_size:
            self.batch_size = max(
                1,
                min(
                    batch_size,
                    100,
                ),
            )

        if flush_interval:
            self.flush_interval = max(
                1,
                min(
                    flush_interval,
                    300,
                ),
            )

    async def _export_loop(
        self,
    ) -> None:

        while self._running:

            try:

                batch: List[
                    AuditEvent
                ] = []

                started = (
                    time.time()
                )

                while (
                    len(batch)
                    < self.batch_size
                ):

                    timeout = max(
                        0.1,
                        self.flush_interval
                        - (
                            time.time()
                            - started
                        ),
                    )

                    try:
                        event = (
                            await asyncio.wait_for(
                                self._queue.get(),
                                timeout=
                                    timeout,
                            )
                        )

                        batch.append(
                            event
                        )

                    except asyncio.TimeoutError:
                        break

                if not batch:
                    continue

                signed_records = (
                    await self._sign_batch(
                        batch
                    )
                )

                for record in (
                    signed_records
                ):
                    await self.file_manager.append(
                        record
                    )

                shipped = (
                    await self.shipper.ship(
                        signed_records
                    )
                )

                if shipped:
                    self.stats[
                        "exported"
                    ] += len(
                        signed_records
                    )

                else:
                    self.stats[
                        "failed"
                    ] += len(
                        signed_records
                    )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.error(
                    traceback.format_exc()
                )

    async def _sign_batch(
        self,
        batch: List[
            AuditEvent
        ],
    ) -> List[
        SignedAuditRecord
    ]:

        records = []

        for event in batch:

            sanitized = (
                self.redactor.sanitize(
                    event.payload
                )
            )

            payload_bytes = (
                json.dumps(
                    sanitized,
                    sort_keys=True,
                ).encode("utf-8")
            )

            signature = (
                self.signer.sign(
                    payload_bytes
                )
            )

            current_hash = (
                self.signer.hash_record(
                    payload_bytes
                    + self._last_hash.encode(
                        "utf-8"
                    )
                )
            )

            self._sequence += 1

            record = (
                SignedAuditRecord(
                    sequence_id=
                        self._sequence,
                    timestamp=
                        event.timestamp,
                    event_type=
                        event.event_type,
                    payload=
                        sanitized,
                    signature=
                        signature,
                    previous_hash=
                        self._last_hash,
                    current_hash=
                        current_hash,
                    severity=
                        event.severity,
                )
            )

            self._last_hash = (
                current_hash
            )

            records.append(
                record
            )

        return records

    async def runtime_state(
        self,
    ) -> Dict[str, Any]:

        return {
            "running":
                self._running,
            "queued_events":
                self._queue.qsize(),
            "sequence":
                self._sequence,
            "last_hash":
                self._last_hash,
            "stats":
                self.stats,
            "timestamp":
                time.time(),
        }

    async def integrity_probe(
        self,
    ) -> Dict[str, Any]:

        return {
            "healthy":
                self._running,
            "queue_depth":
                self._queue.qsize(),
            "signed_records":
                self._sequence,
            "timestamp":
                time.time(),
        }


DEFAULT_AUDIT_EXPORTER = (
    AuditExporter
)
