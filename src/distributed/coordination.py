from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import secrets
import socket
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class NodeRecord:
    node_id: str
    node_type: str
    host: str
    port: int
    agents: List[str]
    heartbeat_at: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class DistributedLock:
    lock_key: str
    owner_node: str
    acquired_at: float
    expires_at: float


class RBACAuthorizationError(
    Exception
):
    pass


class ClusterAuthenticationError(
    Exception
):
    pass


class SQLiteNodeRegistry:
    """
    SQLite WAL-backed node registry.
    """

    SQLITE_BUSY_TIMEOUT = 5000

    def __init__(
        self,
        database_path: str,
    ) -> None:

        self.database_path = Path(
            database_path
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._connection: Optional[
            sqlite3.Connection
        ] = None

    async def initialize(
        self,
    ) -> None:

        self._connection = sqlite3.connect(
            str(self.database_path),
            check_same_thread=False,
            isolation_level=None,
        )

        await asyncio.to_thread(
            self._configure
        )

        await asyncio.to_thread(
            self._create_tables
        )

    async def close(
        self,
    ) -> None:

        if self._connection:
            await asyncio.to_thread(
                self._connection.close
            )

    def _configure(
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
            f"PRAGMA busy_timeout={self.SQLITE_BUSY_TIMEOUT};"
        )

    def _create_tables(
        self,
    ) -> None:

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_nodes (
                node_id TEXT PRIMARY KEY,
                node_type TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                agents TEXT NOT NULL,
                heartbeat_at REAL NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS distributed_locks (
                lock_key TEXT PRIMARY KEY,
                owner_node TEXT NOT NULL,
                acquired_at REAL NOT NULL,
                expires_at REAL NOT NULL
            )
            """
        )

    async def register_node(
        self,
        record: NodeRecord,
    ) -> None:

        await asyncio.to_thread(
            self._register_node_sync,
            record,
        )

    def _register_node_sync(
        self,
        record: NodeRecord,
    ) -> None:

        self._connection.execute(
            """
            INSERT OR REPLACE INTO distributed_nodes (
                node_id,
                node_type,
                host,
                port,
                agents,
                heartbeat_at,
                metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.node_id,
                record.node_type,
                record.host,
                record.port,
                json.dumps(
                    record.agents
                ),
                record.heartbeat_at,
                json.dumps(
                    record.metadata
                ),
            ),
        )

    async def update_heartbeat(
        self,
        node_id: str,
    ) -> None:

        await asyncio.to_thread(
            self._update_heartbeat_sync,
            node_id,
        )

    def _update_heartbeat_sync(
        self,
        node_id: str,
    ) -> None:

        self._connection.execute(
            """
            UPDATE distributed_nodes
            SET heartbeat_at = ?
            WHERE node_id = ?
            """,
            (
                time.time(),
                node_id,
            ),
        )

    async def list_nodes(
        self,
    ) -> List[NodeRecord]:

        return await asyncio.to_thread(
            self._list_nodes_sync
        )

    def _list_nodes_sync(
        self,
    ) -> List[NodeRecord]:

        cursor = self._connection.execute(
            """
            SELECT
                node_id,
                node_type,
                host,
                port,
                agents,
                heartbeat_at,
                metadata
            FROM distributed_nodes
            """
        )

        rows = cursor.fetchall()

        records: List[
            NodeRecord
        ] = []

        for row in rows:
            records.append(
                NodeRecord(
                    node_id=row[0],
                    node_type=row[1],
                    host=row[2],
                    port=row[3],
                    agents=json.loads(
                        row[4]
                    ),
                    heartbeat_at=row[5],
                    metadata=json.loads(
                        row[6]
                    ),
                )
            )

        return records

    async def cleanup_stale_nodes(
        self,
        timeout_seconds: int,
    ) -> None:

        threshold = (
            time.time()
            - timeout_seconds
        )

        await asyncio.to_thread(
            self._cleanup_sync,
            threshold,
        )

    def _cleanup_sync(
        self,
        threshold: float,
    ) -> None:

        self._connection.execute(
            """
            DELETE FROM distributed_nodes
            WHERE heartbeat_at < ?
            """,
            (threshold,),
        )


class DistributedLockManager:
    """
    SQLite-based lightweight distributed lock manager.
    """

    def __init__(
        self,
        registry: SQLiteNodeRegistry,
    ) -> None:

        self.registry = registry

    async def acquire_lock(
        self,
        *,
        lock_key: str,
        owner_node: str,
        ttl_seconds: int = 30,
    ) -> bool:

        return await asyncio.to_thread(
            self._acquire_lock_sync,
            lock_key,
            owner_node,
            ttl_seconds,
        )

    def _acquire_lock_sync(
        self,
        lock_key: str,
        owner_node: str,
        ttl_seconds: int,
    ) -> bool:

        now = time.time()

        expires_at = (
            now + ttl_seconds
        )

        conn = (
            self.registry._connection
        )

        cursor = conn.execute(
            """
            SELECT owner_node, expires_at
            FROM distributed_locks
            WHERE lock_key = ?
            """,
            (lock_key,),
        )

        row = cursor.fetchone()

        if row:
            existing_expiry = row[1]

            if existing_expiry > now:
                return False

            conn.execute(
                """
                DELETE FROM distributed_locks
                WHERE lock_key = ?
                """,
                (lock_key,),
            )

        conn.execute(
            """
            INSERT INTO distributed_locks (
                lock_key,
                owner_node,
                acquired_at,
                expires_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                lock_key,
                owner_node,
                now,
                expires_at,
            ),
        )

        return True

    async def release_lock(
        self,
        *,
        lock_key: str,
        owner_node: str,
    ) -> bool:

        return await asyncio.to_thread(
            self._release_lock_sync,
            lock_key,
            owner_node,
        )

    def _release_lock_sync(
        self,
        lock_key: str,
        owner_node: str,
    ) -> bool:

        conn = (
            self.registry._connection
        )

        cursor = conn.execute(
            """
            DELETE FROM distributed_locks
            WHERE lock_key = ?
            AND owner_node = ?
            """,
            (
                lock_key,
                owner_node,
            ),
        )

        return (
            cursor.rowcount > 0
        )

    async def cleanup_expired_locks(
        self,
    ) -> None:

        await asyncio.to_thread(
            self._cleanup_expired_sync
        )

    def _cleanup_expired_sync(
        self,
    ) -> None:

        now = time.time()

        self.registry._connection.execute(
            """
            DELETE FROM distributed_locks
            WHERE expires_at < ?
            """,
            (now,),
        )


class CryptographicHandshake:
    """
    HMAC cluster token handshake.
    """

    TOKEN_WINDOW_SECONDS = 30

    def __init__(
        self,
        cluster_secret: str,
    ) -> None:

        self.cluster_secret = (
            cluster_secret.encode(
                "utf-8"
            )
        )

    def generate_token(
        self,
        *,
        node_id: str,
        timestamp: Optional[
            int
        ] = None,
    ) -> str:

        ts = (
            timestamp
            or int(time.time())
        )

        payload = (
            f"{node_id}:{ts}"
        ).encode("utf-8")

        signature = hmac.new(
            self.cluster_secret,
            payload,
            hashlib.sha256,
        ).hexdigest()

        return (
            f"{node_id}:{ts}:{signature}"
        )

    def validate_token(
        self,
        token: str,
    ) -> bool:

        try:
            node_id, ts, sig = (
                token.split(":")
            )

            ts_int = int(ts)

            if (
                abs(
                    time.time()
                    - ts_int
                )
                > self.TOKEN_WINDOW_SECONDS
            ):
                return False

            expected = (
                self.generate_token(
                    node_id=node_id,
                    timestamp=ts_int,
                )
            )

            expected_sig = (
                expected.split(":")[2]
            )

            return hmac.compare_digest(
                sig,
                expected_sig,
            )

        except Exception:
            return False


class AuthorizationChecker:
    """
    Default Deny authorization guard.
    """

    REQUIRED_ROLE = (
        "distributed.node"
    )

    REQUIRED_PERMISSION = (
        "cluster.join"
    )

    async def validate_join(
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


class MultiNodeCoordinator:
    """
    Async-first distributed coordination engine.

    Features:
    - Multi-node registration
    - Shared distributed locking
    - Node heartbeat tracking
    - Cluster authentication
    - SQLite WAL persistence
    - Default Deny RBAC
    """

    HEARTBEAT_INTERVAL = 10
    NODE_TIMEOUT_SECONDS = 60
    LOCK_CLEANUP_INTERVAL = 30

    def __init__(
        self,
        *,
        node_id: str,
        node_type: str,
        host: str,
        port: int,
        agents: List[str],
        cluster_secret: str,
        database_path: str = (
            "./data/distributed_cluster.db"
        ),
    ) -> None:

        self.node_id = node_id
        self.node_type = node_type
        self.host = host
        self.port = port
        self.agents = agents

        self.registry = (
            SQLiteNodeRegistry(
                database_path
            )
        )

        self.lock_manager = (
            DistributedLockManager(
                self.registry
            )
        )

        self.handshake = (
            CryptographicHandshake(
                cluster_secret
            )
        )

        self.auth_checker = (
            AuthorizationChecker()
        )

        self._running = False

        self._heartbeat_task: Optional[
            asyncio.Task
        ] = None

        self._cleanup_task: Optional[
            asyncio.Task
        ] = None

    async def start(
        self,
    ) -> None:

        logger.info(
            "Starting MultiNodeCoordinator"
        )

        await self.registry.initialize()

        await self.register_self()

        self._running = True

        self._heartbeat_task = (
            asyncio.create_task(
                self._heartbeat_loop()
            )
        )

        self._cleanup_task = (
            asyncio.create_task(
                self._cleanup_loop()
            )
        )

    async def stop(
        self,
    ) -> None:

        logger.info(
            "Stopping MultiNodeCoordinator"
        )

        self._running = False

        for task in (
            self._heartbeat_task,
            self._cleanup_task,
        ):
            if task:
                task.cancel()

                with contextlib.suppress(
                    asyncio.CancelledError
                ):
                    await task

        await self.registry.close()

    async def register_self(
        self,
    ) -> None:

        record = NodeRecord(
            node_id=self.node_id,
            node_type=
                self.node_type,
            host=self.host,
            port=self.port,
            agents=self.agents,
            heartbeat_at=
                time.time(),
            metadata={
                "hostname":
                    socket.gethostname(),
            },
        )

        await self.registry.register_node(
            record
        )

    async def join_cluster(
        self,
        *,
        token: str,
        roles: Set[str],
        permissions: Set[str],
    ) -> bool:

        valid_token = (
            self.handshake.validate_token(
                token
            )
        )

        if not valid_token:
            raise ClusterAuthenticationError(
                "Invalid cluster token"
            )

        authorized = (
            await self.auth_checker.validate_join(
                roles=roles,
                permissions=
                    permissions,
            )
        )

        if not authorized:
            raise RBACAuthorizationError(
                "RBAC validation failed"
            )

        return True

    async def acquire_task_lock(
        self,
        task_key: str,
        ttl_seconds: int = 30,
    ) -> bool:

        return await self.lock_manager.acquire_lock(
            lock_key=task_key,
            owner_node=self.node_id,
            ttl_seconds=
                ttl_seconds,
        )

    async def release_task_lock(
        self,
        task_key: str,
    ) -> bool:

        return await self.lock_manager.release_lock(
            lock_key=task_key,
            owner_node=self.node_id,
        )

    async def cluster_nodes(
        self,
    ) -> List[NodeRecord]:

        return await self.registry.list_nodes()

    async def _heartbeat_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await self.registry.update_heartbeat(
                    self.node_id
                )

                await asyncio.sleep(
                    self.HEARTBEAT_INTERVAL
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Heartbeat loop failure"
                )

    async def _cleanup_loop(
        self,
    ) -> None:

        while self._running:
            try:
                await self.registry.cleanup_stale_nodes(
                    self.NODE_TIMEOUT_SECONDS
                )

                await self.lock_manager.cleanup_expired_locks()

                await asyncio.sleep(
                    self.LOCK_CLEANUP_INTERVAL
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Cleanup loop failure"
                )

    def generate_cluster_token(
        self,
    ) -> str:

        return self.handshake.generate_token(
            node_id=self.node_id
        )

    def stats(
        self,
    ) -> Dict[str, Any]:

        return {
            "node_id":
                self.node_id,
            "node_type":
                self.node_type,
            "host":
                self.host,
            "port":
                self.port,
            "agents":
                len(self.agents),
            "running":
                self._running,
            "timestamp":
                time.time(),
        }


DEFAULT_MULTI_NODE_COORDINATOR = (
    MultiNodeCoordinator
)
