"""SQLite-backed rolling request budget and fenced collector leases."""

import math
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Self
from uuid import uuid4

from pokemmo_gtl.config import PROJECT_ROOT
from pokemmo_gtl.db.connection import open_connection, write_transaction

DEFAULT_LIMITER_PATH = PROJECT_ROOT / "data" / "api_limiter.db"
COLLECTOR_LOCK_NAME = "official_api"
HEARTBEAT_TIMEOUT_SEC = 600
HEARTBEAT_INTERVAL_SEC = 60.0


class CollectorBusyError(RuntimeError):
    """Another collector owns the lease, or this collector has lost its lease."""


@dataclass(frozen=True)
class CollectorLock:
    """A lease handle; old handles cannot renew or delete a replacement lease."""

    _limiter: "SQLiteRateLimiter"
    lock_name: str
    owner_pid: int
    acquired_at: int

    def heartbeat(self) -> None:
        self._limiter._heartbeat(self)

    def release(self) -> None:
        self._limiter._release(self)


class SQLiteRateLimiter:
    """Reserve tokens before every attempt, sharing a file across live/sim processes.

    Pass ``db_path=":memory:"`` for an isolated, shared-cache test database.
    Every operation opens its own connection; the memory keeper is never used
    for queries or handed to worker threads. No transaction survives a sleep.
    """

    def __init__(
        self,
        db_path: Path | str = DEFAULT_LIMITER_PATH,
        *,
        max_tokens: int = 28_000,
        window_sec: float = 305.0,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if type(max_tokens) is not int or max_tokens <= 0:
            raise ValueError("max_tokens must be a positive integer")
        if not math.isfinite(window_sec) or window_sec <= 0:
            raise ValueError("window_sec must be positive and finite")
        self.max_tokens = max_tokens
        self.window_sec = float(window_sec)
        self._clock = clock
        self._sleeper = sleeper
        self._closed = False
        self._memory_guard = threading.RLock()
        self._memory_uri = None
        self._keeper = None
        self.db_path = Path(db_path)
        if str(db_path) == ":memory:":
            self._memory_uri = f"file:api_limiter_{uuid4().hex}?mode=memory&cache=shared"
            self._keeper = sqlite3.connect(self._memory_uri, uri=True, isolation_level=None)
        with self._connection() as conn, write_transaction(conn):
            conn.execute(
                "CREATE TABLE IF NOT EXISTS api_rate_ledger ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "req_ts REAL NOT NULL, tokens INTEGER NOT NULL) STRICT"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS collector_locks ("
                "lock_name TEXT PRIMARY KEY, owner_pid INTEGER NOT NULL, "
                "acquired_at INTEGER NOT NULL, heartbeat_at INTEGER NOT NULL) STRICT"
            )

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if self._closed:
            raise RuntimeError("Rate limiter is closed")
        if self._memory_uri is None:
            with open_connection(self.db_path) as conn:
                yield conn
        else:
            # Shared-cache SQLITE_LOCKED does not honor busy_timeout; serialize
            # memory operations while still creating connections in their owner thread.
            with self._memory_guard:
                conn = sqlite3.connect(self._memory_uri, uri=True, isolation_level=None)
                try:
                    conn.row_factory = sqlite3.Row
                    conn.execute("PRAGMA journal_mode = WAL")
                    conn.execute("PRAGMA synchronous = NORMAL")
                    conn.execute("PRAGMA foreign_keys = ON")
                    conn.execute("PRAGMA busy_timeout = 30000")
                    yield conn
                finally:
                    conn.close()

    def acquire(
        self, tokens: int, *, heartbeat: Callable[[], None] | None = None
    ) -> None:
        """Atomically debit the budget, waiting outside SQLite when it is full."""
        if type(tokens) is not int or not 0 < tokens <= self.max_tokens:
            raise ValueError("tokens must be a positive integer within max_tokens")
        while True:
            with self._connection() as conn, write_transaction(conn):
                now = float(self._clock())
                conn.execute(
                    "DELETE FROM api_rate_ledger WHERE req_ts < ?",
                    (now - max(310.0, self.window_sec),),
                )
                used, oldest = conn.execute(
                    "SELECT COALESCE(SUM(tokens), 0), MIN(req_ts) "
                    "FROM api_rate_ledger WHERE req_ts > ?",
                    (now - self.window_sec,),
                ).fetchone()
                if used + tokens <= self.max_tokens:
                    conn.execute(
                        "INSERT INTO api_rate_ledger (req_ts, tokens) VALUES (?, ?)",
                        (now, tokens),
                    )
                    return
                delay = max(0.0, oldest + self.window_sec - now)
            self._wait(delay, heartbeat)

    def _wait(self, delay: float, heartbeat: Callable[[], None] | None) -> None:
        if heartbeat is None:
            self._sleeper(delay)
            return
        while delay > 0:
            heartbeat()
            step = min(delay, HEARTBEAT_INTERVAL_SEC)
            self._sleeper(step)
            delay -= step
        heartbeat()

    def acquire_collector_lock(self, lock_name: str = COLLECTOR_LOCK_NAME) -> CollectorLock:
        """Fail immediately on a live owner; take over only after 600 seconds."""
        if not lock_name:
            raise ValueError("lock_name must not be empty")
        owner_pid = os.getpid()
        with self._connection() as conn, write_transaction(conn):
            now = int(self._clock())
            row = conn.execute(
                "SELECT heartbeat_at FROM collector_locks WHERE lock_name = ?", (lock_name,)
            ).fetchone()
            if row is not None and row[0] >= now - HEARTBEAT_TIMEOUT_SEC:
                raise CollectorBusyError("An official API collector is already running")
            conn.execute(
                "INSERT INTO collector_locks (lock_name, owner_pid, acquired_at, heartbeat_at) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(lock_name) DO UPDATE SET "
                "owner_pid = excluded.owner_pid, acquired_at = excluded.acquired_at, "
                "heartbeat_at = excluded.heartbeat_at",
                (lock_name, owner_pid, now, now),
            )
        return CollectorLock(self, lock_name, owner_pid, now)

    @contextmanager
    def collector_lock(self, lock_name: str = COLLECTOR_LOCK_NAME) -> Iterator[CollectorLock]:
        lease = self.acquire_collector_lock(lock_name)
        try:
            yield lease
        finally:
            lease.release()

    def _heartbeat(self, lease: CollectorLock) -> None:
        with self._connection() as conn, write_transaction(conn):
            now = int(self._clock())
            updated = conn.execute(
                "UPDATE collector_locks SET heartbeat_at = ? "
                "WHERE lock_name = ? AND owner_pid = ? AND acquired_at = ? "
                "AND heartbeat_at >= ?",
                (now, lease.lock_name, lease.owner_pid, lease.acquired_at,
                 now - HEARTBEAT_TIMEOUT_SEC),
            ).rowcount
            if updated != 1:
                raise CollectorBusyError("The collector lease has expired or changed owner")

    def _release(self, lease: CollectorLock) -> None:
        with self._connection() as conn, write_transaction(conn):
            conn.execute(
                "DELETE FROM collector_locks "
                "WHERE lock_name = ? AND owner_pid = ? AND acquired_at = ?",
                (lease.lock_name, lease.owner_pid, lease.acquired_at),
            )

    def close(self) -> None:
        if self._keeper is not None:
            self._keeper.close()
            self._keeper = None
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
