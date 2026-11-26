"""Short-lived connections; callers never share connections across threads."""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def open_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    """Open, configure and always close one thread-local connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None, timeout=30.0)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
    finally:
        conn.close()


@contextmanager
def write_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Acquire the write reservation before reading; roll back even on interrupts."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
