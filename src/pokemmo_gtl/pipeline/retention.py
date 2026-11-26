"""Chunked removal of expired raw listing details."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any

from pokemmo_gtl.db.connection import write_transaction

_BATCH_SIZE = 20


def _setting(settings: Any, key: str, default: Any) -> Any:
    if settings is None:
        return default
    if isinstance(settings, Mapping):
        return settings.get(key, default)
    return getattr(settings, key, default)


def prune_raw_listings(
    conn: sqlite3.Connection,
    now_ts: int | None = None,
    *,
    retention_hours: int = 48,
) -> int:
    """Delete old raw rows in 20-snapshot transactions, preserving the newest two runs."""
    if conn.in_transaction:
        raise RuntimeError("Retention must run outside an existing transaction")
    if retention_hours <= 0:
        raise ValueError("retention_hours must be positive")
    if now_ts is None:
        row = conn.execute(
            """
            SELECT MAX(completed_at)
            FROM snapshot_runs
            WHERE status = 'completed' AND completed_at IS NOT NULL
            """
        ).fetchone()
        now_ts = row[0]
    if now_ts is None:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        return 0

    cutoff = int(now_ts) - int(retention_hours) * 3600
    deleted_rows = 0
    while True:
        with write_transaction(conn):
            snapshot_ids = [
                int(row[0])
                for row in conn.execute(
                    """
                    SELECT s.snapshot_id
                    FROM snapshot_runs AS s
                    WHERE s.status = 'completed'
                      AND s.completed_at < ?
                      AND EXISTS (
                          SELECT 1 FROM raw_listings AS r
                          WHERE r.snapshot_id = s.snapshot_id
                      )
                      AND s.snapshot_id NOT IN (
                          SELECT snapshot_id FROM snapshot_runs
                          WHERE status = 'completed'
                          ORDER BY completed_at DESC, snapshot_id DESC
                          LIMIT 2
                      )
                    ORDER BY s.completed_at ASC, s.snapshot_id ASC
                    LIMIT ?
                    """,
                    (cutoff, _BATCH_SIZE),
                ).fetchall()
            ]
            if not snapshot_ids:
                break
            marks = ", ".join("?" for _ in snapshot_ids)
            cursor = conn.execute(
                f"DELETE FROM raw_listings WHERE snapshot_id IN ({marks})",
                snapshot_ids,
            )
            deleted_rows += cursor.rowcount

    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    return deleted_rows


def run_retention(
    conn: sqlite3.Connection, settings: Any = None, now_ts: int | None = None
) -> int:
    """Apply retention using the configured retention window when provided."""
    hours = int(_setting(settings, "retention_hours", 48))
    return prune_raw_listings(conn, now_ts, retention_hours=hours)
