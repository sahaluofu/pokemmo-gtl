"""Chunked raw-detail retention with explicit, NULL-safe frame accounting."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from pokemmo_gtl.db.connection import write_transaction

_BATCH_SIZE = 20


def _setting(settings: Any, key: str, default: Any) -> Any:
    if settings is None:
        return default
    if isinstance(settings, Mapping):
        return settings.get(key, default)
    return getattr(settings, key, default)


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Inspect optional migration features without caching across migrations."""
    identifier = table.replace('"', '""')
    return {
        str(row[1])
        for row in conn.execute(f'PRAGMA table_info("{identifier}")').fetchall()
    }


def check_frames_retained(
    conn: sqlite3.Connection, snapshot_ids: Sequence[int]
) -> bool:
    """Prove that every requested frame exists, is complete, and retains raw data.

    Missing records, pruned records, and legacy NULL markers all fail. A 001-only
    schema cannot prove retention and returns False; compatibility callers must
    explicitly choose their original 001 behavior instead.
    """
    if not snapshot_ids:
        return False
    unique_ids = sorted({int(snapshot_id) for snapshot_id in snapshot_ids})
    if "raw_retained" not in _columns(conn, "snapshot_runs"):
        return False

    marks = ", ".join("?" for _ in unique_ids)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS frame_count,
               SUM(
                   CASE
                       WHEN status = 'completed' AND raw_retained = 1 THEN 0
                       ELSE 1
                   END
               ) AS unsafe_count
        FROM snapshot_runs
        WHERE snapshot_id IN ({marks})
        """,
        unique_ids,
    ).fetchone()
    return int(row["frame_count"]) == len(unique_ids) and row["unsafe_count"] == 0


def prune_raw_listings(
    conn: sqlite3.Connection,
    now_ts: int | None = None,
    *,
    retention_hours: int = 48,
) -> int:
    """Prune in 20-frame transactions, preserving the newest two completed runs."""
    if conn.in_transaction:
        raise RuntimeError("Retention must run outside an existing transaction")
    if retention_hours <= 0:
        raise ValueError("retention_hours must be positive")
    if now_ts is None:
        now_ts = conn.execute(
            """
            SELECT MAX(completed_at)
            FROM snapshot_runs
            WHERE status = 'completed' AND completed_at IS NOT NULL
            """
        ).fetchone()[0]
    if now_ts is None:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        return 0

    now_ts = int(now_ts)
    cutoff = now_ts - int(retention_hours) * 3600
    snapshot_columns = _columns(conn, "snapshot_runs")
    observations_supported = bool(_columns(conn, "orderbook_observations"))
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

            if observations_supported:
                from pokemmo_gtl.pipeline.aggregator import _record_observations

                for snapshot_id in snapshot_ids:
                    _record_observations(conn, snapshot_id)

            marks = ", ".join("?" for _ in snapshot_ids)
            cursor = conn.execute(
                f"DELETE FROM raw_listings WHERE snapshot_id IN ({marks})",
                snapshot_ids,
            )
            batch_deleted = cursor.rowcount
            if "raw_retained" in snapshot_columns:
                conn.execute(
                    f"""
                    UPDATE snapshot_runs
                    SET raw_retained = 0, raw_pruned_at = ?
                    WHERE snapshot_id IN ({marks})
                    """,
                    [now_ts, *snapshot_ids],
                )
            deleted_rows += batch_deleted

    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    return deleted_rows


def run_retention(
    conn: sqlite3.Connection, settings: Any = None, now_ts: int | None = None
) -> int:
    """Apply retention using the configured window."""
    hours = int(_setting(settings, "retention_hours", 48))
    return prune_raw_listings(conn, now_ts, retention_hours=hours)
