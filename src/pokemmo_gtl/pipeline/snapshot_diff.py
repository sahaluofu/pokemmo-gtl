"""Nominally ordered snapshot differences with immutable publication revisions."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from collections.abc import Mapping
from contextlib import nullcontext
from typing import Any

from pokemmo_gtl.db.connection import write_transaction
from pokemmo_gtl.pipeline.aggregator import (
    _bucket_replayable,
    _cascade_snapshot_bars,
    _get_snapshot,
    _nominal_neighbor,
    _publication,
    _publication_has_outputs,
    _record_observations,
    aggregate_listings,
    ensure_items_catalog,
)
from pokemmo_gtl.pipeline.retention import _columns, check_frames_retained

_FLOW_COLUMNS = (
    "snapshot_id",
    "prev_snapshot_id",
    "item_id",
    "captured_at",
    "interval_sec",
    "new_listing_count",
    "new_listed_qty",
    "partial_fill_qty",
    "early_disappear_qty",
    "expired_qty",
    "est_sold_qty",
    "est_sold_vwap",
    "gap_skipped",
)

_FLOW_REVISION_COLUMNS = (
    "snapshot_id",
    "item_id",
    "publish_seq",
    "prev_snapshot_id",
    "revision_available_at",
    "captured_at",
    "interval_sec",
    "new_listing_count",
    "new_listed_qty",
    "partial_fill_qty",
    "early_disappear_qty",
    "expired_qty",
    "est_sold_qty",
    "est_sold_vwap",
    "observed_undercut_count",
    "gap_skipped",
)


def _setting(settings: Any, key: str, default: Any) -> Any:
    if settings is None:
        return default
    if isinstance(settings, Mapping):
        return settings.get(key, default)
    return getattr(settings, key, default)


def _write_scope(conn: sqlite3.Connection):
    if conn.in_transaction:
        return nullcontext(conn)
    return write_transaction(conn)


def _listing_books(
    conn: sqlite3.Connection, snapshot_id: int
) -> dict[int, dict[int, sqlite3.Row]]:
    result: dict[int, dict[int, sqlite3.Row]] = defaultdict(dict)
    rows = conn.execute(
        """
        SELECT auction_id, item_id, price, quantity, expires_at
        FROM raw_listings
        WHERE snapshot_id = ?
        ORDER BY item_id, price, auction_id
        """,
        (snapshot_id,),
    ).fetchall()
    for row in rows:
        if int(row["quantity"]) > 0:
            result[int(row["item_id"])][int(row["auction_id"])] = row
    return result


def _pair_retained(
    conn: sqlite3.Connection, previous_id: int, current_id: int
) -> bool:
    # A 001-only database has no retention ledger. Its original pipeline remains
    # usable, but check_frames_retained itself never claims that proof exists.
    if "raw_retained" not in _columns(conn, "snapshot_runs"):
        return True
    return check_frames_retained(conn, [previous_id, current_id])


def _split_replayable(
    conn: sqlite3.Connection,
    previous: sqlite3.Row | None,
    current: sqlite3.Row,
    successor: sqlite3.Row,
) -> bool:
    """Require the entire late interval split before publishing any correction."""
    current_id = int(current["snapshot_id"])
    successor_id = int(successor["snapshot_id"])
    if previous is not None and not _pair_retained(
        conn, int(previous["snapshot_id"]), current_id
    ):
        return False
    if not _pair_retained(conn, current_id, successor_id):
        return False

    timestamps = (
        int(current["server_started_at"]),
        int(successor["server_started_at"]),
    )
    for timeframe, seconds in (("15m", 900), ("4h", 14_400), ("1d", 86_400)):
        buckets = {timestamp - timestamp % seconds for timestamp in timestamps}
        for bucket_ts in sorted(buckets):
            # Both base buckets are required. Existing parent bars, including
            # immutable history, also require their complete retained windows.
            if seconds != 900 and not any(
                _columns(conn, table)
                and conn.execute(
                    f"""
                    SELECT 1 FROM {table}
                    WHERE timeframe = ? AND bucket_ts = ?
                    LIMIT 1
                    """,
                    (timeframe, bucket_ts),
                ).fetchone()
                is not None
                for table in ("item_bars", "item_bar_revisions")
            ):
                continue
            if not _bucket_replayable(conn, bucket_ts, seconds):
                return False
    return True


def _write_flows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    publish_seq: int | None,
) -> None:
    if not rows:
        return

    source_rows: list[Mapping[str, Any] | sqlite3.Row] = rows
    if publish_seq is not None and _columns(conn, "inferred_flow_revisions"):
        names = ", ".join(_FLOW_REVISION_COLUMNS)
        marks = ", ".join("?" for _ in _FLOW_REVISION_COLUMNS)
        conn.executemany(
            f"""
            INSERT INTO inferred_flow_revisions ({names})
            VALUES ({marks})
            ON CONFLICT(snapshot_id, item_id, publish_seq) DO NOTHING
            """,
            [
                tuple(row[column] for column in _FLOW_REVISION_COLUMNS)
                for row in rows
            ],
        )
        # Reusing a publication must reuse its original payload as well.
        source_rows = conn.execute(
            f"""
            SELECT {names}
            FROM inferred_flow_revisions
            WHERE snapshot_id = ? AND publish_seq = ?
            ORDER BY item_id
            """,
            (rows[0]["snapshot_id"], publish_seq),
        ).fetchall()

    available_columns = _columns(conn, "inferred_flows")
    columns = list(_FLOW_COLUMNS)
    if "observed_undercut_count" in available_columns:
        columns.append("observed_undercut_count")
    if "latest_publish_seq" in available_columns:
        columns.append("latest_publish_seq")

    names = ", ".join(columns)
    marks = ", ".join("?" for _ in columns)
    updates = ", ".join(
        f"{column} = excluded.{column}"
        for column in columns
        if column not in {"snapshot_id", "item_id"}
    )
    guard = ""
    if "latest_publish_seq" in available_columns and publish_seq is not None:
        guard = """
        WHERE inferred_flows.latest_publish_seq IS NULL
           OR inferred_flows.latest_publish_seq <= excluded.latest_publish_seq
        """

    payloads = []
    for source in source_rows:
        values = dict(source)
        values["latest_publish_seq"] = publish_seq
        payloads.append(tuple(values[column] for column in columns))
    conn.executemany(
        f"""
        INSERT INTO inferred_flows ({names})
        VALUES ({marks})
        ON CONFLICT(snapshot_id, item_id) DO UPDATE SET {updates}
        {guard}
        """,
        payloads,
    )


def _diff_pair(
    conn: sqlite3.Connection,
    previous: sqlite3.Row,
    current: sqlite3.Row,
    settings: Any,
    publish_seq: int | None,
    available_at: int,
) -> int:
    previous_id = int(previous["snapshot_id"])
    current_id = int(current["snapshot_id"])
    current_ts = int(current["server_started_at"])
    bucket_ts = current_ts - current_ts % 900

    if not _pair_retained(conn, previous_id, current_id):
        return 0
    if not _bucket_replayable(conn, bucket_ts, 900):
        return 0

    interval_sec = current_ts - int(previous["server_started_at"])
    grace_sec = int(_setting(settings, "expiry_grace_sec", 180))
    max_gap_sec = int(_setting(settings, "max_diff_gap_sec", 5400))
    alpha_near = float(_setting(settings, "alpha_near", 0.75))
    alpha_mid = float(_setting(settings, "alpha_mid", 0.20))
    gap_skipped = int(interval_sec < 60 or interval_sec > max_gap_sec)

    previous_books = _listing_books(conn, previous_id)
    current_books = _listing_books(conn, current_id)
    item_ids = set(previous_books) | set(current_books)
    # A changed predecessor can remove a formerly inferred item entirely.
    # Publish an explicit zero row rather than leave its old flow current.
    item_ids.update(
        int(row[0])
        for row in conn.execute(
            "SELECT item_id FROM inferred_flows WHERE snapshot_id = ?",
            (current_id,),
        )
    )
    ensure_items_catalog(conn, item_ids, current_ts)

    previous_summaries = {
        item_id: aggregate_listings(book.values())
        for item_id, book in previous_books.items()
    }
    current_summaries = {
        item_id: aggregate_listings(book.values())
        for item_id, book in current_books.items()
    }

    rows_to_insert: list[dict[str, Any]] = []
    for item_id in sorted(item_ids):
        old_book = previous_books.get(item_id, {})
        new_book = current_books.get(item_id, {})
        new_rows = [
            row for auction_id, row in new_book.items() if auction_id not in old_book
        ]
        previous_min = (
            min(int(row["price"]) for row in old_book.values())
            if old_book
            else None
        )
        # Count new auction IDs below the previous unfiltered minimum ask.
        # Quantity changes and persistent auction IDs do not add to this count.
        undercut_count = (
            sum(int(row["price"]) < previous_min for row in new_rows)
            if previous_min is not None
            else 0
        )

        partial_fill_qty = 0
        partial_fill_value = 0
        early_disappear_qty = 0
        expired_qty = 0
        disappeared_early: list[sqlite3.Row] = []

        for old_row in old_book.values():
            current_row = new_book.get(int(old_row["auction_id"]))
            if current_row is not None:
                delta = int(old_row["quantity"]) - int(current_row["quantity"])
                if delta > 0:
                    partial_fill_qty += delta
                    partial_fill_value += delta * int(old_row["price"])
            elif int(old_row["expires_at"]) <= current_ts + grace_sec:
                expired_qty += int(old_row["quantity"])
            else:
                early_disappear_qty += int(old_row["quantity"])
                disappeared_early.append(old_row)

        previous_summary = previous_summaries.get(item_id)
        current_summary = current_summaries.get(item_id)
        previous_vwap = (
            float(previous_summary["vwap_top5_qty"]) if previous_summary else 0.0
        )
        current_ask = (
            float(current_summary["robust_best_ask"]) if current_summary else 0.0
        )
        clear_price = max(previous_vwap, current_ask)
        near_limit = clear_price * 1.03
        mid_limit = clear_price * 1.10
        early_near = [
            row for row in disappeared_early if int(row["price"]) <= near_limit
        ]
        early_mid = [
            row
            for row in disappeared_early
            if near_limit < int(row["price"]) <= mid_limit
        ]
        early_near_qty = sum(int(row["quantity"]) for row in early_near)
        early_near_value = sum(
            int(row["quantity"]) * int(row["price"]) for row in early_near
        )
        replacement_qty = sum(
            int(row["quantity"])
            for row in new_rows
            if int(row["price"]) <= near_limit
            and int(row["price"]) < previous_vwap
        )
        near_sold_qty = max(0.0, early_near_qty - 0.5 * replacement_qty)
        near_sold_value = (
            early_near_value * near_sold_qty / early_near_qty
            if early_near_qty
            else 0.0
        )
        mid_qty = sum(int(row["quantity"]) for row in early_mid)
        sold_qty = (
            partial_fill_qty + alpha_near * near_sold_qty + alpha_mid * mid_qty
        )
        sold_value = (
            partial_fill_value
            + alpha_near * near_sold_value
            + alpha_mid
            * sum(int(row["quantity"]) * int(row["price"]) for row in early_mid)
        )

        rows_to_insert.append(
            {
                "snapshot_id": current_id,
                "prev_snapshot_id": previous_id,
                "item_id": item_id,
                "captured_at": current_ts,
                "interval_sec": interval_sec,
                "new_listing_count": len(new_rows),
                "new_listed_qty": sum(int(row["quantity"]) for row in new_rows),
                "partial_fill_qty": partial_fill_qty,
                "early_disappear_qty": early_disappear_qty,
                "expired_qty": expired_qty,
                "est_sold_qty": 0.0 if gap_skipped else float(sold_qty),
                "est_sold_vwap": (
                    sold_value / sold_qty
                    if not gap_skipped and sold_qty > 0
                    else None
                ),
                "observed_undercut_count": int(undercut_count),
                "gap_skipped": gap_skipped,
                "publish_seq": publish_seq,
                "revision_available_at": available_at,
                "latest_publish_seq": publish_seq,
            }
        )

    _write_flows(conn, rows_to_insert, publish_seq)
    return len(rows_to_insert)


def process_snapshot_diff(
    conn: sqlite3.Connection, snapshot_id: int, settings: Any = None
) -> int:
    """Publish a snapshot, its successor correction, evidence, and affected bars.

    Completed snapshots are ordered by ``(server_started_at, snapshot_id)``.
    A late frame and its immediate successor share the late publication's
    sequence and availability time. Previously published revisions are immutable.
    The return value counts this snapshot's flow rows, excluding successor rows.
    """
    with _write_scope(conn):
        current = _get_snapshot(conn, snapshot_id)
        if current is None:
            raise ValueError(f"Unknown snapshot_id: {snapshot_id}")
        if current["status"] != "completed":
            raise ValueError("Snapshot differences require a completed current snapshot")

        current_ts = int(current["server_started_at"])
        bucket_ts = current_ts - current_ts % 900
        if not _bucket_replayable(conn, bucket_ts, 900):
            return 0

        previous = _nominal_neighbor(conn, current, successor=False)
        successor = _nominal_neighbor(conn, current, successor=True)
        if successor is not None and not _split_replayable(
            conn, previous, current, successor
        ):
            return 0

        existing_sequence = current["publish_seq"]
        if existing_sequence is not None and _publication_has_outputs(
            conn, snapshot_id, int(existing_sequence)
        ):
            if previous is not None and not _pair_retained(
                conn, int(previous["snapshot_id"]), snapshot_id
            ):
                return 0
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM inferred_flows WHERE snapshot_id = ?",
                    (snapshot_id,),
                ).fetchone()[0]
            )

        publish_seq, available_at = _publication(conn, current)
        count = 0
        if previous is not None:
            count = _diff_pair(
                conn, previous, current, settings, publish_seq, available_at
            )
        if successor is not None:
            _diff_pair(
                conn, current, successor, settings, publish_seq, available_at
            )

        _record_observations(conn, snapshot_id)
        affected_snapshots = [snapshot_id]
        if successor is not None:
            affected_snapshots.append(int(successor["snapshot_id"]))
        _cascade_snapshot_bars(
            conn,
            affected_snapshots,
            publish_seq=publish_seq,
            available_at=available_at,
        )
        return count
