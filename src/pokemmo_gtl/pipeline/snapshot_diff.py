"""Price-conditioned listing disappearance estimates between completed snapshots."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from collections.abc import Mapping
from contextlib import nullcontext
from typing import Any

from pokemmo_gtl.db.connection import write_transaction
from pokemmo_gtl.pipeline.aggregator import aggregate_listings, ensure_items_catalog


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
        FROM raw_listings WHERE snapshot_id = ?
        ORDER BY item_id, price, auction_id
        """,
        (snapshot_id,),
    ).fetchall()
    for row in rows:
        if row["quantity"] > 0:
            result[int(row["item_id"])][int(row["auction_id"])] = row
    return result


def process_snapshot_diff(
    conn: sqlite3.Connection, snapshot_id: int, settings: Any = None
) -> int:
    """Write flow rows for the immediately preceding completed snapshot pair.

    The first completed snapshot has no predecessor and intentionally produces no rows.
    """
    with _write_scope(conn):
        current = conn.execute(
            """
            SELECT snapshot_id, server_started_at, status
            FROM snapshot_runs WHERE snapshot_id = ?
            """,
            (snapshot_id,),
        ).fetchone()
        if current is None:
            raise ValueError(f"Unknown snapshot_id: {snapshot_id}")
        if current["status"] != "completed":
            raise ValueError("Snapshot differences require a completed current snapshot")

        previous = conn.execute(
            """
            SELECT snapshot_id, server_started_at
            FROM snapshot_runs
            WHERE status = 'completed' AND snapshot_id < ?
            ORDER BY snapshot_id DESC
            LIMIT 1
            """,
            (snapshot_id,),
        ).fetchone()
        if previous is None:
            return 0

        prev_id = int(previous["snapshot_id"])
        current_ts = int(current["server_started_at"])
        interval_sec = current_ts - int(previous["server_started_at"])
        grace_sec = int(_setting(settings, "expiry_grace_sec", 180))
        max_gap_sec = int(_setting(settings, "max_diff_gap_sec", 5400))
        alpha_near = float(_setting(settings, "alpha_near", 0.75))
        alpha_mid = float(_setting(settings, "alpha_mid", 0.20))

        previous_books = _listing_books(conn, prev_id)
        current_books = _listing_books(conn, snapshot_id)
        item_ids = sorted(set(previous_books) | set(current_books))
        ensure_items_catalog(conn, item_ids, current_ts)
        conn.execute("DELETE FROM inferred_flows WHERE snapshot_id = ?", (snapshot_id,))

        prev_summaries = {
            item_id: aggregate_listings(rows.values())
            for item_id, rows in previous_books.items()
        }
        current_summaries = {
            item_id: aggregate_listings(rows.values())
            for item_id, rows in current_books.items()
        }
        rows_to_insert = []
        gap_skipped = int(interval_sec < 60 or interval_sec > max_gap_sec)
        for item_id in item_ids:
            old_book = previous_books.get(item_id, {})
            new_book = current_books.get(item_id, {})
            new_rows = [
                row for auction_id, row in new_book.items() if auction_id not in old_book
            ]
            new_listing_count = len(new_rows)
            new_listed_qty = sum(int(row["quantity"]) for row in new_rows)

            partial_fill_qty = 0
            partial_fill_value = 0
            early_disappear_qty = 0
            expired_qty = 0
            disappeared_early: list[sqlite3.Row] = []
            disappearing = [
                row for auction_id, row in old_book.items() if auction_id not in new_book
            ]
            for old_row in old_book.values():
                current_row = new_book.get(int(old_row["auction_id"]))
                if current_row is None:
                    continue
                quantity_delta = int(old_row["quantity"]) - int(current_row["quantity"])
                if quantity_delta > 0:
                    partial_fill_qty += quantity_delta
                    partial_fill_value += quantity_delta * int(old_row["price"])

            expiry_cutoff = current_ts + grace_sec
            for old_row in disappearing:
                quantity = int(old_row["quantity"])
                if int(old_row["expires_at"]) <= expiry_cutoff:
                    expired_qty += quantity
                else:
                    early_disappear_qty += quantity
                    disappeared_early.append(old_row)

            prev_summary = prev_summaries.get(item_id)
            curr_summary = current_summaries.get(item_id)
            prev_vwap = float(prev_summary["vwap_top5_qty"]) if prev_summary else 0.0
            curr_ask = float(curr_summary["robust_best_ask"]) if curr_summary else 0.0
            clear_price = max(prev_vwap, curr_ask)
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
            # "Below previous" uses the previous frame's top-depth VWAP. Count
            # each new near-price order once; excess replacement supply can zero
            # the whole near estimate. Scale its notional by the same proportion.
            new_near_below_prev_qty = sum(
                int(row["quantity"]) for row in new_rows
                if int(row["price"]) <= near_limit and int(row["price"]) < prev_vwap
            )
            near_sold_qty = max(0.0, early_near_qty - 0.5 * new_near_below_prev_qty)
            near_sold_value = (
                early_near_value * (near_sold_qty / early_near_qty)
                if early_near_qty else 0.0
            )
            mid_qty = sum(int(row["quantity"]) for row in early_mid)
            sold_qty = partial_fill_qty + alpha_near * near_sold_qty + alpha_mid * mid_qty
            sold_value = (
                partial_fill_value
                + alpha_near
                * near_sold_value
                + alpha_mid
                * sum(int(row["quantity"]) * int(row["price"]) for row in early_mid)
            )
            if gap_skipped:
                sold_qty_value = 0.0
                sold_vwap = None
            else:
                sold_qty_value = float(sold_qty)
                sold_vwap = sold_value / sold_qty if sold_qty > 0 else None

            rows_to_insert.append(
                (
                    snapshot_id,
                    prev_id,
                    item_id,
                    current_ts,
                    interval_sec,
                    new_listing_count,
                    new_listed_qty,
                    partial_fill_qty,
                    early_disappear_qty,
                    expired_qty,
                    sold_qty_value,
                    sold_vwap,
                    gap_skipped,
                )
            )

        conn.executemany(
            """
            INSERT INTO inferred_flows (
                snapshot_id, prev_snapshot_id, item_id, captured_at, interval_sec,
                new_listing_count, new_listed_qty, partial_fill_qty,
                early_disappear_qty, expired_qty, est_sold_qty, est_sold_vwap,
                gap_skipped
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        return len(rows_to_insert)
