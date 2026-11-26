"""Robust snapshot book metrics and two-stage item-bar aggregation."""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from typing import Any

from pokemmo_gtl.db.connection import write_transaction

_BAR_COLUMNS = (
    "item_id",
    "timeframe",
    "bucket_ts",
    "close_ts",
    "data_source",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "robust_best_ask",
    "best_ask_min",
    "p10_price",
    "total_listed_qty",
    "listing_count",
    "depth_within_5pct",
    "spread_top5_pct",
    "est_sold_qty_sum",
    "expired_qty_sum",
    "new_listed_qty_sum",
    "sample_count",
    "valid_flow_samples",
)


def _value(row: Mapping[str, Any] | sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def _weighted_quantile(
    listings: list[dict[str, int]], total_qty: int, percentile: float
) -> int:
    target = max(1, math.ceil(total_qty * percentile))
    cumulative = 0
    for listing in listings:
        cumulative += listing["quantity"]
        if cumulative >= target:
            return listing["price"]
    return listings[-1]["price"]


def _weighted_vwap(listings: list[dict[str, int]], qty: int) -> float:
    remaining = qty
    notional = 0
    filled = 0
    for listing in listings:
        take = min(remaining, listing["quantity"])
        notional += listing["price"] * take
        filled += take
        remaining -= take
        if remaining == 0:
            break
    return notional / filled if filled else 0.0


def aggregate_listings(
    listings: Iterable[Mapping[str, Any] | sqlite3.Row],
) -> dict[str, int | float] | None:
    """Summarize a listing book, excluding only a tiny qualifying low-price prefix."""
    book = [
        {
            "auction_id": int(_value(row, "auction_id", 0)),
            "price": int(_value(row, "price", 0)),
            "quantity": int(_value(row, "quantity", 0)),
        }
        for row in listings
        if int(_value(row, "quantity", 0)) > 0
    ]
    if not book:
        return None

    book.sort(key=lambda row: (row["price"], row["auction_id"]))
    total_qty = sum(row["quantity"] for row in book)
    if total_qty <= 0:
        return None

    p10_price = _weighted_quantile(book, total_qty, 0.10)
    anchor = p10_price if total_qty >= 10 else _weighted_quantile(book, total_qty, 0.50)
    filtered = book
    if total_qty >= 3:
        small_order_limit = max(2, math.floor(0.05 * total_qty))
        first_kept = 0
        while first_kept < len(book):
            listing = book[first_kept]
            if not (
                listing["price"] < 0.50 * anchor
                and listing["quantity"] < small_order_limit
            ):
                break
            first_kept += 1
        filtered = book[first_kept:]

    trimmed_qty = sum(row["quantity"] for row in filtered)
    if trimmed_qty == 0:
        return None

    robust_best_ask = filtered[0]["price"]
    top_qty = min(trimmed_qty, 20, max(5, math.floor(0.05 * trimmed_qty)))
    vwap_top5_qty = _weighted_vwap(filtered, top_qty)
    depth_within_5pct = sum(
        row["quantity"]
        for row in filtered
        if row["price"] <= robust_best_ask * 1.05
    )
    return {
        "robust_best_ask": robust_best_ask,
        "best_ask_min": book[0]["price"],
        "vwap_top5_qty": vwap_top5_qty,
        "p10_price": float(p10_price),
        "total_listed_qty": total_qty,
        "listing_count": len(book),
        "depth_within_5pct": depth_within_5pct,
        "spread_top5_pct": (vwap_top5_qty - robust_best_ask) / max(robust_best_ask, 1),
    }


def ensure_items_catalog(
    conn: sqlite3.Connection, item_ids: Iterable[int], updated_at: int
) -> int:
    """Create placeholder catalog entries before downstream foreign-key writes."""
    known_ids = sorted({int(item_id) for item_id in item_ids})
    if not known_ids:
        return 0
    with _write_scope(conn):
        before = conn.total_changes
        conn.executemany(
            """
            INSERT OR IGNORE INTO items_catalog
                (item_id, name_en, category, max_stack_size, updated_at)
            VALUES (?, ?, 'unknown', 99, ?)
            """,
            [(item_id, f"Unknown Item #{item_id}", updated_at) for item_id in known_ids],
        )
        return conn.total_changes - before


@contextmanager
def _write_scope(conn: sqlite3.Connection):
    if conn.in_transaction:
        yield conn
    else:
        with write_transaction(conn):
            yield conn


def store_raw_listings(
    conn: sqlite3.Connection,
    snapshot_id: int,
    listings: Iterable[Mapping[str, Any] | sqlite3.Row],
) -> int:
    """Insert catalog placeholders before raw rows, atomically and with foreign keys on.

    Accepts either API ``reference_id`` or normalized ``item_id`` mappings. Call this
    inside the producer's phase-two write transaction, then call ``process_snapshot``.
    """
    with _write_scope(conn):
        snapshot = conn.execute(
            "SELECT server_started_at FROM snapshot_runs WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        if snapshot is None:
            raise ValueError(f"Unknown snapshot_id: {snapshot_id}")
        rows = [
            (
                snapshot_id,
                int(row["auction_id"]),
                int(_value(row, "item_id", _value(row, "reference_id"))),
                int(row["price"]),
                int(row["quantity"]),
                int(row["expires_at"]),
            )
            for row in listings
        ]
        ensure_items_catalog(conn, (row[2] for row in rows), int(snapshot["server_started_at"]))
        conn.executemany(
            """
            INSERT INTO raw_listings
                (snapshot_id, auction_id, item_id, price, quantity, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)


def _insert_bar(conn: sqlite3.Connection, values: Mapping[str, Any]) -> None:
    placeholders = ", ".join("?" for _ in _BAR_COLUMNS)
    columns = ", ".join(_BAR_COLUMNS)
    updates = ", ".join(
        f"{column} = excluded.{column}"
        for column in _BAR_COLUMNS
        if column not in {"item_id", "timeframe", "bucket_ts", "data_source"}
    )
    conn.execute(
        f"""
        INSERT INTO item_bars ({columns})
        VALUES ({placeholders})
        ON CONFLICT(item_id, timeframe, bucket_ts) DO UPDATE SET
            {updates},
            data_source = 'snapshot'
        WHERE item_bars.data_source != 'manual'
        """,
        tuple(values[column] for column in _BAR_COLUMNS),
    )


def _flow_values(
    conn: sqlite3.Connection, bucket_ts: int, item_id: int
) -> tuple[float, int, int, int]:
    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(est_sold_qty), 0.0) AS sold_qty,
            COALESCE(SUM(expired_qty), 0) AS expired_qty,
            COALESCE(SUM(new_listed_qty), 0) AS new_listed_qty,
            COUNT(*) AS valid_samples
        FROM inferred_flows AS f
        JOIN snapshot_runs AS s ON s.snapshot_id = f.snapshot_id
        WHERE f.item_id = ? AND f.gap_skipped = 0 AND s.status = 'completed'
          AND f.captured_at >= ? AND f.captured_at < ?
        """,
        (item_id, bucket_ts, bucket_ts + 900),
    ).fetchone()
    return (
        float(row["sold_qty"]),
        int(row["expired_qty"]),
        int(row["new_listed_qty"]),
        int(row["valid_samples"]),
    )


def _refresh_15m(conn: sqlite3.Connection, item_id: int, bucket_ts: int) -> None:
    snapshots = conn.execute(
        """
        SELECT DISTINCT sr.snapshot_id
        FROM snapshot_runs AS sr
        JOIN raw_listings AS rl ON rl.snapshot_id = sr.snapshot_id
        WHERE sr.status = 'completed' AND rl.item_id = ?
          AND sr.server_started_at >= ? AND sr.server_started_at < ?
        ORDER BY sr.server_started_at ASC, sr.snapshot_id ASC
        """,
        (item_id, bucket_ts, bucket_ts + 900),
    ).fetchall()
    points = []
    for snapshot in snapshots:
        snapshot_id = int(snapshot["snapshot_id"])
        listings = conn.execute(
            """
            SELECT auction_id, price, quantity
            FROM raw_listings
            WHERE snapshot_id = ? AND item_id = ?
            ORDER BY price ASC, auction_id ASC
            """,
            (snapshot_id, item_id),
        ).fetchall()
        metrics = aggregate_listings(listings)
        if metrics is not None:
            points.append(metrics)
    if not points:
        return

    last_metrics = points[-1]
    sold, expired, new_qty, valid_samples = _flow_values(conn, bucket_ts, item_id)
    vwap_values = [float(point["vwap_top5_qty"]) for point in points]

    _insert_bar(
        conn,
        {
            "item_id": item_id,
            "timeframe": "15m",
            "bucket_ts": bucket_ts,
            "close_ts": bucket_ts + 900,
            "data_source": "snapshot",
            "open_price": vwap_values[0],
            "high_price": max(vwap_values),
            "low_price": min(vwap_values),
            "close_price": vwap_values[-1],
            "robust_best_ask": int(last_metrics["robust_best_ask"]),
            "best_ask_min": min(int(point["best_ask_min"]) for point in points),
            "p10_price": float(last_metrics["p10_price"]),
            "total_listed_qty": int(last_metrics["total_listed_qty"]),
            "listing_count": int(last_metrics["listing_count"]),
            "depth_within_5pct": int(last_metrics["depth_within_5pct"]),
            "spread_top5_pct": float(last_metrics["spread_top5_pct"]),
            "est_sold_qty_sum": sold,
            "expired_qty_sum": expired,
            "new_listed_qty_sum": new_qty,
            "sample_count": len(points),
            "valid_flow_samples": valid_samples,
        },
    )


def _aggregate_parent(
    conn: sqlite3.Connection, item_id: int, bucket_ts: int, timeframe: str, seconds: int
) -> None:
    rows = conn.execute(
        """
        SELECT * FROM item_bars
        WHERE item_id = ? AND timeframe = '15m' AND data_source = 'snapshot'
          AND bucket_ts >= ? AND bucket_ts < ?
        ORDER BY bucket_ts ASC
        """,
        (item_id, bucket_ts, bucket_ts + seconds),
    ).fetchall()
    if not rows:
        return
    first = rows[0]
    last = rows[-1]
    _insert_bar(
        conn,
        {
            "item_id": item_id,
            "timeframe": timeframe,
            "bucket_ts": bucket_ts,
            "close_ts": bucket_ts + seconds,
            "data_source": "snapshot",
            "open_price": float(first["open_price"]),
            "high_price": max(float(row["high_price"]) for row in rows),
            "low_price": min(float(row["low_price"]) for row in rows),
            "close_price": float(last["close_price"]),
            "robust_best_ask": int(last["robust_best_ask"]),
            "best_ask_min": int(last["best_ask_min"]),
            "p10_price": last["p10_price"],
            "total_listed_qty": last["total_listed_qty"],
            "listing_count": last["listing_count"],
            "depth_within_5pct": last["depth_within_5pct"],
            "spread_top5_pct": last["spread_top5_pct"],
            "est_sold_qty_sum": sum(float(row["est_sold_qty_sum"] or 0.0) for row in rows),
            "expired_qty_sum": sum(int(row["expired_qty_sum"] or 0) for row in rows),
            "new_listed_qty_sum": sum(int(row["new_listed_qty_sum"] or 0) for row in rows),
            "sample_count": sum(int(row["sample_count"]) for row in rows),
            "valid_flow_samples": sum(int(row["valid_flow_samples"]) for row in rows),
        },
    )


@contextmanager
def _completed_snapshot(conn: sqlite3.Connection, snapshot_id: int):
    snapshot = conn.execute(
        """
        SELECT snapshot_id, server_started_at, completed_at, status
        FROM snapshot_runs WHERE snapshot_id = ?
        """,
        (snapshot_id,),
    ).fetchone()
    if snapshot is None:
        raise ValueError(f"Unknown snapshot_id: {snapshot_id}")
    if snapshot["status"] not in {"running", "completed"}:
        raise ValueError("process_snapshot requires a running or completed snapshot")
    is_running = snapshot["status"] == "running"
    if is_running:
        conn.execute(
            """
            UPDATE snapshot_runs SET status = 'completed',
                completed_at = COALESCE(completed_at, server_started_at)
            WHERE snapshot_id = ?
            """,
            (snapshot_id,),
        )
    try:
        yield snapshot
    finally:
        if is_running:
            conn.execute(
                "UPDATE snapshot_runs SET status = 'running', completed_at = ? "
                "WHERE snapshot_id = ?",
                (snapshot["completed_at"], snapshot_id),
            )


def process_snapshot(conn: sqlite3.Connection, snapshot_id: int, settings: Any) -> None:
    """Diff and materialize 15m -> 4h/1d inside the producer's atomic transaction.

    Already completed snapshots can also be processed independently. Running snapshots
    keep their original status on return so the producer owns completion and its clock.
    Run retention separately, after the producer commits its phase-two transaction.
    """
    if not conn.in_transaction:
        run = conn.execute(
            "SELECT status FROM snapshot_runs WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        if run is not None and run["status"] == "running":
            raise RuntimeError("A running snapshot requires its producer's write transaction")
    with _write_scope(conn), _completed_snapshot(conn, snapshot_id) as snapshot:
        listing_rows = conn.execute(
            """
            SELECT auction_id, item_id, price, quantity, expires_at
            FROM raw_listings
            WHERE snapshot_id = ?
            ORDER BY item_id, price, auction_id
            """,
            (snapshot_id,),
        ).fetchall()
        item_ids = sorted({int(row["item_id"]) for row in listing_rows})
        ensure_items_catalog(conn, item_ids, int(snapshot["server_started_at"]))

        from pokemmo_gtl.pipeline.snapshot_diff import process_snapshot_diff

        process_snapshot_diff(conn, snapshot_id, settings)
        grouped: dict[int, list[sqlite3.Row]] = defaultdict(list)
        for row in listing_rows:
            grouped[int(row["item_id"])].append(row)

        captured_at = int(snapshot["server_started_at"])
        bucket_ts = captured_at - captured_at % 900
        affected_items = set(grouped)
        affected_items.update(
            int(row[0]) for row in conn.execute(
                "SELECT item_id FROM inferred_flows WHERE snapshot_id = ?", (snapshot_id,)
            )
        )
        for item_id in sorted(affected_items):
            _refresh_15m(conn, item_id, bucket_ts)
            four_hour_bucket = captured_at - captured_at % 14_400
            daily_bucket = captured_at - captured_at % 86_400
            _aggregate_parent(conn, item_id, four_hour_bucket, "4h", 14_400)
            _aggregate_parent(conn, item_id, daily_bucket, "1d", 86_400)
