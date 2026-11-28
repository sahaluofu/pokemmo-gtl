"""Robust book metrics, retained evidence, and bitemporal item-bar aggregation."""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

from pokemmo_gtl.db.connection import write_transaction
from pokemmo_gtl.pipeline.retention import _columns, check_frames_retained
from pokemmo_gtl.quant.entry_calc import build_orderbook_observation

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

_REVISION_COLUMNS = (
    "item_id",
    "timeframe",
    "bucket_ts",
    "publish_seq",
    "close_ts",
    "revision_available_at",
    "data_source",
    "book_state",
    *_BAR_COLUMNS[5:],
)


def _value(row: Mapping[str, Any] | sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


@contextmanager
def _write_scope(conn: sqlite3.Connection):
    if conn.in_transaction:
        yield conn
    else:
        with write_transaction(conn):
            yield conn


def _snapshot_projection(conn: sqlite3.Connection) -> str:
    columns = _columns(conn, "snapshot_runs")
    projection = [
        "snapshot_id",
        "server_started_at",
        "completed_at",
        "status",
    ]
    for name in ("available_at", "publish_seq"):
        projection.append(name if name in columns else f"NULL AS {name}")
    return ", ".join(projection)


def _get_snapshot(
    conn: sqlite3.Connection, snapshot_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        f"""
        SELECT {_snapshot_projection(conn)}
        FROM snapshot_runs WHERE snapshot_id = ?
        """,
        (snapshot_id,),
    ).fetchone()


def _nominal_neighbor(
    conn: sqlite3.Connection,
    snapshot: Mapping[str, Any] | sqlite3.Row,
    *,
    successor: bool,
) -> sqlite3.Row | None:
    comparison = ">" if successor else "<"
    direction = "ASC" if successor else "DESC"
    timestamp = int(snapshot["server_started_at"])
    return conn.execute(
        f"""
        SELECT {_snapshot_projection(conn)}
        FROM snapshot_runs
        WHERE status = 'completed'
          AND (
              server_started_at {comparison} ?
              OR (server_started_at = ? AND snapshot_id {comparison} ?)
          )
        ORDER BY server_started_at {direction}, snapshot_id {direction}
        LIMIT 1
        """,
        (timestamp, timestamp, int(snapshot["snapshot_id"])),
    ).fetchone()


def _publication(
    conn: sqlite3.Connection, snapshot: Mapping[str, Any] | sqlite3.Row
) -> tuple[int | None, int]:
    available_at = next(
        int(value)
        for value in (
            snapshot["available_at"],
            snapshot["completed_at"],
            snapshot["server_started_at"],
        )
        if value is not None
    )
    if not _columns(conn, "publish_events"):
        return None, available_at

    sequence = snapshot["publish_seq"]
    if sequence is None:
        cursor = conn.execute(
            """
            INSERT INTO publish_events (available_at, source_type, source_id)
            VALUES (?, 'snapshot', ?)
            """,
            (available_at, int(snapshot["snapshot_id"])),
        )
        sequence = int(cursor.lastrowid)
    conn.execute(
        """
        UPDATE snapshot_runs
        SET publish_seq = ?, available_at = ?
        WHERE snapshot_id = ?
        """,
        (int(sequence), available_at, int(snapshot["snapshot_id"])),
    )
    return int(sequence), available_at


def _publication_has_outputs(
    conn: sqlite3.Connection, snapshot_id: int, publish_seq: int
) -> bool:
    for table in ("item_bar_revisions", "inferred_flow_revisions"):
        if _columns(conn, table):
            if conn.execute(
                f"SELECT 1 FROM {table} WHERE publish_seq = ? LIMIT 1",
                (publish_seq,),
            ).fetchone():
                return True
    if _columns(conn, "orderbook_observations"):
        return (
            conn.execute(
                """
                SELECT 1 FROM orderbook_observations
                WHERE snapshot_id = ? AND publish_seq = ? LIMIT 1
                """,
                (snapshot_id, publish_seq),
            ).fetchone()
            is not None
        )
    return False


def _bucket_snapshots(
    conn: sqlite3.Connection,
    bucket_ts: int,
    seconds: int,
    publish_seq: int | None = None,
) -> list[sqlite3.Row]:
    visibility = ""
    parameters: list[Any] = [bucket_ts, bucket_ts + seconds]
    if publish_seq is not None and "publish_seq" in _columns(conn, "snapshot_runs"):
        # Untimestamped v1 inputs can be completed before their processing hook.
        # Explicitly timed v2 inputs enter the frame set only after publication.
        visibility = """
          AND (
              publish_seq <= ?
              OR (publish_seq IS NULL AND available_at IS NULL)
          )
        """
        parameters.append(publish_seq)
    return conn.execute(
        f"""
        SELECT {_snapshot_projection(conn)}
        FROM snapshot_runs
        WHERE status = 'completed'
          AND server_started_at >= ? AND server_started_at < ?
          {visibility}
        ORDER BY server_started_at ASC, snapshot_id ASC
        """,
        parameters,
    ).fetchall()


def _bucket_replayable(
    conn: sqlite3.Connection, bucket_ts: int, seconds: int
) -> bool:
    # Deliberately inspect every completed frame, without joining raw_listings
    # and without excluding unpublished or empty frames from the retention test.
    frames = _bucket_snapshots(conn, bucket_ts, seconds)
    if not frames:
        return False
    if "raw_retained" not in _columns(conn, "snapshot_runs"):
        return True
    return check_frames_retained(
        conn, [int(frame["snapshot_id"]) for frame in frames]
    )


def _api_covered(conn: sqlite3.Connection, item_id: int) -> bool:
    columns = _columns(conn, "items_catalog")
    coverage = "api_covered" if "api_covered" in columns else "1 AS api_covered"
    row = conn.execute(
        f"""
        SELECT asset_class, {coverage}
        FROM items_catalog WHERE item_id = ?
        """,
        (item_id,),
    ).fetchone()
    return bool(
        row is not None
        and int(row["api_covered"]) == 1
        and item_id < 900_000
        and row["asset_class"] != "pokemon_category"
    )


def _active_covered_items(conn: sqlite3.Connection) -> set[int]:
    catalog_columns = _columns(conn, "items_catalog")
    coverage = "api_covered = 1" if "api_covered" in catalog_columns else "1 = 1"
    positions = ""
    if _columns(conn, "user_positions"):
        positions = """
            OR item_id IN (
                SELECT item_id FROM user_positions WHERE status = 'OPEN'
            )
        """
    rows = conn.execute(
        f"""
        SELECT item_id FROM items_catalog
        WHERE {coverage} AND (is_watched = 1 {positions})
        """
    ).fetchall()
    return {
        int(row["item_id"])
        for row in rows
        if _api_covered(conn, int(row["item_id"]))
    }


def _affected_items(conn: sqlite3.Connection, snapshot_id: int) -> set[int]:
    items = {
        int(row[0])
        for row in conn.execute(
            "SELECT DISTINCT item_id FROM raw_listings WHERE snapshot_id = ?",
            (snapshot_id,),
        )
    }
    items.update(
        int(row[0])
        for row in conn.execute(
            "SELECT item_id FROM inferred_flows WHERE snapshot_id = ?",
            (snapshot_id,),
        )
    )
    items.update(_active_covered_items(conn))
    return items


def _legacy_projection(
    conn: sqlite3.Connection, item_id: int, bucket_ts: int, seconds: int
) -> bool:
    """Keep v1 latest-view conventions for unmonitored nominal-only inputs.

    Immutable revisions always use the v2 frame count and historical minimum.
    The compatibility projection only affects item_bars: old untimed callers
    counted stocked samples and used the last child's minimum for parent bars.
    Real acquisition latency, a watch, or an open position selects v2 throughout.
    """
    item = conn.execute(
        "SELECT is_watched FROM items_catalog WHERE item_id = ?", (item_id,)
    ).fetchone()
    if item is None or int(item["is_watched"]) == 1:
        return False
    if _columns(conn, "user_positions"):
        if conn.execute(
            """
            SELECT 1 FROM user_positions
            WHERE item_id = ? AND status = 'OPEN' LIMIT 1
            """,
            (item_id,),
        ).fetchone():
            return False
    frames = _bucket_snapshots(conn, bucket_ts, seconds)
    return bool(frames) and all(
        frame["completed_at"] == frame["server_started_at"]
        and (
            frame["available_at"] is None
            or frame["available_at"] == frame["server_started_at"]
        )
        for frame in frames
    )


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
    """Create placeholders before downstream foreign-key writes."""
    known_ids = sorted({int(item_id) for item_id in item_ids})
    if not known_ids:
        return 0
    with _write_scope(conn):
        before = conn.total_changes
        if "api_covered" in _columns(conn, "items_catalog"):
            conn.executemany(
                """
                INSERT OR IGNORE INTO items_catalog
                    (item_id, name_en, category, max_stack_size, updated_at,
                     api_covered)
                VALUES (?, ?, 'unknown', 99, ?, ?)
                """,
                [
                    (
                        item_id,
                        f"Unknown Item #{item_id}",
                        updated_at,
                        int(item_id < 900_000),
                    )
                    for item_id in known_ids
                ],
            )
        else:
            conn.executemany(
                """
                INSERT OR IGNORE INTO items_catalog
                    (item_id, name_en, category, max_stack_size, updated_at)
                VALUES (?, ?, 'unknown', 99, ?)
                """,
                [
                    (item_id, f"Unknown Item #{item_id}", updated_at)
                    for item_id in known_ids
                ],
            )
        return conn.total_changes - before


def store_raw_listings(
    conn: sqlite3.Connection,
    snapshot_id: int,
    listings: Iterable[Mapping[str, Any] | sqlite3.Row],
) -> int:
    """Insert placeholders and raw rows inside the producer's transaction."""
    with _write_scope(conn):
        snapshot = _get_snapshot(conn, snapshot_id)
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
        ensure_items_catalog(
            conn, (row[2] for row in rows), int(snapshot["server_started_at"])
        )
        conn.executemany(
            """
            INSERT INTO raw_listings
                (snapshot_id, auction_id, item_id, price, quantity, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return len(rows)


def _record_observations(conn: sqlite3.Connection, snapshot_id: int) -> None:
    """Retain every auction lot before raw rows can be pruned.

    Retention also calls this for completed frames that never reached the normal
    processing hook. That fallback does not allocate a publication or alter bars.
    """
    if not _columns(conn, "orderbook_observations"):
        return
    snapshot = _get_snapshot(conn, snapshot_id)
    if snapshot is None or snapshot["status"] != "completed":
        return

    current_ts = int(snapshot["server_started_at"])
    available_at = next(
        int(value)
        for value in (
            snapshot["available_at"],
            snapshot["completed_at"],
            current_ts,
        )
        if value is not None
    )

    def books(frame_id: int) -> dict[int, list[dict[str, int]]]:
        grouped: dict[int, list[dict[str, int]]] = defaultdict(list)
        for row in conn.execute(
            """
            SELECT auction_id, item_id, price, quantity, expires_at
            FROM raw_listings
            WHERE snapshot_id = ? AND quantity > 0
            ORDER BY item_id, price, auction_id
            """,
            (frame_id,),
        ):
            grouped[int(row["item_id"])].append(
                {
                    "auction_id": int(row["auction_id"]),
                    "price": int(row["price"]),
                    "qty": int(row["quantity"]),
                    "expires_at": int(row["expires_at"]),
                }
            )
        return grouped

    current_books = books(snapshot_id)
    previous = _nominal_neighbor(conn, snapshot, successor=False)
    previous_books: dict[int, list[dict[str, int]]] = {}
    pair_retained = previous is not None and (
        "raw_retained" not in _columns(conn, "snapshot_runs")
        or check_frames_retained(
            conn, [int(previous["snapshot_id"]), snapshot_id]
        )
    )
    if pair_retained:
        previous_books = books(int(previous["snapshot_id"]))

    catalog_columns = _columns(conn, "items_catalog")
    mode_projection = (
        "purchase_mode"
        if "purchase_mode" in catalog_columns
        else "'unknown' AS purchase_mode"
    )
    for item_id in sorted(_affected_items(conn, snapshot_id)):
        current_lots = current_books.get(item_id, [])
        if not _api_covered(conn, item_id) and not current_lots:
            continue
        if conn.execute(
            """
            SELECT 1 FROM orderbook_observations
            WHERE snapshot_id = ? AND item_id = ?
            """,
            (snapshot_id, item_id),
        ).fetchone():
            continue

        item = conn.execute(
            f"SELECT {mode_projection} FROM items_catalog WHERE item_id = ?",
            (item_id,),
        ).fetchone()
        stored_mode = str(item["purchase_mode"])
        # The descriptive ladder is identical for either purchase mode.
        # Unknown catalog modes remain explicitly unknown in persisted evidence.
        calculation_mode = (
            "bundle_only" if stored_mode == "bundle_only" else "per_unit"
        )
        old_lots = previous_books.get(item_id, [])
        previous_observation = None
        if pair_retained:
            previous_observation = conn.execute(
                """
                SELECT wall_price, wall_persist_frames
                FROM orderbook_observations
                WHERE snapshot_id = ? AND item_id = ?
                """,
                (int(previous["snapshot_id"]), item_id),
            ).fetchone()
        coverage_hours = (
            max(0, current_ts - int(previous["server_started_at"])) / 3600
            if pair_retained
            else 0.0
        )
        observation = build_orderbook_observation(
            current_lots,
            old_lots,
            current_ts=current_ts,
            purchase_mode=calculation_mode,
            coverage_hours=coverage_hours,
            reference_price=(
                min(lot["price"] for lot in old_lots) if old_lots else None
            ),
            prev_wall_price=(
                previous_observation["wall_price"]
                if previous_observation is not None
                else None
            ),
            prev_wall_persist_frames=(
                int(previous_observation["wall_persist_frames"])
                if previous_observation is not None
                else 0
            ),
            max_ladder_lots=max(1, len(current_lots)),
        )
        conn.execute(
            """
            INSERT INTO orderbook_observations (
                snapshot_id, item_id, available_at, publish_seq, book_state,
                purchase_mode, coverage_hours, same_id_qty_drop,
                full_disappear_qty, visible_lower_qty, wall_price, wall_qty,
                wall_persist_frames, ladder_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_id, item_id) DO NOTHING
            """,
            (
                snapshot_id,
                item_id,
                available_at,
                snapshot["publish_seq"],
                observation.book_state,
                stored_mode,
                observation.coverage_hours,
                observation.same_id_qty_drop,
                observation.full_disappear_qty,
                observation.visible_lower_qty,
                observation.wall_price,
                observation.wall_qty,
                observation.wall_persist_frames,
                observation.ladder_json,
            ),
        )


def _insert_bar(
    conn: sqlite3.Connection,
    values: Mapping[str, Any],
    *,
    legacy_overrides: Mapping[str, Any] | None = None,
) -> None:
    existing = conn.execute(
        """
        SELECT data_source FROM item_bars
        WHERE item_id = ? AND timeframe = ? AND bucket_ts = ?
        """,
        (values["item_id"], values["timeframe"], values["bucket_ts"]),
    ).fetchone()
    if existing is not None and existing["data_source"] == "manual":
        return

    canonical = dict(values)
    publish_seq = canonical.get("latest_publish_seq")
    if publish_seq is not None and _columns(conn, "item_bar_revisions"):
        canonical["publish_seq"] = publish_seq
        canonical["revision_available_at"] = canonical["bar_available_at"]
        names = ", ".join(_REVISION_COLUMNS)
        marks = ", ".join("?" for _ in _REVISION_COLUMNS)
        conn.execute(
            f"""
            INSERT INTO item_bar_revisions ({names})
            VALUES ({marks})
            ON CONFLICT(item_id, timeframe, bucket_ts, publish_seq) DO NOTHING
            """,
            tuple(canonical[column] for column in _REVISION_COLUMNS),
        )
        stored = conn.execute(
            f"""
            SELECT {names} FROM item_bar_revisions
            WHERE item_id = ? AND timeframe = ? AND bucket_ts = ?
              AND publish_seq = ?
            """,
            (
                canonical["item_id"],
                canonical["timeframe"],
                canonical["bucket_ts"],
                publish_seq,
            ),
        ).fetchone()
        canonical.update(dict(stored))
        canonical["bar_available_at"] = stored["revision_available_at"]
        canonical["latest_publish_seq"] = stored["publish_seq"]

    projected = dict(canonical)
    if legacy_overrides:
        projected.update(legacy_overrides)
    available_columns = _columns(conn, "item_bars")
    columns = list(_BAR_COLUMNS)
    columns.extend(
        column
        for column in ("book_state", "bar_available_at", "latest_publish_seq")
        if column in available_columns
    )
    names = ", ".join(columns)
    marks = ", ".join("?" for _ in columns)
    updates = ", ".join(
        f"{column} = excluded.{column}"
        for column in columns
        if column not in {"item_id", "timeframe", "bucket_ts", "data_source"}
    )
    sequence_guard = ""
    if "latest_publish_seq" in available_columns and publish_seq is not None:
        sequence_guard = """
          AND (
              item_bars.latest_publish_seq IS NULL
              OR item_bars.latest_publish_seq <= excluded.latest_publish_seq
          )
        """
    conn.execute(
        f"""
        INSERT INTO item_bars ({names}) VALUES ({marks})
        ON CONFLICT(item_id, timeframe, bucket_ts) DO UPDATE SET
            {updates}, data_source = 'snapshot'
        WHERE item_bars.data_source != 'manual'
        {sequence_guard}
        """,
        tuple(projected[column] for column in columns),
    )


def _flow_values(
    conn: sqlite3.Connection,
    bucket_ts: int,
    item_id: int,
    publish_seq: int | None = None,
) -> tuple[float, int, int, int]:
    if publish_seq is not None and _columns(conn, "inferred_flow_revisions"):
        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(f.est_sold_qty), 0.0) AS sold_qty,
                COALESCE(SUM(f.expired_qty), 0) AS expired_qty,
                COALESCE(SUM(f.new_listed_qty), 0) AS new_listed_qty,
                COUNT(*) AS valid_samples
            FROM inferred_flow_revisions AS f
            JOIN snapshot_runs AS s ON s.snapshot_id = f.snapshot_id
            WHERE f.item_id = ? AND f.captured_at >= ? AND f.captured_at < ?
              AND f.publish_seq <= ? AND f.gap_skipped = 0
              AND s.status = 'completed'
              AND (
                  s.publish_seq <= ?
                  OR (s.publish_seq IS NULL AND s.available_at IS NULL)
              )
              AND NOT EXISTS (
                  SELECT 1 FROM inferred_flow_revisions AS newer
                  WHERE newer.snapshot_id = f.snapshot_id
                    AND newer.item_id = f.item_id
                    AND newer.publish_seq > f.publish_seq
                    AND newer.publish_seq <= ?
              )
            """,
            (
                item_id,
                bucket_ts,
                bucket_ts + 900,
                publish_seq,
                publish_seq,
                publish_seq,
            ),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(f.est_sold_qty), 0.0) AS sold_qty,
                COALESCE(SUM(f.expired_qty), 0) AS expired_qty,
                COALESCE(SUM(f.new_listed_qty), 0) AS new_listed_qty,
                COUNT(*) AS valid_samples
            FROM inferred_flows AS f
            JOIN snapshot_runs AS s ON s.snapshot_id = f.snapshot_id
            WHERE f.item_id = ? AND f.gap_skipped = 0
              AND s.status = 'completed'
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


def _refresh_15m(
    conn: sqlite3.Connection,
    item_id: int,
    bucket_ts: int,
    *,
    publish_seq: int | None = None,
    available_at: int | None = None,
) -> None:
    if not _bucket_replayable(conn, bucket_ts, 900):
        return
    snapshots = _bucket_snapshots(conn, bucket_ts, 900, publish_seq)
    if not snapshots:
        return

    covered = _api_covered(conn, item_id)
    frame_metrics: list[dict[str, int | float] | None] = []
    for snapshot in snapshots:
        listings = conn.execute(
            """
            SELECT auction_id, price, quantity
            FROM raw_listings
            WHERE snapshot_id = ? AND item_id = ?
            ORDER BY price ASC, auction_id ASC
            """,
            (int(snapshot["snapshot_id"]), item_id),
        ).fetchall()
        metrics = aggregate_listings(listings)
        # Absence is not collection coverage for local Pokémon categories.
        if metrics is not None or covered:
            frame_metrics.append(metrics)
    if not frame_metrics:
        return
    points = [metrics for metrics in frame_metrics if metrics is not None]

    v2_schema = "book_state" in _columns(conn, "item_bars")
    if not points and not v2_schema:
        return
    last_metrics = frame_metrics[-1]
    if not v2_schema:
        # The 001 schema cannot represent an empty current quote.
        last_metrics = points[-1]
    state = (
        "stocked"
        if last_metrics is not None
        else "mixed_ended_empty" if points else "empty_confirmed"
    )
    vwap_values = [float(point["vwap_top5_qty"]) for point in points]
    sold, expired, new_qty, valid_samples = _flow_values(
        conn, bucket_ts, item_id, publish_seq
    )
    if available_at is None:
        available_at = max(
            int(
                frame["available_at"]
                if frame["available_at"] is not None
                else frame["completed_at"]
                if frame["completed_at"] is not None
                else frame["server_started_at"]
            )
            for frame in snapshots
        )

    values = {
        "item_id": item_id,
        "timeframe": "15m",
        "bucket_ts": bucket_ts,
        "close_ts": bucket_ts + 900,
        "data_source": "snapshot",
        "book_state": state,
        "bar_available_at": available_at,
        "latest_publish_seq": publish_seq,
        "open_price": vwap_values[0] if vwap_values else None,
        "high_price": max(vwap_values) if vwap_values else None,
        "low_price": min(vwap_values) if vwap_values else None,
        "close_price": vwap_values[-1] if vwap_values else None,
        "best_ask_min": (
            min(int(point["best_ask_min"]) for point in points) if points else None
        ),
        "robust_best_ask": (
            int(last_metrics["robust_best_ask"]) if last_metrics else None
        ),
        "p10_price": float(last_metrics["p10_price"]) if last_metrics else None,
        "total_listed_qty": (
            int(last_metrics["total_listed_qty"]) if last_metrics else 0
        ),
        "listing_count": int(last_metrics["listing_count"]) if last_metrics else 0,
        "depth_within_5pct": (
            int(last_metrics["depth_within_5pct"]) if last_metrics else 0
        ),
        "spread_top5_pct": (
            float(last_metrics["spread_top5_pct"]) if last_metrics else None
        ),
        "est_sold_qty_sum": sold,
        "expired_qty_sum": expired,
        "new_listed_qty_sum": new_qty,
        "sample_count": len(frame_metrics),
        "valid_flow_samples": valid_samples,
    }
    overrides = None
    if points and _legacy_projection(conn, item_id, bucket_ts, 900):
        overrides = {"sample_count": len(points)}
    _insert_bar(conn, values, legacy_overrides=overrides)


def _child_bars(
    conn: sqlite3.Connection,
    item_id: int,
    bucket_ts: int,
    seconds: int,
    publish_seq: int | None,
) -> list[sqlite3.Row]:
    if publish_seq is not None and _columns(conn, "item_bar_revisions"):
        projection = ", ".join(f"r.{column}" for column in _REVISION_COLUMNS)
        return conn.execute(
            f"""
            SELECT {projection}
            FROM item_bar_revisions AS r
            WHERE r.item_id = ? AND r.timeframe = '15m'
              AND r.data_source = 'snapshot'
              AND r.bucket_ts >= ? AND r.bucket_ts < ?
              AND r.publish_seq <= ?
              AND NOT EXISTS (
                  SELECT 1 FROM item_bar_revisions AS newer
                  WHERE newer.item_id = r.item_id
                    AND newer.timeframe = r.timeframe
                    AND newer.bucket_ts = r.bucket_ts
                    AND newer.publish_seq > r.publish_seq
                    AND newer.publish_seq <= ?
              )
            ORDER BY r.bucket_ts ASC
            """,
            (item_id, bucket_ts, bucket_ts + seconds, publish_seq, publish_seq),
        ).fetchall()
    columns = list(_BAR_COLUMNS)
    columns.append(
        "book_state"
        if "book_state" in _columns(conn, "item_bars")
        else "'stocked' AS book_state"
    )
    return conn.execute(
        f"""
        SELECT {", ".join(columns)} FROM item_bars
        WHERE item_id = ? AND timeframe = '15m' AND data_source = 'snapshot'
          AND bucket_ts >= ? AND bucket_ts < ?
        ORDER BY bucket_ts ASC
        """,
        (item_id, bucket_ts, bucket_ts + seconds),
    ).fetchall()


def _aggregate_parent(
    conn: sqlite3.Connection,
    item_id: int,
    bucket_ts: int,
    timeframe: str,
    seconds: int,
    *,
    publish_seq: int | None = None,
    available_at: int | None = None,
) -> None:
    # A retained child cannot make an unsafe parent window replayable.
    # In particular, preserve both parent tables when an older sibling was pruned.
    if not _bucket_replayable(conn, bucket_ts, seconds):
        return
    rows = _child_bars(conn, item_id, bucket_ts, seconds, publish_seq)
    if not rows:
        return

    priced = [row for row in rows if row["open_price"] is not None]
    last = rows[-1]
    stocked = last["book_state"] == "stocked"
    state = (
        "stocked"
        if stocked
        else "mixed_ended_empty" if priced else "empty_confirmed"
    )
    minimum_asks = [
        int(row["best_ask_min"])
        for row in rows
        if row["best_ask_min"] is not None
    ]
    if available_at is None:
        available_at = max(
            int(_value(row, "revision_available_at", row["close_ts"]))
            for row in rows
        )

    values = {
        "item_id": item_id,
        "timeframe": timeframe,
        "bucket_ts": bucket_ts,
        "close_ts": bucket_ts + seconds,
        "data_source": "snapshot",
        "book_state": state,
        "bar_available_at": available_at,
        "latest_publish_seq": publish_seq,
        "open_price": float(priced[0]["open_price"]) if priced else None,
        "high_price": (
            max(float(row["high_price"]) for row in priced) if priced else None
        ),
        "low_price": (
            min(float(row["low_price"]) for row in priced) if priced else None
        ),
        "close_price": float(priced[-1]["close_price"]) if priced else None,
        "best_ask_min": min(minimum_asks) if minimum_asks else None,
        "robust_best_ask": last["robust_best_ask"] if stocked else None,
        "p10_price": last["p10_price"] if stocked else None,
        "total_listed_qty": int(last["total_listed_qty"] or 0) if stocked else 0,
        "listing_count": int(last["listing_count"] or 0) if stocked else 0,
        "depth_within_5pct": (
            int(last["depth_within_5pct"] or 0) if stocked else 0
        ),
        "spread_top5_pct": last["spread_top5_pct"] if stocked else None,
        "est_sold_qty_sum": sum(
            float(row["est_sold_qty_sum"] or 0.0) for row in rows
        ),
        "expired_qty_sum": sum(int(row["expired_qty_sum"] or 0) for row in rows),
        "new_listed_qty_sum": sum(
            int(row["new_listed_qty_sum"] or 0) for row in rows
        ),
        "sample_count": sum(int(row["sample_count"]) for row in rows),
        "valid_flow_samples": sum(int(row["valid_flow_samples"]) for row in rows),
    }
    overrides = None
    if _legacy_projection(conn, item_id, bucket_ts, seconds):
        sample_row = conn.execute(
            """
            SELECT COALESCE(SUM(sample_count), 0)
            FROM item_bars
            WHERE item_id = ? AND timeframe = '15m'
              AND data_source = 'snapshot'
              AND bucket_ts >= ? AND bucket_ts < ?
            """,
            (item_id, bucket_ts, bucket_ts + seconds),
        ).fetchone()
        overrides = {"sample_count": int(sample_row[0])}
        if stocked:
            overrides["best_ask_min"] = last["best_ask_min"]
    _insert_bar(conn, values, legacy_overrides=overrides)


def _cascade_snapshot_bars(
    conn: sqlite3.Connection,
    snapshot_ids: Sequence[int],
    *,
    publish_seq: int | None,
    available_at: int,
) -> None:
    buckets: dict[int, set[int]] = defaultdict(set)
    for snapshot_id in set(snapshot_ids):
        snapshot = _get_snapshot(conn, snapshot_id)
        if snapshot is None or snapshot["status"] != "completed":
            continue
        timestamp = int(snapshot["server_started_at"])
        bucket_ts = timestamp - timestamp % 900
        if not _bucket_replayable(conn, bucket_ts, 900):
            continue
        items = _affected_items(conn, snapshot_id)
        frames = _bucket_snapshots(conn, bucket_ts, 900, publish_seq)
        for frame in frames:
            items.update(
                int(row[0])
                for row in conn.execute(
                    """
                    SELECT DISTINCT item_id FROM raw_listings
                    WHERE snapshot_id = ?
                    """,
                    (int(frame["snapshot_id"]),),
                )
            )
        items.update(
            int(row[0])
            for row in conn.execute(
                """
                SELECT item_id FROM item_bars
                WHERE timeframe = '15m' AND bucket_ts = ?
                  AND data_source = 'snapshot'
                """,
                (bucket_ts,),
            )
        )
        buckets[bucket_ts].update(items)

    parents: set[tuple[int, int, str, int]] = set()
    # Finish every affected child before publishing any shared parent under P.
    for bucket_ts, items in sorted(buckets.items()):
        for item_id in sorted(items):
            _refresh_15m(
                conn,
                item_id,
                bucket_ts,
                publish_seq=publish_seq,
                available_at=available_at,
            )
            for timeframe, seconds in (("4h", 14_400), ("1d", 86_400)):
                parent_ts = bucket_ts - bucket_ts % seconds
                parents.add((item_id, parent_ts, timeframe, seconds))
    for item_id, parent_ts, timeframe, seconds in sorted(parents):
        _aggregate_parent(
            conn,
            item_id,
            parent_ts,
            timeframe,
            seconds,
            publish_seq=publish_seq,
            available_at=available_at,
        )


def query_bars_as_of(
    conn: sqlite3.Connection,
    *,
    timeframe: str,
    as_of_ts: int,
    max_publish_seq: int | None = None,
    item_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return the latest visible revision of each closed bar.

    A 001-only database has no trustworthy publication history and therefore
    returns no point-in-time revisions rather than inventing availability.
    """
    if timeframe not in {"15m", "4h", "1d"}:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    if not _columns(conn, "item_bar_revisions"):
        return []

    predicates = [
        "timeframe = ?",
        "close_ts <= ?",
        "revision_available_at <= ?",
    ]
    parameters: list[Any] = [timeframe, int(as_of_ts), int(as_of_ts)]
    if max_publish_seq is not None:
        predicates.append("publish_seq <= ?")
        parameters.append(int(max_publish_seq))
    if item_id is not None:
        predicates.append("item_id = ?")
        parameters.append(int(item_id))

    projection = ", ".join(f"r.{column}" for column in _REVISION_COLUMNS)
    rows = conn.execute(
        f"""
        WITH visible AS (
            SELECT item_id, timeframe, bucket_ts, MAX(publish_seq) AS publish_seq
            FROM item_bar_revisions
            WHERE {" AND ".join(predicates)}
            GROUP BY item_id, timeframe, bucket_ts
        )
        SELECT {projection}
        FROM item_bar_revisions AS r
        JOIN visible AS v
          ON v.item_id = r.item_id
         AND v.timeframe = r.timeframe
         AND v.bucket_ts = r.bucket_ts
         AND v.publish_seq = r.publish_seq
        ORDER BY r.item_id ASC, r.bucket_ts ASC
        """,
        parameters,
    ).fetchall()
    return [dict(row) for row in rows]


@contextmanager
def _completed_snapshot(conn: sqlite3.Connection, snapshot_id: int):
    snapshot = _get_snapshot(conn, snapshot_id)
    if snapshot is None:
        raise ValueError(f"Unknown snapshot_id: {snapshot_id}")
    if snapshot["status"] not in {"running", "completed"}:
        raise ValueError("process_snapshot requires a running or completed snapshot")
    is_running = snapshot["status"] == "running"
    if is_running:
        conn.execute(
            """
            UPDATE snapshot_runs
            SET status = 'completed',
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
                """
                UPDATE snapshot_runs SET status = 'running', completed_at = ?
                WHERE snapshot_id = ?
                """,
                (snapshot["completed_at"], snapshot_id),
            )


def process_snapshot(
    conn: sqlite3.Connection, snapshot_id: int, settings: Any = None
) -> None:
    """Publish differences, evidence, and 15m/4h/1d bars atomically.

    Running snapshots require their producer's transaction and retain their
    original status on return. Retention runs only after that transaction commits.
    """
    if not conn.in_transaction:
        snapshot = _get_snapshot(conn, snapshot_id)
        if snapshot is not None and snapshot["status"] == "running":
            raise RuntimeError(
                "A running snapshot requires its producer's write transaction"
            )

    with _write_scope(conn), _completed_snapshot(conn, snapshot_id) as snapshot:
        timestamp = int(snapshot["server_started_at"])
        bucket_ts = timestamp - timestamp % 900
        if not _bucket_replayable(conn, bucket_ts, 900):
            return
        item_ids = [
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT item_id FROM raw_listings WHERE snapshot_id = ?",
                (snapshot_id,),
            )
        ]
        ensure_items_catalog(conn, item_ids, timestamp)

        from pokemmo_gtl.pipeline.snapshot_diff import process_snapshot_diff

        process_snapshot_diff(conn, snapshot_id, settings)
