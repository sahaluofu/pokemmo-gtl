"""Deterministic local snapshot streams; no difference or aggregation business logic."""

from __future__ import annotations

import math
import random
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pokemmo_gtl.config import Environment, load_config
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.ingestion.catalog_sync import load_seed_catalog, sync_catalog

SECONDS_PER_DAY = 86_400
SNAPSHOT_INTERVAL = 900
SnapshotCallback = Callable[[sqlite3.Connection, int, int | None, int], None]
Listing = tuple[int, int, int, int, int]

_SIMULATION_LOCKS: dict[Path, threading.Lock] = {}
_SIMULATION_LOCKS_GUARD = threading.Lock()

_RESET_TABLES = (
    "opportunity_observations",
    "review_windows",
    "orderbook_observations",
    "item_bar_revisions",
    "inferred_flow_revisions",
    "publish_events",
    "user_orders",
    "user_positions",
    "user_ledger_events",
    "predictions",
    "backtest_runs",
    "inferred_flows",
    "item_bars",
    "raw_listings",
    "snapshot_runs",
)


@dataclass(frozen=True)
class SimulationResult:
    snapshots: int
    listings: int
    start_ts: int
    end_ts: int


def _simulation_lock(db_path: Path) -> threading.Lock:
    with _SIMULATION_LOCKS_GUARD:
        lock = _SIMULATION_LOCKS.get(db_path)
        if lock is None:
            lock = threading.Lock()
            _SIMULATION_LOCKS[db_path] = lock
        return lock


def simulate(
    db_path: Path,
    *,
    env: Environment | str,
    days: int = 90,
    seed: int = 42,
    end_ts: int | None = None,
    reset: bool = False,
    on_snapshot: SnapshotCallback | None = None,
) -> SimulationResult:
    """Generate [end_ts - days*86400, end_ts) at 15-minute intervals.

    The callback receives (connection, snapshot_id, previous_completed_id, UTC_ts)
    after raw rows and watermarks exist inside phase two, before completion.
    Callback writes share the same rollback boundary. This is the M3 extension
    point only. Overlapping completed or active windows require an explicit reset
    or a separate database; reset clears snapshots and their dependent state.
    In-process calls sharing a resolved database path run serially.
    """
    environment = Environment(env)
    if environment is Environment.live:
        raise ValueError("simulation is forbidden for env='live'")
    if type(days) is not int or days <= 0:
        raise ValueError("days must be a positive integer")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    config = load_config(environment)
    anchor = config.settings.simulation_default_end_ts if end_ts is None else end_ts
    if type(anchor) is not int or anchor <= 0 or anchor <= days * SECONDS_PER_DAY:
        raise ValueError("end_ts must be an integer after the requested simulation window")
    db_path = Path(db_path).resolve()
    if db_path == load_config(Environment.live).db_path.resolve():
        raise ValueError("simulation cannot write to the live database path")

    start_ts = anchor - days * SECONDS_PER_DAY
    with _simulation_lock(db_path):
        migrate(db_path, applied_at=anchor)
        sync_catalog(db_path, config=config, offline_only=True)
        items = load_seed_catalog(config.root / "config" / "seed_catalog_minimal.json")
        frames = days * SECONDS_PER_DAY // SNAPSHOT_INTERVAL
        book = _OrderBook(items, seed)
        total_listings = 0
        # All frames share one connection owned by this thread. Each frame releases
        # the write reservation between the two independent phases.
        with open_connection(db_path) as conn:
            snapshot_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(snapshot_runs)")
            }
            retention_clause = (
                ", raw_retained = 0" if "raw_retained" in snapshot_columns else ""
            )
            watermark_columns = tuple(
                column
                for column in ("last_page_server_at", "available_at", "raw_retained")
                if column in snapshot_columns
            )
            watermark_update = (
                "UPDATE snapshot_runs SET "
                + ", ".join(f"{column} = ?" for column in watermark_columns)
                + " WHERE snapshot_id = ?"
            )
            has_api_covered = any(
                row[1] == "api_covered"
                for row in conn.execute("PRAGMA table_info(items_catalog)")
            )
            catalog_columns = [
                "item_id", "name_en", "category", "max_stack_size", "updated_at",
            ]
            if has_api_covered:
                catalog_columns.append("api_covered")
            catalog_insert = (
                "INSERT OR IGNORE INTO items_catalog "
                f"({', '.join(catalog_columns)}) "
                f"VALUES ({', '.join('?' for _ in catalog_columns)})"
            )
            previous_id = None
            for offset in range(frames):
                captured_at = start_ts + offset * SNAPSHOT_INTERVAL
                with write_transaction(conn):
                    if offset == 0 and reset:
                        existing_tables = {
                            row[0]
                            for row in conn.execute(
                                "SELECT name FROM sqlite_master WHERE type = 'table'"
                            )
                        }
                        for table in _RESET_TABLES:
                            if table in existing_tables:
                                conn.execute(f"DELETE FROM {table}")
                    conn.execute(
                        """DELETE FROM raw_listings WHERE snapshot_id IN
                           (SELECT snapshot_id FROM snapshot_runs
                            WHERE status = 'running' AND started_at < ?)""",
                        (captured_at - 1800,),
                    )
                    conn.execute(
                        "UPDATE snapshot_runs SET status = 'failed', completed_at = ?, "
                        "error_message = 'stale snapshot run'"
                        f"{retention_clause} "
                        "WHERE status = 'running' AND started_at < ?",
                        (captured_at, captured_at - 1800),
                    )
                    if offset == 0:
                        # Admission and the first running reservation share the
                        # same write transaction, including across processes.
                        if not reset and conn.execute(
                            """
                            SELECT 1 FROM snapshot_runs
                            WHERE server_started_at >= ? AND server_started_at < ?
                              AND (
                                  status = 'completed'
                                  OR (status = 'running' AND started_at >= ?)
                              )
                            LIMIT 1
                            """,
                            (start_ts, anchor, captured_at - 1800),
                        ).fetchone() is not None:
                            raise ValueError(
                                "Overlapping simulation window requires reset=True "
                                "or an isolated db_path"
                            )
                        previous = conn.execute(
                            "SELECT snapshot_id FROM snapshot_runs "
                            "WHERE status = 'completed' "
                            "ORDER BY snapshot_id DESC LIMIT 1"
                        ).fetchone()
                        previous_id = previous["snapshot_id"] if previous else None
                    snapshot_id = conn.execute(
                        """INSERT INTO snapshot_runs
                           (source, started_at, server_started_at, status)
                           VALUES ('simulator', ?, ?, 'running')""",
                        (captured_at, captured_at),
                    ).lastrowid
                try:
                    # Generation, like future network fetching, holds no write transaction.
                    frame = book.frame(captured_at, start_ts)
                    with write_transaction(conn):
                        conn.executemany(
                            catalog_insert,
                            [
                                (
                                    item_id,
                                    f"Unknown Item #{item_id}",
                                    "unknown",
                                    99,
                                    captured_at,
                                )
                                + ((int(item_id < 900_000),) if has_api_covered else ())
                                for item_id in sorted({row[1] for row in frame})
                            ],
                        )
                        conn.executemany(
                            """INSERT INTO raw_listings
                               (snapshot_id, auction_id, item_id, price, quantity, expires_at)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            [(snapshot_id, *listing) for listing in frame],
                        )
                        if watermark_columns:
                            conn.execute(
                                watermark_update,
                                tuple(
                                    1 if column == "raw_retained" else captured_at
                                    for column in watermark_columns
                                ) + (snapshot_id,),
                            )
                        if on_snapshot is not None:
                            on_snapshot(conn, snapshot_id, previous_id, captured_at)
                        conn.execute(
                            """UPDATE snapshot_runs
                               SET status = 'completed', completed_at = ?,
                                   total_listings = ?, pages_fetched = ?
                               WHERE snapshot_id = ?""",
                            (
                                captured_at,
                                len(frame),
                                (len(frame) + 999) // 1000,
                                snapshot_id,
                            ),
                        )
                    total_listings += len(frame)
                    previous_id = snapshot_id
                except BaseException as error:
                    with (
                        open_connection(db_path) as failure_conn,
                        write_transaction(failure_conn),
                    ):
                        failure_conn.execute(
                            "UPDATE snapshot_runs SET status = 'failed', completed_at = ?, "
                            f"error_message = ?{retention_clause} WHERE snapshot_id = ?",
                            (captured_at, type(error).__name__, snapshot_id),
                        )
                    raise
        return SimulationResult(frames, total_listings, start_ts, anchor)


class _OrderBook:
    """Persistent listing identities with fills, expiry, replenishment and withdrawals."""

    def __init__(self, items: list[dict], seed: int) -> None:
        self.items = sorted(items, key=lambda item: item["item_id"])
        self.seed = seed
        self.rng = random.Random(seed)
        self.slots: dict[tuple[int, int], Listing] = {}
        self.next_auction_id = 1
        self.frame_number = 0

    def frame(self, captured_at: int, start_ts: int) -> list[Listing]:
        for item in self.items:
            item_id = item["item_id"]
            price, pressure = market_profile(item, captured_at, start_ts, self.seed)
            stack = item["max_stack_size"]
            phase = item_id * 0.371 + self.seed * 0.017
            for level in range(5):
                key = (item_id, level)
                existing = self.slots.get(key)
                if level == 4:
                    # Far offers disappear with hours left until expiry, in coherent waves.
                    if pressure > 0.65:
                        self.slots.pop(key, None)
                        continue
                    premium = 0.35
                    quantity = min(stack, 8)
                else:
                    premium = level * (0.015 + 0.04 * (1 - pressure))
                    quantity = min(stack, max(2, round(20 * (1 - 0.7 * pressure))))
                renew = existing is None or existing[4] <= captured_at
                if existing is not None and not renew:
                    # Near offers fill; a replacement always receives a fresh auction ID.
                    renew = self.rng.random() < (0.18 if level < 2 else 0.04)
                    if not renew and level < 2 and existing[3] > 1 and self.rng.random() < 0.25:
                        self.slots[key] = (*existing[:3], existing[3] - 1, existing[4])
                if renew:
                    # Near price noise is small enough to retain coherent order-book levels.
                    offer = max(2, round(price * (1 + premium) * (1 + 0.002 * math.sin(phase))))
                    self.slots[key] = (
                        self.next_auction_id, item_id, offer, quantity,
                        captured_at + (24 if level == 4 else 6) * 3600,
                    )
                    self.next_auction_id += 1
        frame = sorted(self.slots.values())
        # One extra single-unit outlier, never an entire market of one-yen offers.
        if self.frame_number % 97 == 0:
            frame.append(
                (self.next_auction_id, 138, 1, 1, captured_at + 3600)
            )
            self.next_auction_id += 1
        self.frame_number += 1
        return frame


def market_profile(item: dict, captured_at: int, start_ts: int, seed: int) -> tuple[int, float]:
    """Return target price and earlier sell-pressure signal for fixture inspection."""
    item_id = item["item_id"]
    category = item["category"]
    elapsed = (captured_at - start_ts) / SECONDS_PER_DAY
    phase = item_id * 0.371 + seed * 0.017
    cycle = captured_at / SECONDS_PER_DAY * 2 * math.pi / 7 + phase
    weekly = 1 + 0.055 * math.sin(cycle)
    # Pressure peaks a day before the weekly price peak.
    pressure = (1 + math.sin(cycle + 2 * math.pi / 7)) / 2
    inflation = 1 + elapsed * (0.002 if category == "rp_voucher" else 0.00035)
    return (
        max(2, round(_base_price(item_id, category) * weekly * inflation
                     * _holiday_factor(captured_at))),
        pressure,
    )


def _base_price(item_id: int, category: str) -> int:
    if category == "rp_voucher":
        return {1190: 100, 1191: 500, 1192: 1000}[item_id] * 35
    if category == "pokemon_category":
        return 80_000 + (item_id - 900_000) * 7_500
    if category == "cosmetic":
        return 450_000 + (item_id % 5) * 80_000
    if category == "event_box":
        return 20_000 + (item_id % 3) * 4_000
    if category in {"berry", "berry_seed"}:
        return 450 + (item_id % 7) * 95
    if category == "pokeball":
        return 200 + (item_id % 10) * 100
    if item_id == 189:
        return 18_000
    return 2_500 + (item_id % 13) * 550


def _holiday_factor(captured_at: int) -> float:
    factor = 1.0
    for holiday in (1761868800, 1766620800):
        days_from = (captured_at - holiday) / SECONDS_PER_DAY
        if 0 <= days_from < 3:
            factor *= 0.60 - 0.04 * days_from / 3
        elif 3 <= days_from <= 21:
            factor *= 0.56 + 0.54 * (days_from - 3) / 18
    return factor
