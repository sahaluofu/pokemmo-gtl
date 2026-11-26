"""M3 book filtering, rollups, catalog safety and retention contracts."""

import re
from pathlib import Path

import pytest

from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.ingestion.manual_import import import_csv
from pokemmo_gtl.pipeline.aggregator import (
    aggregate_listings,
    process_snapshot,
    store_raw_listings,
)
from pokemmo_gtl.pipeline.retention import prune_raw_listings


@pytest.fixture(autouse=True)
def verify_repository_local_temp_directory(tmp_path, pytestconfig):
    """Honor the CLI's independent basetemp while enforcing repository-local I/O."""
    expected = Path(pytestconfig.option.basetemp).resolve()
    assert expected.is_relative_to(pytestconfig.rootpath.resolve())
    assert tmp_path.resolve().is_relative_to(expected)


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "pipeline.db"
    migrate(path)
    return path


def add_item(conn, item_id=1):
    conn.execute(
        "INSERT OR IGNORE INTO items_catalog (item_id, name_en, updated_at) VALUES (?, ?, 1)",
        (item_id, f"Item {item_id}"),
    )


def add_snapshot(conn, captured_at, listings, *, status="completed"):
    completed_at = captured_at if status == "completed" else None
    cursor = conn.execute(
        """
        INSERT INTO snapshot_runs
            (source, started_at, server_started_at, completed_at, status)
        VALUES ('simulator', ?, ?, ?, ?)
        """,
        (captured_at, captured_at, completed_at, status),
    )
    snapshot_id = cursor.lastrowid
    conn.executemany(
        """
        INSERT INTO raw_listings
            (snapshot_id, auction_id, item_id, price, quantity, expires_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (snapshot_id, auction_id, item_id, price, quantity, captured_at + 10_000)
            for auction_id, item_id, price, quantity in listings
        ],
    )
    return snapshot_id


def test_low_tail_fishing_listing_does_not_change_robust_book_metrics():
    clean = aggregate_listings([{"auction_id": 2, "price": 100, "quantity": 100_000}])
    mixed = aggregate_listings(
        [
            {"auction_id": 1, "price": 1, "quantity": 1},
            {"auction_id": 2, "price": 100, "quantity": 100_000},
            {"auction_id": 3, "price": 0, "quantity": 0},
        ]
    )
    assert clean is not None and mixed is not None
    assert mixed["robust_best_ask"] == clean["robust_best_ask"] == 100
    assert mixed["vwap_top5_qty"] == clean["vwap_top5_qty"] == 100.0
    assert mixed["depth_within_5pct"] == clean["depth_within_5pct"] == 100_000
    assert mixed["best_ask_min"] == 1
    assert aggregate_listings([{"auction_id": 4, "price": 100, "quantity": 0}]) is None


def test_filter_anchors_prefix_threshold_and_weighted_top_depth():
    small = aggregate_listings([
        {"auction_id": 1, "price": 1, "quantity": 1},
        {"auction_id": 2, "price": 100, "quantity": 6},
    ])
    assert small["p10_price"] == 1.0
    assert small["robust_best_ask"] == 100
    assert small["depth_within_5pct"] == 6
    # Each prefix order is below the strict 5%-quantity threshold.
    prefix = aggregate_listings([
        {"auction_id": 1, "price": 1, "quantity": 3},
        {"auction_id": 2, "price": 2, "quantity": 3},
        {"auction_id": 3, "price": 100, "quantity": 94},
    ])
    assert prefix["robust_best_ask"] == 100
    threshold = aggregate_listings([
        {"auction_id": 1, "price": 1, "quantity": 2},
        {"auction_id": 2, "price": 100, "quantity": 38},
    ])
    assert threshold["robust_best_ask"] == 1
    weighted = aggregate_listings([
        {"auction_id": 3, "price": 106, "quantity": 50},
        {"auction_id": 2, "price": 104, "quantity": 2},
        {"auction_id": 1, "price": 100, "quantity": 2},
    ])
    assert weighted["vwap_top5_qty"] == (200 + 208 + 106) / 5
    assert weighted["depth_within_5pct"] == 4
    assert weighted["spread_top5_pct"] == pytest.approx(((514 / 5) - 100) / 100)


def test_parent_bars_use_last_stock_and_sum_only_flow_columns(db_path):
    base = 1_700_000_000 // 86_400 * 86_400 + 100
    settings = {"expiry_grace_sec": 180, "max_diff_gap_sec": 5400}
    with open_connection(db_path) as conn, write_transaction(conn):
        add_item(conn)
        first = add_snapshot(conn, base, [(1, 1, 100, 100)])
        second = add_snapshot(conn, base + 900, [(1, 1, 200, 5)])
    with open_connection(db_path) as conn:
        process_snapshot(conn, first, settings)
        process_snapshot(conn, second, settings)
        for timeframe in ("4h", "1d"):
            bar = conn.execute(
                """
                SELECT * FROM item_bars
                WHERE item_id = 1 AND timeframe = ?
                """,
                (timeframe,),
            ).fetchone()
            assert bar["open_price"] == 100
            assert bar["high_price"] == 200
            assert bar["low_price"] == 100
            assert bar["close_price"] == 200
            assert bar["total_listed_qty"] == 5
            assert bar["robust_best_ask"] == 200
            assert bar["best_ask_min"] == 200
            assert bar["p10_price"] == 200
            assert bar["listing_count"] == 1
            assert bar["depth_within_5pct"] == 5
            assert bar["spread_top5_pct"] == 0.0
            assert bar["sample_count"] == 2
            assert bar["valid_flow_samples"] == 1
            assert bar["est_sold_qty_sum"] == 95.0
        assert conn.execute(
            "SELECT count(*) FROM item_bars WHERE timeframe = '15m'"
        ).fetchone()[0] == 2


def test_same_bucket_is_chronological_and_reprocessing_does_not_double_flows(db_path):
    base = 1_700_000_000 // 86_400 * 86_400 + 100
    with open_connection(db_path) as conn, write_transaction(conn):
        add_item(conn)
        first = add_snapshot(conn, base, [(1, 1, 100, 10)])
        second = add_snapshot(conn, base + 300, [(1, 1, 120, 6)])
        third = add_snapshot(conn, base + 900, [(1, 1, 110, 5)])
    with open_connection(db_path) as conn:
        for snapshot_id in (second, first, third):
            process_snapshot(conn, snapshot_id, {})
        before = [tuple(row) for row in conn.execute(
            "SELECT * FROM item_bars ORDER BY timeframe, bucket_ts"
        )]
        process_snapshot(conn, second, {})
        assert [tuple(row) for row in conn.execute(
            "SELECT * FROM item_bars ORDER BY timeframe, bucket_ts"
        )] == before
        first_bar = conn.execute(
            "SELECT * FROM item_bars WHERE timeframe = '15m' ORDER BY bucket_ts"
        ).fetchone()
        assert first_bar["open_price"] == first_bar["low_price"] == 100
        assert first_bar["high_price"] == first_bar["close_price"] == 120
        assert first_bar["best_ask_min"] == 100
        assert first_bar["total_listed_qty"] == 6
        assert first_bar["sample_count"] == 2
        for timeframe in ("4h", "1d"):
            parent = conn.execute(
                "SELECT * FROM item_bars WHERE timeframe = ?", (timeframe,)
            ).fetchone()
            assert parent["open_price"] == parent["low_price"] == 100
            assert parent["high_price"] == 120
            assert parent["close_price"] == parent["best_ask_min"] == 110
            assert parent["total_listed_qty"] == 5
            assert parent["sample_count"] == 3
            assert parent["valid_flow_samples"] == 2
            assert parent["est_sold_qty_sum"] == 5.0


def test_zero_book_skips_snapshot_point_but_retains_same_bucket_flow(db_path):
    base = 1_700_000_000 // 900 * 900
    with open_connection(db_path) as conn, write_transaction(conn):
        add_item(conn)
        first = add_snapshot(conn, base + 100, [(1, 1, 100, 10)])
        empty = add_snapshot(conn, base + 400, [])
    with open_connection(db_path) as conn:
        process_snapshot(conn, first, {})
        process_snapshot(conn, empty, {})
        for row in conn.execute("SELECT * FROM item_bars"):
            assert row["sample_count"] == 1
            assert row["valid_flow_samples"] == 1
            assert row["est_sold_qty_sum"] == 7.5


def test_gap_flows_are_excluded_from_all_rollups(db_path):
    base = 1_700_000_000 // 86_400 * 86_400
    with open_connection(db_path) as conn, write_transaction(conn):
        add_item(conn)
        first = add_snapshot(conn, base + 1, [(1, 1, 100, 10)])
        second = add_snapshot(conn, base + 50, [(2, 1, 100, 20)])
    with open_connection(db_path) as conn:
        process_snapshot(conn, first, {})
        process_snapshot(conn, second, {})
        for row in conn.execute("SELECT * FROM item_bars"):
            assert row["sample_count"] == 2
            assert row["valid_flow_samples"] == 0
            assert row["est_sold_qty_sum"] == 0.0
            assert row["new_listed_qty_sum"] == 0
            assert row["expired_qty_sum"] == 0


def test_manual_upsert_guard_preserves_snapshot_bar(db_path, tmp_path):
    captured_at = 1_700_000_000
    bucket_ts = captured_at - captured_at % 14_400
    with open_connection(db_path) as conn, write_transaction(conn):
        add_item(conn)
        snapshot_id = add_snapshot(conn, captured_at, [(1, 1, 125, 8)])
    with open_connection(db_path) as conn:
        process_snapshot(conn, snapshot_id, {})
        before = [tuple(row) for row in conn.execute("SELECT * FROM item_bars ORDER BY timeframe")]
    csv_path = tmp_path / "manual.csv"
    csv_path.write_text(
        f"item_id,price,listed_qty,recorded_at\n1,1,999,{captured_at}\n",
        encoding="utf-8",
    )
    result = import_csv(db_path, csv_path, now_ts=bucket_ts)
    with open_connection(db_path) as conn:
        after = [tuple(row) for row in conn.execute("SELECT * FROM item_bars ORDER BY timeframe")]
        assert conn.execute("SELECT count(*) FROM manual_quotes").fetchone()[0] == 1
    assert result.imported_rows == 1
    assert result.skipped_snapshot_bars == 2
    assert result.errors == []
    assert after == before


def test_unknown_item_is_created_before_aggregate_writes(db_path):
    with open_connection(db_path) as conn, write_transaction(conn):
        snapshot_id = add_snapshot(conn, 1_700_000_000, [], status="running")
    with open_connection(db_path) as conn, write_transaction(conn):
        statements = []
        conn.set_trace_callback(statements.append)
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert store_raw_listings(conn, snapshot_id, [{
            "auction_id": 99, "reference_id": 987654, "price": 250,
            "quantity": 3, "expires_at": 1_700_010_000,
        }]) == 1
        process_snapshot(conn, snapshot_id, {})
        item = conn.execute(
            """
            SELECT name_en, category, max_stack_size
            FROM items_catalog WHERE item_id = 987654
            """
        ).fetchone()
        bars = conn.execute(
            "SELECT count(*) FROM item_bars WHERE item_id = 987654"
        ).fetchone()[0]
        run = conn.execute(
            "SELECT status, completed_at FROM snapshot_runs WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        assert tuple(run) == ("running", None)
        # This is the M2 producer's final compare-and-set after its hook returns.
        assert conn.execute(
            "UPDATE snapshot_runs SET status = 'completed', completed_at = 1700000001 "
            "WHERE snapshot_id = ? AND status = 'running'", (snapshot_id,)
        ).rowcount == 1
        assert list(conn.execute("PRAGMA foreign_key_check")) == []
    assert tuple(item) == ("Unknown Item #987654", "unknown", 99)
    assert bars == 3
    catalog_insert = next(i for i, sql in enumerate(statements) if "INSERT OR IGNORE" in sql)
    raw_insert = next(i for i, sql in enumerate(statements) if "INSERT INTO raw_listings" in sql)
    assert catalog_insert < raw_insert


def test_retention_batches_raw_rows_and_keeps_latest_completed_frames(db_path):
    now_ts = 1_800_000_000
    with open_connection(db_path) as conn, write_transaction(conn):
        add_item(conn)
        for index in range(43):
            completed = now_ts - 200_000 if index < 41 else now_ts - (42 - index) * 100
            add_snapshot(conn, completed, [(1, 1, 100, 1)])
        failed = add_snapshot(
            conn, now_ts - 300_000, [(1, 1, 100, 1)], status="failed"
        )
    with open_connection(db_path) as conn:
        statements = []
        conn.set_trace_callback(statements.append)
        deleted_rows = prune_raw_listings(conn)
        newest_raw = conn.execute(
            """
            SELECT count(*) FROM raw_listings
            WHERE snapshot_id IN (
                SELECT snapshot_id FROM snapshot_runs WHERE status = 'completed'
                ORDER BY completed_at DESC, snapshot_id DESC LIMIT 2
            )
            """
        ).fetchone()[0]
        failed_raw = conn.execute(
            "SELECT count(*) FROM raw_listings WHERE snapshot_id = ?", (failed,)
        ).fetchone()[0]
        snapshot_count = conn.execute("SELECT count(*) FROM snapshot_runs").fetchone()[0]

    deletes = [statement for statement in statements if "DELETE FROM raw_listings" in statement]
    batch_sizes = [
        len(re.search(r" IN \((.*?)\)", statement).group(1).split(","))
        for statement in deletes
    ]
    assert deleted_rows == 41
    assert batch_sizes == [20, 20, 1]
    assert newest_raw == 2
    assert failed_raw == 1
    assert snapshot_count == 44
    assert any("wal_checkpoint(PASSIVE)" in statement for statement in statements)


@pytest.mark.parametrize("completed_count", [0, 1, 2, 3])
def test_retention_keeps_two_completed_even_when_every_frame_is_old(db_path, completed_count):
    with open_connection(db_path) as conn, write_transaction(conn):
        add_item(conn)
        completed_ids = [
            add_snapshot(conn, 100 + index, [(1, 1, 100, 1)])
            for index in range(completed_count)
        ]
    with open_connection(db_path) as conn:
        assert prune_raw_listings(conn, now_ts=1_000_000) == max(0, completed_count - 2)
        remaining = [row[0] for row in conn.execute(
            "SELECT DISTINCT snapshot_id FROM raw_listings ORDER BY snapshot_id"
        )]
        assert remaining == completed_ids[-2:]
        assert conn.execute("SELECT count(*) FROM snapshot_runs").fetchone()[0] == completed_count


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
def test_producer_transaction_rolls_back_catalog_raw_and_bars_on_failure(db_path, error):
    with open_connection(db_path) as conn, write_transaction(conn):
        snapshot_id = add_snapshot(conn, 1_700_000_000, [], status="running")
    with open_connection(db_path) as conn:
        with pytest.raises(error), write_transaction(conn):
            store_raw_listings(conn, snapshot_id, [{
                "auction_id": 1, "reference_id": 999, "price": 100,
                "quantity": 5, "expires_at": 1_700_010_000,
            }])
            process_snapshot(conn, snapshot_id, {})
            assert conn.in_transaction
            raise error("producer failed")
        for table in ("items_catalog", "raw_listings", "inferred_flows", "item_bars"):
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
        assert conn.execute("SELECT status FROM snapshot_runs").fetchone()[0] == "running"
