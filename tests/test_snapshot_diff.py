"""Price-conditioned flow estimates for completed snapshot pairs."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.pipeline.snapshot_diff import process_snapshot_diff


@pytest.fixture(autouse=True)
def verify_repository_local_temp_directory(tmp_path, pytestconfig):
    """Honor the CLI's independent basetemp while enforcing repository-local I/O."""
    expected = Path(pytestconfig.option.basetemp).resolve()
    assert expected.is_relative_to(pytestconfig.rootpath.resolve())
    assert tmp_path.resolve().is_relative_to(expected)


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "snapshot-diff.db"
    migrate(path)
    return path


def insert_snapshot(conn, server_ts, listings=(), *, status="completed"):
    completed_at = server_ts if status == "completed" else None
    cursor = conn.execute(
        """
        INSERT INTO snapshot_runs
            (source, started_at, server_started_at, completed_at, status)
        VALUES ('simulator', ?, ?, ?, ?)
        """,
        (server_ts, server_ts, completed_at, status),
    )
    snapshot_id = cursor.lastrowid
    conn.executemany(
        """
        INSERT INTO raw_listings
            (snapshot_id, auction_id, item_id, price, quantity, expires_at)
        VALUES (?, ?, 1, ?, ?, ?)
        """,
        [
            (snapshot_id, auction_id, price, quantity, expires_at)
            for auction_id, price, quantity, expires_at in listings
        ],
    )
    return snapshot_id


def test_first_frame_is_empty_and_disappearances_use_price_weighting(db_path):
    settings = SimpleNamespace(
        expiry_grace_sec=180,
        max_diff_gap_sec=5400,
        alpha_near=0.75,
        alpha_mid=0.20,
        alpha_far=0.0,
    )
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, updated_at) VALUES (1, 'Item', 1)"
        )
        first = insert_snapshot(
            conn,
            100_000,
            [
                (1, 100, 100, 200_000),
                (2, 100, 4, 200_000),
                (3, 107, 5, 200_000),
                (4, 121, 6, 200_000),
                (5, 100, 3, 100_100),
            ],
        )
    with open_connection(db_path) as conn:
        assert process_snapshot_diff(conn, first, settings) == 0
        assert conn.execute("SELECT count(*) FROM inferred_flows").fetchone()[0] == 0

    with open_connection(db_path) as conn, write_transaction(conn):
        insert_snapshot(conn, 100_150, status="failed")
        current = insert_snapshot(
            conn,
            100_300,
            [
                (1, 100, 98, 200_000),
                (10, 90, 2, 200_000),
            ],
        )
    with open_connection(db_path) as conn:
        assert process_snapshot_diff(conn, current, settings) == 1
        flow = conn.execute(
            "SELECT * FROM inferred_flows WHERE snapshot_id = ?", (current,)
        ).fetchone()

    assert flow["prev_snapshot_id"] == first
    assert flow["interval_sec"] == 300
    assert flow["new_listing_count"] == 1
    assert flow["new_listed_qty"] == 2
    assert flow["partial_fill_qty"] == 2
    assert flow["early_disappear_qty"] == 15
    assert flow["expired_qty"] == 3
    assert flow["gap_skipped"] == 0
    assert flow["est_sold_qty"] == pytest.approx(5.25)
    assert flow["est_sold_vwap"] == pytest.approx(532 / 5.25)


@pytest.mark.parametrize(
    ("interval", "gap_skipped"),
    [(59, 1), (60, 0), (5400, 0), (5401, 1)],
)
def test_diff_gap_boundaries(interval, gap_skipped, db_path):
    settings = SimpleNamespace(
        expiry_grace_sec=180,
        max_diff_gap_sec=5400,
        alpha_near=0.75,
        alpha_mid=0.20,
    )
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, updated_at) VALUES (1, 'Item', 1)"
        )
        previous = insert_snapshot(conn, 500_000, [(1, 100, 10, 900_000)])
        current = insert_snapshot(
            conn, 500_000 + interval, [(1, 100, 5, 900_000)]
        )
    with open_connection(db_path) as conn:
        assert process_snapshot_diff(conn, current, settings) == 1
        flow = conn.execute(
            "SELECT * FROM inferred_flows WHERE snapshot_id = ?", (current,)
        ).fetchone()

    assert flow["prev_snapshot_id"] == previous
    assert flow["interval_sec"] == interval
    assert flow["gap_skipped"] == gap_skipped
    if gap_skipped:
        assert flow["est_sold_qty"] == 0.0
        assert flow["est_sold_vwap"] is None
    else:
        assert flow["est_sold_qty"] == 5.0
        assert flow["est_sold_vwap"] == 100.0


@pytest.mark.parametrize("replacement_qty", [2, 10, 100])
def test_near_relisting_deduction_is_not_capped_at_disappearing_volume(db_path, replacement_qty):
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, updated_at) VALUES (1, 'Item', 1)"
        )
        insert_snapshot(conn, 100_000, [
            (1, 100, 100, 200_000), (2, 101, 4, 200_000),
        ])
        current = insert_snapshot(conn, 100_900, [
            (1, 100, 100, 200_000), (3, 99, replacement_qty, 200_000),
        ])
    with open_connection(db_path) as conn:
        process_snapshot_diff(conn, current)
        flow = conn.execute("SELECT * FROM inferred_flows").fetchone()
        assert flow["est_sold_qty"] == 0.75 * max(0, 4 - 0.5 * replacement_qty)
        if flow["est_sold_qty"]:
            assert flow["est_sold_vwap"] == pytest.approx(101)
        else:
            assert flow["est_sold_vwap"] is None


def test_weighted_vwap_uses_each_band_price_and_server_expiry_grace(db_path):
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, updated_at) VALUES (1, 'Item', 1)"
        )
        insert_snapshot(conn, 100_000, [
            (1, 100, 100, 200_000),
            (2, 100, 2, 200_000), (3, 103, 2, 200_000),
            (4, 110, 3, 200_000), (5, 111, 50, 200_000),
            (6, 100, 5, 101_080),
        ])
        current = insert_snapshot(conn, 100_900, [
            (1, 100, 98, 200_000), (7, 99, 2, 200_000),
        ])
        # The local completion timestamp must not affect the expiry cutoff.
        conn.execute(
            "UPDATE snapshot_runs SET completed_at = 999999 WHERE snapshot_id = ?", (current,)
        )
    with open_connection(db_path) as conn:
        process_snapshot_diff(conn, current)
        flow = conn.execute("SELECT * FROM inferred_flows").fetchone()
    expected_qty = 2 + 0.75 * 3 + 0.20 * 3
    expected_notional = 2 * 100 + 0.75 * 3 * 101.5 + 0.20 * 3 * 110
    assert flow["expired_qty"] == 5
    assert flow["early_disappear_qty"] == 57
    assert flow["est_sold_qty"] == pytest.approx(expected_qty)
    assert flow["est_sold_vwap"] == pytest.approx(expected_notional / expected_qty)


def test_current_robust_ask_sets_clear_price_and_far_only_sales_stay_null(db_path):
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, updated_at) VALUES (1, 'Item', 1)"
        )
        insert_snapshot(conn, 100_000, [
            (1, 100, 100, 200_000), (2, 120, 3, 200_000),
        ])
        second = insert_snapshot(conn, 100_900, [(1, 100, 100, 200_000)])
        third = insert_snapshot(conn, 101_800, [(3, 200, 100, 200_000)])
    with open_connection(db_path) as conn:
        process_snapshot_diff(conn, second)
        far = conn.execute(
            "SELECT * FROM inferred_flows WHERE snapshot_id = ?", (second,)
        ).fetchone()
        assert far["est_sold_qty"] == 0.0
        assert far["est_sold_vwap"] is None
        process_snapshot_diff(conn, third)
        near = conn.execute(
            "SELECT * FROM inferred_flows WHERE snapshot_id = ?", (third,)
        ).fetchone()
        assert near["est_sold_qty"] == 75.0
        assert near["est_sold_vwap"] == 100.0
