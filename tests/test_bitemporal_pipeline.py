"""Bitemporal publications, adjacent diffs, empty books, and retention safety."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.pipeline.aggregator import (
    process_snapshot,
    query_bars_as_of,
    store_raw_listings,
)
from pokemmo_gtl.pipeline.retention import (
    check_frames_retained,
    prune_raw_listings,
)
from pokemmo_gtl.pipeline.snapshot_diff import process_snapshot_diff

_DAY = 1_800_000_000 // 86_400 * 86_400
_NOON = _DAY + 12 * 3600
_MATERIALIZED_TABLES = (
    "inferred_flows",
    "inferred_flow_revisions",
    "item_bars",
    "item_bar_revisions",
)


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "bitemporal-pipeline.db"
    migrate(path)
    return path


def _ts(minute: int, second: int = 0) -> int:
    return _NOON + minute * 60 + second


def _item(
    conn,
    item_id=1,
    *,
    watched=False,
    api_covered=True,
    asset_class="item",
    purchase_mode="unknown",
):
    with write_transaction(conn):
        conn.execute(
            """
            INSERT INTO items_catalog (
                item_id, name_en, updated_at, is_watched, api_covered,
                asset_class, purchase_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                f"Item {item_id}",
                _DAY,
                int(watched),
                int(api_covered),
                asset_class,
                purchase_mode,
            ),
        )


def _frame(conn, minute, available_minute, listings=(), *, second=0, publish=True):
    """Insert one complete frame with explicit availability and optionally publish."""
    with write_transaction(conn):
        cursor = conn.execute(
            """
            INSERT INTO snapshot_runs (
                source, started_at, server_started_at, completed_at,
                available_at, status
            ) VALUES ('simulator', ?, ?, ?, ?, 'completed')
            """,
            (
                _ts(minute, second),
                _ts(minute, second),
                _ts(available_minute),
                _ts(available_minute),
            ),
        )
        snapshot_id = int(cursor.lastrowid)
        store_raw_listings(
            conn,
            snapshot_id,
            [
                {
                    "auction_id": auction_id,
                    "item_id": item_id,
                    "price": price,
                    "quantity": quantity,
                    "expires_at": _DAY + 3 * 86_400,
                }
                for auction_id, item_id, price, quantity in listings
            ],
        )
        if publish:
            process_snapshot(conn, snapshot_id, {})
    return snapshot_id


def _rows(conn, table, *, where="", parameters=(), order_by=""):
    columns = [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]
    projection = ", ".join(columns)
    sql = f"SELECT {projection} FROM {table}"
    if where:
        sql += f" WHERE {where}"
    sql += f" ORDER BY {order_by or projection}"
    return [dict(row) for row in conn.execute(sql, parameters)]


def _bar(conn, *, item_id=1, timeframe="15m", bucket_ts=_NOON):
    rows = _rows(
        conn,
        "item_bars",
        where="item_id = ? AND timeframe = ? AND bucket_ts = ?",
        parameters=(item_id, timeframe, bucket_ts),
    )
    assert len(rows) == 1
    return rows[0]


def _revision_at(conn, bucket_ts, as_of_ts, *, item_id=1):
    """Inspect revision availability even while a nominal bar remains open.

    Public query_bars_as_of additionally enforces close_ts <= as_of_ts. The
    adjacent-bucket repro intentionally inspects the still-open 12:30 bar.
    """
    rows = _rows(
        conn,
        "item_bar_revisions",
        where=(
            "item_id = ? AND timeframe = '15m' AND bucket_ts = ? "
            "AND revision_available_at <= ?"
        ),
        parameters=(item_id, bucket_ts, as_of_ts),
        order_by="publish_seq DESC",
    )
    assert rows
    return rows[0]


def _materialized(conn):
    return {table: _rows(conn, table) for table in _MATERIALIZED_TABLES}


def _publication_outputs(conn):
    return {
        table: _rows(conn, table)
        for table in (
            *_MATERIALIZED_TABLES,
            "publish_events",
            "orderbook_observations",
        )
    }


def _assert_publication_blocked(conn, snapshot_id, before):
    changes = conn.total_changes
    assert process_snapshot_diff(conn, snapshot_id, {}) == 0
    assert conn.total_changes == changes
    assert _publication_outputs(conn) == before
    assert conn.execute(
        "SELECT publish_seq FROM snapshot_runs WHERE snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()[0] is None

    process_snapshot(conn, snapshot_id, {})
    assert _publication_outputs(conn) == before
    assert conn.execute(
        "SELECT publish_seq FROM snapshot_runs WHERE snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()[0] is None


def _parents(conn):
    return {
        table: _rows(
            conn,
            table,
            where="timeframe IN ('4h', '1d')",
        )
        for table in ("item_bars", "item_bar_revisions")
    }


def test_cross_boundary_frame_waits_for_actual_availability(db_path):
    with open_connection(db_path) as conn:
        _item(conn)
        snapshot_id = _frame(conn, 14, 17, [(1, 1, 100, 10)])

        assert query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(16), item_id=1
        ) == []
        visible = query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(17), item_id=1
        )
        assert len(visible) == 1
        assert visible[0]["bucket_ts"] == _NOON
        assert visible[0]["close_ts"] == _ts(15)
        assert visible[0]["revision_available_at"] == _ts(17)
        assert visible[0]["sample_count"] == 1
        run = conn.execute(
            """
            SELECT available_at, publish_seq, raw_retained
            FROM snapshot_runs WHERE snapshot_id = ?
            """,
            (snapshot_id,),
        ).fetchone()
        assert tuple(run) == (_ts(17), visible[0]["publish_seq"], 1)


def test_same_bucket_late_frame_does_not_rewrite_earlier_as_of(db_path):
    with open_connection(db_path) as conn:
        _item(conn)
        _frame(conn, 1, 2, [(1, 1, 100, 10)])

        assert query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(10), item_id=1
        ) == []
        before = query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(16), item_id=1
        )
        assert len(before) == 1
        assert before[0]["sample_count"] == 1

        _frame(conn, 14, 17, [(1, 1, 120, 7)])

        assert query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(10), item_id=1
        ) == []
        assert query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(16), item_id=1
        ) == before
        after = query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(17), item_id=1
        )
        assert len(after) == 1
        assert after[0]["sample_count"] == 2
        assert after[0]["open_price"] == 100
        assert after[0]["close_price"] == 120
        assert after[0]["best_ask_min"] == 100
        assert after[0]["total_listed_qty"] == 7


def test_same_second_publications_are_unique_and_sequence_isolates_revision(db_path):
    with open_connection(db_path) as conn:
        _item(conn)
        first = _frame(conn, 1, 5, [(1, 1, 100, 10)])
        second = _frame(conn, 4, 5, [(1, 1, 110, 8)])
        events = _rows(conn, "publish_events", order_by="publish_seq")
        assert len(events) == 2
        assert [event["available_at"] for event in events] == [_ts(5), _ts(5)]
        assert [event["source_type"] for event in events] == ["snapshot", "snapshot"]
        assert [event["source_id"] for event in events] == [first, second]
        first_sequence = events[0]["publish_seq"]
        second_sequence = events[1]["publish_seq"]
        assert second_sequence == first_sequence + 1

        first_view = query_bars_as_of(
            conn,
            timeframe="15m",
            as_of_ts=_ts(15),
            max_publish_seq=first_sequence,
            item_id=1,
        )
        second_view = query_bars_as_of(
            conn,
            timeframe="15m",
            as_of_ts=_ts(15),
            max_publish_seq=second_sequence,
            item_id=1,
        )
        assert first_view[0]["sample_count"] == 1
        assert first_view[0]["close_price"] == 100
        assert second_view[0]["sample_count"] == 2
        assert second_view[0]["close_price"] == 110

        before = _materialized(conn)
        process_snapshot(conn, first, {})
        process_snapshot(conn, second, {})
        assert _materialized(conn) == before
        assert len(_rows(conn, "publish_events")) == 2


def test_late_frame_splits_successor_flow_without_rewriting_old_revision(db_path):
    with open_connection(db_path) as conn:
        _item(conn)
        first = _frame(conn, 10, 11, [(1, 1, 100, 10)])
        successor = _frame(conn, 20, 21, [(1, 1, 100, 4)])
        successor_bucket = _ts(15)
        before = _revision_at(conn, successor_bucket, _ts(22))
        assert before["est_sold_qty_sum"] == 6
        initial_flow = _rows(
            conn,
            "inferred_flows",
            where="snapshot_id = ?",
            parameters=(successor,),
        )[0]
        assert initial_flow["prev_snapshot_id"] == first
        assert initial_flow["partial_fill_qty"] == 6

        late = _frame(conn, 14, 25, [(1, 1, 100, 7)])
        late_sequence = conn.execute(
            "SELECT publish_seq FROM snapshot_runs WHERE snapshot_id = ?",
            (late,),
        ).fetchone()[0]

        assert _revision_at(conn, successor_bucket, _ts(22)) == before
        earlier = _revision_at(conn, _NOON, _ts(25))
        later = _revision_at(conn, successor_bucket, _ts(25))
        assert earlier["est_sold_qty_sum"] == 3
        assert later["est_sold_qty_sum"] == 3
        assert earlier["publish_seq"] == later["publish_seq"] == late_sequence
        assert earlier["revision_available_at"] == _ts(25)
        assert later["revision_available_at"] == _ts(25)

        revisions = _rows(
            conn,
            "inferred_flow_revisions",
            where="snapshot_id = ? AND item_id = 1",
            parameters=(successor,),
            order_by="publish_seq",
        )
        assert [row["partial_fill_qty"] for row in revisions] == [6, 3]
        assert revisions[-1]["prev_snapshot_id"] == late
        assert revisions[-1]["publish_seq"] == late_sequence
        assert revisions[-1]["revision_available_at"] == _ts(25)
        assert revisions[0]["revision_available_at"] == _ts(21)

        # Revision visibility and the closed-bar gate are separate requirements.
        closed_at_22 = query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(22), item_id=1
        )
        assert [row["bucket_ts"] for row in closed_at_22] == [_NOON]
        closed_at_30 = query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(30), item_id=1
        )
        assert [row["est_sold_qty_sum"] for row in closed_at_30] == [3, 3]

        for timeframe in ("4h", "1d"):
            parent = _bar(
                conn,
                timeframe=timeframe,
                bucket_ts=(
                    _NOON - _NOON % 14_400
                    if timeframe == "4h"
                    else _DAY
                ),
            )
            assert parent["est_sold_qty_sum"] == 6
            assert parent["sample_count"] == 3
            assert parent["latest_publish_seq"] == late_sequence


@pytest.mark.parametrize("retained", [0, None], ids=["pruned", "legacy-null"])
def test_late_frame_with_unretained_predecessor_has_zero_publication_writes(
    db_path, retained
):
    with open_connection(db_path) as conn:
        _item(conn, watched=True)
        first = _frame(conn, 10, 11, [(1, 1, 100, 10)])
        successor = _frame(conn, 20, 21, [(1, 1, 100, 4)])
        assert _bar(conn, bucket_ts=_ts(15))["est_sold_qty_sum"] == 6
        with write_transaction(conn):
            conn.execute(
                "DELETE FROM raw_listings WHERE snapshot_id = ?", (first,)
            )
            conn.execute(
                "UPDATE snapshot_runs SET raw_retained = ? WHERE snapshot_id = ?",
                (retained, first),
            )
        before = _publication_outputs(conn)

        late = _frame(conn, 16, 25, [(1, 1, 100, 7)], publish=False)
        assert check_frames_retained(conn, [late, successor])
        assert not check_frames_retained(conn, [first, late])
        _assert_publication_blocked(conn, late, before)

        flow = _rows(
            conn,
            "inferred_flows",
            where="snapshot_id = ?",
            parameters=(successor,),
        )[0]
        assert flow["prev_snapshot_id"] == first
        assert flow["partial_fill_qty"] == flow["est_sold_qty"] == 6
        assert _bar(conn, bucket_ts=_ts(15))["est_sold_qty_sum"] == 6


@pytest.mark.parametrize(
    ("timeframe", "unsafe_minute"),
    [("4h", 1), ("1d", -239)],
)
def test_late_frame_requires_replayable_materialized_parent_before_any_writes(
    db_path, timeframe, unsafe_minute
):
    with open_connection(db_path) as conn:
        _item(conn, watched=True)
        unsafe = _frame(
            conn, unsafe_minute, unsafe_minute + 1, [(1, 1, 100, 12)]
        )
        previous = _frame(conn, 10, 11, [(1, 1, 100, 10)])
        successor = _frame(conn, 20, 21, [(1, 1, 100, 4)])
        with write_transaction(conn):
            conn.execute(
                "DELETE FROM raw_listings WHERE snapshot_id = ?", (unsafe,)
            )
            conn.execute(
                "UPDATE snapshot_runs SET raw_retained = 0 WHERE snapshot_id = ?",
                (unsafe,),
            )
            # Keep only the parent timeframe under test, so each guard must
            # independently prevent a partial publication.
            for table in ("item_bars", "item_bar_revisions"):
                conn.execute(
                    f"DELETE FROM {table} "
                    "WHERE timeframe IN ('4h', '1d') AND timeframe != ?",
                    (timeframe,),
                )
        before = _publication_outputs(conn)

        late = _frame(conn, 16, 25, [(1, 1, 100, 7)], publish=False)
        assert check_frames_retained(conn, [previous, late, successor])
        _assert_publication_blocked(conn, late, before)


@pytest.mark.parametrize("retained", [0, None], ids=["pruned", "legacy-null"])
def test_in_order_tail_with_old_unretained_predecessor_publishes_current_book(
    db_path, retained
):
    with open_connection(db_path) as conn:
        _item(conn, watched=True)
        first = _frame(conn, 10, 11, [(1, 1, 100, 10)])
        with write_transaction(conn):
            conn.execute(
                "DELETE FROM raw_listings WHERE snapshot_id = ?", (first,)
            )
            conn.execute(
                "UPDATE snapshot_runs SET raw_retained = ? WHERE snapshot_id = ?",
                (retained, first),
            )

        minute = 3 * 24 * 60 + 10
        tail = _frame(conn, minute, minute + 1, [(1, 1, 120, 4)])
        assert _rows(
            conn, "inferred_flows", where="snapshot_id = ?", parameters=(tail,)
        ) == []
        run = conn.execute(
            "SELECT publish_seq FROM snapshot_runs WHERE snapshot_id = ?",
            (tail,),
        ).fetchone()
        assert run["publish_seq"] is not None
        assert len(_rows(conn, "publish_events")) == 2
        observation = _rows(
            conn,
            "orderbook_observations",
            where="snapshot_id = ? AND item_id = 1",
            parameters=(tail,),
        )[0]
        assert observation["coverage_hours"] == 0
        assert json.loads(observation["ladder_json"])[0]["qty"] == 4
        for timeframe, seconds in (("15m", 900), ("4h", 14_400), ("1d", 86_400)):
            timestamp = _ts(minute)
            bar = _bar(
                conn,
                timeframe=timeframe,
                bucket_ts=timestamp - timestamp % seconds,
            )
            assert bar["sample_count"] == 1
            assert bar["total_listed_qty"] == 4
            assert bar["close_price"] == 120
            assert bar["est_sold_qty_sum"] == 0
            assert bar["latest_publish_seq"] == run["publish_seq"]


@pytest.mark.parametrize("retained", [0, None], ids=["pruned", "legacy-null"])
def test_unsafe_first_frame_preserves_all_materialized_tables_on_replay(
    db_path, retained
):
    with open_connection(db_path) as conn:
        _item(conn, watched=True)
        first = _frame(conn, 1, 2, [(1, 1, 100, 10)])
        second = _frame(conn, 5, 6, [(1, 1, 100, 8)])
        third = _frame(conn, 10, 11, [(1, 1, 100, 6)])
        assert check_frames_retained(conn, [first, second, third])
        before = _materialized(conn)

        with write_transaction(conn):
            conn.execute(
                "DELETE FROM raw_listings WHERE snapshot_id = ?", (first,)
            )
            conn.execute(
                "UPDATE snapshot_runs SET raw_retained = ? WHERE snapshot_id = ?",
                (retained, first),
            )
        assert not check_frames_retained(conn, [first, second, third])
        assert process_snapshot_diff(conn, second, {}) == 0
        assert process_snapshot_diff(conn, third, {}) == 0
        for snapshot_id in (first, second, third):
            process_snapshot(conn, snapshot_id, {})
        assert _materialized(conn) == before

        observation = _rows(
            conn,
            "orderbook_observations",
            where="snapshot_id = ? AND item_id = 1",
            parameters=(first,),
        )[0]
        assert json.loads(observation["ladder_json"])[0]["qty"] == 10


@pytest.mark.parametrize("retained", [0, None], ids=["pruned", "legacy-null"])
def test_parent_window_guard_preserves_parent_when_other_child_is_unsafe(
    db_path, retained
):
    with open_connection(db_path) as conn:
        _item(conn, watched=True)
        first = _frame(conn, 1, 2, [(1, 1, 100, 10)])
        _frame(conn, 20, 21, [(1, 1, 110, 8)])
        _frame(conn, 40, 41, [(1, 1, 120, 6)])
        before = _parents(conn)

        with write_transaction(conn):
            conn.execute(
                "DELETE FROM raw_listings WHERE snapshot_id = ?", (first,)
            )
            conn.execute(
                "UPDATE snapshot_runs SET raw_retained = ? WHERE snapshot_id = ?",
                (retained, first),
            )
        _frame(conn, 41, 42, [(1, 1, 130, 4)])

        # This 15m bucket is fully retained and can advance independently.
        child = _bar(conn, bucket_ts=_ts(30))
        assert child["total_listed_qty"] == 4
        assert child["sample_count"] == 2
        assert child["close_price"] == 130
        # Both larger windows contain the unsafe first child.
        assert _parents(conn) == before


def test_mixed_empty_and_uncovered_monitored_items(db_path):
    with open_connection(db_path) as conn:
        _item(conn, 1, watched=True)
        _item(conn, 2, watched=True)
        _item(
            conn,
            900001,
            watched=True,
            api_covered=False,
            asset_class="pokemon_category",
        )
        snapshots = [
            _frame(conn, 1, 2, [(1, 1, 100, 10)]),
            _frame(conn, 10, 11),
            _frame(conn, 11, 12),
        ]

        mixed = _bar(conn, item_id=1)
        assert mixed["book_state"] == "mixed_ended_empty"
        assert mixed["sample_count"] == 3
        assert [
            mixed[column]
            for column in ("open_price", "high_price", "low_price", "close_price")
        ] == [100, 100, 100, 100]
        assert mixed["best_ask_min"] == 100
        for column in ("robust_best_ask", "p10_price", "spread_top5_pct"):
            assert mixed[column] is None
        for column in ("total_listed_qty", "listing_count", "depth_within_5pct"):
            assert mixed[column] == 0
        assert mixed["est_sold_qty_sum"] == 7.5

        empty = _bar(conn, item_id=2)
        assert empty["book_state"] == "empty_confirmed"
        assert empty["sample_count"] == 3
        for column in (
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "robust_best_ask",
            "best_ask_min",
            "p10_price",
            "spread_top5_pct",
        ):
            assert empty[column] is None
        for column in ("total_listed_qty", "listing_count", "depth_within_5pct"):
            assert empty[column] == 0

        empty_observations = _rows(
            conn,
            "orderbook_observations",
            where="item_id = 2",
            order_by="snapshot_id",
        )
        assert [row["snapshot_id"] for row in empty_observations] == snapshots
        assert [row["book_state"] for row in empty_observations] == [
            "empty_confirmed",
            "empty_confirmed",
            "empty_confirmed",
        ]
        empty_revisions = _rows(
            conn,
            "item_bar_revisions",
            where="item_id = 2 AND timeframe = '15m'",
            order_by="publish_seq",
        )
        assert len(empty_revisions) == 3
        assert all(row["book_state"] == "empty_confirmed" for row in empty_revisions)
        assert _rows(conn, "item_bars", where="item_id = 900001") == []
        assert _rows(
            conn, "orderbook_observations", where="item_id = 900001"
        ) == []


def test_parent_states_follow_last_child_and_keep_historical_prices(db_path):
    with open_connection(db_path) as conn:
        _item(conn, 1, watched=True)
        _item(conn, 2, watched=True)
        _frame(conn, 1, 2, [(1, 1, 120, 10)])
        _frame(conn, 20, 21)

        for timeframe, seconds in (("4h", 14_400), ("1d", 86_400)):
            parent = _bar(
                conn,
                timeframe=timeframe,
                bucket_ts=_NOON - _NOON % seconds,
            )
            assert parent["book_state"] == "mixed_ended_empty"
            assert parent["open_price"] == parent["close_price"] == 120
            assert parent["robust_best_ask"] is None
            assert parent["total_listed_qty"] == 0

        _frame(conn, 35, 36, [(2, 1, 100, 5)])
        for timeframe, seconds in (("4h", 14_400), ("1d", 86_400)):
            parent = _bar(
                conn,
                timeframe=timeframe,
                bucket_ts=_NOON - _NOON % seconds,
            )
            assert parent["book_state"] == "stocked"
            assert parent["open_price"] == parent["high_price"] == 120
            assert parent["close_price"] == parent["low_price"] == 100
            assert parent["best_ask_min"] == 100
            assert parent["robust_best_ask"] == 100
            assert parent["total_listed_qty"] == 5
            assert parent["sample_count"] == 3

        _frame(conn, 50, 51)
        for timeframe, seconds in (("4h", 14_400), ("1d", 86_400)):
            parent = _bar(
                conn,
                timeframe=timeframe,
                bucket_ts=_NOON - _NOON % seconds,
            )
            assert parent["book_state"] == "mixed_ended_empty"
            assert parent["open_price"] == 120
            assert parent["close_price"] == 100
            assert parent["best_ask_min"] == 100
            assert parent["robust_best_ask"] is None
            assert parent["p10_price"] is None
            assert parent["spread_top5_pct"] is None
            assert parent["total_listed_qty"] == 0
            assert parent["sample_count"] == 4

            empty_parent = _bar(
                conn,
                item_id=2,
                timeframe=timeframe,
                bucket_ts=_NOON - _NOON % seconds,
            )
            assert empty_parent["book_state"] == "empty_confirmed"
            assert empty_parent["open_price"] is None
            assert empty_parent["close_price"] is None
            assert empty_parent["total_listed_qty"] == 0


def test_open_position_extends_covered_empty_item_set(db_path):
    with open_connection(db_path) as conn:
        _item(conn, 3)
        _item(
            conn,
            900002,
            api_covered=False,
            asset_class="pokemon_category",
        )
        with write_transaction(conn):
            for position_id, item_id in ((1, 3), (2, 900002)):
                conn.execute(
                    """
                    INSERT INTO user_positions (
                        position_id, arm_id, item_id, initial_qty, remaining_qty,
                        unallocated_qty, total_cost, remaining_cost, status
                    ) VALUES (?, 'TEST', ?, 1, 1, 1, 100, 100, 'OPEN')
                    """,
                    (position_id, item_id),
                )
        _frame(conn, 1, 2)
        assert _bar(conn, item_id=3)["book_state"] == "empty_confirmed"
        assert _rows(conn, "item_bars", where="item_id = 900002") == []
        assert _rows(
            conn, "orderbook_observations", where="item_id = 900002"
        ) == []


def test_nominal_ties_use_snapshot_id_and_undercuts_count_new_auction_ids(db_path):
    with open_connection(db_path) as conn:
        _item(conn)
        first = _frame(conn, 1, 2, [(1, 1, 100, 10)])
        second = _frame(
            conn,
            1,
            3,
            [
                (1, 1, 100, 8),
                (2, 1, 90, 1),
                (3, 1, 99, 1),
                (4, 1, 101, 1),
            ],
        )
        flow = _rows(
            conn,
            "inferred_flows",
            where="snapshot_id = ?",
            parameters=(second,),
        )[0]
        assert flow["prev_snapshot_id"] == first
        assert flow["interval_sec"] == 0
        assert flow["gap_skipped"] == 1
        assert flow["partial_fill_qty"] == 2
        assert flow["observed_undercut_count"] == 2
        revision = _rows(
            conn,
            "inferred_flow_revisions",
            where="snapshot_id = ?",
            parameters=(second,),
        )[0]
        assert revision["observed_undercut_count"] == 2


def test_retention_helper_checks_missing_status_duplicates_zero_and_null(db_path):
    with open_connection(db_path) as conn:
        _item(conn)
        first = _frame(conn, 1, 2, [(1, 1, 100, 10)])
        second = _frame(conn, 5, 6, [(1, 1, 100, 8)])
        third = _frame(conn, 10, 11, [(1, 1, 100, 6)])
        assert not check_frames_retained(conn, [])
        assert check_frames_retained(conn, [first, first, second, third])
        assert not check_frames_retained(conn, [first, second, third + 100])

        with write_transaction(conn):
            cursor = conn.execute(
                """
                INSERT INTO snapshot_runs (
                    source, started_at, server_started_at, status
                ) VALUES ('simulator', ?, ?, 'failed')
                """,
                (_ts(12), _ts(12)),
            )
            failed = int(cursor.lastrowid)
        assert not check_frames_retained(conn, [first, second, failed])

        for retained in (0, None):
            with write_transaction(conn):
                conn.execute(
                    """
                    UPDATE snapshot_runs SET raw_retained = ?
                    WHERE snapshot_id = ?
                    """,
                    (retained, first),
                )
            assert not check_frames_retained(conn, [first, second, third])


def test_pruning_marks_frame_and_preserves_full_lot_evidence(db_path):
    with open_connection(db_path) as conn:
        _item(conn, purchase_mode="bundle_only")
        first = _frame(
            conn, 1, 2, [(1, 1, 100, 60), (2, 1, 100, 39)]
        )
        second = _frame(conn, 5, 6, [(1, 1, 100, 50)])
        third = _frame(conn, 10, 11, [(1, 1, 100, 40)])
        now_ts = _DAY + 4 * 86_400
        before = _materialized(conn)

        assert prune_raw_listings(
            conn, now_ts=now_ts, retention_hours=1
        ) == 2
        run = conn.execute(
            """
            SELECT raw_retained, raw_pruned_at FROM snapshot_runs
            WHERE snapshot_id = ?
            """,
            (first,),
        ).fetchone()
        assert tuple(run) == (0, now_ts)
        assert not check_frames_retained(conn, [first, second, third])
        assert check_frames_retained(conn, [second, third])
        observation = _rows(
            conn,
            "orderbook_observations",
            where="snapshot_id = ? AND item_id = 1",
            parameters=(first,),
        )[0]
        assert observation["purchase_mode"] == "bundle_only"
        ladder = json.loads(observation["ladder_json"])
        assert [lot["qty"] for lot in ladder] == [60, 39]
        assert [lot["auction_id"] for lot in ladder] == [1, 2]
        assert _materialized(conn) == before


def test_retention_marker_failure_rolls_back_raw_delete(db_path):
    with open_connection(db_path) as conn:
        _item(conn)
        first = _frame(conn, 1, 2, [(1, 1, 100, 10)])
        _frame(conn, 5, 6, [(1, 1, 100, 8)])
        _frame(conn, 10, 11, [(1, 1, 100, 6)])
        with write_transaction(conn):
            conn.execute(
                """
                CREATE TRIGGER reject_retention_marker
                BEFORE UPDATE OF raw_retained ON snapshot_runs
                WHEN NEW.raw_retained = 0
                BEGIN
                    SELECT RAISE(ABORT, 'retention marker rejected');
                END
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="retention marker rejected"):
            prune_raw_listings(
                conn, now_ts=_DAY + 4 * 86_400, retention_hours=1
            )
        assert conn.execute(
            "SELECT COUNT(*) FROM raw_listings WHERE snapshot_id = ?",
            (first,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT raw_retained FROM snapshot_runs WHERE snapshot_id = ?",
            (first,),
        ).fetchone()[0] == 1


def test_missing_available_at_uses_completion_time_and_saves_publication(db_path):
    with open_connection(db_path) as conn:
        _item(conn)
        with write_transaction(conn):
            cursor = conn.execute(
                """
                INSERT INTO snapshot_runs (
                    source, started_at, server_started_at, completed_at, status
                ) VALUES ('simulator', ?, ?, ?, 'completed')
                """,
                (_ts(14), _ts(14), _ts(17)),
            )
            snapshot_id = int(cursor.lastrowid)
            store_raw_listings(
                conn,
                snapshot_id,
                [
                    {
                        "auction_id": 1,
                        "item_id": 1,
                        "price": 100,
                        "quantity": 10,
                        "expires_at": _DAY + 86_400,
                    }
                ],
            )
        process_snapshot(conn, snapshot_id, {})
        run = conn.execute(
            """
            SELECT available_at, publish_seq, raw_retained
            FROM snapshot_runs WHERE snapshot_id = ?
            """,
            (snapshot_id,),
        ).fetchone()
        assert run["available_at"] == _ts(17)
        assert run["raw_retained"] == 1
        event = _rows(
            conn,
            "publish_events",
            where="publish_seq = ?",
            parameters=(run["publish_seq"],),
        )[0]
        assert event["available_at"] == _ts(17)
        assert event["source_id"] == snapshot_id
        assert query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_ts(16)
        ) == []
        assert len(
            query_bars_as_of(conn, timeframe="15m", as_of_ts=_ts(17))
        ) == 1


def test_001_only_database_keeps_original_pipeline_and_retention_usable():
    migration = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "pokemmo_gtl"
        / "db"
        / "migrations"
        / "001_init.sql"
    )
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(migration.read_text(encoding="utf-8"))
        snapshots = []
        with write_transaction(conn):
            conn.execute(
                """
                INSERT INTO items_catalog (item_id, name_en, updated_at)
                VALUES (1, 'Legacy Item', ?)
                """,
                (_DAY,),
            )
            for minute, quantity in ((1, 10), (5, 8), (10, 6)):
                cursor = conn.execute(
                    """
                    INSERT INTO snapshot_runs (
                        source, started_at, server_started_at, completed_at, status
                    ) VALUES ('simulator', ?, ?, ?, 'completed')
                    """,
                    (_ts(minute), _ts(minute), _ts(minute)),
                )
                snapshot_id = int(cursor.lastrowid)
                snapshots.append(snapshot_id)
                store_raw_listings(
                    conn,
                    snapshot_id,
                    [
                        {
                            "auction_id": 1,
                            "item_id": 1,
                            "price": 100,
                            "quantity": quantity,
                            "expires_at": _DAY + 86_400,
                        }
                    ],
                )
                process_snapshot(conn, snapshot_id, {})

        bars = _rows(conn, "item_bars")
        assert len(bars) == 3
        assert all(row["sample_count"] == 3 for row in bars)
        assert all(row["est_sold_qty_sum"] == 4 for row in bars)
        assert not check_frames_retained(conn, snapshots)
        assert query_bars_as_of(
            conn, timeframe="15m", as_of_ts=_DAY + 86_400
        ) == []
        assert prune_raw_listings(
            conn, now_ts=_DAY + 4 * 86_400, retention_hours=1
        ) == 1
        remaining = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT snapshot_id FROM raw_listings ORDER BY snapshot_id"
            )
        ]
        assert remaining == snapshots[-2:]
        assert list(conn.execute("PRAGMA foreign_key_check")) == []
    finally:
        conn.close()
