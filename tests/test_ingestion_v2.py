"""V2 ingestion watermarks, API coverage and simulation isolation contracts."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from threading import Event

import httpx
import pytest

from pokemmo_gtl.config import PROJECT_ROOT
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.ingestion.catalog_sync import sync_catalog
from pokemmo_gtl.ingestion.official_api import (
    ENDPOINT,
    ApiError,
    OfficialApiCollector,
    SnapshotResult,
)
from pokemmo_gtl.ingestion.simulator import (
    SECONDS_PER_DAY,
    SNAPSHOT_INTERVAL,
    _OrderBook,
    simulate,
)

TOKEN = "abcdef0123456789" * 4
COLLECTION_START_TS = int(datetime(2026, 11, 26, 12, 14, 10, tzinfo=timezone.utc).timestamp())
LAST_PAGE_TS = int(datetime(2026, 11, 26, 12, 17, 5, tzinfo=timezone.utc).timestamp())
SIMULATION_END_TS = 1_768_435_200


class FakeClock:
    def __init__(self, now=COLLECTION_START_TS):
        self.now = float(now)

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert seconds >= 0
        self.now += seconds


def _http_date(timestamp):
    return format_datetime(datetime.fromtimestamp(timestamp, timezone.utc), usegmt=True)


def _listing(auction_id, item_id=77):
    return {
        "auction_id": auction_id,
        "reference_id": item_id,
        "price": 100,
        "quantity": 2,
        "expires_at": 2_000_000_000,
    }


def _create_v1_database(path: Path) -> None:
    directory = PROJECT_ROOT / "src" / "pokemmo_gtl" / "db" / "migrations"
    migrations = sorted(directory.glob("001*.sql"))
    assert len(migrations) == 1
    with open_connection(path) as conn:
        conn.executescript(migrations[0].read_text(encoding="utf-8"))


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "ingestion-v2.db"
    migrate(path)
    return path


@pytest.fixture
def small_simulation_book(monkeypatch):
    def frame(self, captured_at, start_ts):
        return [(captured_at, 4, 100, 2, captured_at + SECONDS_PER_DAY)]

    monkeypatch.setattr(_OrderBook, "frame", frame)


@pytest.mark.parametrize("terminal_empty", [False, True])
@pytest.mark.parametrize("completion_offset", [-5, 5])
def test_official_api_watermarks_precede_hook_and_match_published_revisions(
    respx_mock, db_path, terminal_empty, completion_offset,
):
    clock = FakeClock()
    completed_at = LAST_PAGE_TS + completion_offset
    expected_available_at = max(completed_at, LAST_PAGE_TS)
    total_listings = 1 if terminal_empty else 2
    calls = 0
    observed = []

    def respond(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            assert dict(request.url.params) == {"count": "1000"}
            return httpx.Response(
                200,
                json=[_listing(2)],
                headers={"Date": _http_date(COLLECTION_START_TS)},
            )
        assert calls == 2
        assert dict(request.url.params) == {"count": "1000", "skip": "1"}
        clock.now = float(completed_at)
        return httpx.Response(
            200,
            json=[] if terminal_empty else [_listing(1)],
            headers={"Date": _http_date(LAST_PAGE_TS)},
        )

    def publish(conn, snapshot_id):
        assert conn.in_transaction
        row = conn.execute(
            "SELECT status, server_started_at, last_page_server_at, available_at, "
            "raw_retained, total_listings, pages_fetched "
            "FROM snapshot_runs WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        assert tuple(row) == (
            "running", COLLECTION_START_TS, LAST_PAGE_TS, expected_available_at,
            1, total_listings, 2,
        )
        assert conn.execute(
            "SELECT COUNT(*) FROM raw_listings WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()[0] == total_listings
        publish_seq = conn.execute(
            "INSERT INTO publish_events (available_at, source_type, source_id) "
            "VALUES (?, 'snapshot', ?)",
            (row["available_at"], snapshot_id),
        ).lastrowid
        conn.execute(
            "UPDATE snapshot_runs SET publish_seq = ? WHERE snapshot_id = ?",
            (publish_seq, snapshot_id),
        )
        bucket = COLLECTION_START_TS - COLLECTION_START_TS % SNAPSHOT_INTERVAL
        conn.execute(
            """INSERT INTO item_bar_revisions
               (item_id, timeframe, bucket_ts, publish_seq, close_ts,
                revision_available_at, open_price, high_price, low_price, close_price)
               VALUES (77, '15m', ?, ?, ?, ?, 100, 100, 100, 100)""",
            (bucket, publish_seq, bucket + SNAPSHOT_INTERVAL, row["available_at"]),
        )
        observed.append(row["available_at"])
        # A slow hook must not move the snapshot beyond the versions it published.
        clock.now += 10

    route = respx_mock.get(ENDPOINT).mock(side_effect=respond)
    with OfficialApiCollector(
        db_path, TOKEN, clock=clock, sleeper=clock.sleep,
        jitter=lambda: 0, post_snapshot_hook=publish,
    ) as collector:
        result = collector.collect_once()

    assert result == SnapshotResult(
        result.snapshot_id, COLLECTION_START_TS, total_listings, 2
    )
    assert route.call_count == 2
    assert observed == [expected_available_at]
    with open_connection(db_path) as conn:
        snapshot = conn.execute(
            "SELECT status, server_started_at, last_page_server_at, available_at, "
            "raw_retained, completed_at FROM snapshot_runs WHERE snapshot_id = ?",
            (result.snapshot_id,),
        ).fetchone()
        assert tuple(snapshot) == (
            "completed", COLLECTION_START_TS, LAST_PAGE_TS,
            expected_available_at, 1, completed_at,
        )
        assert snapshot["available_at"] >= LAST_PAGE_TS
        assert conn.execute(
            "SELECT revision_available_at FROM item_bar_revisions"
        ).fetchone()[0] == snapshot["available_at"]
        assert conn.execute(
            "SELECT available_at FROM publish_events"
        ).fetchone()[0] == snapshot["available_at"]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize("date_header", [None, "invalid-date"])
def test_official_api_each_page_uses_its_own_clock_fallback(
    respx_mock, db_path, date_header,
):
    clock = FakeClock()
    completed_at = LAST_PAGE_TS + 5
    headers = {} if date_header is None else {"Date": date_header}
    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json=[_listing(2)], headers=headers)
        clock.now = float(completed_at)
        return httpx.Response(200, json=[], headers=headers)

    respx_mock.get(ENDPOINT).mock(side_effect=respond)
    with OfficialApiCollector(
        db_path, TOKEN, clock=clock, sleeper=clock.sleep, jitter=lambda: 0,
    ) as collector:
        result = collector.collect_once()

    assert result.server_started_at == COLLECTION_START_TS
    assert result.pages_fetched == 2
    with open_connection(db_path) as conn:
        assert tuple(conn.execute(
            "SELECT server_started_at, last_page_server_at, available_at, raw_retained "
            "FROM snapshot_runs WHERE snapshot_id = ?",
            (result.snapshot_id,),
        ).fetchone()) == (COLLECTION_START_TS, completed_at, completed_at, 1)


@pytest.mark.parametrize("legacy", [False, True])
def test_official_api_unknown_item_coverage_and_001_compatibility(
    respx_mock, tmp_path, legacy,
):
    path = tmp_path / "unknown-items.db"
    if legacy:
        _create_v1_database(path)
    else:
        migrate(path)
    clock = FakeClock()
    respx_mock.get(ENDPOINT).respond(
        200,
        json=[_listing(2, 77), _listing(1, 900999)],
        headers={"Date": _http_date(COLLECTION_START_TS)},
    )
    with OfficialApiCollector(
        path, TOKEN, clock=clock, sleeper=clock.sleep, jitter=lambda: 0,
    ) as collector:
        result = collector.collect_once()

    assert result.total_listings == 2
    with open_connection(path) as conn:
        assert conn.execute(
            "SELECT status FROM snapshot_runs WHERE snapshot_id = ?",
            (result.snapshot_id,),
        ).fetchone()[0] == "completed"
        assert [tuple(row) for row in conn.execute(
            "SELECT item_id, name_en FROM items_catalog ORDER BY item_id"
        )] == [(77, "Unknown Item #77"), (900999, "Unknown Item #900999")]
        columns = {row[1] for row in conn.execute("PRAGMA table_info(items_catalog)")}
        if legacy:
            assert "api_covered" not in columns
        else:
            assert [tuple(row) for row in conn.execute(
                "SELECT item_id, api_covered FROM items_catalog ORDER BY item_id"
            )] == [(77, 1), (900999, 0)]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize("failure", ["http", "hook"])
def test_official_api_failed_snapshot_has_no_retained_raw(
    respx_mock, db_path, failure,
):
    clock = FakeClock()

    def fail_hook(conn, snapshot_id):
        assert conn.execute(
            "SELECT raw_retained FROM snapshot_runs WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()[0] == 1
        raise RuntimeError("hook failed")

    if failure == "http":
        respx_mock.get(ENDPOINT).respond(403)
    else:
        respx_mock.get(ENDPOINT).respond(200, json=[_listing(1)])
    with OfficialApiCollector(
        db_path, TOKEN, clock=clock, sleeper=clock.sleep, jitter=lambda: 0,
        post_snapshot_hook=fail_hook if failure == "hook" else None,
    ) as collector, pytest.raises(ApiError):
        collector.collect_once()

    with open_connection(db_path) as conn:
        assert tuple(conn.execute(
            "SELECT status, raw_retained, total_listings, pages_fetched FROM snapshot_runs"
        ).fetchone()) == ("failed", 0, 0, 0)
        assert conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM items_catalog").fetchone()[0] == 0


def test_official_api_still_rejects_hook_changes_to_running_status(
    respx_mock, db_path,
):
    clock = FakeClock()

    def change_status(conn, snapshot_id):
        conn.execute(
            "UPDATE snapshot_runs SET status = 'failed' WHERE snapshot_id = ?",
            (snapshot_id,),
        )

    respx_mock.get(ENDPOINT).respond(200, json=[_listing(1)])
    with OfficialApiCollector(
        db_path, TOKEN, clock=clock, sleeper=clock.sleep, jitter=lambda: 0,
        post_snapshot_hook=change_status,
    ) as collector, pytest.raises(ApiError, match="Snapshot no longer has running status"):
        collector.collect_once()

    with open_connection(db_path) as conn:
        assert tuple(conn.execute(
            "SELECT status, raw_retained FROM snapshot_runs"
        ).fetchone()) == ("failed", 0)
        assert conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0] == 0


def test_catalog_sync_coverage_contract_applies_to_inserts_and_updates(
    db_path, tmp_path,
):
    local = tmp_path / "catalog"
    local.mkdir()
    (local / "items.json").write_text(
        json.dumps([
            {
                "item_id": 810001,
                "name": "Pokemon Category Below Synthetic Range",
                "asset_class": "pokemon_category",
                "max_stack_size": 1,
                "api_covered": 1,
            },
            {
                "item_id": 900999,
                "name": "Synthetic Item Without Pokemon Asset Class",
                "asset_class": "item",
                "api_covered": 1,
            },
            {
                "item_id": 810002,
                "name": "Normal Covered Item",
                "asset_class": "item",
            },
            {
                "item_id": 810003,
                "name": "Explicitly Uncovered Normal Item",
                "asset_class": "item",
                "api_covered": 0,
            },
        ]),
        encoding="utf-8",
    )
    expected = [(810001, 0), (810002, 1), (810003, 0), (900999, 0)]
    sync_catalog(db_path, env="sim", from_local=local)
    with open_connection(db_path) as conn, write_transaction(conn):
        assert [tuple(row) for row in conn.execute(
            "SELECT item_id, api_covered FROM items_catalog "
            "WHERE item_id IN (810001, 810002, 810003, 900999) ORDER BY item_id"
        )] == expected
        assert conn.execute(
            "SELECT api_covered FROM items_catalog WHERE item_id = 4"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM items_catalog "
            "WHERE (asset_class = 'pokemon_category' OR item_id >= 900000) "
            "AND api_covered != 0"
        ).fetchone()[0] == 0
        conn.execute(
            "UPDATE items_catalog SET api_covered = 1 - api_covered "
            "WHERE item_id IN (810001, 810002, 810003, 900999)"
        )

    sync_catalog(db_path, env="sim", from_local=local)
    with open_connection(db_path) as conn:
        assert [tuple(row) for row in conn.execute(
            "SELECT item_id, api_covered FROM items_catalog "
            "WHERE item_id IN (810001, 810002, 810003, 900999) ORDER BY item_id"
        )] == expected


def test_catalog_sync_falls_back_for_001_only_schema(tmp_path):
    path = tmp_path / "catalog-v1.db"
    _create_v1_database(path)
    assert sync_catalog(path, env="sim", offline_only=True) == 43
    assert sync_catalog(path, env="sim", offline_only=True) == 43
    with open_connection(path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(items_catalog)")}
        assert "api_covered" not in columns
        assert conn.execute("SELECT COUNT(*) FROM items_catalog").fetchone()[0] == 43
        assert conn.execute(
            "SELECT max_stack_size FROM items_catalog WHERE item_id = 900003"
        ).fetchone()[0] == 1


def test_simulated_watermarks_are_visible_before_callback(
    tmp_path, small_simulation_book,
):
    path = tmp_path / "watermarks.db"
    observed = []

    def observe(conn, snapshot_id, previous_id, captured_at):
        assert conn.in_transaction
        assert previous_id == (observed[-1] if observed else None)
        assert tuple(conn.execute(
            "SELECT status, server_started_at, last_page_server_at, available_at, "
            "raw_retained FROM snapshot_runs WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()) == ("running", captured_at, captured_at, captured_at, 1)
        observed.append(snapshot_id)

    result = simulate(
        path, env="sim", days=1, end_ts=SIMULATION_END_TS, on_snapshot=observe,
    )
    assert len(observed) == result.snapshots == 96
    with open_connection(path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM snapshot_runs WHERE status = 'completed' "
            "AND server_started_at = last_page_server_at "
            "AND server_started_at = available_at AND raw_retained = 1"
        ).fetchone()[0] == 96


@pytest.mark.parametrize("end_offset", [0, SNAPSHOT_INTERVAL, SECONDS_PER_DAY - 900])
def test_simulation_rejects_overlapping_completed_windows(
    tmp_path, small_simulation_book, end_offset,
):
    path = tmp_path / "overlap.db"
    simulate(path, env="sim", days=1, end_ts=SIMULATION_END_TS)
    with pytest.raises(
        ValueError,
        match="Overlapping simulation window requires reset=True or an isolated db_path",
    ):
        simulate(
            path, env="sim", days=1, end_ts=SIMULATION_END_TS + end_offset,
        )
    with open_connection(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshot_runs").fetchone()[0] == 96
        assert conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0] == 96
        assert conn.execute(
            "SELECT COUNT(*) FROM snapshot_runs WHERE status != 'completed'"
        ).fetchone()[0] == 0


@pytest.mark.parametrize("started_offset", [-1800, 0], ids=["boundary", "fresh"])
def test_simulation_rejects_active_running_overlap_and_rolls_back_stale_cleanup(
    db_path, small_simulation_book, started_offset,
):
    first_captured_at = SIMULATION_END_TS - SECONDS_PER_DAY
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, updated_at) "
            "VALUES (77, 'Running Item', ?)",
            (first_captured_at,),
        )
        stale_id = conn.execute(
            """
            INSERT INTO snapshot_runs (
                source, started_at, server_started_at, status, raw_retained
            ) VALUES ('simulator', ?, ?, 'running', 1)
            """,
            (first_captured_at - 1801, first_captured_at - SNAPSHOT_INTERVAL),
        ).lastrowid
        conn.execute(
            """
            INSERT INTO raw_listings (
                snapshot_id, auction_id, item_id, price, quantity, expires_at
            ) VALUES (?, 1, 77, 100, 1, ?)
            """,
            (stale_id, SIMULATION_END_TS + SECONDS_PER_DAY),
        )
        conn.execute(
            """
            INSERT INTO snapshot_runs (
                source, started_at, server_started_at, status
            ) VALUES ('simulator', ?, ?, 'running')
            """,
            (
                first_captured_at + started_offset,
                first_captured_at + SNAPSHOT_INTERVAL,
            ),
        )
        before_runs = [
            tuple(row)
            for row in conn.execute("SELECT * FROM snapshot_runs ORDER BY snapshot_id")
        ]
        before_raw = [
            tuple(row)
            for row in conn.execute(
                "SELECT * FROM raw_listings ORDER BY snapshot_id, auction_id"
            )
        ]

    with pytest.raises(
        ValueError,
        match="Overlapping simulation window requires reset=True or an isolated db_path",
    ):
        simulate(
            db_path, env="sim", days=1, end_ts=SIMULATION_END_TS, reset=False,
        )

    with open_connection(db_path) as conn:
        assert [
            tuple(row)
            for row in conn.execute("SELECT * FROM snapshot_runs ORDER BY snapshot_id")
        ] == before_runs
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT * FROM raw_listings ORDER BY snapshot_id, auction_id"
            )
        ] == before_raw


@pytest.mark.parametrize("end_offset", [0, SNAPSHOT_INTERVAL])
def test_concurrent_simulation_rejects_overlapping_windows(
    db_path, small_simulation_book, monkeypatch, end_offset,
):
    first_frame_started = Event()
    release_first_frame = Event()
    second_call_started = Event()
    original_frame = _OrderBook.frame

    def pause_first_frame(self, captured_at, start_ts):
        if captured_at == start_ts and not first_frame_started.is_set():
            first_frame_started.set()
            assert release_first_frame.wait(10), "first simulation was not released"
        return original_frame(self, captured_at, start_ts)

    monkeypatch.setattr(_OrderBook, "frame", pause_first_frame)
    alias_directory = db_path.parent / "alias"
    alias_directory.mkdir()
    alias_path = alias_directory / ".." / db_path.name

    def competing_simulation():
        second_call_started.set()
        return simulate(
            alias_path,
            env="sim",
            days=1,
            end_ts=SIMULATION_END_TS + end_offset,
            reset=False,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            simulate,
            db_path,
            env="sim",
            days=1,
            end_ts=SIMULATION_END_TS,
            reset=False,
        )
        try:
            assert first_frame_started.wait(10)
            second = executor.submit(competing_simulation)
            assert second_call_started.wait(10)
            # Attempt admission while the first stream has no completed frame.
            # A caller waiting for the path lock proceeds after release below.
            try:
                second.result(timeout=1)
            except (TimeoutError, ValueError):
                pass
        finally:
            release_first_frame.set()

        assert first.result(timeout=30).snapshots == 96
        with pytest.raises(
            ValueError,
            match="Overlapping simulation window requires reset=True or an isolated db_path",
        ):
            second.result(timeout=30)

    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshot_runs").fetchone()[0] == 96
        assert conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0] == 96
        assert conn.execute(
            "SELECT COUNT(DISTINCT server_started_at) FROM snapshot_runs"
        ).fetchone()[0] == 96
        assert conn.execute(
            "SELECT COUNT(*) FROM snapshot_runs WHERE status != 'completed'"
        ).fetchone()[0] == 0


def test_simulation_allows_a_window_strictly_after_existing_snapshots(
    tmp_path, small_simulation_book,
):
    path = tmp_path / "successive.db"
    first = simulate(path, env="sim", days=1, end_ts=SIMULATION_END_TS)
    with open_connection(path) as conn:
        previous_max = conn.execute(
            "SELECT MAX(server_started_at) FROM snapshot_runs WHERE status = 'completed'"
        ).fetchone()[0]
    second = simulate(
        path, env="sim", days=1, end_ts=SIMULATION_END_TS + SECONDS_PER_DAY,
    )
    assert second.start_ts == first.end_ts
    assert second.start_ts > previous_max
    with open_connection(path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM snapshot_runs WHERE status = 'completed'"
        ).fetchone()[0] == first.snapshots + second.snapshots
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_simulation_reset_clears_dependents_and_preserves_catalog_and_events(
    tmp_path, small_simulation_book,
):
    path = tmp_path / "reset.db"
    first = simulate(path, env="sim", days=1, end_ts=SIMULATION_END_TS)
    with open_connection(path) as conn, write_transaction(conn):
        snapshots = conn.execute(
            "SELECT snapshot_id, server_started_at FROM snapshot_runs "
            "ORDER BY snapshot_id LIMIT 2"
        ).fetchall()
        first_id, first_at = tuple(snapshots[0])
        second_id, second_at = tuple(snapshots[1])
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, updated_at) "
            "VALUES (811111, 'Preserved Catalog Sentinel', 1)"
        )
        publish_seq = conn.execute(
            "INSERT INTO publish_events (available_at, source_type, source_id) "
            "VALUES (?, 'snapshot', ?)",
            (first_at, first_id),
        ).lastrowid
        conn.execute(
            "UPDATE snapshot_runs SET publish_seq = ? WHERE snapshot_id = ?",
            (publish_seq, first_id),
        )
        bucket = first_at - first_at % SNAPSHOT_INTERVAL
        conn.execute(
            """INSERT INTO item_bar_revisions
               (item_id, timeframe, bucket_ts, publish_seq, close_ts,
                revision_available_at)
               VALUES (4, '15m', ?, ?, ?, ?)""",
            (bucket, publish_seq, bucket + SNAPSHOT_INTERVAL, first_at),
        )
        conn.execute(
            """INSERT INTO inferred_flow_revisions
               (snapshot_id, item_id, publish_seq, prev_snapshot_id,
                revision_available_at, captured_at, interval_sec,
                new_listing_count, new_listed_qty, partial_fill_qty,
                early_disappear_qty, expired_qty, est_sold_qty)
               VALUES (?, 4, ?, ?, ?, ?, 900, 0, 0, 0, 0, 0, 0.0)""",
            (second_id, publish_seq, first_id, second_at, second_at),
        )
        conn.execute(
            """INSERT INTO orderbook_observations
               (snapshot_id, item_id, available_at, publish_seq, book_state, ladder_json)
               VALUES (?, 4, ?, ?, 'stocked', '[]')""",
            (first_id, first_at, publish_seq),
        )
        window_id = conn.execute(
            """INSERT INTO review_windows
               (pair_block_id, planned_start_at, planned_end_at, planned_review_min,
                item_pool_version, rule_version, assigned_arm)
               VALUES ('reset-test', ?, ?, 10, 'pool-v1', 'rule-v1', 'SIMPLE_LIMIT')""",
            (first_at, first_at + 600),
        ).lastrowid
        conn.execute(
            """INSERT INTO opportunity_observations
               (window_id, opportunity_group_id, arm, rule_version, fee_rule_version,
                source_snapshot_id, read_publish_seq, item_id, available_at,
                quote_expires_at, rank_order, planned_qty, max_total_buy_cost,
                snapshot_ladder_json)
               VALUES (?, 'reset-group', 'SIMPLE_LIMIT', 'rule-v1', 'fee-v1',
                       ?, ?, 4, ?, ?, 1, 1, 100, '[]')""",
            (window_id, first_id, publish_seq, first_at, first_at + 900),
        )
        conn.execute(
            """INSERT INTO user_positions
               (position_id, arm_id, item_id, initial_qty, remaining_qty,
                unallocated_qty, status)
               VALUES (1, 'reset-arm', 4, 2, 2, 0, 'OPEN')"""
        )
        conn.execute(
            """INSERT INTO user_orders
               (order_id, arm_id, position_id, item_id, listed_price, initial_qty,
                remaining_qty, status)
               VALUES (1, 'reset-arm', 1, 4, 100, 2, 2, 'OPEN')"""
        )
        conn.execute(
            """INSERT INTO user_ledger_events
               (idempotency_key, payload_sha256, arm_id, event_ts, event_type,
                item_id, position_id, order_id)
               VALUES ('reset-event', ?, 'reset-arm', ?, 'ACCOUNT_OPEN', 4, 1, 1)""",
            ("0" * 64, first_at),
        )
        migrations_before = [
            tuple(row)
            for row in conn.execute("SELECT * FROM schema_migrations ORDER BY version")
        ]
        events_before = [
            tuple(row)
            for row in conn.execute("SELECT * FROM market_events ORDER BY event_id")
        ]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    observed = []

    def observe_reset(conn, snapshot_id, previous_id, captured_at):
        if not observed:
            assert previous_id is None
        observed.append(snapshot_id)

    repeated = simulate(
        path, env="sim", days=1, end_ts=SIMULATION_END_TS,
        reset=True, on_snapshot=observe_reset,
    )
    assert repeated == first
    assert len(observed) == first.snapshots
    with open_connection(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshot_runs").fetchone()[0] == 96
        assert conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0] == 96
        for table in (
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
        ):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
        assert conn.execute(
            "SELECT name_en FROM items_catalog WHERE item_id = 811111"
        ).fetchone()[0] == "Preserved Catalog Sentinel"
        assert [
            tuple(row)
            for row in conn.execute("SELECT * FROM schema_migrations ORDER BY version")
        ] == migrations_before
        assert [
            tuple(row)
            for row in conn.execute("SELECT * FROM market_events ORDER BY event_id")
        ] == events_before
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
