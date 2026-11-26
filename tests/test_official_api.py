"""Fully respx-mocked M2 HTTP, cursor, clock and atomic snapshot contracts."""

import traceback
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.ingestion.official_api import (
    ENDPOINT,
    ApiAuthError,
    ApiError,
    ApiPaginationError,
    ApiResponseError,
    OfficialApiCollector,
)
from pokemmo_gtl.ingestion.rate_limiter import CollectorBusyError, SQLiteRateLimiter

TOKEN = "abcdef0123456789" * 4
SERVER_TS = 1_700_000_000
SERVER_DATE = "Tue, 14 Nov 2023 22:13:20 GMT"


@pytest.fixture(autouse=True)
def verify_repository_local_temp_directory(tmp_path, pytestconfig):
    # Local override honors the isolated --basetemp while retaining containment.
    base = Path(pytestconfig.option.basetemp).resolve()
    assert base.is_relative_to(pytestconfig.rootpath.resolve())
    assert tmp_path.resolve().is_relative_to(base)


class FakeClock:
    def __init__(self):
        self.now = 1_800_000_000.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "market.db"
    migrate(path)
    return path


@pytest.fixture
def collector_factory(db_path, clock):
    collectors = []

    def create(**kwargs):
        options = {"clock": clock, "sleeper": clock.sleep, "jitter": lambda: 0.25}
        options.update(kwargs)
        token = options.pop("token", SecretStr(TOKEN))
        collector = OfficialApiCollector(db_path, token, **options)
        collectors.append(collector)
        return collector

    yield create
    for collector in reversed(collectors):
        collector.close()


def listing(auction_id=1, **changes):
    row = {"auction_id": auction_id, "reference_id": 77, "price": 100,
           "quantity": 2, "expires_at": 1_900_000_000}
    row.update(changes)
    return row


def latest_snapshot(db_path):
    with open_connection(db_path) as conn:
        return dict(conn.execute(
            "SELECT * FROM snapshot_runs ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone())


def assert_failed_without_raw(db_path):
    snapshot = latest_snapshot(db_path)
    assert snapshot["status"] == "failed"
    assert snapshot["completed_at"] is not None
    assert snapshot["total_listings"] == 0
    assert TOKEN not in snapshot["error_message"]
    with open_connection(db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM raw_listings WHERE snapshot_id = ?",
            (snapshot["snapshot_id"],),
        ).fetchone()[0] == 0
    return snapshot


def test_required_headers_timeout_redirect_policy_and_empty_snapshot(
    respx_mock, collector_factory, db_path, clock,
):
    route = respx_mock.get(ENDPOINT).respond(200, json=[], headers={"Date": SERVER_DATE})
    collector = collector_factory()
    result = collector.collect_once()
    assert collector._client.follow_redirects is False
    request = route.calls[0].request
    assert request.headers["Private-Token"] == TOKEN
    assert request.headers["Accept"] == "application/json"
    assert dict(request.url.params) == {"count": "1000"}
    assert request.extensions["timeout"] == {
        "connect": 10.0, "read": 20.0, "write": 10.0, "pool": 10.0,
    }
    assert result.total_listings == 0 and result.pages_fetched == 1
    assert result.server_started_at == SERVER_TS
    snapshot = latest_snapshot(db_path)
    assert snapshot["started_at"] == int(clock.now)
    assert snapshot["server_started_at"] == SERVER_TS
    assert snapshot["source"] == "official_api"
    assert snapshot["status"] == "completed"
    assert TOKEN not in repr(collector) and TOKEN not in repr(collector._token)


def test_all_3xx_401_403_are_immediate_and_secret_free(
    respx_mock, collector_factory, db_path, caplog,
):
    route = respx_mock.get(ENDPOINT)
    collector = collector_factory()
    for status in [*range(300, 400), 401, 403]:
        route.mock(return_value=httpx.Response(
            status, headers={"Location": f"https://never-follow.example/{TOKEN}"}, text=TOKEN,
        ))
        before = len(respx_mock.calls)
        with pytest.raises(ApiAuthError) as caught:
            collector.collect_once()
        assert len(respx_mock.calls) == before + 1
        formatted = "".join(traceback.format_exception(caught.value))
        assert TOKEN not in str(caught.value)
        assert TOKEN not in repr(caught.value)
        assert TOKEN not in formatted
        assert TOKEN not in caplog.text
        assert_failed_without_raw(db_path)
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshot_runs").fetchone()[0] == 102
        assert conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0] == 0


@pytest.mark.parametrize("token", [None, "", "short", "g" * 64, "a" * 63, "a" * 65])
def test_invalid_token_is_rejected_before_http_or_snapshot(
    respx_mock, db_path, collector_factory, token,
):
    with pytest.raises(ApiAuthError) as caught:
        collector_factory(token=token)
    assert TOKEN not in str(caught.value)
    assert len(respx_mock.calls) == 0
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshot_runs").fetchone()[0] == 0


def test_injected_client_cannot_follow_redirect_and_remains_caller_owned(
    respx_mock, collector_factory,
):
    route = respx_mock.get(ENDPOINT).respond(
        302, headers={"Location": "https://never-follow.example/"},
    )
    with httpx.Client(follow_redirects=True, timeout=None) as client:
        collector = collector_factory(client=client)
        with pytest.raises(ApiAuthError):
            collector.collect_once()
        assert route.call_count == 1
        collector.close()
        assert not client.is_closed
        assert route.calls[0].request.extensions["timeout"]["read"] == 20.0


@pytest.mark.parametrize("status", [429, 500, 503, 599])
def test_retryable_statuses_use_jitter_and_charge_every_attempt(
    respx_mock, collector_factory, db_path, clock, status,
):
    route = respx_mock.get(ENDPOINT).mock(side_effect=[
        httpx.Response(status, text=TOKEN), httpx.Response(200, json=[listing()]),
    ])
    collector_factory().collect_once()
    assert route.call_count == 2
    assert clock.sleeps == [1.25]
    with open_connection(db_path.parent / "api_limiter.db") as conn:
        assert conn.execute("SELECT SUM(tokens) FROM api_rate_ledger").fetchone()[0] == 2000
    assert all(dict(call.request.url.params) == {"count": "1000"} for call in route.calls)


@pytest.mark.parametrize("error_type", [
    httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout,
])
def test_transport_errors_are_retried_without_leaking_exception_messages(
    respx_mock, collector_factory, clock, error_type,
):
    route = respx_mock.get(ENDPOINT).mock(side_effect=[
        error_type(TOKEN), httpx.Response(200, json=[listing()]),
    ])
    collector_factory().collect_once()
    assert route.call_count == 2 and clock.sleeps == [1.25]


@pytest.mark.parametrize("transport", [False, True])
def test_exhaustion_is_three_retries_with_sanitized_traceback(
    respx_mock, collector_factory, db_path, clock, transport,
):
    route = respx_mock.get(ENDPOINT)
    if transport:
        route.mock(side_effect=httpx.ReadTimeout(TOKEN))
    else:
        route.respond(503, text=TOKEN)
    with pytest.raises(ApiError) as caught:
        collector_factory().collect_once()
    assert route.call_count == 4
    assert clock.sleeps == [1.25, 2.25, 4.25]
    assert TOKEN not in "".join(traceback.format_exception(caught.value))
    assert_failed_without_raw(db_path)


@pytest.mark.parametrize("header,expected", [
    ("7", 7.0), ("2.5", 2.5), ("0", 0.0),
    ("Tue, 14 Nov 2023 22:13:27 GMT", 7.0),
    ("bad-date", 1.25), ("NaN", 1.25), ("-1", 1.25),
])
def test_retry_after_seconds_and_http_date_take_priority(
    respx_mock, collector_factory, clock, header, expected,
):
    route = respx_mock.get(ENDPOINT).mock(side_effect=[
        httpx.Response(429, headers={"Retry-After": header, "Date": SERVER_DATE}),
        httpx.Response(200, json=[listing()]),
    ])
    collector_factory().collect_once()
    assert route.call_count == 2
    assert sum(clock.sleeps) == expected


def test_retry_after_absolute_date_without_server_date_uses_clock(
    respx_mock, collector_factory, clock,
):
    retry_at = format_datetime(datetime.fromtimestamp(clock.now + 8, timezone.utc), usegmt=True)
    respx_mock.get(ENDPOINT).mock(side_effect=[
        httpx.Response(503, headers={"Retry-After": retry_at}),
        httpx.Response(200, json=[listing()]),
    ])
    collector_factory().collect_once()
    assert clock.sleeps == [8]


def test_long_retry_after_keeps_collector_lock_alive(
    respx_mock, collector_factory, db_path, clock,
):
    respx_mock.get(ENDPOINT).mock(side_effect=[
        httpx.Response(429, headers={"Retry-After": "1201"}),
        httpx.Response(200, json=[listing()]),
    ])
    path = db_path.parent / "api_limiter.db"
    with SQLiteRateLimiter(path, clock=clock) as competitor:
        def sleep(seconds):
            clock.sleep(seconds)
            with pytest.raises(CollectorBusyError):
                competitor.acquire_collector_lock()

        collector_factory(sleeper=sleep).collect_once()
    assert sum(clock.sleeps) == 1201
    assert max(clock.sleeps) <= 60


def test_other_4xx_is_not_retried(respx_mock, collector_factory, db_path, clock):
    route = respx_mock.get(ENDPOINT).respond(400, text=TOKEN)
    with pytest.raises(ApiResponseError) as caught:
        collector_factory().collect_once()
    assert route.call_count == 1 and clock.sleeps == []
    assert TOKEN not in str(caught.value)
    assert_failed_without_raw(db_path)


@pytest.mark.parametrize("payload", [
    {}, "secret-free-invalid-shape", [1], [{}], [listing(auction_id=True)],
    [listing(reference_id=-1)], [listing(price=0)], [listing(quantity=1.5)],
    [listing(expires_at="1900000000")], [listing(expires_at=0)],
    [listing(price=2**63)], [listing(expires_at=2**80)],
], ids=["mapping", "string", "row-type", "missing", "bool", "negative", "zero",
        "float", "string-int", "expiry-zero", "overflow-price", "overflow-expiry"])
def test_payload_and_positive_integer_validation(
    respx_mock, collector_factory, db_path, payload,
):
    route = respx_mock.get(ENDPOINT).respond(200, json=payload)
    with pytest.raises(ApiResponseError):
        collector_factory().collect_once()
    assert route.call_count == 1
    assert_failed_without_raw(db_path)


def test_invalid_json_is_sanitized_and_not_retried(respx_mock, collector_factory, db_path):
    route = respx_mock.get(ENDPOINT).respond(200, content=TOKEN.encode())
    with pytest.raises(ApiResponseError) as caught:
        collector_factory().collect_once()
    assert route.call_count == 1
    assert TOKEN not in "".join(traceback.format_exception(caught.value))
    assert_failed_without_raw(db_path)


def test_short_pages_keep_going_and_cross_page_duplicates_keep_first_record(
    respx_mock, collector_factory, db_path,
):
    route = respx_mock.get(ENDPOINT).mock(side_effect=[
        httpx.Response(200, json=[listing(10), listing(9, price=200)],
                       headers={"Date": SERVER_DATE}),
        httpx.Response(200, json=[listing(9, price=999), listing(3)],
                       headers={"Date": "Wed, 15 Nov 2023 22:13:20 GMT"}),
        httpx.Response(200, json=[]),
    ])
    result = collector_factory().collect_once()
    assert result.total_listings == 3 and result.pages_fetched == 3
    assert result.server_started_at == SERVER_TS
    assert [dict(call.request.url.params) for call in route.calls] == [
        {"count": "1000"}, {"count": "1000", "skip": "8"},
        {"count": "1000", "skip": "2"},
    ]
    with open_connection(db_path) as conn:
        assert conn.execute(
            "SELECT price FROM raw_listings WHERE auction_id = 9"
        ).fetchone()[0] == 200


@pytest.mark.parametrize("last_page", [[], [listing(1)]], ids=["empty", "min-id-one"])
def test_pagination_never_sends_skip_zero_or_negative(
    respx_mock, collector_factory, last_page,
):
    route = respx_mock.get(ENDPOINT).mock(side_effect=[
        httpx.Response(200, json=[listing(2)]), httpx.Response(200, json=last_page),
    ])
    collector_factory().collect_once()
    assert route.call_count == 2
    assert "skip" not in route.calls[0].request.url.params
    assert route.calls[1].request.url.params["skip"] == "1"


def test_cursor_must_strictly_decrease(respx_mock, collector_factory, db_path):
    route = respx_mock.get(ENDPOINT).respond(200, json=[listing(50)])
    with pytest.raises(ApiPaginationError, match="strictly decrease"):
        collector_factory().collect_once()
    assert route.call_count == 2
    assert_failed_without_raw(db_path)


@pytest.mark.parametrize("terminal", [None, [], [listing(1)]], ids=["limit", "empty", "min-one"])
def test_100_page_hard_limit_and_final_page_termination(
    respx_mock, collector_factory, db_path, terminal,
):
    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        if calls == 100 and terminal is not None:
            return httpx.Response(200, json=terminal)
        return httpx.Response(200, json=[listing(1000 - calls)])

    route = respx_mock.get(ENDPOINT).mock(side_effect=respond)
    collector = collector_factory()
    if terminal is None:
        with pytest.raises(ApiPaginationError, match="100-page"):
            collector.collect_once()
        assert_failed_without_raw(db_path)
    else:
        assert collector.collect_once().pages_fetched == 100
    assert route.call_count == 100


@pytest.mark.parametrize("date", [None, "invalid-date"])
def test_missing_or_invalid_date_falls_back_to_local_utc_seconds(
    respx_mock, collector_factory, clock, date,
):
    headers = {} if date is None else {"Date": date}
    respx_mock.get(ENDPOINT).respond(200, json=[], headers=headers)
    result = collector_factory().collect_once()
    assert result.server_started_at == int(clock.now)


def test_millisecond_expiry_normalization_and_unknown_catalog_before_foreign_key_insert(
    respx_mock, collector_factory, db_path,
):
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, category, max_stack_size, updated_at) "
            "VALUES (88, 'Known Item', 'ball', 1, 1)"
        )
    respx_mock.get(ENDPOINT).respond(200, json=[
        listing(2, expires_at=1_900_000_000_999), listing(1, reference_id=88),
    ])
    collector_factory().collect_once()
    with open_connection(db_path) as conn:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert tuple(conn.execute(
            "SELECT name_en, category, max_stack_size FROM items_catalog WHERE item_id = 77"
        ).fetchone()) == ("Unknown Item #77", "unknown", 99)
        assert tuple(conn.execute(
            "SELECT name_en, category, max_stack_size FROM items_catalog WHERE item_id = 88"
        ).fetchone()) == ("Known Item", "ball", 1)
        assert [tuple(row) for row in conn.execute(
            "SELECT auction_id, expires_at FROM raw_listings ORDER BY auction_id"
        )] == [(1, 1_900_000_000), (2, 1_900_000_000)]


def test_network_phase_has_no_market_write_lock_and_running_snapshot_is_visible(
    respx_mock, collector_factory, db_path,
):
    observed = []

    def respond(request):
        with open_connection(db_path) as conn, write_transaction(conn):
            observed.append(tuple(conn.execute(
                "SELECT status, total_listings FROM snapshot_runs ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()))
            conn.execute(
                "INSERT INTO items_catalog (item_id, name_en, updated_at) "
                "VALUES (88, 'Concurrent', 1)"
            )
        return httpx.Response(200, json=[listing()])

    respx_mock.get(ENDPOINT).mock(side_effect=respond)
    collector_factory().collect_once()
    assert observed == [("running", 0)]
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT name_en FROM items_catalog WHERE item_id = 88").fetchone()[0]


def test_second_page_failure_leaves_no_raw_or_unknown_catalog(
    respx_mock, collector_factory, db_path,
):
    route = respx_mock.get(ENDPOINT).mock(side_effect=[
        httpx.Response(200, json=[listing(20)]), httpx.Response(403, text=TOKEN),
    ])
    with pytest.raises(ApiAuthError):
        collector_factory().collect_once()
    assert route.call_count == 2
    assert_failed_without_raw(db_path)
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM items_catalog").fetchone()[0] == 0


def test_stale_running_recovery_only_after_30_minutes(
    respx_mock, collector_factory, db_path, clock,
):
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog (item_id, name_en, updated_at) VALUES (77, 'Old', 1)"
        )
        for snapshot_id, started_at, status in [
            (1, int(clock.now) - 1801, "running"),
            (2, int(clock.now) - 1800, "running"),
            (3, int(clock.now) - 2000, "completed"),
        ]:
            conn.execute(
                "INSERT INTO snapshot_runs "
                "(snapshot_id, source, started_at, server_started_at, status) "
                "VALUES (?, 'official_api', ?, ?, ?)",
                (snapshot_id, started_at, started_at, status),
            )
            conn.execute("INSERT INTO raw_listings VALUES (?, 1, 77, 100, 1, 1900000000)",
                         (snapshot_id,))

    def respond(request):
        with open_connection(db_path) as conn:
            assert conn.execute(
                "SELECT status FROM snapshot_runs WHERE snapshot_id = 1"
            ).fetchone()[0] == "failed"
            assert conn.execute(
                "SELECT COUNT(*) FROM raw_listings WHERE snapshot_id = 1"
            ).fetchone()[0] == 0
        return httpx.Response(200, json=[])

    respx_mock.get(ENDPOINT).mock(side_effect=respond)
    collector_factory().collect_once()
    with open_connection(db_path) as conn:
        assert [tuple(row) for row in conn.execute(
            "SELECT snapshot_id, status FROM snapshot_runs ORDER BY snapshot_id"
        )] == [(1, "failed"), (2, "running"), (3, "completed"), (4, "completed")]
        assert [row[0] for row in conn.execute(
            "SELECT snapshot_id FROM raw_listings ORDER BY snapshot_id"
        )] == [2, 3]


def test_post_snapshot_hook_sees_rows_and_server_time_in_same_transaction(
    respx_mock, collector_factory, db_path,
):
    observed = []

    def hook(conn, snapshot_id):
        assert conn.in_transaction
        observed.append(snapshot_id)
        assert tuple(conn.execute(
            "SELECT status, server_started_at FROM snapshot_runs WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()) == ("running", SERVER_TS)
        assert conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0] == 1
        conn.execute(
            "INSERT INTO manual_quotes (item_id, recorded_at, price) VALUES (77, ?, 123)",
            (SERVER_TS,),
        )
        with open_connection(db_path) as reader:
            assert reader.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0] == 0
            assert reader.execute("SELECT status FROM snapshot_runs").fetchone()[0] == "running"

    respx_mock.get(ENDPOINT).respond(200, json=[listing()], headers={"Date": SERVER_DATE})
    result = collector_factory(post_snapshot_hook=hook).collect_once()
    assert observed == [result.snapshot_id]
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM manual_quotes").fetchone()[0] == 1
        assert conn.execute("SELECT status FROM snapshot_runs").fetchone()[0] == "completed"


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_hook_failure_rolls_back_raw_catalog_and_hook_writes(
    respx_mock, collector_factory, db_path, failure,
):
    def hook(conn, snapshot_id):
        assert conn.in_transaction
        conn.execute("INSERT INTO manual_quotes (item_id, recorded_at, price) VALUES (77, 1, 123)")
        raise failure(TOKEN if failure is RuntimeError else "interrupted")

    respx_mock.get(ENDPOINT).respond(200, json=[listing()])
    with pytest.raises(ApiError if failure is RuntimeError else KeyboardInterrupt) as caught:
        collector_factory(post_snapshot_hook=hook).collect_once()
    assert TOKEN not in "".join(traceback.format_exception(caught.value))
    assert_failed_without_raw(db_path)
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM items_catalog").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM manual_quotes").fetchone()[0] == 0
    with SQLiteRateLimiter(db_path.parent / "api_limiter.db") as limiter, limiter.collector_lock():
        pass


def test_partial_raw_insert_failure_rolls_back_entire_phase_two(
    respx_mock, collector_factory, db_path,
):
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "CREATE TRIGGER fail_second_raw BEFORE INSERT ON raw_listings "
            "WHEN NEW.auction_id = 1 BEGIN SELECT RAISE(ABORT, 'private db detail'); END"
        )
    respx_mock.get(ENDPOINT).respond(200, json=[listing(2), listing(1)])
    with pytest.raises(ApiError):
        collector_factory().collect_once()
    assert_failed_without_raw(db_path)
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM items_catalog").fetchone()[0] == 0


def test_concurrent_collectors_fail_before_http_or_new_snapshot(
    respx_mock, collector_factory, db_path,
):
    first = collector_factory()
    second = collector_factory()

    def respond(request):
        with pytest.raises(CollectorBusyError):
            second.collect_once()
        return httpx.Response(200, json=[listing()])

    route = respx_mock.get(ENDPOINT).mock(side_effect=respond)
    first.collect_once()
    assert route.call_count == 1
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshot_runs").fetchone()[0] == 1


def test_lost_lease_during_network_cannot_commit_or_release_replacement(
    respx_mock, collector_factory, db_path, clock,
):
    replacement = None
    with SQLiteRateLimiter(db_path.parent / "api_limiter.db", clock=clock) as competitor:
        def respond(request):
            nonlocal replacement
            clock.now += 601
            replacement = competitor.acquire_collector_lock()
            return httpx.Response(200, json=[listing()])

        respx_mock.get(ENDPOINT).mock(side_effect=respond)
        with pytest.raises(CollectorBusyError):
            collector_factory().collect_once()
        assert_failed_without_raw(db_path)
        with pytest.raises(CollectorBusyError):
            competitor.acquire_collector_lock()
        replacement.release()



def test_lost_lease_during_hook_rolls_back_before_commit(
    respx_mock, collector_factory, db_path, clock,
):
    replacement = None
    with SQLiteRateLimiter(db_path.parent / "api_limiter.db", clock=clock) as competitor:
        def hook(conn, snapshot_id):
            nonlocal replacement
            assert conn.in_transaction
            clock.now += 601
            replacement = competitor.acquire_collector_lock()

        respx_mock.get(ENDPOINT).respond(200, json=[listing()])
        with pytest.raises(CollectorBusyError):
            collector_factory(post_snapshot_hook=hook).collect_once()
        assert_failed_without_raw(db_path)
        with pytest.raises(CollectorBusyError):
            competitor.acquire_collector_lock()
        replacement.release()
