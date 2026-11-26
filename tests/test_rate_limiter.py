"""M2 persistent quota, cross-process contention, leases and clock boundaries."""

import multiprocessing
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.ingestion.rate_limiter import CollectorBusyError, SQLiteRateLimiter


@pytest.fixture(autouse=True)
def verify_repository_local_temp_directory(tmp_path, pytestconfig):
    # Override the old conftest fixture locally: honor --basetemp without changing
    # shared files, retaining both workspace containment and the actual temp check.
    base = Path(pytestconfig.option.basetemp).resolve()
    assert base.is_relative_to(pytestconfig.rootpath.resolve())
    assert tmp_path.resolve().is_relative_to(base)


class FakeClock:
    def __init__(self, now=10_000.0):
        self.now = now
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.now += seconds


def _process_acquire(path, tokens, output):
    clock = FakeClock()
    with SQLiteRateLimiter(Path(path), clock=clock, sleeper=clock.sleep) as limiter:
        limiter.acquire(tokens)
    output.put((clock.now, clock.sleeps))


@pytest.mark.parametrize("memory", [False, True])
def test_strict_schema_and_connection_pragmas(tmp_path, memory):
    with SQLiteRateLimiter(":memory:" if memory else tmp_path / "api_limiter.db") as limiter:
        with limiter._connection() as conn:
            strict = {row[1]: row[5] for row in conn.execute("PRAGMA table_list")}
            assert strict["api_rate_ledger"] == strict["collector_locks"] == 1
            assert [row[1] for row in conn.execute("PRAGMA table_info(api_rate_ledger)")] == [
                "id", "req_ts", "tokens",
            ]
            assert [row[1] for row in conn.execute("PRAGMA table_info(collector_locks)")] == [
                "lock_name", "owner_pid", "acquired_at", "heartbeat_at",
            ]
            assert conn.isolation_level is None
            assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
            assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000
            assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1
            if not memory:
                assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            with pytest.raises(sqlite3.IntegrityError), write_transaction(conn):
                conn.execute("INSERT INTO api_rate_ledger (req_ts, tokens) VALUES (1, 'bad')")


def test_two_instances_and_restart_share_28000_budget(tmp_path):
    clock = FakeClock()
    path = tmp_path / "api_limiter.db"
    with SQLiteRateLimiter(path, clock=clock, sleeper=clock.sleep) as first:
        first.acquire(27_000)
    with SQLiteRateLimiter(path, clock=clock, sleeper=clock.sleep) as restarted:
        restarted.acquire(1000)
        assert clock.sleeps == []
        with SQLiteRateLimiter(path, clock=clock, sleeper=clock.sleep) as second:
            second.acquire(1000)
    assert clock.sleeps == [305.0]
    with open_connection(path) as conn:
        assert [row[0] for row in conn.execute("SELECT tokens FROM api_rate_ledger")] == [
            27_000, 1000, 1000,
        ]


def test_independent_processes_atomically_share_budget(tmp_path):
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    path = str(tmp_path / "api_limiter.db")
    processes = [context.Process(target=_process_acquire, args=(path, 14_000, output))
                 for _ in range(2)]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=20)
            assert process.exitcode == 0
        assert [output.get(timeout=5) for _ in processes] == [
            (10_000.0, []), (10_000.0, []),
        ]
        third = context.Process(target=_process_acquire, args=(path, 1000, output))
        processes.append(third)
        third.start()
        third.join(timeout=20)
        assert third.exitcode == 0
        assert output.get(timeout=5) == (10_305.0, [305.0])
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        output.close()
        output.join_thread()


def test_sliding_window_exact_boundary_and_310_second_pruning(tmp_path):
    clock = FakeClock()
    path = tmp_path / "api_limiter.db"
    with SQLiteRateLimiter(path, clock=clock, sleeper=clock.sleep) as limiter:
        limiter.acquire(28_000)
        clock.now += 304.5
        limiter.acquire(1000)
        assert clock.sleeps == [0.5]
        with open_connection(path) as conn:
            assert conn.execute("SELECT COUNT(*) FROM api_rate_ledger").fetchone()[0] == 2
        clock.now = 10_310.0
        limiter.acquire(1)
        with open_connection(path) as conn:
            assert conn.execute("SELECT MIN(req_ts) FROM api_rate_ledger").fetchone()[0] == 10_000
        clock.now += 0.01
        limiter.acquire(1)
        with open_connection(path) as conn:
            assert conn.execute("SELECT MIN(req_ts) FROM api_rate_ledger").fetchone()[0] == 10_305


def test_wait_releases_sqlite_write_transaction(tmp_path):
    clock = FakeClock()
    path = tmp_path / "api_limiter.db"

    def sleep(seconds):
        with open_connection(path) as conn, write_transaction(conn):
            conn.execute("INSERT INTO collector_locks VALUES ('other', 1, 1, 1)")
        clock.sleep(seconds)

    with SQLiteRateLimiter(path, clock=clock, sleeper=sleep) as limiter:
        limiter.acquire(28_000)
        limiter.acquire(1000)
    assert clock.sleeps == [305.0]


def test_memory_mode_is_isolated_and_thread_safe():
    clock = FakeClock()
    with SQLiteRateLimiter(":memory:", clock=clock, sleeper=clock.sleep) as limiter:
        with ThreadPoolExecutor(max_workers=7) as pool:
            list(pool.map(limiter.acquire, [1000] * 28))
        assert clock.sleeps == []
        limiter.acquire(1000)
        assert clock.sleeps == [305.0]
        with SQLiteRateLimiter(":memory:", clock=clock, sleeper=clock.sleep) as other:
            other.acquire(28_000)
        assert clock.sleeps == [305.0]


@pytest.mark.parametrize("tokens", [0, -1, 28_001, True, 1.5, "1000"])
def test_invalid_token_cost_is_rejected_without_wait(tokens):
    with SQLiteRateLimiter(":memory:") as limiter:
        with pytest.raises(ValueError):
            limiter.acquire(tokens)


@pytest.mark.parametrize("kwargs", [
    {"max_tokens": 0}, {"max_tokens": True}, {"window_sec": 0},
    {"window_sec": float("inf")}, {"window_sec": float("nan")},
])
def test_invalid_budget_configuration(kwargs):
    with pytest.raises(ValueError):
        SQLiteRateLimiter(":memory:", **kwargs)


def test_collector_lock_conflicts_even_with_same_pid_and_releases_on_interrupt(tmp_path):
    clock = FakeClock()
    path = tmp_path / "api_limiter.db"
    with (
        SQLiteRateLimiter(path, clock=clock) as first,
        SQLiteRateLimiter(path, clock=clock) as second,
    ):
        with pytest.raises(KeyboardInterrupt), first.collector_lock() as lease:
            assert lease.owner_pid == os.getpid()
            with pytest.raises(CollectorBusyError):
                second.acquire_collector_lock()
            raise KeyboardInterrupt()
        with second.collector_lock():
            with pytest.raises(CollectorBusyError):
                first.acquire_collector_lock()


def test_heartbeat_extends_lease_and_stale_takeover_is_fenced(tmp_path):
    clock = FakeClock()
    path = tmp_path / "api_limiter.db"
    with (
        SQLiteRateLimiter(path, clock=clock) as first,
        SQLiteRateLimiter(path, clock=clock) as second,
    ):
        original = first.acquire_collector_lock()
        clock.now += 599
        original.heartbeat()
        clock.now += 600
        with pytest.raises(CollectorBusyError):
            second.acquire_collector_lock()
        clock.now += 1
        replacement = second.acquire_collector_lock()
        with pytest.raises(CollectorBusyError):
            original.heartbeat()
        original.release()
        with pytest.raises(CollectorBusyError):
            first.acquire_collector_lock()
        replacement.heartbeat()
        replacement.release()
        with first.collector_lock():
            pass


def test_simultaneous_collectors_have_one_winner(tmp_path):
    path = tmp_path / "api_limiter.db"
    clock = FakeClock()
    with (
        SQLiteRateLimiter(path, clock=clock) as first,
        SQLiteRateLimiter(path, clock=clock) as second,
    ):
        barrier = threading.Barrier(2)

        def contend(limiter):
            barrier.wait(timeout=5)
            try:
                return limiter.acquire_collector_lock()
            except CollectorBusyError:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(contend, limiter) for limiter in (first, second)]
            leases = [future.result(timeout=10) for future in futures]
        assert sum(lease is not None for lease in leases) == 1
        next(lease for lease in leases if lease is not None).release()


def test_quota_sleep_renews_lease_in_short_steps(tmp_path):
    clock = FakeClock()
    path = tmp_path / "api_limiter.db"
    with (
        SQLiteRateLimiter(path, clock=clock, sleeper=clock.sleep) as limiter,
        SQLiteRateLimiter(path, clock=clock) as competitor,
        limiter.collector_lock() as lease,
    ):
        limiter.acquire(28_000)
        limiter.acquire(1000, heartbeat=lease.heartbeat)
        assert sum(clock.sleeps) == 305
        assert max(clock.sleeps) <= 60
        with pytest.raises(CollectorBusyError):
            competitor.acquire_collector_lock()


def test_expired_owner_cannot_renew_without_reacquiring():
    clock = FakeClock()
    with SQLiteRateLimiter(":memory:", clock=clock) as limiter:
        lease = limiter.acquire_collector_lock()
        clock.now += 601
        with pytest.raises(CollectorBusyError):
            lease.heartbeat()
        lease.release()
