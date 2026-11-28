"""M0 contracts: schema, isolation, transaction safety and concurrent access."""

import shutil
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from typer.testing import CliRunner

from pokemmo_gtl import config as config_module
from pokemmo_gtl.cli import app
from pokemmo_gtl.config import PROJECT_ROOT, load_config
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import MIGRATIONS_DIR, migrate

TABLES = {
    "schema_migrations", "items_catalog", "snapshot_runs", "raw_listings",
    "inferred_flows", "item_bars", "manual_quotes", "market_events",
    "predictions", "backtest_runs", "publish_events", "item_bar_revisions",
    "inferred_flow_revisions", "orderbook_observations", "review_windows",
    "opportunity_observations", "user_positions", "user_orders", "user_ledger_events",
}
INDEXES = {
    "idx_snapshot_runs_status_ts", "idx_raw_listings_snap_item_price",
    "idx_inferred_flows_item_ts", "idx_inferred_flows_prev_snap", "idx_item_bars_tf_ts",
    "idx_manual_quotes_item_ts", "idx_market_events_window", "idx_predictions_lookup",
    "idx_predictions_rank", "idx_item_bars_avail", "idx_publish_events_avail",
    "idx_bar_rev_lookup", "idx_bar_rev_item_lookup", "idx_flow_rev_item_ts",
    "idx_review_windows_block", "idx_opp_obs_window_arm",
    "idx_user_positions_arm_status", "idx_user_orders_arm_status",
    "idx_ledger_events_arm_order",
}


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "数据库.db"
    migrate(path)
    return path


def insert_item(conn, item_id=4):
    conn.execute(
        "INSERT INTO items_catalog (item_id, name_en, updated_at) VALUES (?, ?, ?)",
        (item_id, "精灵球", 1768435200),
    )


def test_cli_environment_files_are_physically_isolated(tmp_path, monkeypatch):
    shutil.copytree(PROJECT_ROOT / "config", tmp_path / "config")
    monkeypatch.setattr(config_module, "PROJECT_ROOT", tmp_path)
    runner = CliRunner()
    for env in ("sim", "live"):
        result = runner.invoke(app, ["init-db", "--env", env])
        assert result.exit_code == 0, result.output
    sim = load_config("sim").db_path
    live = load_config("live").db_path
    assert sim.name == "gtl_sim.db" and live.name == "gtl_live.db"
    assert sim.is_file() and live.is_file() and not sim.samefile(live)
    with open_connection(sim) as conn, write_transaction(conn):
        insert_item(conn)
    with open_connection(live) as conn:
        assert conn.execute("SELECT count(*) FROM items_catalog").fetchone()[0] == 0
    assert runner.invoke(app, ["init-db"]).exit_code == 0
    assert runner.invoke(app, ["init-db", "--env", "invalid"]).exit_code != 0


def test_every_connection_has_required_pragmas_and_closes(db_path):
    for _ in range(2):
        with open_connection(db_path) as conn:
            assert conn.isolation_level is None
            for pragma, expected in (
                ("journal_mode", "wal"), ("synchronous", 1),
                ("foreign_keys", 1), ("busy_timeout", 30000),
            ):
                assert conn.execute(f"PRAGMA {pragma}").fetchone()[0] == expected
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")


def test_exact_schema_is_strict_and_has_expected_secondary_indexes(db_path):
    with open_connection(db_path) as conn:
        tables = {row[1]: row[5] for row in conn.execute("PRAGMA table_list")}
        assert {name for name in tables if not name.startswith("sqlite_")} == TABLES
        assert all(tables[name] == 1 for name in TABLES)
        indexes = {
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL"
            )
        }
        assert indexes == INDEXES


@pytest.mark.parametrize("value", ["2026-01-15T00:00:00Z", "2026-01-15 00:00:00"])
def test_strict_integer_timestamp_rejects_iso_string(db_path, value):
    with open_connection(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError), write_transaction(conn):
            conn.execute(
                "INSERT INTO items_catalog (item_id, name_en, updated_at) VALUES (4, ?, ?)",
                ("Poke Ball", value),
            )
        assert not conn.in_transaction
        assert conn.execute("SELECT count(*) FROM items_catalog").fetchone()[0] == 0


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
def test_explicit_begin_immediate_commit_and_rollback(db_path, error):
    statements = []
    with open_connection(db_path) as conn:
        conn.set_trace_callback(statements.append)
        with pytest.raises(error), write_transaction(conn):
            insert_item(conn)
            raise error()
        assert not conn.in_transaction
        assert conn.execute("SELECT count(*) FROM items_catalog").fetchone()[0] == 0
        with write_transaction(conn):
            insert_item(conn)
        assert conn.execute("SELECT count(*) FROM items_catalog").fetchone()[0] == 1
    assert statements.count("BEGIN IMMEDIATE") == 2
    assert "ROLLBACK" in statements and "COMMIT" in statements


def test_foreign_key_enforcement(db_path):
    with open_connection(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError), write_transaction(conn):
            conn.execute(
                "INSERT INTO manual_quotes (item_id, recorded_at, price) VALUES (999, 1, 1)"
            )


def test_repeated_migrations_are_idempotent_and_preserve_data(db_path):
    with open_connection(db_path) as conn, write_transaction(conn):
        insert_item(conn)
        before = [tuple(row) for row in conn.execute("SELECT * FROM schema_migrations")]
        schema = [tuple(row) for row in conn.execute("SELECT * FROM sqlite_schema")]
    for _ in range(3):
        assert migrate(db_path) == []
    with open_connection(db_path) as conn:
        assert [tuple(row) for row in conn.execute("SELECT * FROM schema_migrations")] == before
        assert [tuple(row) for row in conn.execute("SELECT * FROM sqlite_schema")] == schema
        assert conn.execute("SELECT name_en FROM items_catalog").fetchone()[0] == "精灵球"
    assert [row[0] for row in before] == ["001_init", "002_v2_ledger_and_observations"]


def test_failed_migration_is_atomic(tmp_path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    shutil.copyfile(MIGRATIONS_DIR / "001_init.sql", migrations / "001_init.sql")
    (migrations / "002_broken.sql").write_text(
        "CREATE TABLE must_rollback (value INTEGER) STRICT;\nINVALID SQL;\n", encoding="utf-8"
    )
    path = tmp_path / "failed.db"
    with pytest.raises(sqlite3.OperationalError):
        migrate(path, migrations_dir=migrations)
    with open_connection(path) as conn:
        assert conn.execute("SELECT count(*) FROM sqlite_schema").fetchone()[0] == 0


def test_concurrent_readers_and_immediate_writers_have_zero_sqlite_busy(db_path):
    workers = 8
    barrier = threading.Barrier(workers)
    with open_connection(db_path) as conn, write_transaction(conn):
        insert_item(conn)

    def work(index):
        # Connections are opened in the owning worker, never passed between threads.
        with open_connection(db_path) as conn:
            barrier.wait(timeout=15)
            for _ in range(30):
                if index < 4:
                    with write_transaction(conn):
                        conn.execute("SELECT updated_at FROM items_catalog").fetchone()
                        conn.execute("UPDATE items_catalog SET updated_at = updated_at + 1")
                        time.sleep(0.001)  # Force real overlapping write contention.
                else:
                    assert conn.execute("SELECT count(*) FROM items_catalog").fetchone()[0] == 1
                    time.sleep(0.002)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(work, index) for index in range(workers)]
        # result() propagates all errors, including SQLITE_BUSY; no retries or suppression.
        for future in futures:
            future.result(timeout=30)
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT updated_at FROM items_catalog").fetchone()[0] == 1768435320


def test_utf8_defaults_and_secret_loading(tmp_path, monkeypatch):
    shutil.copytree(PROJECT_ROOT / "config", tmp_path / "config")
    monkeypatch.delenv("POKEMMO_API_TOKEN", raising=False)
    (tmp_path / ".env").write_text('POKEMMO_API_TOKEN="test-only-placeholder"\n', encoding="utf-8")
    config = load_config(root=tmp_path)
    assert config.env.value == "live"
    assert config.settings.simulation_default_end_ts == 1768435200
    assert str(config.fees.rate) == "0.05"
    assert config.item_aliases[0]["name_zh"] == "精灵球"
    assert len(config.events) == 4
    for event in config.events:
        assert isinstance(event["start_ts"], int)
        assert event["announced_at"] == event["start_ts"] - 90 * 86400
        assert event["end_ts"] > event["start_ts"]
    assert config.api_token.get_secret_value() == "test-only-placeholder"
    assert "test-only-placeholder" not in repr(config)
    monkeypatch.setenv("POKEMMO_API_TOKEN", "environment-placeholder")
    assert load_config(root=tmp_path).api_token.get_secret_value() == "environment-placeholder"
    monkeypatch.setenv("POKEMMO_API_TOKEN", "")
    assert load_config(root=tmp_path).api_token is None
