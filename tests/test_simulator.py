"""M1 simulator, catalog and manual CSV import contracts."""

import hashlib
import json
import shutil
import sqlite3
import time

import pytest
from typer.testing import CliRunner

from pokemmo_gtl import config as config_module
from pokemmo_gtl.cli import app
from pokemmo_gtl.config import PROJECT_ROOT, load_config
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.ingestion.catalog_sync import RAW_BASE, sync_catalog, sync_default_events
from pokemmo_gtl.ingestion.manual_import import (
    MAX_IMPORT_BYTES,
    add_manual_quote,
    import_csv,
)
from pokemmo_gtl.ingestion.simulator import _OrderBook, market_profile, simulate


def _database_digest(path):
    digest = hashlib.sha256()
    with open_connection(path) as conn:
        for table, ordering in (
            ("schema_migrations", "version"),
            ("items_catalog", "item_id"),
            ("market_events", "event_id"),
            ("snapshot_runs", "snapshot_id"),
            ("raw_listings", "snapshot_id, auction_id"),
            ("sqlite_sequence", "name"),
            ("item_bars", "item_id, timeframe, bucket_ts"),
            ("manual_quotes", "quote_id"),
            ("inferred_flows", "snapshot_id, item_id"),
            ("predictions", "prediction_id"),
            ("backtest_runs", "run_id"),
        ):
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY {ordering}")
            for row in rows:
                digest.update(repr(tuple(row)).encode("utf-8"))
                digest.update(b"\n")
    return digest.hexdigest()


def test_same_seed_creates_identical_90_day_snapshot_content(tmp_path, monkeypatch):
    first = tmp_path / "first.db"
    second = tmp_path / "second.db"
    monkeypatch.setattr(time, "time", lambda: 1_800_000_000)
    result_one = simulate(first, env="sim", seed=42)
    monkeypatch.setattr(time, "time", lambda: 1_900_000_000)
    result_two = simulate(second, env="sim", seed=42)

    assert result_one == result_two
    assert result_one.snapshots == 90 * 96
    assert result_one.end_ts == 1_768_435_200
    assert _database_digest(first) == _database_digest(second)
    with open_connection(first) as conn:
        assert conn.execute(
            "SELECT count(*) FROM snapshot_runs WHERE source = 'simulator' AND status = 'completed'"
        ).fetchone()[0] == 8640
        assert conn.execute(
            "SELECT count(*) FROM raw_listings WHERE price = 1"
        ).fetchone()[0] > 0
        assert conn.execute("SELECT count(*) FROM item_bars").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM inferred_flows").fetchone()[0] == 0


def test_simulation_rejects_live_without_creating_database(tmp_path):
    path = tmp_path / "live.db"
    with pytest.raises(ValueError, match="forbidden"):
        simulate(path, env="live", days=1)
    assert not path.exists()


def test_catalog_seed_aliases_and_events_are_idempotent(tmp_path):
    path = tmp_path / "catalog.db"
    migrate(path)
    assert sync_catalog(path, env="sim", offline_only=True) == 43
    assert sync_catalog(path, env="sim", offline_only=True) == 43
    with open_connection(path) as conn:
        assert conn.execute("SELECT count(*) FROM items_catalog").fetchone()[0] == 43
        assert tuple(conn.execute(
            "SELECT name_en, name_zh, max_stack_size FROM items_catalog WHERE item_id = 1192"
        ).fetchone()) == ("1,000RP Reward Point Voucher", "1000RP 点券", 99)
        assert conn.execute(
            "SELECT count(*) FROM market_events WHERE name LIKE '%2025' OR name LIKE '%2026'"
        ).fetchone()[0] == 4
        assert conn.execute(
            "SELECT max_stack_size FROM items_catalog WHERE item_id = 900003"
        ).fetchone()[0] == 1


def test_catalog_ids_are_consistent_and_local_id_keyed_json_loads(tmp_path):
    config = load_config(root=PROJECT_ROOT)
    seed_ids = {
        item["item_id"]
        for item in json.loads(
            (PROJECT_ROOT / "config" / "seed_catalog_minimal.json").read_text(encoding="utf-8")
        )
    }
    alias_ids = {item["item_id"] for item in config.item_aliases}
    assert seed_ids == alias_ids
    assert {row["item_id"] for row in config.item_relations["rp_vouchers"]} <= seed_ids
    for relation in config.item_relations["upstream_relations"]:
        assert relation["target_item_id"] in seed_ids
        assert relation["upstream_item_id"] in seed_ids

    local = tmp_path / "local"
    local.mkdir()
    (local / "items.json").write_text(
        '{"items":{"777":{"name":"Local Item","stack_size":"1"}}}', encoding="utf-8"
    )
    path = tmp_path / "local.db"
    migrate(path)
    assert sync_catalog(path, env="sim", from_local=local) == 44
    with open_connection(path) as conn:
        row = conn.execute(
            "SELECT name_en, max_stack_size FROM items_catalog WHERE item_id = 777"
        ).fetchone()
        assert tuple(row) == ("Local Item", 1)


def test_manual_csv_bom_imports_quotes_without_overwriting_snapshot_bars(tmp_path):
    path = tmp_path / "manual.db"
    migrate(path)
    sync_catalog(path, env="sim", offline_only=True)
    recorded_at = 1_768_435_200
    four_hour = recorded_at - recorded_at % 14_400
    day = recorded_at - recorded_at % 86_400
    with open_connection(path) as conn, write_transaction(conn):
        for timeframe, bucket, seconds in (("4h", four_hour, 14_400), ("1d", day, 86_400)):
            conn.execute(
                """INSERT INTO item_bars
                   (item_id, timeframe, bucket_ts, close_ts, data_source,
                    open_price, high_price, low_price, close_price,
                    robust_best_ask, best_ask_min)
                   VALUES (4, ?, ?, ?, 'snapshot', 10, 12, 9, 11, 11, 9)""",
                (timeframe, bucket, bucket + seconds),
            )

    csv_path = tmp_path / "quotes.csv"
    csv_path.write_text(
        "\ufeffitem_id,price,recorded_at,listed_qty,note\n"
        f"4,321,{recorded_at},7,手工报价\n"
        f"4,0,{recorded_at + 86400},2,invalid price\n",
        encoding="utf-8",
    )
    result = import_csv(path, csv_path, now_ts=recorded_at + 86400)

    assert result.imported_rows == 1
    assert result.skipped_snapshot_bars == 2
    assert len(result.errors) == 1 and "price" in result.errors[0]
    with open_connection(path) as conn:
        rows = conn.execute(
            "SELECT timeframe, data_source, close_price FROM item_bars ORDER BY timeframe"
        ).fetchall()
        assert [tuple(row) for row in rows] == [("1d", "snapshot", 11.0), ("4h", "snapshot", 11.0)]
        quote = conn.execute(
            "SELECT price, note FROM manual_quotes WHERE item_id = 4"
        ).fetchone()
        assert tuple(quote) == (321, "手工报价")


def test_manual_csv_materializes_manual_4h_and_1d_bars(tmp_path):
    path = tmp_path / "manual-bars.db"
    migrate(path)
    sync_catalog(path, env="sim", offline_only=True)
    csv_path = tmp_path / "quotes.csv"
    csv_path.write_text(
        "item_id,price,recorded_at,listed_qty\n4,100,1768435200,8\n4,125,1768442400,6\n",
        encoding="utf-8",
    )

    result = import_csv(path, csv_path)

    assert result.imported_rows == 2
    assert result.skipped_snapshot_bars == 0
    with open_connection(path) as conn:
        bars = conn.execute(
            "SELECT timeframe, open_price, high_price, low_price, close_price, sample_count "
            "FROM item_bars ORDER BY timeframe"
        ).fetchall()
        assert len(bars) == 2
        assert all(row[0] in ("4h", "1d") for row in bars)
        assert all(row[1:] == (100.0, 125.0, 100.0, 125.0, 2) for row in bars)


def test_manual_csv_enforces_stream_limits_and_rolls_back_over_limit(tmp_path):
    path = tmp_path / "bounded.db"
    migrate(path)
    sync_catalog(path, env="sim", offline_only=True)
    csv_path = tmp_path / "quotes.csv"
    csv_path.write_text("item_id,price\n4,100\n4,200\n", encoding="utf-8")

    with pytest.raises(ValueError, match="row limit"):
        import_csv(path, csv_path, now_ts=1_768_435_200, max_rows=1)
    with pytest.raises(ValueError, match="byte limit"):
        import_csv(path, csv_path, now_ts=1_768_435_200, max_bytes=8)
    with open_connection(path) as conn:
        assert conn.execute("SELECT count(*) FROM manual_quotes").fetchone()[0] == 0


def test_manual_csv_counts_physical_lines_inside_quoted_fields(tmp_path):
    path = tmp_path / "multiline.db"
    migrate(path)
    sync_catalog(path, env="sim", offline_only=True)
    csv_path = tmp_path / "multiline.csv"
    csv_path.write_text('item_id,price,note\n4,100,"line one\nline two"\n', encoding="utf-8")

    with pytest.raises(ValueError, match="row limit"):
        import_csv(path, csv_path, now_ts=1_768_435_200, max_rows=1)
    with open_connection(path) as conn:
        assert conn.execute("SELECT count(*) FROM manual_quotes").fetchone()[0] == 0


def test_m1_cli_commands_accept_environment_and_reject_live_simulation(tmp_path, monkeypatch):
    shutil.copytree(PROJECT_ROOT / "config", tmp_path / "config")
    monkeypatch.setattr(config_module, "PROJECT_ROOT", tmp_path)
    runner = CliRunner()

    synced = runner.invoke(app, ["sync-catalog", "--env", "sim", "--offline-only"])
    assert synced.exit_code == 0, synced.output
    csv_path = tmp_path / "quotes.csv"
    csv_path.write_text("item_id,price\n4,123\n", encoding="utf-8")
    imported = runner.invoke(app, ["import-csv", str(csv_path), "--env", "sim"])
    assert imported.exit_code == 0, imported.output
    simulated = runner.invoke(app, ["simulate", "--days", "1", "--seed", "7", "--env", "sim"])
    assert simulated.exit_code == 0, simulated.output
    assert "snapshots=96" in simulated.output
    rejected = runner.invoke(app, ["simulate", "--days", "1", "--env", "live"])
    assert rejected.exit_code != 0


def test_remote_catalog_uses_real_item_ids_and_metadata(respx_mock, tmp_path):
    path = tmp_path / "remote.db"
    migrate(path)
    respx_mock.get(f"{RAW_BASE}/items.json").respond(200, json=[
        {"id": 138, "name": "Leppa Berry", "description": "Restores PP"},
        {"id": 2269, "name": "Santa Hat"},
        {"id": 12345, "name": "Additional Berry"},
        {"id": 12346, "name": "Additional Hat"},
    ])
    respx_mock.get(f"{RAW_BASE}/items-berry.json").respond(
        200, json=[{"id": 6, "items": [138, 12345]}]
    )
    respx_mock.get(f"{RAW_BASE}/items-cosmetic.json").respond(
        200, json=[{"item_id": [2269, 12346]}]
    )
    assert sync_catalog(path, env="sim") == 45
    with open_connection(path) as conn:
        berry = conn.execute("SELECT * FROM items_catalog WHERE item_id = 138").fetchone()
        assert berry["name_en"] == "Leppa Berry"
        assert berry["name_zh"] == "苹野果（PP果）"
        assert berry["asset_class"] == "berry" and berry["max_stack_size"] == 99
        assert conn.execute(
            "SELECT max_stack_size FROM items_catalog WHERE item_id = 12346"
        ).fetchone()[0] == 1
        # Metadata ID 6 must never replace the actual Net Ball with a berry record.
        assert conn.execute(
            "SELECT name_en FROM items_catalog WHERE item_id = 6"
        ).fetchone()[0] == "Net Ball"


def test_network_failure_falls_back_and_offline_makes_no_requests(respx_mock, tmp_path):
    path = tmp_path / "fallback.db"
    migrate(path)
    route = respx_mock.get(f"{RAW_BASE}/items.json").respond(503)
    assert sync_catalog(path, env="sim", offline_only=True) == 43
    assert route.call_count == 0
    with pytest.warns(UserWarning, match="bundled seed"):
        assert sync_catalog(path, env="sim") == 43
    assert route.call_count == 1


def test_local_checkout_ignores_moves_and_metadata_ids(tmp_path):
    local = tmp_path / "checkout" / "data"
    local.mkdir(parents=True)
    for name, content in {
        "items.json": [{"id": 12345, "name": "Local Hat"}],
        "moves.json": [{"id": 4, "name": "Comet Punch"}],
        "items-cosmetic.json": [{"item_id": [12345]}],
    }.items():
        (local / name).write_text(json.dumps(content), encoding="utf-8")
    path = tmp_path / "checkout.db"
    migrate(path)
    assert sync_catalog(path, env="sim", from_local=local.parent) == 44
    with open_connection(path) as conn:
        assert conn.execute(
            "SELECT name_en FROM items_catalog WHERE item_id = 4"
        ).fetchone()[0] == "Poké Ball"
        assert conn.execute(
            "SELECT max_stack_size FROM items_catalog WHERE item_id = 12345"
        ).fetchone()[0] == 1


def test_default_events_refresh_window_without_duplicates(tmp_path):
    path = tmp_path / "events.db"
    migrate(path)
    events = [dict(load_config().events[0])]
    assert sync_default_events(path, events) == 1
    events[0]["start_ts"] += 86400
    events[0]["end_ts"] += 86400
    sync_default_events(path, events)
    with open_connection(path) as conn:
        assert conn.execute("SELECT count(*) FROM market_events").fetchone()[0] == 1
        start_ts = conn.execute("SELECT start_ts FROM market_events").fetchone()[0]
        assert start_ts == events[0]["start_ts"]


@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_callback_observes_two_phases_and_failure_rolls_back_every_write(tmp_path, error_type):
    path = tmp_path / "atomic.db"

    def fail(conn, snapshot_id, previous_id, captured_at):
        assert conn.in_transaction and previous_id is None
        assert conn.execute("SELECT count(*) FROM raw_listings").fetchone()[0] > 0
        with open_connection(path) as reader:
            assert reader.execute("SELECT status FROM snapshot_runs").fetchone()[0] == "running"
            assert reader.execute("SELECT count(*) FROM raw_listings").fetchone()[0] == 0
        conn.execute(
            "INSERT INTO manual_quotes(item_id, price, recorded_at) VALUES (4, 50, ?)",
            (captured_at,),
        )
        raise error_type("callback failure")

    with pytest.raises(error_type):
        simulate(path, env="sim", days=1, on_snapshot=fail)
    with open_connection(path) as conn:
        assert conn.execute("SELECT status FROM snapshot_runs").fetchone()[0] == "failed"
        assert conn.execute("SELECT count(*) FROM raw_listings").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM manual_quotes").fetchone()[0] == 0


def test_generation_is_outside_transaction_and_unknown_items_are_atomic(tmp_path, monkeypatch):
    path = tmp_path / "generation.db"
    previous_ids = []

    def frame(self, captured_at, start_ts):
        # A competing writer can acquire BEGIN IMMEDIATE during generation.
        with open_connection(path) as conn, write_transaction(conn):
            assert conn.execute(
                "SELECT count(*) FROM snapshot_runs WHERE status = 'running'"
            ).fetchone()[0] == 1
        return [(captured_at, 88888, 123, 1, captured_at + 86400)]

    def observe(conn, snapshot_id, previous_id, captured_at):
        assert previous_id == (previous_ids[-1] if previous_ids else None)
        previous_ids.append(snapshot_id)
        assert conn.execute(
            "SELECT name_en FROM items_catalog WHERE item_id = 88888"
        ).fetchone()[0] == "Unknown Item #88888"

    monkeypatch.setattr(_OrderBook, "frame", frame)
    result = simulate(path, env="sim", days=1, on_snapshot=observe)
    assert len(previous_ids) == result.snapshots == 96


def test_stage_two_insert_failure_and_stale_cleanup(tmp_path):
    path = tmp_path / "insert-failure.db"
    migrate(path)
    sync_catalog(path, env="sim", offline_only=True)
    started_at = load_config().settings.simulation_default_end_ts - 86400
    with open_connection(path) as conn, write_transaction(conn):
        stale_id = conn.execute(
            "INSERT INTO snapshot_runs(source, started_at, server_started_at, status) "
            "VALUES ('simulator', ?, ?, 'running')", (started_at - 1801, started_at - 1801)
        ).lastrowid
        conn.execute("INSERT INTO raw_listings VALUES (?, 1, 4, 100, 1, ?)", (stale_id, started_at))
        conn.execute(
            "CREATE TRIGGER fail_raw BEFORE INSERT ON raw_listings "
            "WHEN NEW.auction_id = 2 BEGIN SELECT RAISE(ABORT, 'injected'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        simulate(path, env="sim", days=1)
    with open_connection(path) as conn:
        assert conn.execute("SELECT count(*) FROM raw_listings").fetchone()[0] == 0
        assert {row[0] for row in conn.execute("SELECT status FROM snapshot_runs")} == {"failed"}


def test_market_scenarios_have_cycles_drift_crashes_and_persistent_listings():
    config = load_config("sim")
    items = json.loads(
        (config.root / "config" / "seed_catalog_minimal.json").read_text(encoding="utf-8")
    )
    voucher = next(item for item in items if item["item_id"] == 1192)
    start = config.settings.simulation_default_end_ts - 90 * 86400
    for holiday in (1761868800, 1766620800):
        before = market_profile(voucher, holiday - 900, start, 42)[0]
        crash = market_profile(voucher, holiday + 86400, start, 42)[0]
        rebound = market_profile(voucher, holiday + 13 * 86400, start, 42)[0]
        assert crash < before * 0.7
        assert rebound > crash * 1.3
    price_early = market_profile(voucher, start, start, 42)[0]
    price_late = market_profile(voucher, start + 49 * 86400, start, 42)[0]
    assert price_late > price_early * 1.08
    pressures = [market_profile(voucher, start + day * 86400, start, 42)[1] for day in range(7)]
    prices = [market_profile(voucher, start + day * 86400, start, 42)[0] for day in range(7)]
    assert max(prices) / min(prices) > 1.08
    assert pressures.index(max(pressures)) == (prices.index(max(prices)) - 1) % 7

    book = _OrderBook([voucher], 42)
    previous = {}
    persistent = partial = early_far = False
    for offset in range(7 * 96):
        captured_at = start + offset * 900
        current = {row[0]: row for row in book.frame(captured_at, start) if row[2] > 1}
        common = previous.keys() & current.keys()
        persistent |= bool(common)
        for key in common:
            assert previous[key][1:3] == current[key][1:3]
            assert previous[key][4] == current[key][4]
            partial |= previous[key][3] > current[key][3]
        for key in previous.keys() - current.keys():
            early_far |= previous[key][4] > captured_at + 180 and previous[key][2] > min(
                row[2] for row in current.values()
            ) * 1.2
        previous = current
    assert persistent and partial and early_far


def test_direct_manual_quotes_aggregate_out_of_order_with_null_flows(tmp_path):
    path = tmp_path / "single.db"
    migrate(path)
    sync_catalog(path, env="sim", offline_only=True)
    add_manual_quote(path, 4, 150, recorded_at=1768442400, listed_qty=7)
    add_manual_quote(path, 4, 100, recorded_at=1768435200, listed_qty=9)
    with open_connection(path) as conn:
        for bar in conn.execute("SELECT * FROM item_bars"):
            assert (bar["open_price"], bar["close_price"], bar["sample_count"]) == (100, 150, 2)
            assert bar["total_listed_qty"] == 7
            assert bar["est_sold_qty_sum"] is None and bar["valid_flow_samples"] == 0


@pytest.mark.parametrize("price", ["0", "-1", "1.5", "NaN", "1e3", str(2**63)])
def test_invalid_csv_prices_are_reported_without_writes(tmp_path, price):
    path = tmp_path / "invalid.db"
    migrate(path)
    sync_catalog(path, env="sim", offline_only=True)
    source = tmp_path / "invalid.csv"
    source.write_text(f"item_id,price\n4,{price}\n", encoding="utf-8")
    result = import_csv(path, source)
    assert result.imported_rows == 0 and len(result.errors) == 1
    with open_connection(path) as conn:
        assert conn.execute("SELECT count(*) FROM item_bars").fetchone()[0] == 0


def test_csv_accepts_exact_row_and_byte_limits(tmp_path):
    path = tmp_path / "limits.db"
    migrate(path)
    sync_catalog(path, env="sim", offline_only=True)
    source = tmp_path / "exact-rows.csv"
    source.write_text("item_id,price\n" + "4,100\n" * 50_000, encoding="utf-8")
    assert import_csv(path, source, now_ts=1768435200).imported_rows == 50_000

    prefix = "item_id,price\n4,100\n"
    line = "4,100," + "x" * 100_000 + "\n"
    # Use ordinary, valid <128KB fields and exactly 5 MB of UTF-8 input.
    header = "item_id,price,note\n"
    body = line * 52
    remaining = MAX_IMPORT_BYTES - len((header + body).encode("utf-8"))
    tail = "4,100," + "y" * (remaining - len("4,100,\n")) + "\n"
    source = tmp_path / "exact-bytes.csv"
    source.write_text(header + body + tail, encoding="utf-8", newline="")
    assert source.stat().st_size == MAX_IMPORT_BYTES
    assert import_csv(path, source, now_ts=1768435200).imported_rows == 53
    source.write_text(prefix + "4,100\n" * 50_000, encoding="utf-8")
    with pytest.raises(ValueError, match="row limit"):
        import_csv(path, source, now_ts=1768435200)
