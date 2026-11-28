"""Populated 001 upgrades, explicit Web projections, and real manual-quote chains."""

from __future__ import annotations

import csv
import inspect
import io
import json
from collections.abc import Mapping
from dataclasses import replace
from functools import wraps
from pathlib import Path
from shutil import copytree
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from pokemmo_gtl import cli
from pokemmo_gtl.config import PROJECT_ROOT, Environment, load_config
from pokemmo_gtl.db import repository
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.quant import backtester, predictor
from pokemmo_gtl.quant.ledger import append_ledger_events
from pokemmo_gtl.web import app as web
from pokemmo_gtl.web import schemas as s

BASE_URL = "http://127.0.0.1:8000"
ANCHOR = 1761868800
DAY = 86400
GRID = 14400
HISTORY_DAYS = 28
MANUAL_ITEM = 900001
LEGACY_TABLES = (
    "items_catalog",
    "snapshot_runs",
    "raw_listings",
    "item_bars",
    "inferred_flows",
    "manual_quotes",
    "market_events",
    "predictions",
    "backtest_runs",
)


def _insert_record(conn, table, values):
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    return conn.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
        tuple(values.values()),
    ).lastrowid


def _legacy_price(item_id, day, period=0):
    if item_id == 1190:
        return 3500
    if item_id == 1191:
        return 20000
    if item_id == 1192:
        return 5000
    if item_id == MANUAL_ITEM:
        return 80000 + day * 100 + period * 10
    return 1000 + day * 3 + period


def _insert_legacy_bar(conn, item_id, close_ts, timeframe, price):
    seconds = {"15m": 900, "4h": GRID, "1d": DAY}[timeframe]
    manual = item_id == MANUAL_ITEM
    _insert_record(conn, "item_bars", {
        "item_id": item_id,
        "timeframe": timeframe,
        "bucket_ts": close_ts - seconds,
        "close_ts": close_ts,
        "data_source": "manual" if manual else "snapshot",
        "open_price": float(price),
        "high_price": float(price + 20),
        "low_price": float(price - 20),
        "close_price": float(price),
        "robust_best_ask": price,
        "best_ask_min": price - 20,
        "p10_price": float(price),
        "total_listed_qty": 3 if manual else 100,
        "listing_count": None if manual else 5,
        "depth_within_5pct": None if manual else 30,
        "spread_top5_pct": None if manual else 0.02,
        "est_sold_qty_sum": None if manual else 5.0,
        "expired_qty_sum": None if manual else 1,
        "new_listed_qty_sum": None if manual else 4,
        "sample_count": 1 if manual else 16,
        "valid_flow_samples": 0 if manual else 15,
    })


def _create_populated_001_database(db_path):
    """Apply only the original SQL before inserting any historical data."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    migration_path = (
        Path(repository.__file__).resolve().parent / "migrations" / "001_init.sql"
    )
    with open_connection(db_path) as conn:
        conn.executescript(migration_path.read_text(encoding="utf-8"))
        with write_transaction(conn):
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                ("001_init", ANCHOR - HISTORY_DAYS * DAY),
            )
            for item_id, name, category, asset_class, stack in (
                (138, "Historical Stone", "stone", "item", 99),
                (1190, "100RP Voucher", "rp_voucher", "item", 99),
                (1191, "500RP Voucher", "rp_voucher", "item", 99),
                (1192, "1000RP Voucher", "rp_voucher", "item", 99),
                (MANUAL_ITEM, "Manual Pokemon Material", "pokemon_category",
                 "pokemon_category", 1),
            ):
                _insert_record(conn, "items_catalog", {
                    "item_id": item_id,
                    "name_en": name,
                    "name_zh": None,
                    "category": category,
                    "asset_class": asset_class,
                    "max_stack_size": stack,
                    "is_watched": 1,
                    "updated_at": ANCHOR - DAY,
                })
            start = ANCHOR - HISTORY_DAYS * DAY
            previous_snapshot_id = None
            for day in range(HISTORY_DAYS):
                day_close = start + (day + 1) * DAY
                snapshot_id = _insert_record(conn, "snapshot_runs", {
                    "source": "simulator",
                    "started_at": day_close - 120,
                    "server_started_at": day_close - 120,
                    "completed_at": day_close - 60,
                    "status": "completed",
                    "total_listings": 1,
                    "pages_fetched": 1,
                })
                _insert_record(conn, "raw_listings", {
                    "snapshot_id": snapshot_id,
                    "auction_id": day + 1,
                    "item_id": 138,
                    "price": _legacy_price(138, day),
                    "quantity": 10,
                    "expires_at": day_close + DAY,
                })
                if previous_snapshot_id is not None:
                    _insert_record(conn, "inferred_flows", {
                        "snapshot_id": snapshot_id,
                        "prev_snapshot_id": previous_snapshot_id,
                        "item_id": 138,
                        "captured_at": day_close - 120,
                        "interval_sec": DAY,
                        "new_listing_count": 1,
                        "new_listed_qty": 4,
                        "partial_fill_qty": 2,
                        "early_disappear_qty": 1,
                        "expired_qty": 1,
                        "est_sold_qty": 2.5,
                        "est_sold_vwap": float(_legacy_price(138, day)),
                        "gap_skipped": 0,
                    })
                previous_snapshot_id = snapshot_id
                for item_id in (138, 1190, 1191, 1192, MANUAL_ITEM):
                    for period in range(6):
                        close_ts = start + day * DAY + (period + 1) * GRID
                        if item_id == MANUAL_ITEM and close_ts == ANCHOR:
                            continue
                        _insert_legacy_bar(
                            conn, item_id, close_ts, "4h",
                            _legacy_price(item_id, day, period),
                        )
                    if item_id != MANUAL_ITEM:
                        _insert_legacy_bar(
                            conn, item_id, day_close, "15m",
                            _legacy_price(item_id, day),
                        )
                    if item_id != MANUAL_ITEM or day_close < ANCHOR:
                        _insert_legacy_bar(
                            conn, item_id, day_close, "1d",
                            _legacy_price(item_id, day),
                        )
                if day_close < ANCHOR:
                    _insert_record(conn, "manual_quotes", {
                        "item_id": MANUAL_ITEM,
                        "recorded_at": day_close - 1800,
                        "price": _legacy_price(MANUAL_ITEM, day),
                        "listed_qty": 3,
                        "note": "001 historical quote",
                        "source_tag": "manual",
                    })
            _insert_record(conn, "market_events", {
                "name": "Historical event",
                "event_type": "holiday",
                "announced_at": start,
                "start_ts": ANCHOR - 2 * DAY,
                "end_ts": ANCHOR + DAY,
                "impact_scope": "all",
                "notes": "Preserved across migration",
            })
            for item_id in (138, 1190, 1191, 1192, MANUAL_ITEM):
                price = _legacy_price(item_id, HISTORY_DAYS - 1)
                for horizon in (24, 72, 168):
                    _insert_record(conn, "predictions", {
                        "generated_at": ANCHOR - GRID,
                        "item_id": item_id,
                        "horizon_hours": horizon,
                        "model_name": "naive",
                        "current_price": float(price),
                        "ref_qty": 1,
                        "prob_up": 0.5,
                        "pred_q10": price * 0.9,
                        "pred_q50": float(price),
                        "pred_q90": price * 1.1,
                        "expected_net_roi": 0.01,
                        "signal": "NEUTRAL",
                        "regime_note": "001 historical forecast",
                        "top_drivers_json": json.dumps([
                            {"feature": "ret_24h", "contribution": 0.0, "value": 0.0},
                        ]),
                    })
            _insert_record(conn, "backtest_runs", {
                "created_at": ANCHOR - DAY,
                "item_scope": "all",
                "horizon_hours": 24,
                "model_name": "naive",
                "config_json": '{"models":["naive"]}',
                "metrics_json": '{"trade_count":0}',
                "equity_curve_json": "[]",
            })
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(item_bars)")
        }
        assert "book_state" not in columns
        assert "bar_available_at" not in columns
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'publish_events'",
        ).fetchone() is None


def _legacy_counts(db_path):
    with open_connection(db_path) as conn:
        return {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in LEGACY_TABLES
        }


def _upgrade_database(config):
    before = _legacy_counts(config.db_path)
    assert all(count > 0 for count in before.values())
    migrate(config.db_path)
    assert _legacy_counts(config.db_path) == before
    with open_connection(config.db_path) as conn:
        versions = [
            row["version"] for row in conn.execute("SELECT version FROM schema_migrations")
        ]
        assert any(str(version).startswith("002") for version in versions)
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute(
            "SELECT COUNT(*) FROM snapshot_runs WHERE raw_retained IS NULL",
        ).fetchone()[0] == before["snapshot_runs"]
        assert conn.execute(
            "SELECT COUNT(*) FROM item_bars WHERE book_state = 'stocked' "
            "AND bar_available_at IS NULL AND latest_publish_seq IS NULL",
        ).fetchone()[0] == before["item_bars"]
        assert conn.execute(
            "SELECT COUNT(*) FROM manual_quotes WHERE ingested_at IS NULL",
        ).fetchone()[0] == before["manual_quotes"]
        assert conn.execute(
            "SELECT api_covered FROM items_catalog WHERE item_id = ?", (MANUAL_ITEM,),
        ).fetchone()[0] == 0
    return config


@pytest.fixture
def legacy_config(tmp_path):
    copytree(PROJECT_ROOT / "config", tmp_path / "config")
    base = load_config(Environment.sim)
    relations = dict(base.item_relations)
    relations["rp_vouchers"] = [
        {"item_id": 1190, "denomination": 100},
        {"item_id": 1191, "denomination": 500},
        {"item_id": 1192, "denomination": 1000},
    ]
    config = replace(base, root=tmp_path, api_token=None, item_relations=relations)
    _create_populated_001_database(config.db_path)
    return config


@pytest.fixture
def upgraded_config(legacy_config):
    return _upgrade_database(legacy_config)


def _publish_state_bar(conn, item_id, book_state, close_ts=ANCHOR + GRID):
    snapshot_id = _insert_record(conn, "snapshot_runs", {
        "source": "simulator",
        "started_at": close_ts - 60,
        "server_started_at": close_ts - 60,
        "completed_at": close_ts,
        "status": "completed",
        "total_listings": 0,
        "pages_fetched": 1,
        "last_page_server_at": close_ts - 60,
        "available_at": close_ts,
        "raw_retained": 1,
    })
    publish_seq = _insert_record(conn, "publish_events", {
        "available_at": close_ts,
        "source_type": "snapshot",
        "source_id": snapshot_id,
    })
    conn.execute(
        "UPDATE snapshot_runs SET publish_seq = ? WHERE snapshot_id = ?",
        (publish_seq, snapshot_id),
    )
    mixed = book_state == "mixed_ended_empty"
    _insert_record(conn, "item_bars", {
        "item_id": item_id,
        "timeframe": "4h",
        "bucket_ts": close_ts - GRID,
        "close_ts": close_ts,
        "data_source": "snapshot",
        "book_state": book_state,
        "bar_available_at": close_ts,
        "latest_publish_seq": publish_seq,
        "open_price": 5000.0 if mixed else None,
        "high_price": 5200.0 if mixed else None,
        "low_price": 4800.0 if mixed else None,
        "close_price": 5000.0 if mixed else None,
        "robust_best_ask": None,
        "best_ask_min": 4800 if mixed else None,
        "p10_price": None,
        "total_listed_qty": 0,
        "listing_count": 0,
        "depth_within_5pct": 0,
        "spread_top5_pct": None,
        "est_sold_qty_sum": 3.0,
        "expired_qty_sum": 1,
        "new_listed_qty_sum": 0,
        "sample_count": 2,
        "valid_flow_samples": 1,
    })
    return publish_seq


def test_populated_001_upgrade_preserves_all_history_and_is_idempotent(legacy_config):
    _upgrade_database(legacy_config)
    before = _legacy_counts(legacy_config.db_path)
    assert not migrate(legacy_config.db_path)
    assert _legacy_counts(legacy_config.db_path) == before


def test_upgraded_web_contracts_project_legacy_and_empty_bars_explicitly(upgraded_config):
    config = upgraded_config
    with open_connection(config.db_path) as conn, write_transaction(conn):
        empty_seq = _publish_state_bar(conn, 138, "empty_confirmed")
        mixed_seq = _publish_state_bar(conn, 1192, "mixed_ended_empty")
        # Future storage columns must not silently become strict response fields.
        conn.execute("ALTER TABLE item_bars ADD COLUMN future_measurement TEXT")
    with TestClient(web.create_app(config=config), base_url=BASE_URL) as client:
        for path in (
            "/api/health",
            "/api/overview",
            "/api/items",
            "/api/items/138/bars?timeframe=15m&days=0",
            "/api/items/138/bars?timeframe=4h&days=0",
            "/api/items/138/bars?timeframe=1d&days=0",
            "/api/items/1192/bars?timeframe=4h&days=0",
            "/api/items/1192/forecast",
        ):
            response = client.get(path)
            assert response.status_code == 200, (path, response.text)
            assert "extra_forbidden" not in response.text
        health = s.HealthResponse.model_validate(client.get("/api/health").json())
        assert health.item_count == 5
        items = {
            item["item_id"]: item for item in client.get("/api/items").json()
        }
        assert items[138]["current_price"] is None
        assert items[138]["book_state"] == "empty_confirmed"
        assert items[1192]["current_price"] is None
        assert items[1192]["book_state"] == "mixed_ended_empty"
        assert items[1192]["total_listed_qty"] == 0
        assert items[1192]["change_24h_pct"] is None
        empty = s.BarsResponse.model_validate(
            client.get("/api/items/138/bars?days=0").json(),
        )
        assert any(bar.bar_available_at is None for bar in empty.bars[:-1])
        assert empty.bars[-1].latest_publish_seq == empty_seq
        assert empty.bars[-1].open_price is empty.bars[-1].close_price is None
        assert empty.bars[-1].total_listed_qty == 0
        mixed = s.BarsResponse.model_validate(
            client.get("/api/items/1192/bars?days=0").json(),
        )
        assert mixed.bars[-1].latest_publish_seq == mixed_seq
        assert mixed.bars[-1].close_price == 5000
        assert mixed.bars[-1].robust_best_ask is None
        overview = s.OverviewResponse.model_validate(client.get("/api/overview").json())
        assert overview.rp_index.value == pytest.approx(37.5)
        forecast = s.ForecastResponse.model_validate(
            client.get("/api/items/1192/forecast").json(),
        )
        assert len(forecast.forecasts) == 3
        exported = client.get("/api/items/1192/export.csv")
        assert exported.status_code == 200, exported.text
        exported_rows = list(csv.DictReader(io.StringIO(exported.text)))
        assert exported_rows[-1]["book_state"] == "mixed_ended_empty"
        assert exported_rows[-1]["close_price"] == "5000.0"
        assert "future_measurement" not in exported_rows[-1]
        backtest = client.post("/api/backtest/run", json={
            "item_id": 138, "horizon_hours": 24, "models": ["naive"],
        })
        assert backtest.status_code == 200, backtest.text
        assert "extra_forbidden" not in backtest.text
        assert s.BacktestResponse.model_validate(backtest.json()).run_id > 1


def test_overview_rp_uses_configured_denominations_empty_barriers_and_ttl(upgraded_config):
    config = upgraded_config
    with open_connection(config.db_path) as conn, write_transaction(conn):
        _publish_state_bar(conn, 1192, "mixed_ended_empty")
    with TestClient(web.create_app(config=config), base_url=BASE_URL) as client:
        response = client.get("/api/overview")
        assert response.status_code == 200, response.text
        assert response.json()["rp_index"]["value"] == pytest.approx(37.5)
        with open_connection(config.db_path) as conn, write_transaction(conn):
            _publish_state_bar(conn, 1190, "empty_confirmed")
        response = client.get("/api/overview")
        assert response.status_code == 200, response.text
        assert response.json()["rp_index"]["value"] == pytest.approx(40.0)
        max_age = int(getattr(
            config.settings, "max_rp_quote_age_sec",
            getattr(config, "max_rp_quote_age_sec", DAY),
        ))
        with open_connection(config.db_path) as conn, write_transaction(conn):
            conn.execute(
                "DELETE FROM item_bars WHERE item_id = 1191 AND timeframe = '4h' "
                "AND close_ts >= ?",
                (ANCHOR + GRID - max_age,),
            )
        response = client.get("/api/overview")
        assert response.status_code == 200, response.text
        rp_index = response.json()["rp_index"]
        assert rp_index is None or rp_index["value"] is None
        if rp_index is not None:
            assert rp_index["change_24h_pct"] is None
            assert rp_index["inflation_level"] is None


def _feature_loaders():
    return [
        (module, getattr(module, "_load_feature_panel"))
        for module in (backtester, predictor)
        if callable(getattr(module, "_load_feature_panel", None))
    ]


def _panel_frame(value):
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, Mapping):
        candidates = value.values()
    elif isinstance(value, (list, tuple)):
        candidates = value
    else:
        candidates = ()
    for candidate in candidates:
        if isinstance(candidate, pd.DataFrame) and {
            "item_id", "close_ts", "manual_1d_close_price",
        }.issubset(candidate.columns):
            return candidate
    pytest.fail("_load_feature_panel must return its actual feature DataFrame")


def _load_panel(config, *, as_of_ts):
    """Bind the shared loader's named inputs, including its explicit cutoff."""
    loaders = _feature_loaders()
    assert loaders, "The causal quant feature loader must be available"
    loader = loaders[0][1]
    signature = inspect.signature(loader)
    assert "as_of_ts" in signature.parameters
    with open_connection(config.db_path) as conn:
        available = {
            "config": config,
            "cfg": config,
            "conn": conn,
            "connection": conn,
            "db_path": config.db_path,
            "settings": config.settings,
            "as_of_ts": as_of_ts,
        }
        args = []
        kwargs = {}
        for name, parameter in signature.parameters.items():
            if name not in available:
                continue
            if parameter.kind == inspect.Parameter.POSITIONAL_ONLY:
                args.append(available[name])
            else:
                kwargs[name] = available[name]
        signature.bind(*args, **kwargs)
        return _panel_frame(loader(*args, **kwargs)).copy(deep=True)


def _latest_manual_feature(frame):
    selected = frame.loc[frame["item_id"].eq(MANUAL_ITEM)].sort_values("close_ts")
    assert not selected.empty, "Legacy manual history must survive the upgrade"
    return selected.iloc[-1]


def _assert_quote_publication(config, quote_id, price):
    with open_connection(config.db_path) as conn:
        quote = conn.execute(
            "SELECT recorded_at, ingested_at, publish_seq FROM manual_quotes WHERE quote_id = ?",
            (quote_id,),
        ).fetchone()
        assert quote["recorded_at"] < quote["ingested_at"] == ANCHOR
        assert quote["publish_seq"] > 0
        revisions = conn.execute(
            "SELECT timeframe, close_price, revision_available_at, data_source "
            "FROM item_bar_revisions WHERE item_id = ? AND publish_seq = ? "
            "ORDER BY timeframe",
            (MANUAL_ITEM, quote["publish_seq"]),
        ).fetchall()
        assert {row["timeframe"] for row in revisions} == {"4h", "1d"}
        assert len(revisions) == 2
        assert all(row["close_price"] == price for row in revisions)
        assert all(row["revision_available_at"] == ANCHOR for row in revisions)
        assert all(row["data_source"] == "manual" for row in revisions)
        return quote["publish_seq"]


def _capture_feature_panels(monkeypatch):
    """Observe real loader results without replacing any quant calculations."""
    observed = []

    def capture(loader):
        @wraps(loader)
        def wrapped(*args, **kwargs):
            result = loader(*args, **kwargs)
            frame = _panel_frame(result)
            selected = frame.loc[
                frame["item_id"].eq(MANUAL_ITEM),
                ["close_ts", "close_price", "manual_1d_close_price"],
            ].sort_values("close_ts")
            if not selected.empty:
                observed.append(selected.iloc[-1].to_dict())
            return result

        return wrapped

    loaders = _feature_loaders()
    assert loaders
    for module, loader in loaders:
        monkeypatch.setattr(module, "_load_feature_panel", capture(loader))
    return observed


def test_upgraded_manual_and_csv_quotes_reach_features_forecast_and_backtest(
    upgraded_config, monkeypatch,
):
    config = upgraded_config
    # ingested_at is the server's actual import time, never client-controlled.
    # Both imports deliberately publish in one second with distinct sequences.
    monkeypatch.setattr(web.manual_import.time, "time", lambda: ANCHOR)
    with TestClient(web.create_app(config=config), base_url=BASE_URL) as client:
        response = client.post("/api/quotes/manual", json={
            "item_id": MANUAL_ITEM,
            "price": 90000,
            "listed_qty": 3,
            "recorded_at": ANCHOR - 3600,
            "note": "Late manual quote after 001 upgrade",
        })
        assert response.status_code == 200, response.text
        assert "extra_forbidden" not in response.text
        manual = s.ManualQuoteResponse.model_validate(response.json())
        first_seq = _assert_quote_publication(config, manual.quote_id, 90000)

        before = _load_panel(config, as_of_ts=ANCHOR - 1)
        after = _load_panel(config, as_of_ts=ANCHOR)
        before_item = before.loc[before["item_id"].eq(MANUAL_ITEM)]
        assert not before_item["close_price"].eq(90000).any()
        assert not before_item["manual_1d_close_price"].eq(90000).any()
        manual_feature = _latest_manual_feature(after)
        assert manual_feature["close_ts"] == ANCHOR
        assert manual_feature["close_price"] == 90000
        assert manual_feature["manual_1d_close_price"] == 90000

        observed = _capture_feature_panels(monkeypatch)
        first_forecasts = predictor.run_forecast(
            config, horizons=(24,), item_ids=[MANUAL_ITEM],
        )
        assert first_forecasts
        assert all(record["current_price"] == 90000 for record in first_forecasts)
        assert any(row["manual_1d_close_price"] == 90000 for row in observed)

        body = (
            "item_id,price,listed_qty,recorded_at,note\n"
            f"{MANUAL_ITEM},120000,4,{ANCHOR - 1800},升级后 CSV 报价\n"
        ).encode("utf-8")
        response = client.post("/api/quotes/import-csv", files={
            "file": ("upgrade-quotes.csv", body, "text/csv"),
        })
        assert response.status_code == 200, response.text
        imported = s.CSVImportResponse.model_validate(response.json())
        assert imported.imported_rows == 1
        assert imported.skipped_snapshot_bars == 0
        assert imported.errors == []
        with open_connection(config.db_path) as conn:
            csv_quote_id = conn.execute(
                "SELECT quote_id FROM manual_quotes WHERE item_id = ? AND price = 120000 "
                "ORDER BY quote_id DESC LIMIT 1",
                (MANUAL_ITEM,),
            ).fetchone()[0]
        second_seq = _assert_quote_publication(config, csv_quote_id, 120000)
        assert second_seq > first_seq

        before_csv = _load_panel(config, as_of_ts=ANCHOR - 1)
        after_csv = _load_panel(config, as_of_ts=ANCHOR)
        before_csv_item = before_csv.loc[before_csv["item_id"].eq(MANUAL_ITEM)]
        assert not before_csv_item["close_price"].isin([90000, 120000]).any()
        assert not before_csv_item["manual_1d_close_price"].isin([90000, 120000]).any()
        csv_feature = _latest_manual_feature(after_csv)
        assert csv_feature["close_ts"] == ANCHOR
        assert csv_feature["open_price"] == 90000
        assert csv_feature["close_price"] == 120000
        assert csv_feature["manual_1d_close_price"] == 120000
        assert csv_feature["manual_1d_listed_qty"] == 4

        observed.clear()
        second_forecasts = predictor.run_forecast(
            config, horizons=(24,), item_ids=[MANUAL_ITEM],
        )
        assert second_forecasts
        assert all(record["current_price"] == 120000 for record in second_forecasts)
        assert first_forecasts[0]["current_price"] != second_forecasts[0]["current_price"]
        assert any(
            row["close_price"] == row["manual_1d_close_price"] == 120000
            for row in observed
        )

        observed.clear()
        result = backtester.run_backtest(
            config, horizon_hours=24, models=("naive",), item_id=MANUAL_ITEM,
        )
        assert result["run_id"] > 1
        assert result["comparison_table"]
        assert any(
            row["close_price"] == row["manual_1d_close_price"] == 120000
            for row in observed
        ), "The real backtest must consume the newly published manual features"
        forecast_response = client.get(
            f"/api/items/{MANUAL_ITEM}/forecast?horizons=24",
        )
        assert forecast_response.status_code == 200, forecast_response.text
        forecast = s.ForecastResponse.model_validate(forecast_response.json())
        assert forecast.item.current_price == 120000
        assert forecast.forecasts[0].current_price == 120000
        with open_connection(config.db_path) as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM predictions WHERE item_id = ? AND generated_at = ?",
                (MANUAL_ITEM, ANCHOR),
            ).fetchone()[0] >= 2
            assert conn.execute(
                "SELECT COUNT(*) FROM backtest_runs",
            ).fetchone()[0] >= 2


def test_simulate_cli_forwards_reset_and_keeps_default_false(upgraded_config, monkeypatch):
    config = upgraded_config
    calls = []
    monkeypatch.setattr(cli, "load_config", lambda _env: config)

    def simulate(db_path, **kwargs):
        assert db_path == config.db_path
        calls.append(kwargs["reset"])
        return SimpleNamespace(
            snapshots=0, listings=0, start_ts=ANCHOR - DAY, end_ts=ANCHOR,
        )

    monkeypatch.setattr(cli, "simulate", simulate)
    monkeypatch.setattr(cli, "run_retention", lambda *_args, **_kwargs: 0)
    runner = CliRunner()
    for extra in ([], ["--reset"], ["--no-reset"]):
        result = runner.invoke(cli.app, [
            "simulate", "--env", "sim", "--days", "1", *extra,
        ])
        assert result.exit_code == 0, result.output
    assert calls == [False, True, False]


def test_ledger_replay_cli_reports_global_arm_and_as_of_balances(
    upgraded_config, monkeypatch,
):
    config = upgraded_config
    events = [
        {
            "idempotency_key": "upgrade:A:open",
            "arm_id": "A",
            "event_type": "ACCOUNT_OPEN",
            "event_ts": ANCHOR - 100,
            "available_cash_delta": 1_000_000,
            "slot_limit": 5,
            "max_loss_budget": 100_000,
        },
        {
            "idempotency_key": "upgrade:B:open",
            "arm_id": "B",
            "event_type": "ACCOUNT_OPEN",
            "event_ts": ANCHOR - 100,
            "available_cash_delta": 20_000,
            "unclaimed_delta": 300,
            "slot_limit": 2,
            "max_loss_budget": 10_000,
        },
        {
            "idempotency_key": "upgrade:A:buy",
            "arm_id": "A",
            "event_type": "BUY",
            "event_ts": ANCHOR - 80,
            "item_id": 138,
            "position_id": 1,
            "qty_delta": 99,
            "total_amount": 989_000,
            "available_cash_delta": -989_000,
            "settlement_due_at": ANCHOR + DAY,
        },
        {
            "idempotency_key": "upgrade:A:list",
            "arm_id": "A",
            "event_type": "LIST",
            "event_ts": ANCHOR - 70,
            "item_id": 138,
            "position_id": 1,
            "order_id": 1,
            "qty_delta": 99,
            "new_listed_price": 11_000,
            "new_public_visible_at": ANCHOR - 70,
            "new_expires_at": ANCHOR + DAY,
            "fee_paid": 500,
            "available_cash_delta": -500,
            "slot_delta": 1,
        },
        {
            "idempotency_key": "upgrade:A:sale",
            "arm_id": "A",
            "event_type": "SALE_CONFIRMED",
            "event_ts": ANCHOR - 60,
            "position_id": 1,
            "order_id": 1,
            "qty_delta": -1,
            "total_amount": 11_000,
            "unclaimed_delta": 11_000,
        },
    ]
    with open_connection(config.db_path) as conn:
        assert append_ledger_events(conn, events) == len(events)
    monkeypatch.setattr(cli, "load_config", lambda _env: config)
    runner = CliRunner()
    result = runner.invoke(cli.app, ["ledger-replay", "--env", "sim"])
    assert result.exit_code == 0, result.output
    global_state = json.loads(result.output)
    assert global_state["available_cash"] == 30_500
    assert global_state["unclaimed_proceeds"] == 11_300
    assert global_state["occupied_slots"] == 1
    assert global_state["arms"]["A"]["available_cash"] == 10_500
    assert len(global_state["open_positions"]) == 1
    assert global_state["open_positions"][0]["remaining_qty"] == 98
    assert global_state["open_positions"][0]["total_cost"] == 989_000

    result = runner.invoke(cli.app, [
        "ledger-replay", "--env", "sim", "--arm", "B",
    ])
    assert result.exit_code == 0, result.output
    arm_state = json.loads(result.output)
    assert arm_state["arm_id"] == "B"
    assert arm_state["available_cash"] == 20_000
    assert arm_state["unclaimed_proceeds"] == 300
    assert arm_state["occupied_slots"] == 0
    assert arm_state["open_positions"] == []

    result = runner.invoke(cli.app, [
        "ledger-replay", "--env", "sim", "--as-of", str(ANCHOR - 90),
    ])
    assert result.exit_code == 0, result.output
    earlier = json.loads(result.output)
    assert earlier["available_cash"] == 1_020_000
    assert earlier["unclaimed_proceeds"] == 300
    assert earlier["occupied_slots"] == 0
    assert earlier["open_positions"] == []
