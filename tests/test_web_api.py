"""Browser contracts, local service security and real SQLite integration tests."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import math
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Response
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.datastructures import UploadFile

from pokemmo_gtl.config import Environment, load_config
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.ingestion.manual_import import ImportResult
from pokemmo_gtl.ingestion.rate_limiter import CollectorBusyError, SQLiteRateLimiter
from pokemmo_gtl.quant import fees
from pokemmo_gtl.web import app as web
from pokemmo_gtl.web import schemas as s

BASE_URL = "http://127.0.0.1:8000"
ANCHOR = 1761868800  # Historical SIM close, exactly on every supported UTC grid.
TOKEN = "a1" * 32
BACKTEST = {"horizon_hours": 72, "models": ["naive"]}


@pytest.fixture(autouse=True)
def verify_repository_local_temp_directory(tmp_path, pytestconfig):
    """Honor explicit --basetemp as well as the repository's default ini value."""
    base = Path(pytestconfig.option.basetemp).resolve()
    assert base.is_relative_to(pytestconfig.rootpath)
    assert tmp_path.resolve().is_relative_to(base)


@pytest.fixture
def config(tmp_path):
    return replace(
        load_config(Environment.sim), root=tmp_path, api_token=SecretStr(TOKEN),
        item_aliases=[{"item_id": 1, "aliases_json": '["Growth Stone", "进化石"]'}],
    )


def insert_bar(conn, item_id, close_ts, price, depth=100, timeframe="4h", **extra):
    seconds = {"15m": 900, "4h": 14400, "1d": 86400}[timeframe]
    values = {
        "item_id": item_id, "timeframe": timeframe, "bucket_ts": close_ts - seconds,
        "close_ts": close_ts, "data_source": "snapshot", "open_price": price,
        "high_price": price, "low_price": price, "close_price": price,
        "robust_best_ask": int(price), "best_ask_min": int(price), "p10_price": price,
        "total_listed_qty": depth * 2, "listing_count": 5, "depth_within_5pct": depth,
        "spread_top5_pct": 1.2, "est_sold_qty_sum": 4.5,
        "expired_qty_sum": 2, "new_listed_qty_sum": 7,
        "sample_count": 16, "valid_flow_samples": 15,
    }
    values.update(extra)
    columns = ",".join(values)
    marks = ",".join("?" for _ in values)
    conn.execute(f"INSERT INTO item_bars ({columns}) VALUES ({marks})", tuple(values.values()))


def insert_prediction(conn, item_id=1, horizon=72, roi=0.1, prob=0.8, **extra):
    values = {
        "generated_at": ANCHOR + 100, "item_id": item_id, "horizon_hours": horizon,
        "model_name": "lgbm", "current_price": 120.0, "ref_qty": 10,
        "prob_up": prob, "pred_q10": 110.0, "pred_q50": 140.0, "pred_q90": 160.0,
        "expected_net_roi": roi, "signal": "BUY_WATCH", "regime_note": "SIM test regime",
        "top_drivers_json": json.dumps([
            {"feature": "pressure", "contribution": 0.1, "value": 0.5},
            {"feature": "fee", "contribution": -0.2},
            {"feature": "constant", "contribution": 0},
        ]),
    }
    values.update(extra)
    conn.execute(
        f"INSERT INTO predictions ({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
        tuple(values.values()),
    )


def seed_database(config):
    with open_connection(config.db_path) as conn, write_transaction(conn):
        conn.executemany(
            "INSERT INTO items_catalog (item_id,name_en,name_zh,category,asset_class,"
            "max_stack_size,is_watched,updated_at) VALUES (?,?,?,?,?,?,?,?)",
            [
                (1, "Alpha Stone", "阿尔法石", "stone", "item", 99, 1, ANCHOR),
                (2, "Beta Berry", "乙果", "berry", "berry", 99, 0, ANCHOR),
                (3, "Empty Hat", None, "cosmetic", "cosmetic", 1, 0, ANCHOR),
                (1192, "1000RP Voucher", "千点券", "rp_voucher", "item", 99, 1, ANCHOR),
                (900001, "Pokemon Material", None, "pokemon_category",
                 "pokemon_category", 1, 0, ANCHOR),
            ],
        )
        for item_id, previous, current, depth in (
            (1, 100, 120, 30), (2, 100, 50, 80), (1192, 35000, 40000, 300),
        ):
            insert_bar(conn, item_id, ANCHOR - 86400, previous)
            insert_bar(conn, item_id, ANCHOR, current, depth)
        insert_bar(conn, 1, ANCHOR - 120 * 86400, 80)
        insert_prediction(conn, roi=0.99, generated_at=ANCHOR - 100)
        insert_prediction(conn)
        insert_prediction(conn, 2, roi=0.2, prob=0.7)
        insert_prediction(conn, 1192, roi=0.2, prob=0.9)
        insert_prediction(conn, horizon=24, roi=0.7)
        insert_prediction(conn, 2, horizon=24, roi=-0.2)
        conn.executemany(
            "INSERT INTO market_events (name,event_type,announced_at,start_ts,end_ts) "
            "VALUES (?,'holiday',?,?,?)",
            [
                ("Ended", ANCHOR - 200000, ANCHOR - 172800, ANCHOR - 86401),
                ("Ongoing", ANCHOR - 86400, ANCHOR - 3600, ANCHOR + 3600),
                ("Upcoming", ANCHOR - 5000, ANCHOR + 86400, ANCHOR + 172800),
            ],
        )
        conn.executemany(
            "INSERT INTO snapshot_runs "
            "(source,started_at,server_started_at,completed_at,status) "
            "VALUES ('simulator',?,?,?,?)",
            [(ANCHOR - 1000, ANCHOR - 1000, ANCHOR - 600, "completed"),
             (ANCHOR, ANCHOR, ANCHOR + 999999, "failed")],
        )


@pytest.fixture
def app(config):
    application = web.create_app(config=config)
    seed_database(config)
    return application


@pytest.fixture
def client(app):
    with TestClient(app, base_url=BASE_URL) as test_client:
        yield test_client


def assert_error(response, status, code=None):
    assert response.status_code == status, response.text
    envelope = s.ErrorEnvelope.model_validate(response.json())
    if code:
        assert envelope.error.code == code
    assert envelope.error.message
    assert TOKEN not in response.text
    assert "Traceback" not in response.text
    return envelope


def test_factory_migrates_empty_database_and_empty_contracts(config):
    with TestClient(web.create_app(config=config), base_url=BASE_URL) as client:
        health = s.HealthResponse.model_validate(client.get("/api/health").json())
        assert health.env == "sim"
        assert health.item_count == health.bar_count == 0
        assert health.last_snapshot_at is None
        assert health.active_job is None
        overview = s.OverviewResponse.model_validate(client.get("/api/overview").json())
        assert overview.rp_index is None
        assert overview.next_event is None
        assert overview.top_bullish == overview.top_movers_24h == []
        assert client.get("/api/items").json() == []
        assert client.get("/api/events").json() == []
    # Repeated factories must not reset an existing database.
    web.create_app(config=config)


def test_factory_loads_requested_environment(monkeypatch, config):
    seen = []

    def load(env):
        seen.append(env)
        return config

    monkeypatch.setattr(web, "load_config", load)
    with TestClient(web.create_app("sim"), base_url=BASE_URL) as client:
        assert client.get("/api/health").json()["env"] == "sim"
    assert seen == ["sim"]


def test_health_counts_completed_snapshot_token_and_default_budget(client, config):
    response = client.get("/api/health")
    assert response.status_code == 200
    health = s.HealthResponse.model_validate(response.json())
    assert health.item_count == 5
    assert health.bar_count == 7
    assert health.last_snapshot_at == ANCHOR - 600
    assert health.db_path == str(config.db_path)
    assert health.token_configured is True
    assert health.rate_budget_remaining == config.settings.rate_limit_tokens
    assert TOKEN not in response.text
    assert not (config.root / "data/api_limiter.db").exists()


def test_health_reads_only_recent_shared_limiter_debits(client, config):
    now = time.time()
    path = config.root / "data/api_limiter.db"
    with SQLiteRateLimiter(path) as limiter, limiter._connection() as conn:
        conn.executemany(
            "INSERT INTO api_rate_ledger(req_ts,tokens) VALUES (?,?)",
            [(now - 5, 7), (now - 10, 11),
             (now - config.settings.rate_limit_window_sec - 10, 100)],
        )
    data = client.get("/api/health").json()
    assert data["rate_budget_remaining"] == config.settings.rate_limit_tokens - 18
    with open_connection(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM api_rate_ledger").fetchone()[0] == 3


def test_health_without_token(config):
    with TestClient(web.create_app(config=replace(config, api_token=None)),
                    base_url=BASE_URL) as client:
        assert client.get("/api/health").json()["token_configured"] is False


def test_overview_ranks_latest_predictions_depth_and_movers_at_sim_time(client):
    data = s.OverviewResponse.model_validate(client.get("/api/overview?horizon=72").json())
    assert data.rp_index.value == 40
    assert data.rp_index.change_24h_pct == pytest.approx(100 / 7)
    assert data.rp_index.inflation_level == "high"
    assert data.rp_index.updated_at == ANCHOR
    assert data.next_event.name == "Ongoing"
    assert [item.item_id for item in data.top_bullish] == [1192, 2, 1]
    assert [item.item_id for item in data.top_thinning_depth] == [1, 2]
    assert data.top_thinning_depth[0].depth_change_24h_pct == pytest.approx(-70)
    assert [item.item_id for item in data.top_movers_24h] == [2, 1, 1192]
    assert {item.item_id for item in data.watchlist} == {1, 1192}
    data24 = client.get("/api/overview?horizon=24").json()
    assert [item["item_id"] for item in data24["top_bullish"]] == [1, 2]
    assert client.get("/api/overview?horizon=168").json()["top_bullish"] == []


def test_overview_missing_rp_baseline_and_no_predictions(client, config):
    with open_connection(config.db_path) as conn, write_transaction(conn):
        conn.execute("DELETE FROM predictions")
        conn.execute("DELETE FROM item_bars WHERE item_id=1192 AND close_ts < ?", (ANCHOR,))
    data = client.get("/api/overview").json()
    assert data["top_bullish"] == []
    assert data["rp_index"]["inflation_level"] is None
    assert data["rp_index"]["change_24h_pct"] is None
    with open_connection(config.db_path) as conn:
        conn.execute("DELETE FROM item_bars WHERE item_id=1192")
    assert client.get("/api/overview").json()["rp_index"] is None


def test_overview_uses_snapshot_anchor_without_bars(client, config):
    with open_connection(config.db_path) as conn:
        conn.execute("DELETE FROM item_bars")
    assert client.get("/api/overview").json()["next_event"]["name"] == "Ongoing"


@pytest.mark.parametrize("query,expected", [
    ("", [1, 2, 3, 1192, 900001]), ("q=aLpHa", [1]), ("q=阿尔法", [1]),
    ("q=进化石", [1]), ("q=GROWTH", [1]), ("q=90000", [900001]),
    ("category=berry", [2]), ("category=Berry", []), ("watched_only=true", [1, 1192]),
    ("limit=1", [1]), ("q=missing", []), ("q=%27%20OR%201%3D1", []),
    ("category=berry&watched_only=true", []),
])
def test_item_search_filters_are_bare_arrays(client, query, expected):
    response = client.get("/api/items?" + query)
    assert response.status_code == 200
    items = s.ItemListResponse.model_validate(response.json()).root
    assert [item.item_id for item in items] == expected


def test_search_supports_optional_catalog_alias_column(client, config):
    with open_connection(config.db_path) as conn:
        conn.execute("ALTER TABLE items_catalog ADD COLUMN aliases_json TEXT")
        conn.execute("UPDATE items_catalog SET aliases_json='[\"FANCY CAP\"]' WHERE item_id=3")
    assert client.get("/api/items?q=fancy").json()[0]["item_id"] == 3


def test_items_are_enriched_and_missing_measurements_stay_null(client):
    items = {row["item_id"]: row for row in client.get("/api/items").json()}
    item = s.ItemSummary.model_validate(items[1])
    assert item.current_price == 120
    assert item.change_24h_pct == pytest.approx(20)
    assert item.total_listed_qty == 60
    assert item.depth_within_5pct == 30
    assert item.depth_change_24h_pct == pytest.approx(-70)
    assert item.expected_net_roi == 0.7  # Latest across horizons for the item table.
    assert item.signal == "BUY_WATCH"
    assert item.prob_up == 0.8
    assert items[3]["current_price"] is None
    assert items[3]["signal"] is None
    assert items[3]["depth_change_24h_pct"] is None
    assert isinstance(items[1]["is_watched"], bool)


def test_watch_persists_and_updates_overview(client, config):
    response = client.post("/api/items/2/watch", json={"is_watched": True})
    assert response.status_code == 200
    assert s.WatchResponse.model_validate(response.json()).is_watched is True
    watched = client.get("/api/items?watched_only=true").json()
    assert {item["item_id"] for item in watched} == {1, 2, 1192}
    assert client.post("/api/items/2/watch", json={"is_watched": False}).json() == {
        "item_id": 2, "is_watched": False,
    }
    with open_connection(config.db_path) as conn:
        row = conn.execute("SELECT is_watched FROM items_catalog WHERE item_id=2").fetchone()
        assert row[0] == 0


def test_bars_trailing_windows_anchor_to_item_history_and_return_overlaps(client):
    all_data = s.BarsResponse.model_validate(client.get("/api/items/1/bars?days=0").json())
    assert len(all_data.bars) == 3
    assert len(all_data.events) == 2  # The future event lies outside the history.
    assert len(client.get("/api/items/1/bars").json()["bars"]) == 2
    trailing = client.get("/api/items/1/bars?days=1").json()
    assert [row["close_ts"] for row in trailing["bars"]] == [ANCHOR - 86400, ANCHOR]
    assert [event["name"] for event in trailing["events"]] == ["Ended", "Ongoing"]
    assert trailing["bars"][-1]["robust_best_ask"] == 120
    assert trailing["bars"][-1]["best_ask_min"] == 120
    assert trailing["bars"][-1]["est_sold_qty_sum"] == 4.5
    empty = client.get("/api/items/3/bars").json()
    assert empty["bars"] == []
    assert len(empty["events"]) == 3


@pytest.mark.parametrize("timeframe", ["15m", "4h", "1d"])
def test_bars_support_all_timeframes_with_item_specific_anchor(client, config, timeframe):
    seconds = {"15m": 900, "4h": 14400, "1d": 86400}[timeframe]
    with open_connection(config.db_path) as conn:
        insert_bar(conn, 3, ANCHOR - 20 * 86400, 100, timeframe=timeframe)
        insert_bar(conn, 3, ANCHOR - 10 * 86400, 200, timeframe=timeframe)
    data = client.get(f"/api/items/3/bars?timeframe={timeframe}&days=1").json()
    assert len(data["bars"]) == 1
    bar = s.Bar.model_validate(data["bars"][0])
    assert bar.bucket_ts == ANCHOR - 10 * 86400 - seconds


def test_forecast_selects_latest_per_requested_horizon_and_adapts_drivers(client, config):
    response = client.get("/api/items/1/forecast?horizons=72,24")
    assert response.status_code == 200
    data = s.ForecastResponse.model_validate(response.json())
    assert data.item.item_id == 1
    assert [row.horizon_hours for row in data.forecasts] == [72, 24]
    assert data.forecasts[0].expected_net_roi == 0.1
    assert [driver.direction for driver in data.forecasts[0].top_drivers] == [
        "positive", "negative", "neutral",
    ]
    with open_connection(config.db_path) as conn:
        insert_prediction(conn, roi=0.25, top_drivers_json="not JSON")
    latest = client.get("/api/items/1/forecast?horizons=72").json()["forecasts"][0]
    assert latest["expected_net_roi"] == 0.25
    assert latest["top_drivers"] == []
    assert client.get("/api/items/3/forecast").json()["forecasts"] == []


def test_forecast_caps_top_drivers_and_preserves_explicit_direction(client, config):
    with open_connection(config.db_path) as conn:
        insert_prediction(conn, top_drivers_json=json.dumps([
            {"feature": f"driver{i}", "contribution": 1, "direction": "negative"}
            for i in range(9)
        ]))
    drivers = client.get("/api/items/1/forecast?horizons=72").json()["forecasts"][0]["top_drivers"]
    assert len(drivers) == 5
    assert all(driver["direction"] == "negative" for driver in drivers)


@pytest.mark.parametrize("body,stack,pokemon", [
    ({"item_id": 1}, 99, False), ({"item_id": 3}, 1, False),
    ({"item_id": 900001}, 1, True), ({"asset_class": "pokemon_category"}, 1, True),
    ({"asset_class": "cosmetic"}, 1, False), ({}, 99, False),
    ({"item_id": 3, "asset_class": "item", "max_stack_size": 20, "relist_rate": 0}, 20, False),
    ({"item_id": 900001, "asset_class": "item", "max_stack_size": 99}, 99, True),
])
def test_fees_match_decimal_engine_catalog_defaults_and_overrides(
    client, config, body, stack, pokemon,
):
    payload = {"buy_price": 10, "sell_price": 20, "qty": 100, **body}
    response = client.post("/api/fees/simulate", json=payload)
    assert response.status_code == 200, response.text
    result = s.FeeSimulationResponse.model_validate(response.json())
    kwargs = {"min_fee": config.fees.pokemon_min_fee if pokemon else config.fees.item_min_fee,
              "rate": config.fees.rate, "max_fee_per_slot": config.fees.max_fee_per_slot}
    relist = Decimal(str(body.get("relist_rate", config.fees.default_relist_rate)))
    assert result.slots_used == math.ceil(100 / stack)
    assert result.listing_fee == fees.listing_fee(20, 100, stack, **kwargs)
    assert result.effective_fee == fees.effective_fee(20, 100, stack, relist_rate=relist, **kwargs)
    assert result.net_profit == fees.net_profit(10, 20, 100, stack, relist_rate=relist, **kwargs)
    assert result.net_roi == result.net_profit / Decimal(1000)
    assert result.breakeven_sell_price == fees.breakeven_sell_price(
        10, 100, stack, relist_rate=relist, **kwargs,
    )


def test_zero_buy_price_roi_is_null_and_slot_fee_is_capped(client):
    data = client.post("/api/fees/simulate", json={
        "buy_price": 0, "sell_price": 1_000_000, "qty": 100, "relist_rate": 0,
    }).json()
    assert data["net_roi"] is None
    assert data["slots_used"] == 2
    assert data["listing_fee"] == 50000


def fake_backtest_result(names=("lgbm", "naive")):
    return {
        "run_id": 17,
        "comparison_table": [{
            "model_name": name, "net_return_pct": 12.5, "max_drawdown_pct": 8,
            "win_rate": 0.75, "trade_count": 4, "pinball_mean": 0.03,
            "roc_auc": 0.8, "coverage_80": 0.79, "ignored_metric": 999,
        } for name in names],
        "equity_curves": {name: [
            {"close_ts": ANCHOR + i * 14400, "equity": equity, "cash": 25}
            for i, equity in enumerate([100, 110, 99, 120])
        ] for name in names},
    }


def test_backtest_adapter_model_names_metric_units_curves_and_worker_thread(
    client, config, monkeypatch,
):
    calls = []

    def run(cfg, **kwargs):
        calls.append((cfg, kwargs))
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio.get_running_loop()
        return fake_backtest_result()

    monkeypatch.setattr(web.backtester, "run_backtest", run)
    response = client.post("/api/backtest/run", json={
        "item_id": 1, "horizon_hours": 72, "models": ["lightgbm", "naive"],
    })
    assert response.status_code == 200, response.text
    data = s.BacktestResponse.model_validate(response.json())
    assert calls == [(config, {"item_id": 1, "category": None, "horizon_hours": 72,
                              "models": ["lgbm", "naive"]})]
    assert data.run_id == 17
    assert [row.model_name for row in data.comparison_table] == ["lightgbm", "naive"]
    metrics = data.comparison_table[0].metrics
    assert metrics.total_return == 0.125
    assert metrics.max_drawdown == 0.08
    assert metrics.win_rate == 0.75
    assert metrics.trade_count == 4
    assert metrics.pinball_loss == 0.03
    assert metrics.roc_auc == 0.8
    assert metrics.interval_coverage == 0.79
    returns = [110 / 100 - 1, 99 / 110 - 1, 120 / 99 - 1]
    assert metrics.sharpe_ratio == pytest.approx(
        statistics.mean(returns) / statistics.stdev(returns) * math.sqrt(365 * 6),
    )
    assert [curve.model_name for curve in data.equity_curves] == ["lightgbm", "naive"]
    assert data.equity_curves[0].points[1].model_dump() == {"ts": ANCHOR + 14400, "equity": 110}
    assert client.get("/api/health").json()["active_job"] is None


def test_real_backtest_on_small_dataset_persists_run(client, config):
    response = client.post("/api/backtest/run", json=BACKTEST)
    assert response.status_code == 200, response.text
    data = s.BacktestResponse.model_validate(response.json())
    assert data.comparison_table[0].model_name == "naive"
    assert data.comparison_table[0].metrics.trade_count == 0
    assert data.comparison_table[0].metrics.sharpe_ratio is None
    with open_connection(config.db_path) as conn:
        row = conn.execute("SELECT model_name FROM backtest_runs WHERE run_id=?",
                           (data.run_id,)).fetchone()
        assert row[0] == "naive"


def test_manual_quotes_materialize_bars_and_preserve_snapshot_bars(client, config):
    body = {"item_id": 3, "price": 250, "recorded_at": ANCHOR + 300,
            "listed_qty": 0, "note": "manual sample"}
    response = client.post("/api/quotes/manual", json=body)
    assert response.status_code == 200
    data = s.ManualQuoteResponse.model_validate(response.json())
    assert data.quote_id > 0
    assert data.source_tag == "manual"
    assert data.skipped_snapshot_bars == 0
    assert data.listed_qty == 0
    second = client.post("/api/quotes/manual", json={**body, "price": 300,
                                                   "recorded_at": ANCHOR + 600}).json()
    assert second["quote_id"] > data.quote_id
    bar = client.get("/api/items/3/bars").json()["bars"][0]
    assert bar["open_price"] == 250
    assert bar["close_price"] == bar["high_price"] == 300
    assert bar["data_source"] == "manual"
    assert bar["sample_count"] == 2
    assert bar["est_sold_qty_sum"] is None
    protected = client.post("/api/quotes/manual", json={
        "item_id": 1, "price": 999, "recorded_at": ANCHOR - 600,
    }).json()
    assert protected["skipped_snapshot_bars"] == 1
    with open_connection(config.db_path) as conn:
        assert conn.execute("SELECT close_price FROM item_bars WHERE item_id=1 AND timeframe='4h' "
                            "AND close_ts=?", (ANCHOR,)).fetchone()[0] == 120


def test_manual_quote_defaults_timestamp_and_handles_importer_validation(client):
    before = int(time.time())
    data = client.post("/api/quotes/manual", json={"item_id": 3, "price": 12}).json()
    assert before <= data["recorded_at"] <= int(time.time())
    assert data["note"] is None
    assert_error(client.post("/api/quotes/manual", json={
        "item_id": 3, "price": 12, "recorded_at": 0,
    }), 400, "INVALID_QUOTE")


def test_csv_import_bom_row_errors_and_materialization(client):
    body = ("\ufeffitem_id,price,listed_qty,recorded_at,note\r\n"
            f"3,200,5,{ANCHOR + 100},中文\r\n"
            f"3,-2,5,{ANCHOR + 100},bad\r\n"
            f"999,100,5,{ANCHOR + 100},missing\r\n"
            f"1,300,5,{ANCHOR - 100},protected\r\n").encode()
    response = client.post("/api/quotes/import-csv", files={
        "file": ("quotes.csv", body, "text/csv"),
    })
    assert response.status_code == 200, response.text
    data = s.CSVImportResponse.model_validate(response.json())
    assert data.imported_rows == 2
    assert data.skipped_snapshot_bars == 1
    assert [error.row for error in data.errors] == [3, 4]
    assert "integer" in data.errors[0].message
    assert client.get("/api/items/3/bars").json()["bars"][0]["close_price"] == 200


@pytest.mark.parametrize("body", [b"", b"wrong,headers\n1,2", b"item_id,price\n\xff,1"])
def test_invalid_csv_returns_error_envelope(client, body):
    assert_error(client.post("/api/quotes/import-csv", files={"file": ("bad.csv", body)}),
                 400, "INVALID_CSV")


def test_csv_large_multipart_upload_is_rejected(client):
    body = b"item_id,price\n" + b"x" * s.MAX_CSV_BYTES
    assert_error(client.post("/api/quotes/import-csv", files={"file": ("big.csv", body)}),
                 413, "CSV_TOO_LARGE")


def test_csv_without_declared_size_reads_bounded_chunks_and_cleans_temp(client, monkeypatch):
    actual_read = UploadFile.read
    actual_seek = UploadFile.seek
    actual_file = web.tempfile.NamedTemporaryFile
    sizes = []
    paths = []

    async def read(file, size=-1):
        sizes.append(size)
        return await actual_read(file, size)

    async def seek(file, offset):
        await actual_seek(file, offset)
        # The multipart parser has finished counting bytes when it rewinds.
        # Omit its size hint to exercise the endpoint's own streaming bound.
        file.size = None

    def temporary(*args, **kwargs):
        file = actual_file(*args, **kwargs)
        paths.append(Path(file.name))
        return file

    # Smaller route bound forces the streaming branch; importer is never reached.
    monkeypatch.setattr(web, "MAX_CSV_BYTES", 128)
    monkeypatch.setattr(UploadFile, "read", read)
    monkeypatch.setattr(UploadFile, "seek", seek)
    monkeypatch.setattr(web.tempfile, "NamedTemporaryFile", temporary)
    body = b"item_id,price\n" + b"x" * 130
    assert_error(client.post("/api/quotes/import-csv", files={"file": ("big.csv", body)}), 413)
    assert sizes and all(0 < size <= 65536 for size in sizes)
    assert paths and all(not path.exists() for path in paths)


def test_csv_exact_size_boundary_passes_limits_and_cleanup(client, monkeypatch):
    body = b"item_id,price\n3,200\n"
    monkeypatch.setattr(web, "MAX_CSV_BYTES", len(body))
    actual = web.manual_import.import_csv
    calls = []

    def import_file(db_path, path, **kwargs):
        calls.append((path, kwargs, path.read_bytes()))
        return actual(db_path, path, **kwargs)

    monkeypatch.setattr(web.manual_import, "import_csv", import_file)
    response = client.post("/api/quotes/import-csv", files={"file": ("exact.csv", body)})
    assert response.status_code == 200, response.text
    assert calls[0][1] == {"max_bytes": len(body), "max_rows": s.MAX_CSV_ROWS}
    assert calls[0][2] == body
    assert not calls[0][0].exists()


def test_csv_row_limit_rolls_back_and_temp_file_is_removed(client, config, monkeypatch):
    monkeypatch.setattr(web, "MAX_CSV_ROWS", 1)
    actual = web.manual_import.import_csv
    paths = []

    def import_file(db_path, path, **kwargs):
        paths.append(path)
        return actual(db_path, path, **kwargs)

    monkeypatch.setattr(web.manual_import, "import_csv", import_file)
    response = client.post("/api/quotes/import-csv", files={
        "file": ("rows.csv", b"item_id,price\n3,100\n3,200\n"),
    })
    assert_error(response, 400, "INVALID_CSV")
    assert paths and not paths[0].exists()
    with open_connection(config.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM manual_quotes").fetchone()[0] == 0


def test_csv_generic_import_issue_has_nullable_row(client, monkeypatch):
    monkeypatch.setattr(web.manual_import, "import_csv",
                        lambda *args, **kwargs: ImportResult(0, 0, ["Unnumbered issue"]))
    data = client.post("/api/quotes/import-csv", files={
        "file": ("x.csv", b"item_id,price\n"),
    }).json()
    assert data["errors"] == [{"row": None, "message": "Unnumbered issue"}]


@pytest.mark.parametrize("formula", ["=CMD()", "+1", "-1", "@SUM", "\t=1", "\r=1"])
def test_export_csv_escapes_formula_injection_in_all_free_text_fields(client, config, formula):
    with open_connection(config.db_path) as conn:
        conn.execute("UPDATE items_catalog SET name_en=?,name_zh=?,category=?,description=? "
                     "WHERE item_id=1", (formula, formula, formula, formula))
    response = client.get("/api/items/1/export.csv?timeframe=4h")
    assert response.status_code == 200
    assert response.headers["content-type"] == s.CSV_MEDIA_TYPE
    assert 'filename="item-1-4h.csv"' in response.headers["content-disposition"]
    rows = list(csv.DictReader(io.StringIO(response.text, newline="")))
    assert len(rows) == 3
    assert rows[-1]["item_id"] == "1"
    assert rows[-1]["close_price"] == "120.0"
    for row in rows:
        for column in ("name_en", "name_zh", "category", "description"):
            assert row[column] == "'" + formula


def test_export_empty_timeframe_and_ordinary_metadata(client):
    response = client.get("/api/items/1/export.csv?timeframe=1d")
    assert list(csv.DictReader(io.StringIO(response.text))) == []
    response = client.get("/api/items/1/export.csv")
    assert list(csv.DictReader(io.StringIO(response.text)))[0]["name_en"] == "Alpha Stone"


def test_event_crud_returns_exact_models_and_orders_events(client):
    response = client.get("/api/events")
    assert response.status_code == 200
    original = s.EventListResponse.model_validate(response.json()).root
    assert [event.name for event in original] == ["Ended", "Ongoing", "Upcoming"]
    body = {"name": "New event", "event_type": "update", "announced_at": ANCHOR,
            "start_ts": ANCHOR + 10, "end_ts": ANCHOR + 10, "notes": "event notes"}
    created = client.post("/api/events", json=body)
    assert created.status_code == 200
    event = s.EventCreateResponse.model_validate(created.json())
    assert event.impact_scope == "all"
    assert [row["name"] for row in client.get("/api/events").json()] == [
        "Ended", "Ongoing", "New event", "Upcoming",
    ]
    deleted = client.delete(f"/api/events/{event.event_id}")
    assert deleted.status_code == 200
    assert s.EventDeleteResponse.model_validate(deleted.json()).deleted is True
    assert_error(client.delete(f"/api/events/{event.event_id}"), 404, "EVENT_NOT_FOUND")


@pytest.mark.parametrize("action", ["simulate", "collect", "sync_catalog", "forecast"])
def test_admin_actions_use_real_adapters_hooks_retention_and_worker_threads(
    client, config, monkeypatch, action,
):
    calls = []
    sentinel = object()

    def assert_worker():
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio.get_running_loop()

    def simulate(path, **kwargs):
        assert_worker()
        assert path == config.db_path
        assert kwargs["days"] == 90 and kwargs["seed"] == 42
        assert kwargs["env"] == Environment.sim
        kwargs["on_snapshot"](sentinel, 12, 11, ANCHOR)
        calls.append("simulate")
        return SimpleNamespace(end_ts=ANCHOR)

    def collect(cfg, *, post_snapshot_hook):
        assert_worker()
        assert cfg is config
        post_snapshot_hook(sentinel, 12)
        calls.append("collect")

    def process(conn, sid, settings):
        assert conn is sentinel and sid == 12 and settings is config.settings
        calls.append("process")

    def retention(conn, settings, now_ts=None):
        assert settings is config.settings
        assert conn.execute("SELECT COUNT(*) FROM items_catalog").fetchone()[0] == 5
        assert not conn.in_transaction
        assert now_ts == (ANCHOR if action == "simulate" else None)
        calls.append("retention")

    def sync(path, *, config):
        assert_worker()
        assert path == config.db_path
        calls.append("sync_catalog")

    def forecast(cfg):
        assert_worker()
        assert cfg is config
        calls.append("forecast")

    monkeypatch.setattr(web.simulator, "simulate", simulate)
    monkeypatch.setattr(web.official_api, "collect_once", collect)
    monkeypatch.setattr(web, "process_snapshot", process)
    monkeypatch.setattr(web, "run_retention", retention)
    monkeypatch.setattr(web.catalog_sync, "sync_catalog", sync)
    monkeypatch.setattr(web.predictor, "run_forecast", forecast)
    response = client.post("/api/admin/trigger", json={"action": action})
    assert response.status_code == 200, response.text
    result = s.AdminTriggerResponse.model_validate(response.json())
    assert result.action == action and result.status == "completed" and result.active_job is None
    expected = ["process", action, "retention"] if action in {"simulate", "collect"} else [action]
    assert calls == expected
    assert client.get("/api/health").json()["active_job"] is None


def test_live_simulation_is_forbidden_before_calling_simulator(config, monkeypatch):
    def fail(*args, **kwargs):
        pytest.fail("live simulation reached simulator")

    monkeypatch.setattr(web.simulator, "simulate", fail)
    live = replace(config, env=Environment.live)
    with TestClient(web.create_app(config=live), base_url=BASE_URL) as client:
        assert_error(client.post("/api/admin/trigger", json={"action": "simulate"}),
                     403, "FORBIDDEN_SIMULATION_IN_LIVE")


@pytest.mark.parametrize("exception,status,code", [
    (web.official_api.ApiAuthError, 401, "API_AUTH_ERROR"),
    (CollectorBusyError, 409, "COLLECTOR_BUSY"),
    (web.official_api.ApiResponseError, 502, "UPSTREAM_ERROR"),
    (RuntimeError, 500, "INTERNAL_ERROR"),
])
def test_admin_failures_have_safe_errors_and_release_mutex(
    client, config, monkeypatch, exception, status, code,
):
    def collect(*args, **kwargs):
        raise exception(f"secret {TOKEN} private path {config.root / 'secret.env'}")

    monkeypatch.setattr(web.official_api, "collect_once", collect)
    response = client.post("/api/admin/trigger", json={"action": "collect"})
    assert_error(response, status, code)
    assert str(config.root) not in response.text
    assert "secret.env" not in response.text
    assert client.get("/api/health").json()["active_job"] is None
    monkeypatch.setattr(web.predictor, "run_forecast", lambda *args: [])
    assert client.post("/api/admin/trigger", json={"action": "forecast"}).status_code == 200


@pytest.mark.parametrize("running_job", ["forecast", "backtest"])
def test_global_nonblocking_job_conflicts_across_routes_apps_and_loop_threads(
    client, config, monkeypatch, running_job,
):
    started, finish = threading.Event(), threading.Event()

    def block(*args, **kwargs):
        started.set()
        assert finish.wait(10), "test did not release job"
        return fake_backtest_result(("naive",))

    monkeypatch.setattr(web.predictor, "run_forecast", block)
    monkeypatch.setattr(web.backtester, "run_backtest", block)
    other_config = replace(config, root=config.root / "other", env=Environment.live)
    other_app = web.create_app(config=other_config)
    with TestClient(other_app, base_url=BASE_URL) as other, ThreadPoolExecutor() as pool:
        path, body = (("/api/backtest/run", BACKTEST) if running_job == "backtest" else (
            "/api/admin/trigger", {"action": "forecast"}
        ))
        future = pool.submit(client.post, path, json=body)
        try:
            assert started.wait(5)
            assert client.get("/api/health").json()["active_job"] == running_job
            assert other.get("/api/health").json()["active_job"] == running_job
            before = time.monotonic()
            assert_error(client.post("/api/backtest/run", json=BACKTEST), 409, "JOB_BUSY")
            assert_error(other.post("/api/admin/trigger", json={"action": "collect"}), 409)
            assert time.monotonic() - before < 3
            assert_error(other.post("/api/admin/trigger", json={"action": "simulate"}),
                         403, "FORBIDDEN_SIMULATION_IN_LIVE")
        finally:
            finish.set()
        assert future.result(timeout=10).status_code == 200
    assert client.get("/api/health").json()["active_job"] is None


def test_backtest_failure_releases_job_and_redacts_details(client, config, monkeypatch):
    def run(*args, **kwargs):
        raise RuntimeError(f"{TOKEN} {config.root / 'passwords'}")

    monkeypatch.setattr(web.backtester, "run_backtest", run)
    response = client.post("/api/backtest/run", json=BACKTEST)
    assert_error(response, 500, "INTERNAL_ERROR")
    assert "passwords" not in response.text
    assert client.get("/api/health").json()["active_job"] is None


@pytest.mark.parametrize("host,expected", [
    ("127.0.0.1", True), ("127.0.0.2", True), ("localhost", True),
    ("LOCALHOST", True), ("::1", True), ("[::1]", True),
    ("0.0.0.0", False), ("::", False), ("192.168.1.1", False),
    ("evil.example", False), ("localhost.evil.example", False),
    ("localhost:8000", False), ("", False),
])
def test_is_loopback_host_never_uses_dns(host, expected):
    assert web.is_loopback_host(host) is expected


@pytest.mark.parametrize("host", ["127.0.0.1:8000", "localhost", "LOCALHOST:80", "[::1]:8000",
                                "[::1]", "::1"])
def test_default_loopback_host_headers_are_allowed(client, host):
    assert client.get("/api/health", headers={"Host": host}).status_code == 200


@pytest.mark.parametrize("host", [
    "evil.example", "127.0.0.1.evil.example", "localhost.evil.example",
    "192.168.1.2", "127.0.0.2", "0.0.0.0", "evil@localhost", "localhost/path",
    "localhost:bad", "[::1", "localhost:99999", "localhost,evil.example", "",
])
def test_forged_and_malformed_hosts_rejected_for_api_and_static(client, host):
    for path in ("/api/health", "/static/app.js"):
        assert_error(client.get(path, headers={"Host": host}), 403, "FORBIDDEN_HOST")


def test_duplicate_host_headers_are_rejected(client):
    assert_error(client.get("/api/health", headers=[("Host", "localhost"),
                                                  ("Host", "evil.example")]), 403)


@pytest.mark.parametrize("method", ["POST", "DELETE", "PUT", "PATCH"])
@pytest.mark.parametrize("origin", [
    "https://evil.example", "http://localhost.evil.example", "null",
    "file://localhost", "ftp://127.0.0.1", "http://evil@localhost", "http://localhost:bad",
])
def test_forged_origins_rejected_on_every_mutation_method(client, method, origin):
    assert_error(client.request(method, "/api/events", headers={"Origin": origin}),
                 403, "FORBIDDEN_ORIGIN")


@pytest.mark.parametrize("origin", ["http://127.0.0.1:8000", "https://localhost:9000",
                                  "http://[::1]:8000"])
def test_loopback_origins_allowed_and_no_wildcard_cors(client, origin):
    response = client.post("/api/items/2/watch", json={"is_watched": True},
                           headers={"Origin": origin})
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") != "*"


def test_lan_opt_in_and_explicit_host_list(config):
    with TestClient(web.create_app(config=config, allow_lan=True),
                    base_url="http://192.168.1.10:8000") as client:
        assert client.get("/api/health").status_code == 200
        assert client.post("/api/admin/trigger", json={"action": "invalid"},
                           headers={"Origin": "http://192.168.1.10:8000"}).status_code == 422
        assert_error(client.post("/api/events", headers={"Origin": "http://evil.example"}), 403)
    with TestClient(web.create_app(config=config, allow_lan=True, allowed_hosts=["gtl.local"]),
                    base_url="http://gtl.local") as client:
        assert client.get("/api/health").status_code == 200
        assert_error(client.get("/api/health", headers={"Host": "evil.example"}), 403)
    with TestClient(web.create_app(config=config, allowed_hosts=["evil.example", "localhost"]),
                    base_url="http://localhost") as client:
        assert client.get("/api/health").status_code == 200
        assert_error(client.get("/api/health", headers={"Host": "evil.example"}), 403)
    with pytest.raises(ValueError):
        web.create_app(config=config, allowed_hosts=["*"])


@pytest.mark.parametrize("method,path,body", [
    ("GET", "/api/items/999/bars", None), ("GET", "/api/items/999/forecast", None),
    ("GET", "/api/items/999/export.csv", None),
    ("POST", "/api/items/999/watch", {"is_watched": True}),
    ("POST", "/api/fees/simulate", {"item_id": 999, "buy_price": 1, "sell_price": 2, "qty": 1}),
    ("POST", "/api/quotes/manual", {"item_id": 999, "price": 100}),
    ("POST", "/api/backtest/run", {**BACKTEST, "item_id": 999}),
    ("DELETE", "/api/events/999", None), ("GET", "/missing", None),
    ("GET", "/static/missing.js", None),
])
def test_missing_entities_and_routes_use_error_envelope(client, method, path, body):
    assert_error(client.request(method, path, json=body), 404)


@pytest.mark.parametrize("path", [
    "/api/overview?horizon=1", "/api/items?limit=0", "/api/items?limit=1001",
    "/api/items?watched_only=unknown", "/api/items?q=" + "a" * 201,
    "/api/items/0/bars", "/api/items/bad/forecast", "/api/items/1/bars?days=-1",
    "/api/items/1/bars?timeframe=1h", "/api/items/1/export.csv?timeframe=bad",
    "/api/items/1/forecast?horizons=1", "/api/items/1/forecast?horizons=24,24",
    "/api/items/1/forecast?horizons=", "/api/events?unexpected=yes",
])
def test_invalid_query_and_path_parameters(client, path):
    assert_error(client.get(path), 422, "VALIDATION_ERROR")


@pytest.mark.parametrize("path,body", [
    ("/api/items/1/watch", {"is_watched": 1}),
    ("/api/items/1/watch", {"is_watched": True, "extra": 1}),
    ("/api/fees/simulate", {"buy_price": -1, "sell_price": 2, "qty": 1}),
    ("/api/fees/simulate", {"buy_price": 0, "sell_price": 0, "qty": 1}),
    ("/api/fees/simulate", {"buy_price": 1, "sell_price": 2, "qty": 0}),
    ("/api/fees/simulate", {"buy_price": 1, "sell_price": 2, "qty": 1, "relist_rate": 2}),
    ("/api/quotes/manual", {"item_id": 1, "price": 0}),
    ("/api/backtest/run", {**BACKTEST, "models": ["lgbm"]}),
    ("/api/backtest/run", {**BACKTEST, "models": ["naive", "naive"]}),
    ("/api/backtest/run", {**BACKTEST, "item_id": 1, "category": "stone"}),
    ("/api/events", {"name": "invalid", "event_type": "x", "announced_at": ANCHOR,
                     "start_ts": ANCHOR, "end_ts": ANCHOR - 1}),
    ("/api/admin/trigger", {"action": "unknown"}),
])
def test_invalid_bodies_use_exact_request_models(client, path, body):
    assert_error(client.post(path, json=body), 422, "VALIDATION_ERROR")


def test_validation_does_not_echo_secret_input_or_exception_objects(client, config):
    response = client.post("/api/events", json={"name": TOKEN, "notes": str(config.root)})
    assert_error(response, 422)
    assert str(config.root) not in response.text
    assert_error(client.post("/api/quotes/import-csv"), 422)
    assert_error(client.post("/api/events", content=b'{"broken":'), 422)
    assert_error(client.post("/api/health", json={}), 405)


def test_http_exception_unexpected_exception_and_raw_error_response_are_enveloped(app, config):
    @app.get("/test/http-error")
    def http_error():
        raise HTTPException(418, f"{TOKEN} C:/private/secrets.txt")

    @app.get("/test/exception")
    def unexpected():
        raise RuntimeError(f"{TOKEN} {config.root} C:/private/secrets.txt")

    @app.get("/test/raw-error")
    def raw():
        return Response(f"{TOKEN} C:/private/secrets.txt", status_code=400)

    with TestClient(app, base_url=BASE_URL) as client:
        for path, status in [("http-error", 418), ("exception", 500), ("raw-error", 400)]:
            response = client.get("/test/" + path)
            assert_error(response, status)
            assert "secrets.txt" not in response.text
            assert str(config.root) not in response.text


def test_nonfinite_values_are_recursively_null_in_all_json_layers(app, config):
    @app.get("/test/nonfinite")
    def measurements():
        return {"nan": float("nan"), "nested": [float("inf"), {"neg": -float("inf")}],
                "decimal": Decimal("NaN"), "zero": 0, "bool": False}

    with open_connection(config.db_path) as conn:
        conn.execute("UPDATE item_bars SET close_price=?,p10_price=?,spread_top5_pct=? "
                     "WHERE item_id=1 AND close_ts=?", (float("inf"),) * 3 + (ANCHOR,))
        insert_prediction(conn, prob=float("inf"), roi=-float("inf"),
                          top_drivers_json='[{"feature":"missing","contribution":NaN}]')
    with TestClient(app, base_url=BASE_URL) as client:
        response = client.get("/test/nonfinite")
        assert response.status_code == 200
        assert response.json() == {"nan": None, "nested": [None, {"neg": None}],
                                   "decimal": None, "zero": 0, "bool": False}
        item = client.get("/api/items?q=Alpha").json()[0]
        assert item["current_price"] is None
        assert item["change_24h_pct"] is None
        forecast = client.get("/api/items/1/forecast?horizons=72").json()["forecasts"][0]
        assert forecast["prob_up"] is None and forecast["expected_net_roi"] is None
        assert forecast["top_drivers"][0]["contribution"] is None
        bar = client.get("/api/items/1/bars").json()["bars"][-1]
        assert bar["close_price"] is bar["p10_price"] is bar["spread_top5_pct"] is None


def test_backtest_nonfinite_metrics_and_equity_become_null(client, monkeypatch):
    result = fake_backtest_result(("naive",))
    result["comparison_table"][0].update(net_return_pct=float("nan"), roc_auc=float("inf"))
    result["equity_curves"]["naive"][1]["equity"] = -float("inf")
    monkeypatch.setattr(web.backtester, "run_backtest", lambda *args, **kwargs: result)
    response = client.post("/api/backtest/run", json=BACKTEST)
    assert response.status_code == 200, response.text
    data = s.BacktestResponse.model_validate(response.json())
    assert data.comparison_table[0].metrics.total_return is None
    assert data.comparison_table[0].metrics.roc_auc is None
    assert data.comparison_table[0].metrics.sharpe_ratio is None
    assert data.equity_curves[0].points[1].equity is None


def test_index_frontend_assets_and_real_echarts_are_served_locally(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "/static/app.js" in response.text
    assert "/static/vendor/echarts.min.js" in response.text
    assert client.get("/static/app.js").status_code == 200
    chart = client.get("/static/vendor/echarts.min.js")
    assert chart.status_code == 200
    assert len(chart.content) > 500_000
    assert chart.headers.get("access-control-allow-origin") != "*"


def test_openapi_declares_exact_schema_response_models(client):
    paths = client.get("/openapi.json").json()["paths"]
    for path, method, name in [
        ("/api/health", "get", "HealthResponse"),
        ("/api/overview", "get", "OverviewResponse"),
        ("/api/items", "get", "ItemListResponse"),
        ("/api/items/{item_id}/watch", "post", "WatchResponse"),
        ("/api/items/{item_id}/bars", "get", "BarsResponse"),
        ("/api/items/{item_id}/forecast", "get", "ForecastResponse"),
        ("/api/fees/simulate", "post", "FeeSimulationResponse"),
        ("/api/backtest/run", "post", "BacktestResponse"),
        ("/api/quotes/manual", "post", "ManualQuoteResponse"),
        ("/api/quotes/import-csv", "post", "CSVImportResponse"),
        ("/api/events", "get", "EventListResponse"),
        ("/api/events", "post", "EventCreateResponse"),
        ("/api/events/{event_id}", "delete", "EventDeleteResponse"),
        ("/api/admin/trigger", "post", "AdminTriggerResponse"),
    ]:
        schema = paths[path][method]["responses"]["200"]["content"]["application/json"]["schema"]
        assert schema["$ref"] == "#/components/schemas/" + name
