"""Isolated M5 integration tests with cyclic demand and observable leading depth."""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from pokemmo_gtl.config import AppConfig, load_config
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.quant.backtester import (
    _simulate_portfolio,
    purged_walk_forward_split,
    run_backtest,
)
from pokemmo_gtl.quant.features import GRID_SECONDS
from pokemmo_gtl.quant.fees import net_profit
from pokemmo_gtl.quant.predictor import (
    PREDICTION_COLUMNS,
    _load_feature_panel,
    _model_frame,
    _target_panel,
    run_forecast,
)

START_TS = 1_751_328_000
HORIZON = 72
LONG_ITEMS = (1001, 1002, 1003, 1004, 1005)
TIER0_ITEM = 1100
TIER1_ITEM = 1200


@pytest.fixture(autouse=True)
def verify_repository_local_temp_directory(tmp_path: Path, pytestconfig: pytest.Config) -> None:
    """Respect the explicit concurrent basetemp without changing shared conftest.py."""
    base = Path(pytestconfig.option.basetemp).resolve()
    assert base.is_relative_to(pytestconfig.rootpath.resolve())
    assert tmp_path.resolve().is_relative_to(base)


def _create_market(config: AppConfig, path: Path) -> None:
    """Generate latent cyclical demand, not a feature copied from future prices."""
    migrate(path, applied_at=START_TS)
    rng = np.random.default_rng(20261125)
    catalog = []
    records = []
    for number, item in enumerate((*LONG_ITEMS, TIER0_ITEM, TIER1_ITEM)):
        days = 10 if item == TIER0_ITEM else 20 if item == TIER1_ITEM else 90
        catalog.append((item, f"M5 item {item}", "cold" if days < 90 else "cycle", 99, START_TS))
        phase_offset = (number % 4) * 0.7
        previous = None
        for step in range(540 - days * 6, 540):
            phase = 2 * np.pi * step / 60 + phase_offset
            # Demand/depth lead the price cycle by 1.5 days. All observations
            # depend only on the current deterministic latent phase plus noise.
            pressure = float(np.cos(phase + np.pi * 18 / 60))
            drift = 0.003 if item == 1005 else 0.0012
            amplitude = 0.04 if item == 1005 else 0.22
            price = round(
                (12_000 + number * 2_000)
                * np.exp(
                    drift * step
                    + amplitude * np.sin(phase)
                    + 0.008 * np.sin(2 * np.pi * step / 6)
                    + rng.normal(0, 0.001),
                ),
                4,
            )
            opening = price if previous is None else previous
            sold = 160 + 80 * pressure
            new = int(110 - 65 * pressure)
            qty = int(900 - 300 * pressure)
            depth = int(300 - 180 * pressure)
            spread = 0.025 - 0.015 * pressure
            close_ts = START_TS + (step + 1) * GRID_SECONDS
            records.append(
                (
                    item,
                    "4h",
                    close_ts - GRID_SECONDS,
                    close_ts,
                    "snapshot",
                    opening,
                    max(opening, price) * 1.002,
                    min(opening, price) * 0.998,
                    price,
                    int(price * 0.995),
                    int(price * 0.99),
                    price * 0.997,
                    qty,
                    max(5, qty // 12),
                    depth,
                    spread,
                    sold,
                    2,
                    new,
                    16,
                    16,
                )
            )
            previous = price
    with open_connection(path) as conn, write_transaction(conn):
        conn.executemany(
            "INSERT INTO items_catalog (item_id, name_en, category, max_stack_size, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            catalog,
        )
        conn.executemany(
            "INSERT INTO item_bars (item_id, timeframe, bucket_ts, close_ts, data_source, "
            "open_price, high_price, low_price, close_price, robust_best_ask, best_ask_min, "
            "p10_price, total_listed_qty, listing_count, depth_within_5pct, spread_top5_pct, "
            "est_sold_qty_sum, expired_qty_sum, new_listed_qty_sum, "
            "sample_count, valid_flow_samples) "
            "VALUES (" + ", ".join("?" for _ in range(21)) + ")",
            records,
        )
        conn.execute(
            "INSERT INTO market_events (name, event_type, announced_at, start_ts, end_ts) "
            "VALUES (?, ?, ?, ?, ?)",
            ("测试季节活动", "seasonal", START_TS, START_TS + 50 * 86400, START_TS + 55 * 86400),
        )
        # A separate daily manual quote exercises the causal multi-timeframe input.
        conn.execute(
            "INSERT INTO item_bars (item_id, timeframe, bucket_ts, close_ts, data_source, "
            "open_price, high_price, low_price, close_price, robust_best_ask, best_ask_min, "
            "p10_price, total_listed_qty) VALUES (1001, '1d', ?, ?, 'manual', "
            "12000, 12000, 12000, 12000, 12000, 12000, 12000, 50)",
            (START_TS, START_TS + 86400),
        )


@pytest.fixture(scope="module")
def market(tmp_path_factory: pytest.TempPathFactory) -> tuple[AppConfig, Path]:
    root = tmp_path_factory.mktemp("m5_bt_market")
    config = replace(
        load_config("sim"),
        root=root,
        item_relations={
            "rp_vouchers": [{"item_id": 1004, "denomination": 100}],
            "upstream_relations": [
                {"target_item_id": 1001, "upstream_item_id": 1002, "weight": 1.0},
            ],
        },
    )
    path = root / "market.db"
    _create_market(config, path)
    return config, path


@pytest.fixture(scope="module")
def feature_panel(market: tuple[AppConfig, Path]) -> pd.DataFrame:
    config, path = market
    return _load_feature_panel(config, path)


@pytest.fixture(scope="module")
def forecasts(market: tuple[AppConfig, Path]) -> list[dict[str, Any]]:
    config, path = market
    return run_forecast(config, db_path=path)


@pytest.fixture(scope="module")
def backtest(market: tuple[AppConfig, Path]) -> dict[str, Any]:
    config, path = market
    return run_backtest(config, db_path=path)


def test_forecast_routes_10_20_90_days_and_persists_exact_schema(
    market: tuple[AppConfig, Path],
    forecasts: list[dict[str, Any]],
) -> None:
    _, path = market
    assert len(forecasts) == 7 * 3
    expected = {TIER0_ITEM: "tier0_rule", TIER1_ITEM: "tier1_ridge"}
    for row in forecasts:
        assert set(row) == set(PREDICTION_COLUMNS)
        assert row["model_name"] == expected.get(row["item_id"], "lgbm")
        assert row["horizon_hours"] in (24, 72, 168)
        assert row["ref_qty"] > 0
        assert 0 <= row["prob_up"] <= 1
        assert row["pred_q10"] <= row["pred_q50"] <= row["pred_q90"]
        assert np.isfinite(row["expected_net_roi"])
        assert len(json.loads(row["top_drivers_json"])) <= 5
    with open_connection(path) as conn:
        saved = [
            dict(row)
            for row in conn.execute(
                "SELECT "
                + ", ".join(PREDICTION_COLUMNS)
                + " FROM predictions ORDER BY prediction_id",
            )
        ]
    assert saved == forecasts


def test_tier1_handles_all_nan_30d_features_and_feature_allowlist(
    feature_panel: pd.DataFrame,
    forecasts: list[dict[str, Any]],
) -> None:
    tier1 = feature_panel.loc[feature_panel["item_id"].eq(TIER1_ITEM)]
    assert len(tier1) == 120
    assert tier1[["ret_30d", "zscore_30d"]].isna().all().all()
    poisoned = tier1.assign(y_reg=99, y_cls=1, p_exit=10**9, target_close_price=10**9)
    pd.testing.assert_frame_equal(_model_frame(tier1), _model_frame(poisoned))
    assert all("tier1" in row["regime_note"] for row in forecasts if row["item_id"] == TIER1_ITEM)


def test_forecast_at_inflation_high_can_predict_above_current_price(
    feature_panel: pd.DataFrame,
    forecasts: list[dict[str, Any]],
) -> None:
    history = feature_panel.loc[feature_panel["item_id"].eq(1005)]
    assert history.iloc[-1]["close_price"] == history["close_price"].max()
    prediction = next(
        row for row in forecasts if row["item_id"] == 1005 and row["horizon_hours"] == 72
    )
    assert prediction["pred_q50"] > prediction["current_price"]


def test_global_purge_for_every_training_row_and_synchronized_test_fold(
    market: tuple[AppConfig, Path],
    feature_panel: pd.DataFrame,
    backtest: dict[str, Any],
) -> None:
    config, _ = market
    targets = _target_panel(feature_panel, config, HORIZON)
    targets = targets.loc[targets["y_reg"].notna() & targets["y_cls"].notna()]
    folds = list(purged_walk_forward_split(targets, HORIZON))
    assert len(folds) == len(backtest["folds"]) == 4
    test_keys = set()
    for (train, test), summary in zip(folds, backtest["folds"], strict=True):
        first = int(test["close_ts"].min())
        assert (train["close_ts"] + HORIZON * 3600 <= first).all()
        assert int(train["close_ts"].max()) + HORIZON * 3600 <= first
        assert summary["train_end_ts"] == train["close_ts"].max()
        assert summary["test_start_ts"] == first
        assert summary["train_rows"] == len(train)
        assert summary["test_rows"] == len(test)
        for item in LONG_ITEMS:
            assert test.loc[test["item_id"].eq(item), "close_ts"].min() == first
        keys = set(map(tuple, test[["item_id", "close_ts"]].to_numpy()))
        assert not test_keys.intersection(keys)
        test_keys.update(keys)


def test_no_overlapping_positions_caps_and_continuous_equity(backtest: dict[str, Any]) -> None:
    assert backtest["trades"]["lgbm"]
    for name, trades in backtest["trades"].items():
        last_exit = {}
        for trade in sorted(trades, key=lambda row: (row["entry_ts"], row["item_id"])):
            item = trade["item_id"]
            assert trade["entry_ts"] >= last_exit.get(item, 0)
            assert trade["exit_ts"] == trade["entry_ts"] + HORIZON * 3600
            assert trade["qty"] <= trade["quantity_cap"] <= trade["ref_qty"]
            assert trade["cost"] <= trade["entry_equity"] * 0.10 + 1e-8
            assert trade["fees"] > 0
            last_exit[item] = trade["exit_ts"]
        curve = backtest["equity_curves"][name]
        assert curve[0]["equity"] == 10_000_000
        assert curve[-1]["positions"] == 0
        assert all(point["cash"] >= 0 for point in curve)
        assert curve[-1]["equity"] == pytest.approx(
            10_000_000 + sum(t["net_profit"] for t in trades)
        )


def test_lgbm_beats_naive_after_fees_and_persists_full_comparison(
    market: tuple[AppConfig, Path],
    backtest: dict[str, Any],
) -> None:
    _, path = market
    results = {row["model_name"]: row for row in backtest["comparison_table"]}
    naive, lgbm = results["naive"], results["lgbm"]
    assert naive["trade_count"] == 0
    assert naive["net_return_pct"] == 0
    assert lgbm["pinball_mean"] < naive["pinball_mean"]
    assert lgbm["roc_auc"] > naive["roc_auc"]
    assert lgbm["net_return_pct"] > 0
    metric_names = {
        "roc_auc",
        "brier_score",
        "pinball_q10",
        "pinball_q50",
        "pinball_q90",
        "pinball_mean",
        "coverage_80",
        "raw_quantile_crossing_rate",
        "net_return_pct",
        "win_rate",
        "trade_count",
        "max_drawdown_pct",
    }
    with open_connection(path) as conn:
        saved = [dict(row) for row in conn.execute("SELECT * FROM backtest_runs ORDER BY run_id")]
    assert len(saved) == 3
    for row in saved:
        name = row["model_name"]
        metrics = json.loads(row["metrics_json"])
        assert set(metrics) == metric_names
        assert metrics == {
            key: value for key, value in results[name].items() if key in metric_names
        }
        assert row["run_id"] == backtest["run_ids"][name]
        assert row["horizon_hours"] == HORIZON
        assert row["item_scope"] == "all"
        assert json.loads(row["config_json"])["folds"] == backtest["folds"]
        curve = json.loads(row["equity_curve_json"])
        assert curve == backtest["equity_curves"][name]
        equity = np.array([point["equity"] for point in curve])
        expected_drawdown = np.max(1 - equity / np.maximum.accumulate(equity)) * 100
        assert metrics["max_drawdown_pct"] == pytest.approx(expected_drawdown)
        assert 0 <= metrics["brier_score"] <= 1
        assert 0 <= metrics["coverage_80"] <= 1
        assert 0 <= metrics["raw_quantile_crossing_rate"] <= 1
        assert metrics["pinball_mean"] == pytest.approx(
            np.mean(
                [
                    metrics["pinball_q10"],
                    metrics["pinball_q50"],
                    metrics["pinball_q90"],
                ]
            )
        )
    print("\n90-day comparison: " + json.dumps(backtest["comparison_table"], ensure_ascii=False))


def _execution_rows(
    *,
    listed: float = 100,
    sold: float = 2000,
    source: str = "snapshot",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "item_id": 1,
                "close_ts": START_TS + step * GRID_SECONDS,
                "observed_bar": True,
                "close_price": close,
                "p10_price": p10,
                "max_stack_size": 99,
                "asset_class": "item",
                "data_source": source,
                "total_listed_qty": listed,
                "est_sold_qty_24h": sold,
            }
            for step, close, p10 in (
                (0, 100000, 99000),
                (1, 130000, 120000),
                (18, 150000, 140000),
                (36, 160000, np.nan),
            )
        ]
    )


def _buy_forecast(timestamp: int, *, ref_qty: int = 99) -> dict[str, Any]:
    return {
        "item_id": 1,
        "close_ts": timestamp,
        "signal": "BUY_WATCH",
        "prob_up": 0.9,
        "expected_net_roi": 0.5,
        "pred_q10": 200000,
        "pred_q50": 200000,
        "pred_q90": 200000,
        "ref_qty": ref_qty,
    }


def test_account_uses_taker_maker_decimal_fees_and_locks_until_exact_horizon(
    market: tuple[AppConfig, Path],
) -> None:
    config, _ = market
    panel = _execution_rows()
    decisions = [_buy_forecast(START_TS + offset) for offset in (0, GRID_SECONDS, HORIZON * 3600)]
    metrics, curve, trades = _simulate_portfolio(config, panel, decisions, HORIZON)
    assert len(trades) == 2
    assert [trade["qty"] for trade in trades] == [10, 6]
    assert trades[0]["entry_price"] == 100000
    assert trades[0]["exit_price"] == 140000
    assert trades[1]["exit_price"] == 160000  # NULL p10 falls back to close.
    assert trades[0]["exit_ts"] == trades[1]["entry_ts"]
    expected = net_profit(100000, 140000, 10, 99) + net_profit(150000, 160000, 6, 99)
    assert expected == Decimal("402500.00")
    assert curve[-1]["equity"] == float(Decimal("10000000") + expected)
    assert metrics["net_return_pct"] == pytest.approx(float(expected / 10_000_000 * 100))
    assert metrics["max_drawdown_pct"] > 0


@pytest.mark.parametrize("listed,sold", [(2, 100), (100, 0), (100, np.nan), (np.nan, 100)])
def test_snapshot_illiquidity_never_opens(
    market: tuple[AppConfig, Path],
    listed: float,
    sold: float,
) -> None:
    config, _ = market
    metrics, curve, trades = _simulate_portfolio(
        config,
        _execution_rows(listed=listed, sold=sold),
        [_buy_forecast(START_TS)],
        HORIZON,
    )
    assert not trades
    assert metrics["net_return_pct"] == 0
    assert all(point["positions"] == 0 for point in curve)


def test_quantity_is_capped_by_twenty_percent_of_estimated_daily_sales(
    market: tuple[AppConfig, Path],
) -> None:
    config, _ = market
    _, _, trades = _simulate_portfolio(
        config,
        _execution_rows(sold=17),
        [_buy_forecast(START_TS)],
        HORIZON,
    )
    assert len(trades) == 1
    assert trades[0]["qty"] == trades[0]["quantity_cap"] == 3


def test_filtering_override_path_and_empty_backtest_are_serializable(
    market: tuple[AppConfig, Path],
    tmp_path: Path,
) -> None:
    config, path = market
    config = replace(config, root=tmp_path / "unused_default_database")
    rows = run_forecast(config, horizons=(24,), item_ids=(TIER0_ITEM,), db_path=path)
    assert len(rows) == 1
    assert rows[0]["item_id"] == TIER0_ITEM
    result = run_backtest(
        config,
        models=("naive",),
        item_id=TIER1_ITEM,
        category="cold",
        db_path=path,
    )
    assert not result["folds"]
    assert result["comparison_table"][0]["roc_auc"] is None
    assert result["comparison_table"][0]["trade_count"] == 0
    assert result["item_scope"] == f"item:{TIER1_ITEM};category:cold"
    json.dumps(result, allow_nan=False)
    assert not config.db_path.exists()


def test_invalid_requests_do_not_write(market: tuple[AppConfig, Path]) -> None:
    config, path = market
    with open_connection(path) as conn:
        before = tuple(
            conn.execute(
                "SELECT (SELECT count(*) FROM predictions), (SELECT count(*) FROM backtest_runs)",
            ).fetchone()
        )
    with pytest.raises(ValueError, match="horizons"):
        run_forecast(config, horizons=(12,), db_path=path)
    with pytest.raises(ValueError, match="models"):
        run_backtest(config, models=("unknown",), db_path=path)
    with open_connection(path) as conn:
        after = tuple(
            conn.execute(
                "SELECT (SELECT count(*) FROM predictions), (SELECT count(*) FROM backtest_runs)",
            ).fetchone()
        )
    assert after == before
