from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pokemmo_gtl.quant.models import (
    NaiveForecaster,
    Tier0RuleForecaster,
    Tier1RidgeForecaster,
    Tier2LightGBMForecaster,
    select_forecaster,
)
from pokemmo_gtl.quant.models.base import build_prediction_output, signal_for


@pytest.fixture
def tmp_path(request: pytest.FixtureRequest, pytestconfig: pytest.Config) -> Path:
    """Keep the repo guard valid when pytest receives its dedicated M5 basetemp."""
    base = pytestconfig.rootpath / pytestconfig.getini("basetemp") / ".pytest_tmp_m5_models"
    path = base / request.node.name
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_shared_postprocessing_uses_monotone_prices_and_fee_aware_signal() -> None:
    result = build_prediction_output(
        current_price=10_000,
        q10_ret=0.10,
        q50_ret=0.02,
        q90_ret=0.05,
        prob_up=0.7,
        ref_qty=99,
    )

    assert result.raw_quantile_crossed
    assert result.pred_q10 <= result.pred_q50 <= result.pred_q90
    assert result.pred_q10_ret <= result.pred_q50_ret <= result.pred_q90_ret
    assert result.signal in {"BUY_WATCH", "NEUTRAL", "SELL_CAUTION"}
    assert signal_for(0.02, 0.55) == "BUY_WATCH"
    assert signal_for(-0.001, 0.99) == "SELL_CAUTION"
    assert signal_for(0.019, 0.99) == "NEUTRAL"


def test_naive_uses_historical_tails_and_neutral_signal() -> None:
    returns = np.array([-0.12, -0.04, -0.01, 0.02, 0.03, 0.08])
    model = NaiveForecaster().fit(None, returns)
    result = model.predict(current_price=12_000, ref_qty=10)

    assert result.pred_q50_ret == 0
    assert result.pred_q10_ret == min(np.quantile(returns, 0.1), 0)
    assert result.pred_q90_ret == max(np.quantile(returns, 0.9), 0)
    assert result.prob_up == np.mean(returns > 0)
    assert result.signal == "NEUTRAL"


def test_tier0_uses_market_pressure_zscore_and_empirical_residuals() -> None:
    rng = np.random.default_rng(13)
    returns = rng.normal(0.002, 0.025, 120)
    features = pd.DataFrame(
        {
            "flow_imbalance_24h": [0.4],
            "total_listed_qty": [30],
            "est_sold_24h": [20],
            "zscore_30d": [-1.5],
            "spread_top5_pct": [0.02],
            "depth_ratio_24h": [1.4],
            "current_price": [5_000],
        }
    )
    model = Tier0RuleForecaster().fit(features, returns)
    result = model.predict(features, ref_qty=20)

    assert result.pred_q10 <= result.pred_q50 <= result.pred_q90
    assert 0 <= result.prob_up <= 1
    assert result.top_drivers
    assert len(result.top_drivers) <= 5
    assert "tier0" in result.regime_note


def test_tier1_pipeline_keeps_all_nan_30d_features() -> None:
    rng = np.random.default_rng(42)
    size = 110
    trend = np.linspace(-0.5, 0.7, size)
    features = pd.DataFrame(
        {
            "flow_imbalance_24h": rng.normal(size=size),
            "zscore_30d": np.full(size, np.nan),
            "return_30d": np.full(size, np.nan),
        }
    )
    targets = 0.012 * trend + 0.004 * features["flow_imbalance_24h"].to_numpy()
    model = Tier1RidgeForecaster().fit(features, targets)
    result = model.predict(
        {"flow_imbalance_24h": 1.0, "zscore_30d": np.nan, "return_30d": np.nan},
        current_price=20_000,
        ref_qty=20,
    )

    assert np.isfinite(result.pred_q50_ret)
    assert result.pred_q10 <= result.pred_q50 <= result.pred_q90
    assert 0 <= result.prob_up <= 1
    assert result.top_drivers
    assert len(result.top_drivers) <= 5
    assert isinstance(select_forecaster(83), Tier0RuleForecaster)
    assert isinstance(select_forecaster(84), Tier1RidgeForecaster)
    assert isinstance(select_forecaster(180), Tier2LightGBMForecaster)


def test_tier2_purges_platt_calibrates_cqr_and_handles_new_inflation_high() -> None:
    rng = np.random.default_rng(27)
    time_count = 240
    item_count = 3
    rows: list[dict[str, float | int]] = []
    returns: list[float] = []
    up_labels: list[int] = []
    for item in range(item_count):
        for step in range(time_count):
            inflation = step * 0.012 + item * 0.003
            ret = (
                0.004
                + 0.004 * inflation
                + 0.007 * np.sin((step + item * 2) / 7)
                + rng.normal(0, 0.0015)
            )
            rows.append(
                {
                    "item_id": item + 1,
                    "close_ts": step * 14_400,
                    "current_price": 100_000 + item * 1_000,
                    "inflation_yoy": inflation,
                    "inflation_new_high": int(step >= 190),
                    "flow_imbalance_24h": np.sin(step / 11),
                    "zscore_30d": np.cos(step / 9),
                }
            )
            returns.append(float(ret))
            up_labels.append(int(ret >= 0.014))

    X = pd.DataFrame(rows)
    y = np.asarray(returns)
    model = Tier2LightGBMForecaster(horizon_hours=72).fit(
        X,
        y,
        np.asarray(up_labels),
    )

    assert model.train_close_ts_max + 72 * 3600 <= model.validation_start_ts
    assert np.all(model.train_sample_weights > 0)
    assert np.all(model.train_sample_weights <= 1)
    assert model.direction_model.get_params().get("class_weight") is None
    assert model.platt_model is not None
    assert 0 < model.platt_shrinkage < 1
    assert 0 <= model.raw_quantile_crossing_rate <= 1
    assert model.c80 >= 0
    assert model.validation_coverage_after >= 0.80

    forecast = model.predict(
        {
            "current_price": 110_000,
            "inflation_yoy": 3.8,
            "inflation_new_high": 1,
            "flow_imbalance_24h": 0.8,
            "zscore_30d": 2.0,
        },
        ref_qty=99,
    )
    assert forecast.pred_q10 <= forecast.pred_q50 <= forecast.pred_q90
    assert forecast.pred_q50 > 110_000
    assert 0 <= forecast.prob_up <= 1
    assert len(forecast.top_drivers) <= 5
    assert forecast.top_drivers
