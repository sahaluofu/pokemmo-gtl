"""Database-backed, causal cold-start routing for CLI and Web forecasts."""

from __future__ import annotations

import json
import math
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from pokemmo_gtl.config import AppConfig
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.quant.features import FEATURE_COLUMNS, add_targets, build_feature_frame
from pokemmo_gtl.quant.models.base import Forecaster, PredictionOutput
from pokemmo_gtl.quant.models.lgbm_model import Tier2LightGBMForecaster
from pokemmo_gtl.quant.models.naive import NaiveForecaster
from pokemmo_gtl.quant.models.statistical import (
    Tier0RuleForecaster,
    Tier1RidgeForecaster,
    select_forecaster,
)

HORIZONS = (24, 72, 168)
MODEL_FEATURES = (*FEATURE_COLUMNS, "ret_30d", "zscore_30d")
PREDICTION_COLUMNS = (
    "generated_at",
    "item_id",
    "horizon_hours",
    "model_name",
    "current_price",
    "ref_qty",
    "prob_up",
    "pred_q10",
    "pred_q50",
    "pred_q90",
    "expected_net_roi",
    "signal",
    "regime_note",
    "top_drivers_json",
)


def _validate_horizons(horizons: Iterable[int]) -> tuple[int, ...]:
    values = tuple(dict.fromkeys(horizons))
    if any(type(value) is not int or value not in HORIZONS for value in values):
        raise ValueError("horizons must contain only 24, 72 or 168 hours")
    return values


def _database_path(config: AppConfig, db_path: str | Path | None) -> Path:
    path = Path(config.db_path if db_path is None else db_path)
    if not path.is_file():
        raise FileNotFoundError(f"Market database does not exist: {path}")
    return path


def _load_feature_panel(config: AppConfig, db_path: Path) -> pd.DataFrame:
    """Read one SQLite snapshot, retaining all items needed for causal joins."""
    with open_connection(db_path) as conn:
        conn.execute("BEGIN")
        try:
            bars = pd.DataFrame.from_records(
                [
                    dict(row)
                    for row in conn.execute(
                        "SELECT * FROM item_bars WHERE close_ts <= ? AND "
                        "(timeframe = '4h' OR (timeframe = '1d' AND data_source = 'manual')) "
                        "ORDER BY item_id, close_ts",
                        (int(time.time()),),
                    )
                ]
            )
            catalog = pd.DataFrame.from_records(
                [dict(row) for row in conn.execute("SELECT * FROM items_catalog ORDER BY item_id")]
            )
            events = [dict(row) for row in conn.execute("SELECT * FROM market_events")]
        finally:
            conn.execute("ROLLBACK")
    if bars.empty:
        return pd.DataFrame()
    prices = pd.to_numeric(bars["close_price"], errors="coerce")
    bars = bars.loc[np.isfinite(prices) & prices.gt(0) & bars["sample_count"].gt(0)]
    primary = bars.loc[bars["timeframe"].eq("4h")].copy()
    if primary.empty:
        return pd.DataFrame()
    frame = build_feature_frame(
        primary,
        manual_bars=bars.loc[bars["timeframe"].eq("1d")],
        market_events=events,
        config=config,
    )
    observed = primary[["item_id", "close_ts"]].drop_duplicates().assign(observed_bar=True)
    frame = frame.merge(observed, on=["item_id", "close_ts"], how="left", validate="one_to_one")
    frame["observed_bar"] = frame["observed_bar"].eq(True)
    frame["valid_bar_count"] = frame.groupby("item_id")["observed_bar"].cumsum().astype(int)
    frame = frame.merge(
        catalog[["item_id", "asset_class", "category", "max_stack_size"]],
        on="item_id",
        how="left",
        validate="many_to_one",
    )
    # The current feature contract has no 30d columns. Keep these causal, full-window
    # columns here so short-history imputation is exercised without changing features.py.
    grouped = frame.groupby("item_id")["close_price"]
    prior = grouped.shift(180)
    frame["ret_30d"] = frame["close_price"].div(prior.where(prior.gt(0))).sub(1)
    mean = grouped.transform(lambda values: values.rolling(180, min_periods=180).mean())
    std = grouped.transform(lambda values: values.rolling(180, min_periods=180).std())
    frame["zscore_30d"] = (frame["close_price"] - mean).div(std.where(std.gt(0)))
    return frame.sort_values(["close_ts", "item_id"], kind="stable").reset_index(drop=True)


def _target_panel(frame: pd.DataFrame, config: AppConfig, horizon: int) -> pd.DataFrame:
    result = add_targets(
        frame,
        horizon,
        config.fees,
        ref_notional=config.settings.ref_notional_yen,
    )
    observed_keys = set(
        map(tuple, frame.loc[frame["observed_bar"], ["item_id", "close_ts"]].to_numpy())
    )
    has_exit = np.fromiter(
        (
            (int(item), int(ts) + horizon * 3600) in observed_keys
            for item, ts in result[["item_id", "close_ts"]].itertuples(index=False, name=None)
        ),
        dtype=bool,
        count=len(result),
    )
    valid = result["observed_bar"] & has_exit
    result.loc[~valid, ["y_reg", "y_cls", "p_exit"]] = np.nan
    return result


def _model_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Use an explicit allowlist: targets and execution metadata never enter a model."""
    return (
        frame.reindex(columns=MODEL_FEATURES)
        .apply(pd.to_numeric, errors="coerce")
        .replace(
            [np.inf, -np.inf],
            np.nan,
        )
    )


def _fit_model(
    family: str,
    training: pd.DataFrame,
    horizon: int,
    *,
    bar_count: int = 0,
) -> Forecaster:
    if family == "naive":
        return NaiveForecaster().fit(
            _model_frame(training),
            training["y_reg"],
            positive_labels=training["y_cls"],
        )
    if family == "lgbm":
        return Tier2LightGBMForecaster(horizon_hours=horizon).fit(
            _model_frame(training),
            training["y_reg"],
            training["y_cls"],
            close_ts=training["close_ts"],
            item_ids=training["item_id"],
        )
    if family != "statistical":
        raise ValueError(f"Unknown model: {family}")
    # The comparison's statistical family stays Ridge even for long histories.
    model = select_forecaster(min(bar_count, 179), horizon_hours=horizon)
    return model.fit(_model_frame(training), training["y_reg"])


def _fee_kwargs(config: AppConfig, row: dict[str, Any] | pd.Series) -> dict[str, Any]:
    pokemon = (
        row.get("asset_class") in {"pokemon", "pokemon_category"} or int(row["item_id"]) >= 900000
    )
    return {
        "min_fee": config.fees.pokemon_min_fee if pokemon else config.fees.item_min_fee,
        "rate": config.fees.rate,
        "relist_rate": config.fees.default_relist_rate,
        "max_fee_per_slot": config.fees.max_fee_per_slot,
    }


def _reference_quantity(config: AppConfig, row: dict[str, Any] | pd.Series) -> int:
    k = 1
    for column in ("K_t", "k_t", "depth_within_5pct", "listing_count", "total_listed_qty"):
        value = row.get(column)
        if value is not None and pd.notna(value) and math.isfinite(float(value)):
            k = max(1, int(value))
            break
    return min(
        int(row["max_stack_size"]),
        k,
        max(1, math.floor(config.settings.ref_notional_yen / max(float(row["close_price"]), 1))),
    )


def _predict_row(model: Forecaster, row: pd.DataFrame, config: AppConfig) -> PredictionOutput:
    values = row.iloc[0]
    return model.predict(
        _model_frame(row),
        current_price=float(values["close_price"]),
        ref_qty=_reference_quantity(config, values),
        max_stack_size=int(values["max_stack_size"]),
        **_fee_kwargs(config, values),
    )


def run_forecast(
    config: AppConfig,
    horizons: Iterable[int] = (24, 72, 168),
    item_ids: Iterable[int] | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Fit causal per-horizon models, route by actual 4h bars, and atomically persist."""
    horizons = _validate_horizons(horizons)
    selected = None if item_ids is None else set(item_ids)
    if selected is not None and any(type(item) is not int or item <= 0 for item in selected):
        raise ValueError("item_ids must contain positive integers")
    if not horizons or selected == set():
        return []
    path = _database_path(config, db_path)
    panel = _load_feature_panel(config, path)
    if panel.empty:
        return []
    latest = panel.loc[panel["observed_bar"]].groupby("item_id", sort=True).tail(1)
    if selected is not None:
        latest = latest.loc[latest["item_id"].isin(selected)]
    generated_at = int(time.time())
    records: list[dict[str, Any]] = []
    for horizon in horizons:
        targets = _target_panel(panel, config, horizon)
        panel_models: dict[int, Forecaster] = {}
        for _, row in latest.sort_values("item_id").iterrows():
            cutoff = int(row["close_ts"])
            training = targets.loc[
                targets["y_reg"].notna()
                & targets["y_cls"].notna()
                & (targets["close_ts"] + horizon * 3600).le(cutoff)
            ]
            count = int(row["valid_bar_count"])
            if count >= 180:
                if cutoff not in panel_models:
                    panel_models[cutoff] = _fit_model("lgbm", training, horizon)
                model = panel_models[cutoff]
                name = "lgbm"
            else:
                model = _fit_model(
                    "statistical",
                    training.loc[training["item_id"].eq(row["item_id"])],
                    horizon,
                    bar_count=count,
                )
                name = "tier0_rule" if isinstance(model, Tier0RuleForecaster) else "tier1_ridge"
                assert isinstance(model, (Tier0RuleForecaster, Tier1RidgeForecaster))
            output = _predict_row(model, row.to_frame().T, config)
            records.append(
                {
                    "generated_at": generated_at,
                    "item_id": int(row["item_id"]),
                    "horizon_hours": horizon,
                    "model_name": name,
                    "current_price": float(row["close_price"]),
                    "ref_qty": _reference_quantity(config, row),
                    "prob_up": output.prob_up,
                    "pred_q10": output.pred_q10,
                    "pred_q50": output.pred_q50,
                    "pred_q90": output.pred_q90,
                    "expected_net_roi": output.expected_net_roi,
                    "signal": output.signal,
                    "regime_note": output.regime_note,
                    "top_drivers_json": json.dumps(
                        output.top_drivers,
                        ensure_ascii=False,
                        allow_nan=False,
                    ),
                }
            )
    if records:
        columns = ", ".join(PREDICTION_COLUMNS)
        placeholders = ", ".join("?" for _ in PREDICTION_COLUMNS)
        with open_connection(path) as conn, write_transaction(conn):
            conn.executemany(
                f"INSERT INTO predictions ({columns}) VALUES ({placeholders})",
                [tuple(record[column] for column in PREDICTION_COLUMNS) for record in records],
            )
    return records


__all__ = ["run_forecast"]
