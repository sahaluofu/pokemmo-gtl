"""Globally purged walk-forward comparisons and one continuous taxed portfolio."""

from __future__ import annotations

import json
import math
import time
from collections.abc import Iterator, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, mean_pinball_loss, roc_auc_score

from pokemmo_gtl.config import AppConfig
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.quant.features import GRID_SECONDS
from pokemmo_gtl.quant.fees import net_profit
from pokemmo_gtl.quant.models.base import tukey_expected_price
from pokemmo_gtl.quant.predictor import (
    _database_path,
    _fee_kwargs,
    _fit_model,
    _load_feature_panel,
    _predict_row,
    _reference_quantity,
    _target_panel,
    _validate_horizons,
)

INITIAL_CAPITAL = Decimal("10000000")
MODEL_NAMES = ("naive", "statistical", "lgbm")


def purged_walk_forward_split(
    frame: pd.DataFrame,
    horizon_hours: int = 72,
    *,
    n_splits: int = 4,
    min_train_bars: int = 180,
) -> Iterator[tuple[pd.DataFrame, pd.DataFrame]]:
    """Expand training by global UTC timestamps, with H seconds before each test.

    Every item uses the same fold boundaries. A train label ending exactly at
    the first test close is available and is therefore allowed by the contract.
    """
    _validate_horizons((horizon_hours,))
    if type(n_splits) is not int or n_splits <= 0:
        raise ValueError("n_splits must be a positive integer")
    if type(min_train_bars) is not int or min_train_bars < 3:
        raise ValueError("min_train_bars must be at least three")
    if frame.empty:
        return
    times = np.sort(frame["close_ts"].unique())
    horizon_sec = horizon_hours * 3600
    first_test = int(times[0]) + (min_train_bars - 1) * GRID_SECONDS + horizon_sec
    test_times = times[times >= first_test]
    for block in np.array_split(test_times, min(n_splits, len(test_times)) or 1):
        if not len(block):
            continue
        training = frame.loc[(frame["close_ts"] + horizon_sec).le(int(block[0]))].copy()
        test = frame.loc[frame["close_ts"].between(int(block[0]), int(block[-1]))].copy()
        if training.empty or test.empty:
            continue
        if int(training["close_ts"].max()) + horizon_sec > int(test["close_ts"].min()):
            raise RuntimeError("training labels overlap the global test window")
        yield training, test


def _fold_summary(index: int, train: pd.DataFrame, test: pd.DataFrame) -> dict[str, Any]:
    return {
        "fold": index,
        "train_start_ts": int(train["close_ts"].min()),
        "train_end_ts": int(train["close_ts"].max()),
        "test_start_ts": int(test["close_ts"].min()),
        "test_end_ts": int(test["close_ts"].max()),
        "train_rows": len(train),
        "test_rows": len(test),
        "train_item_ids": sorted(int(value) for value in train["item_id"].unique()),
        "test_item_ids": sorted(int(value) for value in test["item_id"].unique()),
    }


def _forecast_fold(
    config: AppConfig,
    family: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    horizon: int,
) -> list[dict[str, Any]]:
    pooled = _fit_model("lgbm", train, horizon) if family == "lgbm" else None
    forecasts = []
    for item_id, rows in test.groupby("item_id", sort=True):
        history = train.loc[train["item_id"].eq(item_id)]
        model = pooled or _fit_model(
            family,
            history,
            horizon,
            bar_count=int(rows.iloc[0]["valid_bar_count"]),
        )
        for index in rows.index:
            row = rows.loc[[index]]
            values = row.iloc[0]
            output = _predict_row(model, row, config)
            forecasts.append(
                {
                    "item_id": int(item_id),
                    "close_ts": int(values["close_ts"]),
                    "prob_up": output.prob_up,
                    "signal": output.signal,
                    "expected_net_roi": output.expected_net_roi,
                    "pred_q10": output.pred_q10,
                    "pred_q50": output.pred_q50,
                    "pred_q90": output.pred_q90,
                    "pred_q10_ret": output.pred_q10_ret,
                    "pred_q50_ret": output.pred_q50_ret,
                    "pred_q90_ret": output.pred_q90_ret,
                    "raw_quantile_crossed": output.raw_quantile_crossed,
                    "y_reg": float(values["y_reg"]),
                    "y_cls": int(values["y_cls"]),
                    "ref_qty": _reference_quantity(config, values),
                }
            )
    return forecasts


def _maker_price(row: dict[str, Any]) -> Decimal:
    close = Decimal(str(row["close_price"]))
    p10 = row.get("p10_price")
    return close if p10 is None or pd.isna(p10) else min(close, Decimal(str(p10)))


def _simulate_portfolio(
    config: AppConfig,
    panel: pd.DataFrame,
    forecasts: list[dict[str, Any]],
    horizon: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep cash and fee calculations in Decimal; never pre-book future proceeds."""
    if not forecasts:
        return (
            {"net_return_pct": 0.0, "win_rate": 0.0, "trade_count": 0, "max_drawdown_pct": 0.0},
            [],
            [],
        )
    decisions: dict[int, list[dict[str, Any]]] = {}
    for forecast in forecasts:
        decisions.setdefault(forecast["close_ts"], []).append(forecast)
    start = min(decisions)
    end = max(decisions) + horizon * 3600
    bars: dict[int, dict[int, dict[str, Any]]] = {}
    available = panel.loc[panel["close_ts"].between(start, end)]
    for row in available.to_dict("records"):
        if row["observed_bar"] and math.isfinite(float(row["close_price"])):
            bars.setdefault(int(row["close_ts"]), {})[int(row["item_id"])] = row
    # All scheduled closes are represented even when other items have missing bars.
    timeline = sorted(
        set(int(ts) for ts in available["close_ts"])
        | set(decisions)
        | {ts + horizon * 3600 for ts in decisions}
    )
    cash = INITIAL_CAPITAL
    positions: dict[int, dict[str, Any]] = {}
    marks: dict[int, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    curve = [
        {
            "close_ts": start - GRID_SECONDS,
            "equity": float(cash),
            "cash": float(cash),
            "positions": 0,
        }
    ]
    peak = INITIAL_CAPITAL
    max_drawdown = Decimal("0")

    def equity() -> Decimal:
        value = cash
        for item, position in positions.items():
            mark = marks[item]
            value += position["cost"] + net_profit(
                position["buy_price"],
                _maker_price(mark),
                position["qty"],
                int(mark["max_stack_size"]),
                **_fee_kwargs(config, mark),
            )
        return value

    for timestamp in timeline:
        current = bars.get(timestamp, {})
        marks.update(current)
        for item, position in list(positions.items()):
            if position["exit_ts"] != timestamp:
                continue
            if item not in current:
                raise ValueError("an open position has no observed bar at its exact t+H close")
            row = current[item]
            sale = _maker_price(row)
            profit = net_profit(
                position["buy_price"],
                sale,
                position["qty"],
                int(row["max_stack_size"]),
                **_fee_kwargs(config, row),
            )
            cash += position["cost"] + profit
            gross = (sale - position["buy_price"]) * position["qty"]
            trades.append(
                {
                    "item_id": item,
                    "entry_ts": position["entry_ts"],
                    "exit_ts": timestamp,
                    "entry_price": float(position["buy_price"]),
                    "exit_price": float(sale),
                    "qty": position["qty"],
                    "ref_qty": position["ref_qty"],
                    "quantity_cap": position["quantity_cap"],
                    "entry_equity": float(position["entry_equity"]),
                    "cost": float(position["cost"]),
                    "fees": float(gross - profit),
                    "net_profit": float(profit),
                    "net_roi": float(profit / position["cost"]),
                }
            )
            del positions[item]
        ordered = sorted(
            decisions.get(timestamp, []),
            key=lambda value: (-value["expected_net_roi"], value["item_id"]),
        )
        for forecast in ordered:
            item = forecast["item_id"]
            if forecast["signal"] != "BUY_WATCH" or item in positions or item not in current:
                continue
            row = current[item]
            sold = row.get("est_sold_qty_24h", np.nan)
            listed = row.get("total_listed_qty", np.nan)
            liquid = pd.notna(sold) and math.isfinite(float(sold)) and float(sold) >= 1
            if row["data_source"] == "snapshot" and (
                pd.isna(listed) or float(listed) < 3 or not liquid
            ):
                continue
            quantity_cap = min(
                int(forecast["ref_qty"]),
                max(1, math.floor(0.20 * float(sold))) if liquid else 1,
            )
            buy = Decimal(str(row["close_price"]))
            worth = equity()
            budget = min(cash, worth * Decimal("0.10"))
            qty = min(quantity_cap, max(0, int(budget // buy)))
            if qty <= 0:
                continue
            # A smaller actual fill can cost more per unit than the reference fill.
            expected_sale = tukey_expected_price(
                forecast["pred_q10"],
                forecast["pred_q50"],
                forecast["pred_q90"],
            )
            expected_profit = net_profit(
                buy,
                expected_sale,
                qty,
                int(row["max_stack_size"]),
                **_fee_kwargs(config, row),
            )
            if expected_profit / (buy * qty) < Decimal("0.02"):
                continue
            cost = buy * qty
            cash -= cost
            positions[item] = {
                "entry_ts": timestamp,
                "exit_ts": timestamp + horizon * 3600,
                "buy_price": buy,
                "qty": qty,
                "cost": cost,
                "ref_qty": int(forecast["ref_qty"]),
                "quantity_cap": quantity_cap,
                "entry_equity": worth,
            }
        worth = equity()
        peak = max(peak, worth)
        max_drawdown = max(max_drawdown, (peak - worth) / peak)
        curve.append(
            {
                "close_ts": timestamp,
                "equity": float(worth),
                "cash": float(cash),
                "positions": len(positions),
            }
        )
    if positions:
        raise RuntimeError("the backtest ended with positions that were not closed at t+H")
    metrics = {
        "net_return_pct": float((cash / INITIAL_CAPITAL - 1) * 100),
        "win_rate": (
            sum(trade["net_profit"] > 0 for trade in trades) / len(trades) if trades else 0.0
        ),
        "trade_count": len(trades),
        "max_drawdown_pct": float(max_drawdown * 100),
    }
    return metrics, curve, trades


def _prediction_metrics(forecasts: list[dict[str, Any]]) -> dict[str, Any]:
    names = (
        "roc_auc",
        "brier_score",
        "pinball_q10",
        "pinball_q50",
        "pinball_q90",
        "pinball_mean",
        "coverage_80",
        "raw_quantile_crossing_rate",
    )
    if not forecasts:
        return dict.fromkeys(names)
    frame = pd.DataFrame(forecasts)
    labels = frame["y_cls"].to_numpy()
    targets = frame["y_reg"].to_numpy()
    metrics = {
        "roc_auc": (
            float(roc_auc_score(labels, frame["prob_up"])) if np.unique(labels).size > 1 else None
        ),
        "brier_score": float(brier_score_loss(labels, frame["prob_up"])),
    }
    for suffix, alpha in (("q10", 0.1), ("q50", 0.5), ("q90", 0.9)):
        metrics[f"pinball_{suffix}"] = float(
            mean_pinball_loss(
                targets,
                frame[f"pred_{suffix}_ret"],
                alpha=alpha,
            )
        )
    metrics["pinball_mean"] = (
        sum(metrics[f"pinball_{suffix}"] for suffix in ("q10", "q50", "q90")) / 3
    )
    metrics["coverage_80"] = float(
        np.mean(
            (targets >= frame["pred_q10_ret"]) & (targets <= frame["pred_q90_ret"]),
        )
    )
    metrics["raw_quantile_crossing_rate"] = float(frame["raw_quantile_crossed"].mean())
    return metrics


def run_backtest(
    config: AppConfig,
    horizon_hours: int = 72,
    models: Sequence[str] = ("naive", "statistical", "lgbm"),
    item_id: int | None = None,
    category: str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate the same purged folds, simulate each account, and persist one row/model.

    Pinball losses are measured in log-return space, rates in [0, 1], and
    financial return/drawdown in percent. Undefined one-class AUC is JSON null.
    """
    _validate_horizons((horizon_hours,))
    models = tuple(dict.fromkeys(models))
    if not models or any(model not in MODEL_NAMES for model in models):
        raise ValueError("models must contain naive, statistical or lgbm")
    if item_id is not None and (type(item_id) is not int or item_id <= 0):
        raise ValueError("item_id must be a positive integer")
    path = _database_path(config, db_path)
    panel = _load_feature_panel(config, path)
    # Build cross-item features before restricting the requested evaluation scope.
    if not panel.empty and item_id is not None:
        panel = panel.loc[panel["item_id"].eq(item_id)]
    if not panel.empty and category is not None:
        panel = panel.loc[panel["category"].eq(category)]
    folds = []
    if not panel.empty:
        targets = _target_panel(panel, config, horizon_hours)
        targets = targets.loc[targets["y_reg"].notna() & targets["y_cls"].notna()]
        folds = list(purged_walk_forward_split(targets, horizon_hours))
    summaries = [_fold_summary(index, train, test) for index, (train, test) in enumerate(folds)]
    forecasts: dict[str, list[dict[str, Any]]] = {name: [] for name in models}
    for train, test in folds:
        for name in models:
            forecasts[name].extend(_forecast_fold(config, name, train, test, horizon_hours))
    curves = {}
    trades = {}
    comparison = []
    for name in models:
        financial, curves[name], trades[name] = _simulate_portfolio(
            config,
            panel,
            forecasts[name],
            horizon_hours,
        )
        comparison.append({"model_name": name, **_prediction_metrics(forecasts[name]), **financial})
    scope = f"item:{item_id}" if item_id is not None else "all"
    if category is not None:
        scope += f";category:{category}"
    settings = {
        "horizon_hours": horizon_hours,
        "models": list(models),
        "item_id": item_id,
        "category": category,
        "initial_capital": int(INITIAL_CAPITAL),
        "max_position_fraction": 0.10,
        "max_sold_fraction": 0.20,
        "ref_notional_yen": config.settings.ref_notional_yen,
        "fees": config.fees.model_dump(mode="json"),
        "folds": summaries,
        "purge_seconds": horizon_hours * 3600,
        "pinball_units": "log_return",
    }
    created_at = int(time.time())
    run_ids = {}
    with open_connection(path) as conn, write_transaction(conn):
        for result in comparison:
            name = result["model_name"]
            metrics = {key: value for key, value in result.items() if key != "model_name"}
            cursor = conn.execute(
                "INSERT INTO backtest_runs (created_at, item_scope, horizon_hours, model_name, "
                "config_json, metrics_json, equity_curve_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    created_at,
                    scope,
                    horizon_hours,
                    name,
                    json.dumps(settings, ensure_ascii=False, allow_nan=False),
                    json.dumps(metrics, allow_nan=False),
                    json.dumps(curves[name], allow_nan=False),
                ),
            )
            result["run_id"] = int(cursor.lastrowid)
            run_ids[name] = result["run_id"]
    return {
        "run_id": run_ids[models[0]],
        "run_ids": run_ids,
        "item_scope": scope,
        "horizon_hours": horizon_hours,
        "comparison_table": comparison,
        "equity_curves": curves,
        "trades": trades,
        "folds": summaries,
    }


__all__ = ["run_backtest", "purged_walk_forward_split"]
