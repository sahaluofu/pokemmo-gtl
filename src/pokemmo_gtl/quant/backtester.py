"""Frozen-calendar, three-cutoff forecasts and cash-conserving paper portfolios."""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, mean_pinball_loss, roc_auc_score

from pokemmo_gtl.config import AppConfig
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.quant.features import GRID_SECONDS
from pokemmo_gtl.quant.fees import listing_fee, net_profit
from pokemmo_gtl.quant.models.base import tukey_expected_price
from pokemmo_gtl.quant.predictor import (
    _columns,
    _database_path,
    _fee_kwargs,
    _fit_model,
    _load_feature_panel,
    _predict_row,
    _reference_quantity,
    _target_panel,
    _v2_fees_verified,
    _validate_horizons,
)

DEFAULT_FOLD_STEP_SEC = 7 * 86400
INITIAL_CAPITAL = Decimal("10000000")
MODEL_NAMES = ("naive", "statistical", "lgbm")
_LEGACY_METRICS = (
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
)


def _has_delayed_publications(db_path: Path) -> bool:
    """Detect bars whose publication requires rebuilding features at each cutoff."""
    with open_connection(db_path) as conn:
        for table, column in (
            ("item_bar_revisions", "revision_available_at"),
            ("item_bars", "bar_available_at"),
        ):
            columns = _columns(conn, table)
            if column not in columns or "close_ts" not in columns:
                continue
            row = conn.execute(
                f"SELECT 1 FROM {table} WHERE {column} > close_ts LIMIT 1"
            ).fetchone()
            if row is not None:
                return True
    return False


def _has_v2_publications(db_path: Path) -> bool:
    """Recognize populated v2 databases while allowing untouched 001 schemas."""
    with open_connection(db_path) as conn:
        for table in ("publish_events", "item_bar_revisions"):
            if not _columns(conn, table):
                continue
            if conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None:
                return True
        if "bar_available_at" not in _columns(conn, "item_bars"):
            return False
        return conn.execute(
            "SELECT 1 FROM item_bars WHERE bar_available_at IS NOT NULL LIMIT 1"
        ).fetchone() is not None


def freeze_walk_forward_calendar(
    eval_start_ts: int,
    eval_end_ts: int,
    horizon_hours: int,
    *,
    fold_step_sec: int | None = None,
    n_splits: int = 4,
) -> list[dict[str, int]]:
    """Freeze inclusive UTC 4h windows using only explicit calendar inputs.

    A split occurs at its first scheduled decision. Training-label maturity
    supplies the H-second purge; subtracting H here would purge it twice.
    Persist and reuse this result when comparing differently populated data.
    """
    if type(horizon_hours) is not int or horizon_hours <= 0:
        raise ValueError("horizon_hours must be a positive integer")
    if type(n_splits) is not int or n_splits <= 0:
        raise ValueError("n_splits must be a positive integer")
    if fold_step_sec is not None and (
        type(fold_step_sec) is not int
        or fold_step_sec <= 0
        or fold_step_sec % GRID_SECONDS
    ):
        raise ValueError("fold_step_sec must be a positive multiple of the 4h grid")
    start = -(-int(eval_start_ts) // GRID_SECONDS) * GRID_SECONDS
    end = int(eval_end_ts) // GRID_SECONDS * GRID_SECONDS
    if end < start:
        return []
    count = (end - start) // GRID_SECONDS + 1
    sizes: list[int] = []
    if fold_step_sec is not None:
        step = fold_step_sec // GRID_SECONDS
        remaining = count
        while remaining:
            size = min(step, remaining)
            sizes.append(size)
            remaining -= size
    else:
        folds = min(n_splits, count)
        quotient, remainder = divmod(count, folds)
        sizes = [quotient + int(index < remainder) for index in range(folds)]
    result = []
    offset = 0
    for index, size in enumerate(sizes):
        first = start + offset * GRID_SECONDS
        result.append(
            {
                "fold": index,
                "split_ts": first,
                "test_start_ts": first,
                "test_end_ts": first + (size - 1) * GRID_SECONDS,
            }
        )
        offset += size
    return result


def _normalise_calendar(
    calendar_folds: Sequence[Mapping[str, int]],
) -> list[dict[str, int]]:
    result = []
    for index, spec in enumerate(calendar_folds):
        required = ("split_ts", "test_start_ts", "test_end_ts")
        if any(key not in spec for key in required):
            raise ValueError("each calendar fold requires split_ts and test start/end timestamps")
        if any(type(spec[key]) is not int for key in required):
            raise ValueError("calendar timestamps must be integers")
        fold = spec.get("fold", index)
        if type(fold) is not int:
            raise ValueError("fold must be an integer")
        entry = {"fold": fold, **{key: int(spec[key]) for key in required}}
        if entry["split_ts"] > entry["test_start_ts"]:
            raise ValueError("split_ts must not follow test_start_ts")
        if entry["test_end_ts"] < entry["test_start_ts"]:
            raise ValueError("test_end_ts must not precede test_start_ts")
        result.append(entry)
    result.sort(key=lambda spec: spec["test_start_ts"])
    if len({spec["fold"] for spec in result}) != len(result):
        raise ValueError("calendar fold identifiers must be unique")
    for previous, current in zip(result, result[1:]):
        if previous["test_end_ts"] >= current["test_start_ts"]:
            raise ValueError("calendar test windows must not overlap")
    return result


def _scheduled_times(spec: Mapping[str, int]) -> range:
    start = -(-spec["test_start_ts"] // GRID_SECONDS) * GRID_SECONDS
    return range(start, spec["test_end_ts"] + 1, GRID_SECONDS)


def _mature_training(frame: pd.DataFrame, split_ts: int, horizon: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    mask = (frame["close_ts"] + horizon * 3600).le(split_ts)
    for column in ("y_reg", "y_cls"):
        if column in frame:
            mask &= pd.to_numeric(frame[column], errors="coerce").notna()
    return frame.loc[mask].copy()


def purged_walk_forward_split(
    frame: pd.DataFrame,
    horizon_hours: int = 72,
    *,
    n_splits: int = 4,
    min_train_bars: int = 180,
    calendar_folds: Sequence[Mapping[str, int]] | None = None,
    eval_start_ts: int | None = None,
    eval_end_ts: int | None = None,
    fold_step_sec: int | None = None,
) -> Iterator[tuple[pd.DataFrame, pd.DataFrame]]:
    """Slice frozen global windows without filtering test rows by their labels.

    The legacy convenience call derives endpoints from its input. Invariance
    experiments must pass a previously frozen calendar or explicit endpoints.
    """
    _validate_horizons((horizon_hours,))
    if type(n_splits) is not int or n_splits <= 0:
        raise ValueError("n_splits must be a positive integer")
    if type(min_train_bars) is not int or min_train_bars < 3:
        raise ValueError("min_train_bars must be at least three")
    if frame.empty:
        return
    if calendar_folds is None:
        start = (
            int(frame["close_ts"].min())
            + (min_train_bars - 1) * GRID_SECONDS
            + horizon_hours * 3600
            if eval_start_ts is None
            else int(eval_start_ts)
        )
        end = int(frame["close_ts"].max()) if eval_end_ts is None else int(eval_end_ts)
        specs = freeze_walk_forward_calendar(
            start,
            end,
            horizon_hours,
            fold_step_sec=fold_step_sec,
            n_splits=n_splits,
        )
    else:
        specs = _normalise_calendar(calendar_folds)
    for spec in specs:
        train = _mature_training(frame, spec["split_ts"], horizon_hours)
        test = frame.loc[
            frame["close_ts"].between(spec["test_start_ts"], spec["test_end_ts"])
        ].copy()
        if train.empty or test.empty:
            continue
        yield train, test


def _fold_summary(
    index: int,
    train: pd.DataFrame,
    test: pd.DataFrame,
    spec: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    return {
        "fold": index if spec is None else spec["fold"],
        "train_start_ts": None if train.empty else int(train["close_ts"].min()),
        "train_end_ts": None if train.empty else int(train["close_ts"].max()),
        "test_start_ts": (
            int(test["close_ts"].min()) if spec is None else spec["test_start_ts"]
        ),
        "test_end_ts": int(test["close_ts"].max()) if spec is None else spec["test_end_ts"],
        "train_rows": len(train),
        "test_rows": len(test),
        "train_item_ids": sorted(int(value) for value in train["item_id"].unique()),
        "test_item_ids": sorted(int(value) for value in test["item_id"].unique()),
    }


def _finite(value: Any) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def _stocked(row: Mapping[str, Any] | pd.Series) -> bool:
    return (
        row.get("book_state", "stocked") == "stocked"
        and _finite(row.get("close_price"))
        and float(row["close_price"]) > 0
    )


def _observed_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return frame.loc[frame["observed_bar"].eq(True)].copy()


def _forecast_fold(
    config: AppConfig,
    family: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    horizon: int,
    *,
    model_cache: dict[Any, Any] | None = None,
    enforce_v2_gates: bool = False,
) -> list[dict[str, Any]]:
    """Generate decisions with deliberately absent realized targets."""
    if train.empty or test.empty:
        return []
    cache = {} if model_cache is None else model_cache
    if family == "lgbm" and "pooled" not in cache:
        cache["pooled"] = _fit_model("lgbm", train, horizon)
    forecasts = []
    for item_id, rows in test.groupby("item_id", sort=True):
        rows = rows.sort_values("close_ts", kind="stable")
        if family != "lgbm" and int(item_id) not in cache:
            history = train.loc[train["item_id"].eq(item_id)]
            cache[int(item_id)] = _fit_model(
                family,
                history,
                horizon,
                bar_count=int(rows.iloc[0]["valid_bar_count"]),
            )
        model = cache["pooled"] if family == "lgbm" else cache[int(item_id)]
        for index in rows.index:
            row = rows.loc[[index]]
            values = row.iloc[0]
            if not bool(values["observed_bar"]) or not _stocked(values):
                continue
            output = _predict_row(model, row, config)
            signal = output.signal
            if enforce_v2_gates and not _v2_fees_verified(config, values):
                if signal == "BUY_WATCH":
                    signal = "WATCH"
            upstream = values.get("upstream_ret_24h")
            forecasts.append(
                {
                    "item_id": int(item_id),
                    "close_ts": int(values["close_ts"]),
                    "decision_ts": int(values["close_ts"]),
                    "horizon_hours": horizon,
                    "current_price": float(values["close_price"]),
                    "entry_price": float(values["close_price"]),
                    "asset_class": str(values.get("asset_class", "item")),
                    "max_stack_size": int(values["max_stack_size"]),
                    "prob_up": float(output.prob_up),
                    "signal": signal,
                    "expected_net_roi": float(output.expected_net_roi),
                    "pred_q10": float(output.pred_q10),
                    "pred_q50": float(output.pred_q50),
                    "pred_q90": float(output.pred_q90),
                    "pred_q10_ret": float(output.pred_q10_ret),
                    "pred_q50_ret": float(output.pred_q50_ret),
                    "pred_q90_ret": float(output.pred_q90_ret),
                    "raw_quantile_crossed": bool(output.raw_quantile_crossed),
                    "upstream_ret_24h": float(upstream) if _finite(upstream) else None,
                    "y_reg": None,
                    "y_cls": None,
                    "ref_qty": _reference_quantity(config, values),
                }
            )
    return forecasts


def _maker_price(row: Mapping[str, Any]) -> Decimal:
    close = Decimal(str(row["close_price"]))
    p10 = row.get("p10_price")
    return close if not _finite(p10) else min(close, Decimal(str(p10)))


def _discrete_fee(
    config: AppConfig,
    row: Mapping[str, Any],
    price: Decimal,
    qty: int,
) -> Decimal:
    options = _fee_kwargs(config, dict(row))
    options.pop("relist_rate")
    return Decimal(
        listing_fee(
            int(price),
            qty,
            int(row["max_stack_size"]),
            fee_basis=getattr(config.fees, "fee_basis", "per_slot"),
            rounding_mode=getattr(config.fees, "rounding_mode", "floor"),
            **options,
        )
    )


def _consume_shared_flow(
    shared_flow_budget: dict[tuple[int, int], int],
    item_id: int,
    window: int,
    available_qty: float,
    requested_qty: int,
) -> int:
    """Consume a window's inferred quantity once across every paper order."""
    key = (int(item_id), int(window))
    if key not in shared_flow_budget:
        shared_flow_budget[key] = (
            max(0, math.floor(float(available_qty))) if _finite(available_qty) else 0
        )
    filled = min(max(0, int(requested_qty)), shared_flow_budget[key])
    shared_flow_budget[key] -= filled
    return filled


def _parse_ladder(value: str) -> list[dict[str, int]]:
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return []
    if isinstance(decoded, dict):
        decoded = decoded.get("lots", decoded.get("listings", decoded.get("ladder", [])))
    if not isinstance(decoded, list):
        return []
    lots = []
    for index, value in enumerate(decoded):
        if not isinstance(value, dict):
            continue
        price = value.get("price", value.get("unit_price"))
        qty = value.get("qty", value.get("quantity"))
        if not _finite(price) or not _finite(qty) or float(price) <= 0 or float(qty) <= 0:
            continue
        lots.append(
            {
                "auction_id": int(value.get("auction_id", index)),
                "price": math.ceil(float(price)),
                "qty": math.floor(float(qty)),
                "lot_index": index,
            }
        )
    return sorted(lots, key=lambda lot: (lot["price"], lot["auction_id"], lot["lot_index"]))


def _execution_ladder(
    db_path: Path | None,
    item_id: int,
    execution_ts: int,
    max_publish_seq: int | None,
) -> dict[str, Any] | None:
    if db_path is None:
        return None
    with open_connection(db_path) as conn:
        observation_columns = _columns(conn, "orderbook_observations")
        if not observation_columns or not _columns(conn, "snapshot_runs"):
            return None
        predicates = [
            "o.item_id = ?",
            "o.available_at <= ?",
            "s.server_started_at <= ?",
            "s.status = 'completed'",
        ]
        parameters: list[Any] = [item_id, execution_ts, execution_ts]
        if max_publish_seq is not None and "publish_seq" in observation_columns:
            predicates.append("o.publish_seq <= ?")
            parameters.append(max_publish_seq)
        row = conn.execute(
            f"""
            SELECT o.snapshot_id, o.available_at, o.book_state,
                   o.purchase_mode, o.ladder_json
            FROM orderbook_observations AS o
            JOIN snapshot_runs AS s ON s.snapshot_id = o.snapshot_id
            WHERE {" AND ".join(predicates)}
            ORDER BY s.server_started_at DESC, o.snapshot_id DESC
            LIMIT 1
            """,
            parameters,
        ).fetchone()
    if row is None:
        return None
    return {**dict(row), "lots": _parse_ladder(row["ladder_json"])}


def _walk_ladder(
    lots: Sequence[Mapping[str, int]],
    requested_qty: int,
    budget: Decimal,
    purchase_mode: str,
) -> tuple[int, Decimal, list[dict[str, int]]]:
    """Find an affordable fill, retaining each indivisible auction lot."""
    limit = max(0, int(requested_qty))
    if not limit:
        return 0, Decimal(0), []
    if purchase_mode == "bundle_only":
        states: dict[int, tuple[int, list[dict[str, int]]]] = {0: (0, [])}
        for lot in lots:
            qty = int(lot["qty"])
            cost = int(lot["price"]) * qty
            for previous_qty, (previous_cost, previous_fills) in list(states.items()):
                total_qty = previous_qty + qty
                total_cost = previous_cost + cost
                if total_qty > limit or Decimal(total_cost) > budget:
                    continue
                existing = states.get(total_qty)
                if existing is None or total_cost < existing[0]:
                    states[total_qty] = (
                        total_cost,
                        [
                            *previous_fills,
                            {
                                "auction_id": int(lot["auction_id"]),
                                "lot_index": int(lot["lot_index"]),
                                "price": int(lot["price"]),
                                "qty": qty,
                            },
                        ],
                    )
        quantity = max(states)
        cost, fills = states[quantity]
        return quantity, Decimal(cost), fills
    quantity = 0
    cost = Decimal(0)
    fills = []
    for lot in lots:
        price = int(lot["price"])
        take = min(
            limit - quantity,
            int(lot["qty"]),
            max(0, int((budget - cost) // Decimal(price))),
        )
        if take:
            fills.append(
                {
                    "auction_id": int(lot["auction_id"]),
                    "lot_index": int(lot["lot_index"]),
                    "price": price,
                    "qty": take,
                }
            )
            quantity += take
            cost += Decimal(price * take)
        if quantity == limit:
            break
    return quantity, cost, fills


def _simulate_portfolio(
    config: AppConfig,
    panel: pd.DataFrame,
    forecasts: list[dict[str, Any]],
    horizon: int,
    *,
    strict_point_in_time: bool = False,
    eval_as_of_ts: int | None = None,
    max_publish_seq: int | None = None,
    manual_delay_sec: int = 0,
    db_path: Path | None = None,
    point_in_time_loader: Callable[[int], pd.DataFrame] | None = None,
    decision_records: list[dict[str, Any]] | None = None,
    decision_calendar: Sequence[int] | None = None,
    decision_metadata: Mapping[int, Mapping[str, Any]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep unsold inventory out of cash and share execution flow across orders.

    Legacy mode retains its single-position-per-item and expected-fee behavior.
    Strict paper execution permits independent orders, charges discrete sale
    fees, and allocates each item's observed 4h flow only once.
    """
    if type(manual_delay_sec) is not int or manual_delay_sec < 0:
        raise ValueError("manual_delay_sec must be a non-negative integer")
    decisions: dict[int, list[dict[str, Any]]] = {
        int(timestamp): [] for timestamp in (decision_calendar or ())
    }
    for forecast in forecasts:
        decisions.setdefault(int(forecast["close_ts"]), []).append(forecast)
    zero_band = {"conservative": 0.0, "neutral": 0.0, "cost_basis": 0.0}
    if not decisions:
        return (
            {
                "net_return_pct": 0.0,
                "win_rate": 0.0,
                "trade_count": 0,
                "max_drawdown_pct": 0.0,
                "available_cash": float(INITIAL_CAPITAL),
                "inventory_cost": 0.0,
                "open_positions": [],
                "residual_valuation_band": zero_band,
                "pending_orders": [],
            },
            [],
            [],
        )
    start = min(decisions)
    end = max(decisions) + horizon * 3600
    if eval_as_of_ts is not None:
        end = min(end, max(start, int(eval_as_of_ts)))
    timeline = set(range(start, end + 1, GRID_SECONDS))
    timeline.add(end)
    timeline.update(timestamp for timestamp in decisions if timestamp <= end)
    timeline.update(
        timestamp + manual_delay_sec
        for timestamp in decisions
        if timestamp + manual_delay_sec <= end
    )
    timeline.update(
        timestamp + horizon * 3600
        for timestamp in decisions
        if timestamp + horizon * 3600 <= end
    )
    bars: dict[int, dict[int, dict[str, Any]]] = {}
    if not panel.empty:
        for row in _observed_rows(panel).to_dict("records"):
            timestamp = int(row["close_ts"])
            if start <= timestamp <= end:
                bars.setdefault(timestamp, {})[int(row["item_id"])] = row
    cash = INITIAL_CAPITAL
    reserved = Decimal(0)
    positions: dict[int, dict[str, Any]] = {}
    pending: dict[int, list[dict[str, Any]]] = {}
    marks: dict[int, dict[str, Any]] = {}
    shared_flow_budget: dict[tuple[int, int], int] = {}
    retained_books: dict[tuple[int, int], list[dict[str, int]]] = {}
    trades: list[dict[str, Any]] = []
    curve = [
        {
            "close_ts": start - GRID_SECONDS,
            "equity": float(cash),
            "cash": float(cash),
            "positions": 0,
            "inventory_cost": 0.0,
            "reserved_cash": 0.0,
        }
    ]
    peak = INITIAL_CAPITAL
    max_drawdown = Decimal(0)
    next_position_id = 1

    def position_values(position: dict[str, Any]) -> tuple[Decimal, Decimal]:
        row = marks.get(position["item_id"])
        if row is None or not _stocked(row):
            return Decimal(0), Decimal(0)
        sale = _maker_price(row)
        qty = position["qty"]
        if strict_point_in_time:
            sale = Decimal(int(sale))
            neutral = max(Decimal(0), sale * qty - _discrete_fee(config, row, sale, qty))
            conservative = Decimal(0) if position["exit_attempted"] else neutral
            return conservative, neutral
        value = position["cost"] + net_profit(
            position["buy_price"],
            sale,
            qty,
            int(row["max_stack_size"]),
            **_fee_kwargs(config, row),
        )
        return max(Decimal(0), value), max(Decimal(0), value)

    def equity() -> Decimal:
        return cash + sum(
            (position_values(position)[0] for position in positions.values()),
            Decimal(0),
        )

    def budget_state() -> dict[str, Any]:
        worth = equity()
        return {
            "cash": float(cash),
            "available_cash": float(cash - reserved),
            "reserved_cash": float(reserved),
            "inventory_cost": float(
                sum((position["cost"] for position in positions.values()), Decimal(0))
            ),
            "open_position_count": len(positions),
            "equity": float(worth),
            "max_position_budget": float(
                max(Decimal(0), min(cash - reserved, worth * Decimal("0.10")))
            ),
        }

    def execute(order: dict[str, Any], timestamp: int) -> None:
        nonlocal cash, reserved, next_position_id
        reserved -= order["reserved_budget"]
        item = order["item_id"]
        if not strict_point_in_time and any(
            position["item_id"] == item for position in positions.values()
        ):
            return
        row = marks.get(item, order["row"])
        if not _stocked(row):
            return
        if strict_point_in_time and not _v2_fees_verified(config, row):
            return
        budget = min(order["reserved_budget"], cash - reserved)
        if budget <= 0:
            return
        observation = _execution_ladder(db_path, item, timestamp, max_publish_seq)
        mode = "coarse_bar_approximation"
        fills: list[dict[str, int]] = []
        book: list[dict[str, int]] | None = None
        if observation is not None:
            if observation["book_state"] != "stocked":
                return
            mode = "lot_ladder"
            key = (item, int(observation["snapshot_id"]))
            book = retained_books.setdefault(
                key, [dict(lot) for lot in observation["lots"]]
            )
            qty, cost, fills = _walk_ladder(
                book,
                order["requested_qty"],
                budget,
                str(observation["purchase_mode"]),
            )
        else:
            buy = Decimal(str(row["close_price"]))
            if strict_point_in_time:
                buy = Decimal(math.ceil(buy))
            qty = min(order["requested_qty"], max(0, int(budget // buy)))
            cost = buy * qty
        if qty <= 0 or cost <= 0:
            return
        buy = cost / qty
        forecast = order["forecast"]
        expected_sale = tukey_expected_price(
            forecast["pred_q10"],
            forecast["pred_q50"],
            forecast["pred_q90"],
        )
        if strict_point_in_time:
            expected_sale_decimal = Decimal(int(Decimal(str(expected_sale))))
            expected_profit = (
                expected_sale_decimal * qty
                - cost
                - _discrete_fee(config, row, expected_sale_decimal, qty)
            )
        else:
            expected_profit = net_profit(
                buy,
                expected_sale,
                qty,
                int(row["max_stack_size"]),
                **_fee_kwargs(config, row),
            )
        if expected_profit / cost < Decimal("0.02"):
            return
        if book is not None:
            used = {fill["lot_index"]: fill["qty"] for fill in fills}
            for lot in book:
                lot["qty"] -= used.get(lot["lot_index"], 0)
        cash -= cost
        positions[next_position_id] = {
            "position_id": next_position_id,
            "item_id": item,
            "entry_ts": order["decision_ts"],
            "executed_at": timestamp,
            "exit_ts": order["decision_ts"] + horizon * 3600,
            "buy_price": buy,
            "qty": qty,
            "initial_qty": qty,
            "sold_qty": 0,
            "cost": cost,
            "total_buy_cost": cost,
            "ref_qty": int(forecast["ref_qty"]),
            "quantity_cap": order["quantity_cap"],
            "entry_equity": order["entry_equity"],
            "execution_mode": mode,
            "fills_detail": [
                {key: value for key, value in fill.items() if key != "lot_index"}
                for fill in fills
            ],
            "exit_attempted": False,
            "residual_reason": "not_yet_due",
        }
        next_position_id += 1

    for timestamp in sorted(timeline):
        current = bars.get(timestamp, {})
        if point_in_time_loader is not None:
            visible = _observed_rows(point_in_time_loader(timestamp))
            if visible.empty:
                current = {}
            else:
                visible = visible.loc[visible["close_ts"].le(timestamp)]
                latest = visible.sort_values(
                    ["close_ts", "item_id"], kind="stable"
                ).groupby("item_id", sort=True).tail(1)
                marks.update(
                    {int(row["item_id"]): row for row in latest.to_dict("records")}
                )
                current = {
                    int(row["item_id"]): row
                    for row in visible.loc[visible["close_ts"].eq(timestamp)].to_dict("records")
                }
        marks.update(current)
        for position_id, position in list(positions.items()):
            if position["exit_attempted"] or position["exit_ts"] > timestamp:
                continue
            position["exit_attempted"] = True
            row = current.get(position["item_id"])
            if row is None:
                position["residual_reason"] = "missing_exit_bar"
                continue
            if not _stocked(row):
                position["residual_reason"] = "empty_exit_book"
                continue
            sale = _maker_price(row)
            old_qty = position["qty"]
            if strict_point_in_time:
                sale = Decimal(int(sale))
                flow = row.get("est_sold_qty_sum")
                if not _finite(flow):
                    daily = row.get("est_sold_qty_24h")
                    flow = float(daily) / 6 if _finite(daily) else 0
                sold_qty = _consume_shared_flow(
                    shared_flow_budget,
                    position["item_id"],
                    timestamp,
                    float(flow),
                    old_qty,
                )
            else:
                sold_qty = old_qty
            if not sold_qty:
                position["residual_reason"] = "insufficient_shared_flow"
                continue
            allocated_cost = position["cost"]
            if sold_qty != old_qty:
                allocated_cost = (
                    position["cost"] * Decimal(sold_qty) / Decimal(old_qty)
                ).to_integral_value(rounding="ROUND_FLOOR")
            if strict_point_in_time:
                fees = _discrete_fee(config, row, sale, sold_qty)
                proceeds = sale * sold_qty - fees
                profit = proceeds - allocated_cost
            else:
                profit = net_profit(
                    position["buy_price"],
                    sale,
                    sold_qty,
                    int(row["max_stack_size"]),
                    **_fee_kwargs(config, row),
                )
                proceeds = allocated_cost + profit
                fees = sale * sold_qty - proceeds
            cash += proceeds
            remaining = old_qty - sold_qty
            trades.append(
                {
                    "position_id": position_id,
                    "item_id": position["item_id"],
                    "entry_ts": position["entry_ts"],
                    "executed_at": position["executed_at"],
                    "exit_ts": timestamp,
                    "entry_price": float(position["buy_price"]),
                    "exit_price": float(sale),
                    "qty": sold_qty,
                    "sold_qty": sold_qty,
                    "unsold_qty": remaining,
                    "initial_qty": position["initial_qty"],
                    "ref_qty": position["ref_qty"],
                    "quantity_cap": position["quantity_cap"],
                    "entry_equity": float(position["entry_equity"]),
                    "cost": float(allocated_cost),
                    "fees": float(fees),
                    "net_profit": float(profit),
                    "net_roi": float(profit / allocated_cost) if allocated_cost else 0.0,
                    "execution_mode": position["execution_mode"],
                }
            )
            if remaining:
                position["qty"] = remaining
                position["sold_qty"] += sold_qty
                position["cost"] -= allocated_cost
                position["residual_reason"] = "insufficient_shared_flow"
            else:
                del positions[position_id]
        for order in pending.pop(timestamp, []):
            execute(order, timestamp)
        if timestamp in decisions:
            before = budget_state()
            ordered = sorted(
                decisions[timestamp],
                key=lambda value: (-value["expected_net_roi"], value["item_id"]),
            )
            candidates = []
            for forecast in ordered:
                item = int(forecast["item_id"])
                row = current.get(item)
                if forecast["signal"] != "BUY_WATCH" or row is None or not _stocked(row):
                    continue
                if strict_point_in_time and not _v2_fees_verified(config, row):
                    continue
                if not strict_point_in_time and any(
                    position["item_id"] == item for position in positions.values()
                ):
                    continue
                sold = row.get("est_sold_qty_24h")
                listed = row.get("total_listed_qty")
                liquid = _finite(sold) and float(sold) >= 1
                if row.get("data_source", "snapshot") == "snapshot" and (
                    not _finite(listed) or float(listed) < 3 or not liquid
                ):
                    continue
                if strict_point_in_time and (not _finite(listed) or float(listed) <= 0):
                    continue
                quantity_cap = min(
                    int(forecast["ref_qty"]),
                    max(1, math.floor(0.20 * float(sold))) if liquid else 1,
                )
                buy = Decimal(str(row["close_price"]))
                worth = equity()
                budget = max(
                    Decimal(0), min(cash - reserved, worth * Decimal("0.10"))
                )
                qty = min(quantity_cap, max(0, int(budget // buy)))
                candidates.append(
                    {
                        "item_id": item,
                        "rank": len(candidates) + 1,
                        "expected_net_roi": float(forecast["expected_net_roi"]),
                        "entry_price": float(buy),
                        "recommended_qty": qty,
                        "quantity_cap": quantity_cap,
                        "ref_qty": int(forecast["ref_qty"]),
                        "budget": float(budget),
                    }
                )
                if qty <= 0:
                    continue
                order = {
                    "item_id": item,
                    "decision_ts": timestamp,
                    "forecast": forecast,
                    "row": row,
                    "requested_qty": qty,
                    "quantity_cap": quantity_cap,
                    "entry_equity": worth,
                    "reserved_budget": budget,
                }
                reserved += budget
                execution_ts = timestamp + manual_delay_sec
                if execution_ts == timestamp:
                    execute(order, timestamp)
                else:
                    pending.setdefault(execution_ts, []).append(order)
            if decision_records is not None:
                metadata = dict((decision_metadata or {}).get(timestamp, {}))
                decision_records.append(
                    {
                        **metadata,
                        "decision_ts": timestamp,
                        "forecasts": [
                            {**forecast, "y_reg": None, "y_cls": None}
                            for forecast in ordered
                        ],
                        "candidates": candidates,
                        "ranking": [candidate["item_id"] for candidate in candidates],
                        "budget_before": before,
                        "budget_after": budget_state(),
                    }
                )
        worth = equity()
        peak = max(peak, worth)
        max_drawdown = max(max_drawdown, (peak - worth) / peak)
        curve.append(
            {
                "close_ts": timestamp,
                "equity": float(worth),
                "cash": float(cash),
                "positions": len(positions),
                "inventory_cost": float(
                    sum((position["cost"] for position in positions.values()), Decimal(0))
                ),
                "reserved_cash": float(reserved),
            }
        )
    open_positions = []
    band = dict(zero_band)
    for position in positions.values():
        conservative, neutral = position_values(position)
        band["conservative"] += float(conservative)
        band["neutral"] += float(neutral)
        band["cost_basis"] += float(position["cost"])
        open_positions.append(
            {
                **{
                    key: value
                    for key, value in position.items()
                    if key not in {"buy_price", "cost", "total_buy_cost", "entry_equity"}
                },
                "buy_price": float(position["buy_price"]),
                "cost": float(position["cost"]),
                "remaining_cost": float(position["cost"]),
                "total_buy_cost": float(position["total_buy_cost"]),
                "entry_equity": float(position["entry_equity"]),
                "remaining_qty": position["qty"],
                "unsold_qty": position["qty"],
            }
        )
    pending_orders = [
        {
            "item_id": order["item_id"],
            "decision_ts": order["decision_ts"],
            "execution_ts": timestamp,
            "requested_qty": order["requested_qty"],
            "reserved_budget": float(order["reserved_budget"]),
        }
        for timestamp, orders in sorted(pending.items())
        for order in orders
    ]
    metrics = {
        "net_return_pct": float((equity() / INITIAL_CAPITAL - 1) * 100),
        "win_rate": (
            sum(trade["net_profit"] > 0 for trade in trades) / len(trades) if trades else 0.0
        ),
        "trade_count": len(trades),
        "max_drawdown_pct": float(max_drawdown * 100),
        "available_cash": float(cash - reserved),
        "inventory_cost": band["cost_basis"],
        "open_positions": open_positions,
        "residual_valuation_band": band,
        "pending_orders": pending_orders,
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
    metrics: dict[str, Any] = dict.fromkeys(names)
    frame = pd.DataFrame(forecasts)
    matured = pd.DataFrame()
    if not frame.empty:
        labels = pd.to_numeric(frame["y_cls"], errors="coerce")
        targets = pd.to_numeric(frame["y_reg"], errors="coerce")
        mask = np.isfinite(labels) & np.isfinite(targets)
        matured = frame.loc[mask].copy()
        crossed = pd.to_numeric(frame["raw_quantile_crossed"], errors="coerce")
        if crossed.notna().any():
            metrics["raw_quantile_crossing_rate"] = float(crossed.mean())
    metrics.update(
        {
            "total_forecast_count": len(forecasts),
            "matured_forecast_count": len(matured),
            "unmatured_or_unlabeled_count": len(forecasts) - len(matured),
        }
    )
    if matured.empty:
        return metrics
    labels = pd.to_numeric(matured["y_cls"]).to_numpy(dtype=int)
    targets = pd.to_numeric(matured["y_reg"]).to_numpy(dtype=float)
    metrics["roc_auc"] = (
        float(roc_auc_score(labels, matured["prob_up"]))
        if np.unique(labels).size > 1
        else None
    )
    metrics["brier_score"] = float(brier_score_loss(labels, matured["prob_up"]))
    for suffix, alpha in (("q10", 0.1), ("q50", 0.5), ("q90", 0.9)):
        metrics[f"pinball_{suffix}"] = float(
            mean_pinball_loss(targets, matured[f"pred_{suffix}_ret"], alpha=alpha)
        )
    metrics["pinball_mean"] = (
        sum(metrics[f"pinball_{suffix}"] for suffix in ("q10", "q50", "q90")) / 3
    )
    metrics["coverage_80"] = float(
        np.mean(
            (targets >= matured["pred_q10_ret"])
            & (targets <= matured["pred_q90_ret"])
        )
    )
    return metrics


def _attach_realized_labels(
    config: AppConfig,
    forecasts: list[dict[str, Any]],
    panel: pd.DataFrame,
    horizon: int,
    eval_as_of_ts: int,
) -> None:
    """Score the decision's recorded entry against a visible, exact exit bar."""
    exits = {
        (int(row["item_id"]), int(row["close_ts"])): row
        for row in _observed_rows(panel).to_dict("records")
    }
    for forecast in forecasts:
        forecast["y_reg"] = None
        forecast["y_cls"] = None
        exit_ts = int(forecast["close_ts"]) + horizon * 3600
        if exit_ts > eval_as_of_ts:
            continue
        row = exits.get((int(forecast["item_id"]), exit_ts))
        if row is None or not _stocked(row):
            continue
        entry = float(forecast["current_price"])
        sale = _maker_price(row)
        if entry <= 0 or sale <= 0:
            continue
        forecast["y_reg"] = math.log(float(sale) / entry)
        profit = net_profit(
            entry,
            sale,
            int(forecast["ref_qty"]),
            int(forecast["max_stack_size"]),
            **_fee_kwargs(config, forecast),
        )
        forecast["y_cls"] = int(profit > 0)


def run_backtest(
    config: AppConfig,
    horizon_hours: int = 72,
    models: Sequence[str] = ("naive", "statistical", "lgbm"),
    item_id: int | None = None,
    category: str | None = None,
    db_path: str | Path | None = None,
    *,
    calendar_folds: Sequence[Mapping[str, int]] | None = None,
    eval_as_of_ts: int | None = None,
    max_publish_seq: int | None = None,
    manual_delay_sec: int = 0,
    strict_point_in_time: bool = False,
    eval_start_ts: int | None = None,
    eval_end_ts: int | None = None,
    fold_step_sec: int | None = None,
    n_splits: int = 4,
    min_train_bars: int = 180,
) -> dict[str, Any]:
    """Separate training maturity, causal decisions, and post-hoc scoring.

    Published markets use fixed forward windows and retain decisions without
    mature labels. Pure unpublished v1 databases preserve their legacy calendar
    and persisted metric schema. All training and inference use causal inputs.
    """
    _validate_horizons((horizon_hours,))
    models = tuple(dict.fromkeys(models))
    if not models or any(model not in MODEL_NAMES for model in models):
        raise ValueError("models must contain naive, statistical or lgbm")
    if item_id is not None and (type(item_id) is not int or item_id <= 0):
        raise ValueError("item_id must be a positive integer")
    if type(manual_delay_sec) is not int or manual_delay_sec < 0:
        raise ValueError("manual_delay_sec must be a non-negative integer")
    if type(n_splits) is not int or n_splits <= 0:
        raise ValueError("n_splits must be a positive integer")
    if type(min_train_bars) is not int or min_train_bars < 3:
        raise ValueError("min_train_bars must be at least three")
    evaluation_cutoff = int(time.time()) if eval_as_of_ts is None else int(eval_as_of_ts)
    path = _database_path(config, db_path)
    v2_calendar_mode = (
        _has_v2_publications(path)
        or strict_point_in_time
        or calendar_folds is not None
        or eval_as_of_ts is not None
        or max_publish_seq is not None
        or eval_start_ts is not None
        or eval_end_ts is not None
        or fold_step_sec is not None
        or manual_delay_sec != 0
    )
    reload_point_in_time = (
        _has_delayed_publications(path)
        or strict_point_in_time
        or max_publish_seq is not None
    )

    def scope(frame: pd.DataFrame) -> pd.DataFrame:
        if not frame.empty and item_id is not None:
            frame = frame.loc[frame["item_id"].eq(item_id)]
        if not frame.empty and category is not None:
            frame = frame.loc[frame["category"].eq(category)]
        return frame.copy()

    def load(timestamp: int) -> pd.DataFrame:
        return scope(
            _load_feature_panel(
                config,
                path,
                as_of_ts=timestamp,
                max_publish_seq=max_publish_seq,
                require_verified_watermark=strict_point_in_time,
            )
        )

    panel = load(evaluation_cutoff)
    decision_end_ts: int | None = None
    if calendar_folds is not None:
        specs = _normalise_calendar(calendar_folds)
    elif panel.empty:
        specs = []
    else:
        start = (
            int(panel["close_ts"].min())
            + (min_train_bars - 1) * GRID_SECONDS
            + horizon_hours * 3600
            if eval_start_ts is None
            else int(eval_start_ts)
        )
        if eval_end_ts is None:
            end = int(panel["close_ts"].max())
            if not v2_calendar_mode:
                end -= horizon_hours * 3600
        else:
            end = int(eval_end_ts)
        effective_fold_step_sec = fold_step_sec
        if v2_calendar_mode and effective_fold_step_sec is None:
            effective_fold_step_sec = DEFAULT_FOLD_STEP_SEC
        specs = freeze_walk_forward_calendar(
            start,
            end,
            horizon_hours,
            fold_step_sec=effective_fold_step_sec,
            n_splits=n_splits,
        )
        decision_end_ts = end
        if effective_fold_step_sec is not None:
            # Keep the final window fixed when the observed future tail changes.
            # Decisions still stop at the requested or observed endpoint.
            for spec in specs:
                spec["test_end_ts"] = (
                    spec["test_start_ts"] + effective_fold_step_sec - GRID_SECONDS
                )
    cached_panels: dict[int, pd.DataFrame] = {}

    def visible_panel(timestamp: int) -> pd.DataFrame:
        if timestamp not in cached_panels:
            if reload_point_in_time:
                cached_panels[timestamp] = load(timestamp)
            else:
                visible = (
                    panel.copy()
                    if panel.empty
                    else panel.loc[panel["close_ts"].le(timestamp)].copy()
                )
                visible.attrs["as_of_ts"] = timestamp
                visible.attrs["read_publish_seq"] = panel.attrs.get("read_publish_seq")
                cached_panels[timestamp] = visible
        return cached_panels[timestamp]

    forecasts: dict[str, list[dict[str, Any]]] = {name: [] for name in models}
    summaries = []
    scheduled: list[int] = []
    metadata: dict[int, dict[str, Any]] = {}
    for spec in specs:
        training_panel = visible_panel(spec["split_ts"])
        if training_panel.empty:
            train = pd.DataFrame(columns=["item_id", "close_ts", "y_reg", "y_cls"])
        else:
            targets = _target_panel(training_panel, config, horizon_hours)
            train = _mature_training(targets, spec["split_ts"], horizon_hours)
        test_pieces = []
        caches: dict[str, dict[Any, Any]] = {name: {} for name in models}
        for timestamp in _scheduled_times(spec):
            if decision_end_ts is not None and timestamp > decision_end_ts:
                break
            scheduled.append(timestamp)
            current_panel = visible_panel(timestamp)
            sequence = current_panel.attrs.get("read_publish_seq", max_publish_seq)
            metadata[timestamp] = {
                "fold": spec["fold"],
                "split_ts": spec["split_ts"],
                "test_start_ts": spec["test_start_ts"],
                "test_end_ts": spec["test_end_ts"],
                "read_publish_seq": sequence,
            }
            if current_panel.empty:
                continue
            test = current_panel.loc[
                current_panel["close_ts"].eq(timestamp)
                & current_panel["observed_bar"].eq(True)
            ].copy()
            if test.empty:
                continue
            test_pieces.append(test)
            for name in models:
                outputs = _forecast_fold(
                    config,
                    name,
                    train,
                    test,
                    horizon_hours,
                    model_cache=caches[name],
                    enforce_v2_gates=strict_point_in_time,
                )
                for output in outputs:
                    output.update(metadata[timestamp])
                forecasts[name].extend(outputs)
        test_frame = (
            pd.concat(test_pieces, ignore_index=True)
            if test_pieces
            else pd.DataFrame(columns=["item_id", "close_ts"])
        )
        summaries.append(_fold_summary(spec["fold"], train, test_frame, spec))
    curves: dict[str, list[dict[str, Any]]] = {}
    trades: dict[str, list[dict[str, Any]]] = {}
    records: dict[str, list[dict[str, Any]]] = {name: [] for name in models}
    open_positions = {}
    residual_bands = {}
    comparison = []
    for name in models:
        forecasts[name].sort(key=lambda value: (value["close_ts"], value["item_id"]))
        financial, curves[name], trades[name] = _simulate_portfolio(
            config,
            panel,
            forecasts[name],
            horizon_hours,
            strict_point_in_time=strict_point_in_time,
            eval_as_of_ts=evaluation_cutoff,
            max_publish_seq=max_publish_seq,
            manual_delay_sec=manual_delay_sec,
            db_path=path,
            point_in_time_loader=visible_panel,
            decision_records=records[name],
            decision_calendar=scheduled,
            decision_metadata=metadata,
        )
        _attach_realized_labels(
            config, forecasts[name], panel, horizon_hours, evaluation_cutoff
        )
        open_positions[name] = financial["open_positions"]
        residual_bands[name] = financial["residual_valuation_band"]
        comparison.append(
            {"model_name": name, **_prediction_metrics(forecasts[name]), **financial}
        )
    scope_name = f"item:{item_id}" if item_id is not None else "all"
    if category is not None:
        scope_name += f";category:{category}"
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
        "calendar_folds": specs,
        "eval_as_of_ts": evaluation_cutoff,
        "max_publish_seq": max_publish_seq,
        "manual_delay_sec": manual_delay_sec,
        "strict_point_in_time": strict_point_in_time,
        "purge_seconds": horizon_hours * 3600,
        "pinball_units": "log_return",
    }
    run_ids = {}
    with open_connection(path) as conn, write_transaction(conn):
        for result in comparison:
            name = result["model_name"]
            metrics = {key: value for key, value in result.items() if key != "model_name"}
            if not v2_calendar_mode:
                metrics = {key: metrics[key] for key in _LEGACY_METRICS}
            cursor = conn.execute(
                "INSERT INTO backtest_runs (created_at, item_scope, horizon_hours, model_name, "
                "config_json, metrics_json, equity_curve_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    int(time.time()),
                    scope_name,
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
        "item_scope": scope_name,
        "horizon_hours": horizon_hours,
        "comparison_table": comparison,
        "equity_curves": curves,
        "trades": trades,
        "folds": summaries,
        "calendar_folds": specs,
        "forecasts": forecasts,
        "decision_records": records,
        "open_positions": open_positions,
        "residual_valuation_band": residual_bands,
    }


__all__ = ["run_backtest", "purged_walk_forward_split", "freeze_walk_forward_calendar"]
