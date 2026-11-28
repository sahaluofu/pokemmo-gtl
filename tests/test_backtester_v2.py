"""Frozen-calendar, bitemporal, three-cutoff and residual-inventory regressions."""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from pokemmo_gtl.config import AppConfig, load_config
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.quant.backtester import (
    DEFAULT_FOLD_STEP_SEC,
    INITIAL_CAPITAL,
    _consume_shared_flow,
    _prediction_metrics,
    _simulate_portfolio,
    _walk_ladder,
    freeze_walk_forward_calendar,
    purged_walk_forward_split,
    run_backtest,
)
from pokemmo_gtl.quant.features import GRID_SECONDS
from pokemmo_gtl.quant.fees import listing_fee
from pokemmo_gtl.quant.predictor import _load_feature_panel

START_TS = (1_750_000_000 // GRID_SECONDS) * GRID_SECONDS
HORIZON = 24
CALENDAR = freeze_walk_forward_calendar(
    START_TS + 100 * GRID_SECONDS,
    START_TS + 220 * GRID_SECONDS,
    HORIZON,
)
DECISION_TS = CALENDAR[1]["test_start_ts"] + 2 * GRID_SECONDS
DECISION_STEP = (DECISION_TS - START_TS) // GRID_SECONDS


def _fake_fit(
    family: str,
    training: pd.DataFrame,
    horizon: int,
    *,
    bar_count: int = 0,
) -> tuple[float, int]:
    del family, horizon, bar_count
    value = float(training["y_reg"].mean()) if not training.empty else 0.0
    return value, len(training)


def _fake_predict(
    model: tuple[float, int],
    row: pd.DataFrame,
    config: AppConfig,
) -> SimpleNamespace:
    del config
    values = row.iloc[0]
    upstream = values.get("upstream_ret_24h")
    upstream = 0.0 if pd.isna(upstream) else float(upstream)
    center = 0.30 + model[0] * 0.01 + upstream * 0.001
    price = float(values["close_price"])
    return SimpleNamespace(
        prob_up=0.85,
        signal="BUY_WATCH",
        expected_net_roi=0.25 + center * 0.01,
        pred_q10=price * math.exp(center - 0.02),
        pred_q50=price * math.exp(center),
        pred_q90=price * math.exp(center + 0.02),
        pred_q10_ret=center - 0.02,
        pred_q50_ret=center,
        pred_q90_ret=center + 0.02,
        raw_quantile_crossed=False,
    )


def _write_revision(
    conn: Any,
    item_id: int,
    close_ts: int,
    price: int | None,
    *,
    available_at: int | None = None,
    state: str = "stocked",
    timeframe: str = "4h",
    source: str = "snapshot",
) -> int:
    available_at = close_ts if available_at is None else available_at
    cursor = conn.execute(
        "INSERT INTO publish_events (available_at, source_type, source_id) VALUES (?, ?, ?)",
        (
            available_at,
            "manual_quote" if source == "manual" else "snapshot",
            close_ts,
        ),
    )
    sequence = int(cursor.lastrowid)
    seconds = GRID_SECONDS if timeframe == "4h" else 86_400
    stocked = state == "stocked"
    values = {
        "item_id": item_id,
        "timeframe": timeframe,
        "bucket_ts": close_ts - seconds,
        "close_ts": close_ts,
        "data_source": source,
        "book_state": state,
        "open_price": price,
        "high_price": price,
        "low_price": price,
        "close_price": price,
        "robust_best_ask": price if stocked else None,
        "best_ask_min": price,
        "p10_price": price if stocked else None,
        "total_listed_qty": 100 if stocked else 0,
        "listing_count": 10 if stocked else 0,
        "depth_within_5pct": 50 if stocked else 0,
        "spread_top5_pct": 0.01 if stocked else None,
        "est_sold_qty_sum": 200.0,
        "expired_qty_sum": 0,
        "new_listed_qty_sum": 50,
        "sample_count": 16,
        "valid_flow_samples": 16,
    }
    revision = {
        **values,
        "publish_seq": sequence,
        "revision_available_at": available_at,
    }
    columns = list(revision)
    conn.execute(
        f"INSERT INTO item_bar_revisions ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        tuple(revision[column] for column in columns),
    )
    current = {
        **values,
        "bar_available_at": available_at,
        "latest_publish_seq": sequence,
    }
    columns = list(current)
    updates = ", ".join(
        f"{column} = excluded.{column}"
        for column in columns
        if column not in {"item_id", "timeframe", "bucket_ts"}
    )
    conn.execute(
        f"INSERT INTO item_bars ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)}) "
        f"ON CONFLICT(item_id, timeframe, bucket_ts) DO UPDATE SET {updates}",
        tuple(current[column] for column in columns),
    )
    return sequence


def _snapshot(
    conn: Any,
    *,
    nominal_ts: int,
    available_at: int,
    publish_seq: int,
) -> int:
    """Supply required 001 columns without assuming optional collector metadata."""
    supplied: dict[str, Any] = {
        "server_started_at": nominal_ts,
        "started_at": nominal_ts,
        "completed_at": available_at,
        "available_at": available_at,
        "publish_seq": publish_seq,
        "raw_retained": 1,
        "status": "completed",
    }
    values = {}
    for column in conn.execute("PRAGMA table_info(snapshot_runs)"):
        name = str(column["name"])
        if name == "snapshot_id":
            continue
        if name in supplied:
            values[name] = supplied[name]
        elif column["notnull"] and column["dflt_value"] is None:
            values[name] = "" if "TEXT" in str(column["type"]).upper() else 0
    names = list(values)
    cursor = conn.execute(
        f"INSERT INTO snapshot_runs ({', '.join(names)}) "
        f"VALUES ({', '.join('?' for _ in names)})",
        tuple(values[name] for name in names),
    )
    return int(cursor.lastrowid)


def _copy_database(source: Path, destination: Path) -> None:
    with sqlite3.connect(source) as source_conn, sqlite3.connect(destination) as destination_conn:
        source_conn.backup(destination_conn)


@pytest.fixture
def causal_market(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[AppConfig, Path]:
    import pokemmo_gtl.quant.backtester as backtester

    monkeypatch.setattr(backtester, "_fit_model", _fake_fit)
    monkeypatch.setattr(backtester, "_predict_row", _fake_predict)
    config = load_config("sim")
    config = replace(
        config,
        root=tmp_path,
        fees=config.fees.model_copy(update={"verified": True}),
        item_relations={
            "upstream_relations": [
                {"target_item_id": 1, "upstream_item_id": 10, "weight": 1.0},
                {"target_item_id": 1, "upstream_item_id": 11, "weight": 1.0},
            ]
        },
    )
    path = tmp_path / "causal.db"
    migrate(path, applied_at=START_TS)
    with open_connection(path) as conn, write_transaction(conn):
        conn.executemany(
            "INSERT INTO items_catalog "
            "(item_id, name_en, category, max_stack_size, updated_at, fee_verified) "
            "VALUES (?, ?, ?, 99, ?, 1)",
            [
                (1, "Target one", "target", START_TS),
                (2, "Target two", "target", START_TS),
                (10, "Observed ingredient", "ingredient", START_TS),
                (11, "Future ingredient", "ingredient", START_TS),
            ],
        )
        for step in range(1, 253):
            timestamp = START_TS + step * GRID_SECONDS
            _write_revision(conn, 1, timestamp, 100_000 + step * 20)
            _write_revision(conn, 2, timestamp, 120_000 + step * 10)
            upstream_price = 100 if step <= DECISION_STEP - 6 else 200
            _write_revision(conn, 10, timestamp, upstream_price)
    return config, path


def _run(
    config: AppConfig,
    path: Path,
    **kwargs: Any,
) -> dict[str, Any]:
    arguments = {
        "horizon_hours": HORIZON,
        "models": ("naive",),
        "category": "target",
        "db_path": path,
        "calendar_folds": CALENDAR,
        "eval_as_of_ts": START_TS + 252 * GRID_SECONDS,
        "strict_point_in_time": True,
    }
    arguments.update(kwargs)
    return run_backtest(config, **arguments)


def _run_default(config: AppConfig, path: Path) -> dict[str, Any]:
    """Include DECISION_TS using only model and warmup options."""
    return run_backtest(
        config,
        horizon_hours=HORIZON,
        models=("naive",),
        category="target",
        db_path=path,
        min_train_bars=55,
    )


def _decision(result: dict[str, Any], timestamp: int = DECISION_TS) -> dict[str, Any]:
    return next(
        record
        for record in result["decision_records"]["naive"]
        if record["decision_ts"] == timestamp
    )


def test_default_v2_calendar_training_and_candidates_survive_deleting_future_bars(
    causal_market: tuple[AppConfig, Path],
    tmp_path: Path,
) -> None:
    config, path = causal_market
    truncated = tmp_path / "default_truncated.db"
    _copy_database(path, truncated)
    with open_connection(truncated) as conn, write_transaction(conn):
        conn.execute("DELETE FROM item_bar_revisions WHERE close_ts > ?", (DECISION_TS,))
        conn.execute("DELETE FROM item_bars WHERE close_ts > ?", (DECISION_TS,))
    full_result = _run_default(config, path)
    short_result = _run_default(config, truncated)
    full_calendar = full_result["calendar_folds"]
    short_calendar = short_result["calendar_folds"]
    assert len(full_calendar) > len(short_calendar)
    assert short_calendar == full_calendar[:len(short_calendar)]
    for spec in full_calendar:
        assert (
            spec["test_end_ts"] - spec["test_start_ts"] + GRID_SECONDS
            == DEFAULT_FOLD_STEP_SEC
        )
    full_decision = _decision(full_result)
    short_decision = _decision(short_result)
    assert full_decision["candidates"]
    for key in (
        "fold",
        "split_ts",
        "test_start_ts",
        "test_end_ts",
        "candidates",
        "ranking",
        "budget_before",
        "budget_after",
    ):
        assert full_decision[key] == short_decision[key]
    fold = full_decision["fold"]
    for key in ("train_start_ts", "train_end_ts", "train_rows"):
        assert full_result["folds"][fold][key] == short_result["folds"][fold][key]
    train_end_ts = full_result["folds"][fold]["train_end_ts"]
    assert train_end_ts is not None
    assert train_end_ts + HORIZON * 3600 <= full_decision["split_ts"]


@pytest.mark.parametrize(
    "revision_offset",
    [0, -60 * GRID_SECONDS],
    ids=["decision_bar", "training_bar"],
)
def test_default_late_revision_cannot_change_training_or_inference(
    causal_market: tuple[AppConfig, Path],
    tmp_path: Path,
    revision_offset: int,
) -> None:
    config, path = causal_market
    late_path = tmp_path / "default_late.db"
    _copy_database(path, late_path)
    with open_connection(late_path) as conn, write_transaction(conn):
        _write_revision(
            conn,
            1,
            DECISION_TS + revision_offset,
            10_000,
            available_at=DECISION_TS + 300,
        )
    baseline = _run_default(config, path)
    late = _run_default(config, late_path)
    baseline_decision = _decision(baseline)
    late_decision = _decision(late)
    assert baseline_decision["candidates"]
    for key in (
        "fold",
        "split_ts",
        "test_start_ts",
        "test_end_ts",
        "candidates",
        "ranking",
        "budget_before",
        "budget_after",
    ):
        assert baseline_decision[key] == late_decision[key]
    # Read watermarks may differ between a synchronous slice and an as-of reload.
    for before, after in zip(
        baseline_decision["forecasts"],
        late_decision["forecasts"],
        strict=True,
    ):
        assert {
            key: value for key, value in before.items() if key != "read_publish_seq"
        } == {
            key: value for key, value in after.items() if key != "read_publish_seq"
        }
    fold = baseline_decision["fold"]
    assert baseline["folds"][fold]["train_end_ts"] == late["folds"][fold]["train_end_ts"]


def test_default_v2_retains_trailing_decisions_unscored_forecasts_and_open_positions(
    causal_market: tuple[AppConfig, Path],
) -> None:
    config, path = causal_market
    result = _run_default(config, path)
    max_ts = START_TS + 252 * GRID_SECONDS
    trailing_start = max_ts - HORIZON * 3600
    expected_times = set(range(trailing_start + GRID_SECONDS, max_ts + 1, GRID_SECONDS))
    decisions = {
        record["decision_ts"]: record
        for record in result["decision_records"]["naive"]
    }
    assert expected_times <= decisions.keys()
    assert max(decisions) == max_ts
    trailing_forecasts = [
        forecast
        for forecast in result["forecasts"]["naive"]
        if trailing_start < forecast["decision_ts"] <= max_ts
    ]
    assert {forecast["decision_ts"] for forecast in trailing_forecasts} == expected_times
    assert len(trailing_forecasts) == 2 * len(expected_times)
    for forecast in trailing_forecasts:
        assert forecast["y_reg"] is None
        assert forecast["y_cls"] is None
    for timestamp in expected_times:
        recorded = decisions[timestamp]["forecasts"]
        assert len(recorded) == 2
        assert all(forecast["y_reg"] is None for forecast in recorded)
        assert all(forecast["y_cls"] is None for forecast in recorded)
    metrics = result["comparison_table"][0]
    assert metrics["total_forecast_count"] == len(result["forecasts"]["naive"])
    assert metrics["unmatured_or_unlabeled_count"] == len(trailing_forecasts)
    positions = result["open_positions"]["naive"]
    assert positions
    assert all(position["entry_ts"] > trailing_start for position in positions)
    assert all(position["remaining_qty"] > 0 for position in positions)
    json.dumps(result, allow_nan=False)


def test_default_synchronous_market_loads_feature_panel_once(
    causal_market: tuple[AppConfig, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pokemmo_gtl.quant.backtester as backtester

    config, path = causal_market
    original_loader = backtester._load_feature_panel
    calls: list[int] = []

    def counted_load(
        config: AppConfig,
        path: Path,
        **kwargs: Any,
    ) -> pd.DataFrame:
        calls.append(int(kwargs["as_of_ts"]))
        return original_loader(config, path, **kwargs)

    monkeypatch.setattr(backtester, "_load_feature_panel", counted_load)
    result = _run_default(config, path)
    assert _decision(result)["candidates"]
    assert len(calls) == 1


def test_frozen_calendar_and_complete_decision_survive_deleting_every_future_row(
    causal_market: tuple[AppConfig, Path],
    tmp_path: Path,
) -> None:
    config, path = causal_market
    truncated = tmp_path / "truncated.db"
    _copy_database(path, truncated)
    with open_connection(truncated) as conn, write_transaction(conn):
        conn.execute("DELETE FROM item_bar_revisions WHERE close_ts > ?", (DECISION_TS,))
        conn.execute("DELETE FROM item_bars WHERE close_ts > ?", (DECISION_TS,))
    full_result = _run(config, path)
    short_result = _run(config, truncated)
    assert full_result["calendar_folds"] == short_result["calendar_folds"] == CALENDAR
    assert CALENDAR[1]["test_start_ts"] <= DECISION_TS <= CALENDAR[1]["test_end_ts"]
    for key in ("train_start_ts", "train_end_ts", "test_start_ts"):
        assert full_result["folds"][1][key] == short_result["folds"][1][key]
    full_decision = _decision(full_result)
    short_decision = _decision(short_result)
    assert full_decision["candidates"]
    assert full_decision == short_decision
    assert full_decision["ranking"] == short_decision["ranking"]
    assert full_decision["budget_before"] == short_decision["budget_before"]
    assert full_decision["budget_after"] == short_decision["budget_after"]
    assert all(forecast["y_reg"] is None for forecast in full_decision["forecasts"])
    assert all(forecast["y_cls"] is None for forecast in full_decision["forecasts"])


def test_nominally_earlier_but_late_available_frame_cannot_change_decision(
    causal_market: tuple[AppConfig, Path],
    tmp_path: Path,
) -> None:
    config, path = causal_market
    late_path = tmp_path / "late.db"
    _copy_database(path, late_path)
    with open_connection(late_path) as conn, write_transaction(conn):
        sequence = _write_revision(
            conn,
            1,
            DECISION_TS,
            10_000,
            available_at=DECISION_TS + 300,
        )
        snapshot_id = _snapshot(
            conn,
            nominal_ts=DECISION_TS - 600,
            available_at=DECISION_TS + 300,
            publish_seq=sequence,
        )
        conn.execute(
            "INSERT INTO orderbook_observations "
            "(snapshot_id, item_id, available_at, publish_seq, book_state, "
            "purchase_mode, ladder_json) VALUES (?, 1, ?, ?, 'stocked', 'per_unit', ?)",
            (
                snapshot_id,
                DECISION_TS + 300,
                sequence,
                json.dumps([{"auction_id": 1, "price": 10_000, "qty": 99}]),
            ),
        )
    baseline = _run(config, path)
    late = _run(config, late_path)
    assert _decision(baseline) == _decision(late)
    assert baseline["folds"][1]["train_end_ts"] == late["folds"][1]["train_end_ts"]


def test_first_ever_future_upstream_quote_does_not_reweight_past_backtest_features(
    causal_market: tuple[AppConfig, Path],
    tmp_path: Path,
) -> None:
    config, path = causal_market
    future_path = tmp_path / "future_ingredient.db"
    _copy_database(path, future_path)
    with open_connection(future_path) as conn, write_transaction(conn):
        _write_revision(conn, 11, DECISION_TS + GRID_SECONDS, 500)
    before = _run(config, path)
    after = _run(config, future_path)
    assert _decision(before) == _decision(after)
    forecast = next(
        forecast
        for forecast in _decision(after)["forecasts"]
        if forecast["item_id"] == 1
    )
    assert forecast["upstream_ret_24h"] == 1.0


@pytest.mark.parametrize("exit_state", ["missing", "empty_confirmed", "mixed_ended_empty"])
def test_missing_or_empty_exact_exit_keeps_forecast_and_cost_locked_in_inventory(
    causal_market: tuple[AppConfig, Path],
    exit_state: str,
) -> None:
    config, path = causal_market
    exit_ts = DECISION_TS + HORIZON * 3600
    with open_connection(path) as conn, write_transaction(conn):
        conn.execute(
            "DELETE FROM item_bar_revisions WHERE item_id = 1 AND close_ts = ?",
            (exit_ts,),
        )
        conn.execute(
            "DELETE FROM item_bars WHERE item_id = 1 AND close_ts = ?",
            (exit_ts,),
        )
        if exit_state != "missing":
            _write_revision(
                conn,
                1,
                exit_ts,
                110_000 if exit_state == "mixed_ended_empty" else None,
                state=exit_state,
            )
    calendar = [
        {
            "fold": 0,
            "split_ts": DECISION_TS,
            "test_start_ts": DECISION_TS,
            "test_end_ts": DECISION_TS,
        }
    ]
    result = _run(
        config,
        path,
        item_id=1,
        calendar_folds=calendar,
        eval_as_of_ts=exit_ts,
    )
    metrics = result["comparison_table"][0]
    assert metrics["total_forecast_count"] == 1
    assert metrics["matured_forecast_count"] == 0
    assert metrics["unmatured_or_unlabeled_count"] == 1
    assert result["forecasts"]["naive"][0]["y_reg"] is None
    assert result["forecasts"]["naive"][0]["y_cls"] is None
    assert _decision(result)["forecasts"][0]["y_reg"] is None
    positions = result["open_positions"]["naive"]
    assert len(positions) == 1
    position = positions[0]
    assert position["remaining_qty"] == position["initial_qty"] > 0
    assert position["remaining_cost"] == position["total_buy_cost"] > 0
    assert not result["trades"]["naive"]
    assert metrics["available_cash"] == float(INITIAL_CAPITAL) - position["remaining_cost"]
    assert result["equity_curves"]["naive"][-1]["cash"] == metrics["available_cash"]
    assert result["residual_valuation_band"]["naive"]["cost_basis"] == position["remaining_cost"]
    assert metrics["open_positions"] == positions
    json.dumps(result, allow_nan=False)


def test_three_cutoffs_retain_unmatured_predictions_even_with_an_observed_future_exit(
    causal_market: tuple[AppConfig, Path],
) -> None:
    config, path = causal_market
    calendar = [
        {
            "fold": 0,
            "split_ts": DECISION_TS,
            "test_start_ts": DECISION_TS,
            "test_end_ts": DECISION_TS,
        }
    ]
    result = _run(
        config,
        path,
        item_id=1,
        calendar_folds=calendar,
        eval_as_of_ts=DECISION_TS + GRID_SECONDS,
    )
    metrics = result["comparison_table"][0]
    assert metrics["total_forecast_count"] == 1
    assert metrics["matured_forecast_count"] == 0
    assert metrics["unmatured_or_unlabeled_count"] == 1
    assert metrics["brier_score"] is None
    assert result["open_positions"]["naive"]
    assert result["forecasts"]["naive"][0]["y_reg"] is None


def _paper_forecast(timestamp: int) -> dict[str, Any]:
    return {
        "item_id": 1,
        "close_ts": timestamp,
        "signal": "BUY_WATCH",
        "prob_up": 0.9,
        "expected_net_roi": 0.5,
        "pred_q10": 200_000,
        "pred_q50": 200_000,
        "pred_q90": 200_000,
        "ref_qty": 10,
    }


def test_two_orders_share_one_observed_flow_budget_and_keep_the_unsold_remainder(
    causal_market: tuple[AppConfig, Path],
) -> None:
    config, _ = causal_market
    exit_ts = DECISION_TS + HORIZON * 3600
    panel = pd.DataFrame(
        [
            {
                "item_id": 1,
                "close_ts": timestamp,
                "observed_bar": True,
                "book_state": "stocked",
                "fee_verified": 1,
                "close_price": price,
                "p10_price": price,
                "max_stack_size": 99,
                "asset_class": "item",
                "data_source": "snapshot",
                "total_listed_qty": 100,
                "est_sold_qty_24h": 2000.0,
                "est_sold_qty_sum": flow,
            }
            for timestamp, price, flow in (
                (DECISION_TS, 100_000, 100.0),
                (exit_ts, 140_000, 3.0),
            )
        ]
    )
    metrics, curve, trades = _simulate_portfolio(
        config,
        panel,
        [_paper_forecast(DECISION_TS), _paper_forecast(DECISION_TS)],
        HORIZON,
        strict_point_in_time=True,
        eval_as_of_ts=exit_ts,
    )
    assert sum(trade["sold_qty"] for trade in trades) == 3
    positions = metrics["open_positions"]
    assert len(positions) == 2
    initial_qty = sum(position["initial_qty"] for position in positions)
    assert initial_qty > 3
    assert sum(position["remaining_qty"] for position in positions) == initial_qty - 3
    initial_cost = sum(position["total_buy_cost"] for position in positions)
    proceeds = sum(
        trade["exit_price"] * trade["sold_qty"] - trade["fees"]
        for trade in trades
    )
    assert curve[-1]["cash"] == float(INITIAL_CAPITAL) - initial_cost + proceeds
    assert metrics["inventory_cost"] > 0
    assert curve[-1]["positions"] == 2


def test_shared_flow_helper_never_resets_an_already_consumed_window() -> None:
    budget: dict[tuple[int, int], int] = {}
    assert _consume_shared_flow(budget, 1, DECISION_TS, 5.9, 4) == 4
    assert _consume_shared_flow(budget, 1, DECISION_TS, 5.9, 4) == 1
    assert _consume_shared_flow(budget, 1, DECISION_TS, 500, 4) == 0
    assert _consume_shared_flow(budget, 1, DECISION_TS + GRID_SECONDS, 2, 4) == 2


def test_ladder_preserves_integer_batch_cost_and_indivisible_lot_feasibility() -> None:
    lots = [
        {"auction_id": 1, "lot_index": 0, "price": 9000, "qty": 1},
        {"auction_id": 2, "lot_index": 1, "price": 10000, "qty": 99},
    ]
    qty, cost, fills = _walk_ladder(lots, 99, Decimal(1_000_000), "per_unit")
    assert qty == 99
    assert cost == Decimal(989_000)
    assert [fill["qty"] for fill in fills] == [1, 98]
    feasible = [
        {"auction_id": 1, "lot_index": 0, "price": 10000, "qty": 60},
        {"auction_id": 2, "lot_index": 1, "price": 10000, "qty": 39},
    ]
    infeasible = [
        {"auction_id": 1, "lot_index": 0, "price": 10000, "qty": 50},
        {"auction_id": 2, "lot_index": 1, "price": 10000, "qty": 49},
    ]
    assert _walk_ladder(feasible, 39, Decimal(1_000_000), "bundle_only")[:2] == (
        39,
        Decimal(390_000),
    )
    assert _walk_ladder(infeasible, 39, Decimal(1_000_000), "bundle_only")[:2] == (
        0,
        Decimal(0),
    )


def test_delayed_execution_reads_retained_lots_at_execution_time(
    causal_market: tuple[AppConfig, Path],
) -> None:
    config, path = causal_market
    execution_ts = DECISION_TS + 300
    with open_connection(path) as conn, write_transaction(conn):
        sequence = int(
            conn.execute(
                "INSERT INTO publish_events (available_at, source_type, source_id) "
                "VALUES (?, 'snapshot', 1)",
                (execution_ts,),
            ).lastrowid
        )
        snapshot_id = _snapshot(
            conn,
            nominal_ts=execution_ts,
            available_at=execution_ts,
            publish_seq=sequence,
        )
        conn.execute(
            "INSERT INTO orderbook_observations "
            "(snapshot_id, item_id, available_at, publish_seq, book_state, "
            "purchase_mode, ladder_json) VALUES (?, 1, ?, ?, 'stocked', 'per_unit', ?)",
            (
                snapshot_id,
                execution_ts,
                sequence,
                json.dumps(
                    [
                        {"auction_id": 1, "price": 90_000, "qty": 1},
                        {"auction_id": 2, "price": 100_000, "qty": 99},
                    ]
                ),
            ),
        )
    calendar = [
        {
            "fold": 0,
            "split_ts": DECISION_TS,
            "test_start_ts": DECISION_TS,
            "test_end_ts": DECISION_TS,
        }
    ]
    result = _run(
        config,
        path,
        item_id=1,
        calendar_folds=calendar,
        manual_delay_sec=300,
        eval_as_of_ts=execution_ts,
    )
    position = result["open_positions"]["naive"][0]
    assert position["execution_mode"] == "lot_ladder"
    assert position["executed_at"] == execution_ts
    qty = position["initial_qty"]
    assert qty > 1
    assert position["total_buy_cost"] == 90_000 + (qty - 1) * 100_000
    assert sum(fill["qty"] for fill in position["fills_detail"]) == qty
    assert _decision(result)["budget_after"]["reserved_cash"] > 0


def test_loader_applies_time_and_publication_cutoffs_and_preserves_empty_states(
    causal_market: tuple[AppConfig, Path],
) -> None:
    config, path = causal_market
    with open_connection(path) as conn, write_transaction(conn):
        first = _write_revision(
            conn, 1, DECISION_TS, 123_000, available_at=DECISION_TS + 60
        )
        second = _write_revision(
            conn, 1, DECISION_TS, 124_000, available_at=DECISION_TS + 60
        )
        _write_revision(
            conn,
            2,
            DECISION_TS,
            None,
            state="empty_confirmed",
            available_at=DECISION_TS + 60,
        )
    before = _load_feature_panel(
        config, path, as_of_ts=DECISION_TS, require_verified_watermark=True
    )
    capped = _load_feature_panel(
        config,
        path,
        as_of_ts=DECISION_TS + 60,
        max_publish_seq=first,
        require_verified_watermark=True,
    )
    latest = _load_feature_panel(
        config, path, as_of_ts=DECISION_TS + 60, require_verified_watermark=True
    )
    key_before = before.loc[before["item_id"].eq(1) & before["close_ts"].eq(DECISION_TS)]
    key_capped = capped.loc[capped["item_id"].eq(1) & capped["close_ts"].eq(DECISION_TS)]
    key_latest = latest.loc[latest["item_id"].eq(1) & latest["close_ts"].eq(DECISION_TS)]
    assert key_before.iloc[0]["close_price"] != 123_000
    assert key_capped.iloc[0]["close_price"] == 123_000
    assert key_latest.iloc[0]["close_price"] == 124_000
    assert key_latest.iloc[0]["latest_publish_seq"] == second
    empty = latest.loc[latest["item_id"].eq(2) & latest["close_ts"].eq(DECISION_TS)].iloc[0]
    assert empty["book_state"] == "empty_confirmed"
    assert pd.isna(empty["close_price"])
    assert bool(empty["observed_bar"])


def test_legacy_fallback_is_per_key_and_strict_mode_excludes_unwatermarked_rows(
    causal_market: tuple[AppConfig, Path],
) -> None:
    config, path = causal_market
    with open_connection(path) as conn, write_transaction(conn):
        conn.execute(
            "INSERT INTO items_catalog "
            "(item_id, name_en, category, max_stack_size, updated_at) "
            "VALUES (99, 'Legacy fixture', 'legacy', 99, ?)",
            (START_TS,),
        )
        conn.execute(
            "INSERT INTO item_bars "
            "(item_id, timeframe, bucket_ts, close_ts, data_source, "
            "open_price, high_price, low_price, close_price, sample_count) "
            "VALUES (99, '4h', ?, ?, 'snapshot', 100, 100, 100, 100, 1)",
            (DECISION_TS - GRID_SECONDS, DECISION_TS),
        )
    compatible = _load_feature_panel(config, path, as_of_ts=DECISION_TS)
    strict = _load_feature_panel(
        config, path, as_of_ts=DECISION_TS, require_verified_watermark=True
    )
    disabled = _load_feature_panel(
        config, path, as_of_ts=DECISION_TS, legacy_fallback=False
    )
    assert 99 in compatible["item_id"].unique()
    assert 99 not in strict["item_id"].unique()
    assert 99 not in disabled["item_id"].unique()


def test_frozen_split_keeps_unlabeled_test_rows_and_purges_training_labels() -> None:
    frame = pd.DataFrame(
        {
            "item_id": [1] * 30,
            "close_ts": [START_TS + step * GRID_SECONDS for step in range(30)],
            "y_reg": [0.1] * 20 + [None] * 10,
            "y_cls": [1] * 20 + [None] * 10,
        }
    )
    specs = freeze_walk_forward_calendar(
        START_TS + 18 * GRID_SECONDS,
        START_TS + 29 * GRID_SECONDS,
        HORIZON,
        fold_step_sec=6 * GRID_SECONDS,
    )
    folds = list(
        purged_walk_forward_split(
            frame, HORIZON, calendar_folds=specs, min_train_bars=3
        )
    )
    assert len(folds) == 2
    assert folds[-1][1]["y_reg"].isna().all()
    for spec, (train, test) in zip(specs, folds, strict=True):
        assert train["y_reg"].notna().all()
        assert train["y_cls"].notna().all()
        assert (train["close_ts"] + HORIZON * 3600 <= spec["split_ts"]).all()
        assert test["close_ts"].min() == spec["test_start_ts"]
        assert test["close_ts"].max() == spec["test_end_ts"]


def test_prediction_metrics_score_only_matured_labels_and_count_every_forecast() -> None:
    common = {
        "prob_up": 0.8,
        "pred_q10_ret": -0.1,
        "pred_q50_ret": 0.0,
        "pred_q90_ret": 0.1,
        "raw_quantile_crossed": False,
    }
    forecasts = [
        {**common, "y_reg": 0.05, "y_cls": 1},
        {**common, "y_reg": None, "y_cls": None},
        {**common, "y_reg": float("nan"), "y_cls": float("nan")},
    ]
    metrics = _prediction_metrics(forecasts)
    assert metrics["total_forecast_count"] == 3
    assert metrics["matured_forecast_count"] == 1
    assert metrics["unmatured_or_unlabeled_count"] == 2
    assert metrics["brier_score"] == pytest.approx(0.04)
    assert metrics["roc_auc"] is None
    assert metrics["pinball_q50"] == pytest.approx(0.025)
    assert len(forecasts) == 3
    json.dumps(metrics, allow_nan=False)


def test_strict_partial_sale_uses_discrete_fees(
    causal_market: tuple[AppConfig, Path],
) -> None:
    config, _ = causal_market
    budget: dict[tuple[int, int], int] = {}
    assert _consume_shared_flow(budget, 1, DECISION_TS, 3, 99) == 3
    fee = listing_fee(
        140_000,
        3,
        99,
        min_fee=config.fees.item_min_fee,
        rate=config.fees.rate,
        max_fee_per_slot=config.fees.max_fee_per_slot,
        fee_basis=getattr(config.fees, "fee_basis", "per_slot"),
        rounding_mode=getattr(config.fees, "rounding_mode", "floor"),
    )
    assert isinstance(fee, int)
    assert Decimal(140_000 * 3 - fee).as_integer_ratio()[1] == 1
