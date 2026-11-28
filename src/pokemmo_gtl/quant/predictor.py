"""Database-backed, point-in-time cold-start routing for CLI and Web forecasts."""

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
_BAR_COLUMNS = (
    "item_id",
    "timeframe",
    "bucket_ts",
    "close_ts",
    "data_source",
    "book_state",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "robust_best_ask",
    "best_ask_min",
    "p10_price",
    "total_listed_qty",
    "listing_count",
    "depth_within_5pct",
    "spread_top5_pct",
    "est_sold_qty_sum",
    "expired_qty_sum",
    "new_listed_qty_sum",
    "sample_count",
    "valid_flow_samples",
)
_CATALOG_COLUMNS = (
    "item_id",
    "asset_class",
    "category",
    "max_stack_size",
    "fee_verified",
    "purchase_mode",
)
_SQL_DEFAULTS = {
    "data_source": "'snapshot'",
    "book_state": "'stocked'",
    "sample_count": "1",
    "valid_flow_samples": "0",
    "asset_class": "'item'",
    "category": "'unknown'",
    "max_stack_size": "99",
    "fee_verified": "0",
    "purchase_mode": "'unknown'",
}


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


def _columns(conn: Any, table: str) -> set[str]:
    """Inspect only internal, constant table names."""
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _projection(columns: set[str], names: Iterable[str], alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return ", ".join(
        f"{prefix}{name}" if name in columns else f"{_SQL_DEFAULTS.get(name, 'NULL')} AS {name}"
        for name in names
    )


def _bar_projection(columns: set[str], alias: str, *, revision: bool) -> str:
    projection = _projection(columns, _BAR_COLUMNS, alias)
    available = "revision_available_at" if revision else "bar_available_at"
    sequence = "publish_seq" if revision else "latest_publish_seq"
    available_sql = f"{alias}.{available}" if available in columns else "NULL"
    sequence_sql = f"{alias}.{sequence}" if sequence in columns else "NULL"
    return (
        f"{projection}, {available_sql} AS bar_available_at, "
        f"{sequence_sql} AS latest_publish_seq"
    )


def _load_feature_panel(
    config: AppConfig,
    db_path: Path,
    *,
    as_of_ts: int | None = None,
    max_publish_seq: int | None = None,
    legacy_fallback: bool = True,
    require_verified_watermark: bool = False,
) -> pd.DataFrame:
    """Read closed bars through one immutable time/publication boundary.

    Once any revisions exist, the latest-view table is not a substitute for
    unavailable revisions. Its only fallback is an explicitly allowed 001 row:
    no availability watermark and no revision of that bar at any publication.
    Strict callers exclude those historically unverifiable rows altogether.
    """
    cutoff = int(time.time()) if as_of_ts is None else int(as_of_ts)
    if max_publish_seq is not None and (
        type(max_publish_seq) is not int or max_publish_seq < 0
    ):
        raise ValueError("max_publish_seq must be a non-negative integer")
    allow_legacy = legacy_fallback and not require_verified_watermark
    read_sequence = max_publish_seq
    with open_connection(db_path) as conn:
        conn.execute("BEGIN")
        try:
            bar_columns = _columns(conn, "item_bars")
            revision_columns = _columns(conn, "item_bar_revisions")
            has_revisions = bool(revision_columns) and (
                conn.execute("SELECT 1 FROM item_bar_revisions LIMIT 1").fetchone() is not None
            )
            records: list[dict[str, Any]] = []
            if has_revisions:
                predicates = [
                    "close_ts <= ?",
                    "revision_available_at <= ?",
                    "(timeframe = '4h' OR (timeframe = '1d' AND data_source = 'manual'))",
                ]
                parameters: list[Any] = [cutoff, cutoff]
                if max_publish_seq is not None:
                    predicates.append("publish_seq <= ?")
                    parameters.append(max_publish_seq)
                records.extend(
                    dict(row)
                    for row in conn.execute(
                        f"""
                        WITH visible AS (
                            SELECT item_id, timeframe, bucket_ts,
                                   MAX(publish_seq) AS publish_seq
                            FROM item_bar_revisions
                            WHERE {" AND ".join(predicates)}
                            GROUP BY item_id, timeframe, bucket_ts
                        )
                        SELECT {_bar_projection(revision_columns, "r", revision=True)}
                        FROM item_bar_revisions AS r
                        JOIN visible AS v
                          ON v.item_id = r.item_id
                         AND v.timeframe = r.timeframe
                         AND v.bucket_ts = r.bucket_ts
                         AND v.publish_seq = r.publish_seq
                        ORDER BY r.item_id, r.close_ts, r.timeframe
                        """,
                        parameters,
                    )
                )
            if bar_columns and (not has_revisions or allow_legacy):
                predicates = [
                    "b.close_ts <= ?",
                    "(b.timeframe = '4h' OR "
                    "(b.timeframe = '1d' AND b.data_source = 'manual'))",
                ]
                parameters = [cutoff]
                if has_revisions:
                    if "bar_available_at" in bar_columns:
                        predicates.append("b.bar_available_at IS NULL")
                    predicates.append(
                        """
                        NOT EXISTS (
                            SELECT 1 FROM item_bar_revisions AS r
                            WHERE r.item_id = b.item_id
                              AND r.timeframe = b.timeframe
                              AND r.bucket_ts = b.bucket_ts
                        )
                        """
                    )
                elif "bar_available_at" in bar_columns:
                    availability = "b.bar_available_at <= ?"
                    if allow_legacy:
                        availability = f"({availability} OR b.bar_available_at IS NULL)"
                    predicates.append(availability)
                    parameters.append(cutoff)
                elif not allow_legacy:
                    predicates.append("0 = 1")
                if max_publish_seq is not None and "latest_publish_seq" in bar_columns:
                    predicates.append(
                        "(b.latest_publish_seq IS NULL OR b.latest_publish_seq <= ?)"
                    )
                    parameters.append(max_publish_seq)
                records.extend(
                    dict(row)
                    for row in conn.execute(
                        f"""
                        SELECT {_bar_projection(bar_columns, "b", revision=False)}
                        FROM item_bars AS b
                        WHERE {" AND ".join(predicates)}
                        ORDER BY b.item_id, b.close_ts, b.timeframe
                        """,
                        parameters,
                    )
                )
            catalog_columns = _columns(conn, "items_catalog")
            catalog = pd.DataFrame.from_records(
                [
                    dict(row)
                    for row in conn.execute(
                        f"SELECT {_projection(catalog_columns, _CATALOG_COLUMNS)} "
                        "FROM items_catalog ORDER BY item_id"
                    )
                ],
                columns=_CATALOG_COLUMNS,
            )
            event_columns = _columns(conn, "market_events")
            events = (
                [
                    dict(row)
                    for row in conn.execute(
                        "SELECT "
                        + _projection(
                            event_columns,
                            ("name", "event_type", "announced_at", "start_ts", "end_ts"),
                        )
                        + " FROM market_events"
                    )
                ]
                if event_columns
                else []
            )
            if read_sequence is None and _columns(conn, "publish_events"):
                row = conn.execute(
                    "SELECT MAX(publish_seq) AS publish_seq FROM publish_events "
                    "WHERE available_at <= ?",
                    (cutoff,),
                ).fetchone()
                read_sequence = row["publish_seq"] if row is not None else None
        finally:
            conn.execute("ROLLBACK")
    bars = pd.DataFrame.from_records(records)
    if bars.empty:
        return pd.DataFrame()
    prices = pd.to_numeric(bars["close_price"], errors="coerce")
    empty_state = bars["book_state"].isin(["empty_confirmed", "mixed_ended_empty"])
    samples = pd.to_numeric(bars["sample_count"], errors="coerce")
    bars = bars.loc[((np.isfinite(prices) & prices.gt(0)) | empty_state) & samples.gt(0)]
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
    frame["book_state"] = frame["book_state"].fillna("collection_missing")
    usable = (
        frame["observed_bar"]
        & frame["book_state"].eq("stocked")
        & pd.to_numeric(frame["close_price"], errors="coerce").gt(0)
    )
    frame["valid_bar_count"] = usable.groupby(frame["item_id"]).cumsum().astype(int)
    frame = frame.merge(catalog, on="item_id", how="left", validate="many_to_one")
    frame["max_stack_size"] = frame["max_stack_size"].fillna(99).clip(lower=1).astype(int)
    frame["fee_verified"] = frame["fee_verified"].fillna(0).astype(int)
    grouped = frame.groupby("item_id")["close_price"]
    prior = grouped.shift(180)
    frame["ret_30d"] = frame["close_price"].div(prior.where(prior.gt(0))).sub(1)
    mean = grouped.transform(lambda values: values.rolling(180, min_periods=180).mean())
    std = grouped.transform(lambda values: values.rolling(180, min_periods=180).std())
    frame["zscore_30d"] = (frame["close_price"] - mean).div(std.where(std.gt(0)))
    frame = frame.sort_values(["close_ts", "item_id"], kind="stable").reset_index(drop=True)
    frame.attrs["as_of_ts"] = cutoff
    frame.attrs["read_publish_seq"] = None if read_sequence is None else int(read_sequence)
    return frame


def _target_panel(frame: pd.DataFrame, config: AppConfig, horizon: int) -> pd.DataFrame:
    result = add_targets(
        frame,
        horizon,
        config.fees,
        ref_notional=config.settings.ref_notional_yen,
    )
    stocked = frame.get("book_state", pd.Series("stocked", index=frame.index)).eq("stocked")
    observed = frame["observed_bar"] & stocked
    observed_keys = set(map(tuple, frame.loc[observed, ["item_id", "close_ts"]].to_numpy()))
    has_exit = np.fromiter(
        (
            (int(item), int(ts) + horizon * 3600) in observed_keys
            for item, ts in result[["item_id", "close_ts"]].itertuples(index=False, name=None)
        ),
        dtype=bool,
        count=len(result),
    )
    valid = observed & has_exit
    result.loc[~valid, ["y_reg", "y_cls", "p_exit"]] = np.nan
    return result


def _model_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Use an explicit allowlist: targets and execution metadata never enter a model."""
    return (
        frame.reindex(columns=MODEL_FEATURES)
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
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


def _v2_fees_verified(config: AppConfig, row: dict[str, Any] | pd.Series) -> bool:
    value = row.get("fee_verified", 0)
    return (
        pd.notna(value)
        and int(value) == 1
        and bool(getattr(config.fees, "verified", True))
    )


def _reference_quantity(config: AppConfig, row: dict[str, Any] | pd.Series) -> int:
    k = 1
    for column in ("K_t", "k_t", "depth_within_5pct", "listing_count", "total_listed_qty"):
        value = row.get(column)
        if value is not None and pd.notna(value) and math.isfinite(float(value)):
            k = max(1, int(value))
            break
    return min(
        max(1, int(row["max_stack_size"])),
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
    *,
    as_of_ts: int | None = None,
    max_publish_seq: int | None = None,
    legacy_fallback: bool = True,
    require_verified_watermark: bool = False,
    enforce_v2_gates: bool = False,
) -> list[dict[str, Any]]:
    """Fit causal models and atomically persist the existing prediction schema."""
    horizons = _validate_horizons(horizons)
    selected = None if item_ids is None else set(item_ids)
    if selected is not None and any(type(item) is not int or item <= 0 for item in selected):
        raise ValueError("item_ids must contain positive integers")
    if not horizons or selected == set():
        return []
    path = _database_path(config, db_path)
    generated_at = int(time.time()) if as_of_ts is None else int(as_of_ts)
    panel = _load_feature_panel(
        config,
        path,
        as_of_ts=generated_at,
        max_publish_seq=max_publish_seq,
        legacy_fallback=legacy_fallback,
        require_verified_watermark=require_verified_watermark,
    )
    if panel.empty:
        return []
    latest = panel.loc[panel["observed_bar"]].groupby("item_id", sort=True).tail(1)
    if selected is not None:
        latest = latest.loc[latest["item_id"].isin(selected)]
    records: list[dict[str, Any]] = []
    for horizon in horizons:
        targets = _target_panel(panel, config, horizon)
        panel_models: dict[int, Forecaster] = {}
        for _, row in latest.sort_values("item_id").iterrows():
            price = pd.to_numeric(row["close_price"], errors="coerce")
            if (
                row["book_state"] != "stocked"
                or pd.isna(price)
                or not math.isfinite(float(price))
                or price <= 0
            ):
                continue
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
            signal = output.signal
            note = output.regime_note
            if enforce_v2_gates and not _v2_fees_verified(config, row):
                if signal == "BUY_WATCH":
                    signal = "WATCH"
                note = f"{note}; v2 fee verification required"
            records.append(
                {
                    "generated_at": generated_at,
                    "item_id": int(row["item_id"]),
                    "horizon_hours": horizon,
                    "model_name": name,
                    "current_price": float(row["close_price"]),
                    "ref_qty": _reference_quantity(config, row),
                    "prob_up": float(output.prob_up),
                    "pred_q10": float(output.pred_q10),
                    "pred_q50": float(output.pred_q50),
                    "pred_q90": float(output.pred_q90),
                    "expected_net_roi": float(output.expected_net_roi),
                    "signal": signal,
                    "regime_note": note,
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
