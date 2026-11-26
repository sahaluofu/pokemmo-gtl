"""Causal market features and execution-aware targets.

The feature builder works on the ``item_bars`` shaped records produced by the
pipeline, but deliberately accepts ordinary mappings and lists of mappings as
well.  Every feature is indexed by a UTC four-hour close timestamp.  A value
at ``close_ts`` therefore only uses records whose timestamp is at most that
timestamp; this is also true for the RP, upstream, manual and event joins.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from math import floor, isfinite, log
from typing import Any

import numpy as np
import pandas as pd

from pokemmo_gtl.quant.fees import breakeven_sell_price, net_profit

GRID_SECONDS = 14_400
DAY_SECONDS = 86_400
REF_NOTIONAL = 500_000

# These are the columns consumed by the model layer.  ``build_feature_frame``
# also retains the source bar columns, which is useful for diagnostics.
FEATURE_COLUMNS = [
    "close_price",
    "open_price",
    "high_price",
    "low_price",
    "robust_best_ask",
    "best_ask_min",
    "p10_price",
    "total_listed_qty",
    "listing_count",
    "depth_within_5pct",
    "spread_top5_pct",
    "bars_since_last_obs",
    "est_sold_qty_24h",
    "new_listed_qty_24h",
    "expired_qty_24h",
    "flow_coverage_24h",
    "flow_imbalance_24h",
    "expiry_pressure_24h",
    "depth_ratio_24h",
    "sell_through_24h",
    "ret_4h",
    "ret_24h",
    "ret_3d",
    "ret_7d",
    "log_ret_4h",
    "log_ret_24h",
    "price_ma_24h",
    "price_ma_7d",
    "price_vs_ma_24h",
    "price_vs_ma_7d",
    "mean_reversion_24h",
    "mean_reversion_7d",
    "volatility_24h",
    "volatility_7d",
    "p10_discount_pct",
    "ask_spread_pct",
    "depth_change_24h",
    "listing_change_24h",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "is_weekend",
    "rp_index",
    "rp_ret_24h",
    "upstream_ret_24h",
    "manual_1d_close_price",
    "manual_1d_p10_price",
    "manual_1d_listed_qty",
    "manual_1d_price_gap",
    "is_in_event",
    "days_to_next_event",
    "days_since_event_end",
]

_PRICE_COLUMNS = (
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "robust_best_ask",
    "best_ask_min",
    "p10_price",
    "vwap_top5_qty",
)
_FLOW_ALIASES = {
    "est_sold_qty": "est_sold_qty_sum",
    "new_listed_qty": "new_listed_qty_sum",
    "expired_qty": "expired_qty_sum",
}
_FLOW_COLUMNS = (
    "est_sold_qty_sum",
    "new_listed_qty_sum",
    "expired_qty_sum",
    "sample_count",
    "valid_flow_samples",
)


def _as_frame(value: Any) -> pd.DataFrame:
    """Convert supported record containers to a private DataFrame copy."""

    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value.copy(deep=True)
    if isinstance(value, Mapping):
        # A mapping of item_id -> records is a convenient form for callers.
        numeric_keys = all(str(key).lstrip("-").isdigit() for key in value)
        if numeric_keys and value and all(
            isinstance(v, (pd.DataFrame, list, tuple)) for v in value.values()
        ):
            pieces: list[pd.DataFrame] = []
            for item_id, records in value.items():
                piece = _as_frame(records)
                if "item_id" not in piece.columns:
                    piece.insert(0, "item_id", item_id)
                pieces.append(piece)
            return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
        return pd.DataFrame(value)
    return pd.DataFrame(list(value))


def _number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _normalise_timestamp(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    if "close_ts" not in frame.columns:
        if "bucket_ts" in frame.columns:
            tf = frame.get("timeframe", pd.Series("4h", index=frame.index))
            seconds = tf.map({"15m": 900, "4h": GRID_SECONDS, "1d": DAY_SECONDS}).fillna(
                GRID_SECONDS
            )
            frame["close_ts"] = _number(frame["bucket_ts"]) + seconds
        elif "recorded_at" in frame.columns:
            frame["close_ts"] = (_number(frame["recorded_at"]) // DAY_SECONDS + 1) * DAY_SECONDS
    if "close_ts" not in frame.columns:
        raise ValueError("bars must contain close_ts (or bucket_ts/recorded_at)")
    frame["close_ts"] = _number(frame["close_ts"])
    frame = frame.loc[frame["close_ts"].notna()].copy()
    frame["close_ts"] = frame["close_ts"].astype("int64")
    if "item_id" not in frame.columns:
        raise ValueError("bars must contain item_id")
    frame["item_id"] = _number(frame["item_id"]).astype("int64")
    return frame


def _normalise_bars(value: Any, *, timeframe: str | None = None) -> pd.DataFrame:
    frame = _as_frame(value)
    if frame.empty:
        return frame
    frame = _normalise_timestamp(frame)
    if timeframe is not None and "timeframe" in frame.columns:
        selected = frame.loc[frame["timeframe"].astype(str).eq(timeframe)].copy()
        # If a caller supplied an unlabelled table, retain it rather than
        # silently returning an empty feature frame.
        if not selected.empty:
            frame = selected
    for old, new in _FLOW_ALIASES.items():
        if old in frame.columns and new not in frame.columns:
            frame[new] = frame[old]
    if "price" in frame.columns and "close_price" not in frame.columns:
        frame["close_price"] = frame["price"]
    if "listed_qty" in frame.columns and "total_listed_qty" not in frame.columns:
        frame["total_listed_qty"] = frame["listed_qty"]
    if "data_source" not in frame.columns:
        frame["data_source"] = "snapshot"
    frame["data_source"] = frame["data_source"].fillna("snapshot").astype(str)
    numeric_columns = _PRICE_COLUMNS + _FLOW_COLUMNS + (
        "total_listed_qty",
        "listing_count",
        "depth_within_5pct",
    )
    for column in set(numeric_columns):
        if column in frame.columns:
            frame[column] = _number(frame[column])
    return frame.sort_values(["item_id", "close_ts"], kind="mergesort").reset_index(drop=True)


def _with_grid(
    source: pd.DataFrame,
    *,
    grid_start: int | None,
    grid_end: int | None,
) -> pd.DataFrame:
    """Reindex each item to a causal, continuous UTC 4h grid."""

    rows: list[pd.DataFrame] = []
    if source.empty:
        return pd.DataFrame(columns=["item_id", "close_ts"])
    for item_id, group in source.groupby("item_id", sort=True):
        group = group.sort_values("close_ts", kind="mergesort")
        group = group.drop_duplicates("close_ts", keep="last")
        first = int(group["close_ts"].min())
        last = int(group["close_ts"].max())
        start = int(grid_start) if grid_start is not None else first
        end = int(grid_end) if grid_end is not None else last
        start = start - start % GRID_SECONDS
        end = end - end % GRID_SECONDS
        if end < start:
            continue
        timestamps = np.arange(start, end + GRID_SECONDS, GRID_SECONDS, dtype="int64")
        index = pd.DataFrame({"close_ts": timestamps})
        index["item_id"] = int(item_id)
        observed = group.copy()
        observed["__observed"] = True
        merged = index.merge(observed, on=["item_id", "close_ts"], how="left", sort=True)
        merged["item_id"] = int(item_id)
        merged["__observed"] = merged["__observed"].fillna(False).astype(bool)
        pure_manual = bool(group["data_source"].eq("manual").all())
        limit = 18 if pure_manual else 6
        for column in _PRICE_COLUMNS:
            if column in merged.columns:
                merged[column] = _number(merged[column]).ffill(limit=limit)
        # The source columns are intentionally not forward filled for flow.
        if "data_source" in merged.columns:
            merged["data_source"] = merged["data_source"].ffill().fillna(
                "manual" if pure_manual else "snapshot"
            )
        positions = np.arange(len(merged), dtype=float)
        last_observed = np.where(merged["__observed"].to_numpy(), positions, np.nan)
        last_observed = pd.Series(last_observed).ffill().to_numpy()
        age = positions - last_observed
        merged["bars_since_last_obs"] = age
        merged.loc[np.isnan(last_observed), "bars_since_last_obs"] = np.nan
        rows.append(merged)
    if rows:
        return pd.concat(rows, ignore_index=True)
    return pd.DataFrame(columns=["item_id", "close_ts"])


def _rolling_sum(series: pd.Series, window: int = 6) -> pd.Series:
    rolling = series.rolling(window=window, min_periods=1)
    return rolling.sum().where(rolling.count().gt(0))


def _safe_return(series: pd.Series, periods: int) -> pd.Series:
    previous = series.shift(periods)
    return series.div(previous.where(previous > 0)).sub(1.0)


def _technical_features(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.sort_values(["item_id", "close_ts"], kind="mergesort").copy()
    grouped = frame.groupby("item_id", sort=False, group_keys=False)

    missing_flow_fields = set()
    for raw in _FLOW_COLUMNS:
        if raw not in frame.columns:
            frame[raw] = np.nan
            if raw in {"est_sold_qty_sum", "new_listed_qty_sum", "expired_qty_sum"}:
                missing_flow_fields.add(raw)
        frame[raw] = _number(frame[raw])
    flow_sums = {}
    for raw in ("est_sold_qty_sum", "new_listed_qty_sum", "expired_qty_sum"):
        flow_sums[raw] = grouped[raw].apply(_rolling_sum).reset_index(level=0, drop=True)
    sample_sum = grouped["sample_count"].apply(_rolling_sum).reset_index(level=0, drop=True)
    valid_sum = grouped["valid_flow_samples"].apply(_rolling_sum).reset_index(level=0, drop=True)
    coverage = valid_sum.div(sample_sum.clip(lower=1))
    frame["flow_coverage_24h"] = coverage.where(coverage.ge(0.5))
    for raw, name in (
        ("est_sold_qty_sum", "est_sold_qty_24h"),
        ("new_listed_qty_sum", "new_listed_qty_24h"),
        ("expired_qty_sum", "expired_qty_24h"),
    ):
        frame[name] = flow_sums[raw].div(coverage.where(coverage.ge(0.5)))
        if raw in missing_flow_fields:
            frame[name] = np.nan

    close = _number(frame.get("close_price", pd.Series(np.nan, index=frame.index)))
    for periods, name in ((1, "ret_4h"), (6, "ret_24h"), (18, "ret_3d"), (42, "ret_7d")):
        returns = grouped["close_price"].apply(lambda s, p=periods: _safe_return(s, p))
        frame[name] = returns.reset_index(level=0, drop=True)
    prior_4h = grouped["close_price"].shift(1)
    prior_24h = grouped["close_price"].shift(6)
    frame["log_ret_4h"] = np.log(close.div(prior_4h.where(prior_4h > 0)))
    frame["log_ret_24h"] = np.log(close.div(prior_24h.where(prior_24h > 0)))

    for periods, suffix in ((6, "24h"), (42, "7d")):
        ma = grouped["close_price"].apply(
            lambda s, p=periods: s.rolling(p, min_periods=1).mean()
        )
        ma = ma.reset_index(level=0, drop=True)
        frame[f"price_ma_{suffix}"] = ma
        frame[f"price_vs_ma_{suffix}"] = close.div(ma.where(ma > 0)).sub(1.0)
        frame[f"mean_reversion_{suffix}"] = -frame[f"price_vs_ma_{suffix}"]
        vol = grouped["log_ret_4h"].apply(
            lambda s, p=periods: s.rolling(p, min_periods=2).std()
        )
        vol = vol.reset_index(level=0, drop=True)
        frame[f"volatility_{suffix}"] = vol

    sold = frame["est_sold_qty_24h"]
    listed = frame.get("total_listed_qty", pd.Series(np.nan, index=frame.index))
    new_listed = frame["new_listed_qty_24h"]
    expired = frame["expired_qty_24h"]
    frame["flow_imbalance_24h"] = (sold - new_listed).div(sold + new_listed + 1.0)
    frame["expiry_pressure_24h"] = expired.div(sold + expired + 1.0)
    depth = _number(frame.get("depth_within_5pct", pd.Series(np.nan, index=frame.index)))
    if "depth_within_5pct" in frame:
        depth_mean = grouped["depth_within_5pct"].apply(
            lambda s: s.rolling(6, min_periods=1).mean()
        )
        depth_mean = depth_mean.reset_index(level=0, drop=True)
    else:
        depth_mean = pd.Series(np.nan, index=frame.index)
    frame["depth_ratio_24h"] = depth.div(depth_mean + 1.0)
    frame["sell_through_24h"] = sold.div(sold + listed + 1.0)
    if "depth_within_5pct" in frame:
        depth_change = grouped["depth_within_5pct"].apply(lambda s: _safe_return(s, 6))
        frame["depth_change_24h"] = depth_change.reset_index(level=0, drop=True)
    else:
        frame["depth_change_24h"] = np.nan
    if "total_listed_qty" in frame:
        listing_change = grouped["total_listed_qty"].apply(lambda s: _safe_return(s, 6))
        frame["listing_change_24h"] = listing_change.reset_index(level=0, drop=True)
    else:
        frame["listing_change_24h"] = np.nan

    ask = _number(frame.get("robust_best_ask", pd.Series(np.nan, index=frame.index)))
    p10 = _number(frame.get("p10_price", pd.Series(np.nan, index=frame.index)))
    spread = _number(frame.get("spread_top5_pct", pd.Series(np.nan, index=frame.index)))
    if "vwap_top5_qty" in frame:
        fallback_spread = _number(frame["vwap_top5_qty"]).sub(ask).div(ask.clip(lower=1))
        spread = spread.where(spread.notna(), fallback_spread)
    frame["spread_top5_pct"] = spread
    frame["ask_spread_pct"] = spread
    frame["p10_discount_pct"] = p10.div(close.where(close > 0)).sub(1.0)

    timestamps = pd.to_datetime(frame["close_ts"], unit="s", utc=True)
    hour = timestamps.dt.hour + timestamps.dt.minute / 60.0
    dow = timestamps.dt.day_of_week
    frame["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    frame["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    frame["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    frame["dow_cos"] = np.cos(2 * np.pi * dow / 7)
    frame["is_weekend"] = dow.ge(5).astype(int)
    return frame


def _relation_mapping(
    item_relations: Mapping[str, Any] | None, key: str
) -> list[Mapping[str, Any]]:
    if not isinstance(item_relations, Mapping):
        return []
    values = item_relations.get(key, [])
    if isinstance(values, Mapping):
        values = [values]
    return [value for value in values if isinstance(value, Mapping)]


def _source_for_item(source: pd.DataFrame, item_id: int) -> pd.DataFrame:
    if source.empty or "item_id" not in source.columns:
        return pd.DataFrame(columns=["close_ts", "close_price"])
    result = source.loc[source["item_id"].eq(item_id)].copy()
    if result.empty or "close_price" not in result.columns:
        return pd.DataFrame(columns=["close_ts", "close_price"])
    result["close_price"] = _number(result["close_price"])
    result = result.loc[result["close_price"].notna()].sort_values("close_ts")
    return result.drop_duplicates("close_ts", keep="last")


def _asof_price(
    timeline: pd.DataFrame, source: pd.DataFrame, column: str = "close_price"
) -> pd.Series:
    if source.empty:
        return pd.Series(np.nan, index=timeline.index, dtype=float)
    right = source[["close_ts", column]].rename(columns={column: "__value"}).sort_values("close_ts")
    left = timeline[["close_ts"]].sort_values("close_ts")
    joined = pd.merge_asof(left, right, on="close_ts", direction="backward")
    return joined.set_index(left.index).reindex(timeline.index)["__value"]


def _add_rp_features(
    frame: pd.DataFrame,
    source: pd.DataFrame,
    relations: list[Mapping[str, Any]],
    rp_bars: pd.DataFrame,
) -> pd.DataFrame:
    frame = frame.copy()
    frame["rp_index"] = np.nan
    if not relations:
        frame["rp_ret_24h"] = np.nan
        return frame
    voucher_source = rp_bars if not rp_bars.empty else source
    timeline = pd.DataFrame({"close_ts": np.sort(frame["close_ts"].unique())})
    points: list[pd.Series] = []
    for relation in relations:
        item_id = relation.get("item_id")
        denomination = pd.to_numeric(
            pd.Series([relation.get("denomination")]), errors="coerce"
        ).iloc[0]
        if item_id is None or not isfinite(float(denomination)) or denomination <= 0:
            continue
        values = _source_for_item(voucher_source, int(item_id))
        if values.empty:
            continue
        values = values.copy()
        values["__rp"] = values["close_price"] / float(denomination)
        points.append(_asof_price(timeline, values.rename(columns={"__rp": "close_price"})))
    if not points:
        frame["rp_ret_24h"] = np.nan
        return frame
    rp = pd.concat(points, axis=1).median(axis=1, skipna=True)
    rp_by_ts = pd.DataFrame({"close_ts": timeline["close_ts"], "rp_index": rp.to_numpy()})
    rp_by_ts["rp_ret_24h"] = rp_by_ts["rp_index"].div(
        rp_by_ts["rp_index"].shift(6)
    ).sub(1.0)
    index_lookup = dict(zip(rp_by_ts["close_ts"], rp_by_ts["rp_index"], strict=True))
    return_values = dict(zip(rp_by_ts["close_ts"], rp_by_ts["rp_ret_24h"], strict=True))
    frame["rp_index"] = frame["close_ts"].map(index_lookup)
    frame["rp_ret_24h"] = frame["close_ts"].map(return_values)
    return frame


def _return_series(source: pd.DataFrame, item_id: int) -> pd.DataFrame:
    values = _source_for_item(source, item_id)
    if values.empty:
        return pd.DataFrame(columns=["close_ts", "upstream_ret_24h"])
    timeline = values[["close_ts"]].drop_duplicates().sort_values("close_ts")
    prices = values[["close_ts", "close_price"]]
    now = pd.merge_asof(timeline, prices, on="close_ts", direction="backward")
    prior = timeline.assign(close_ts=timeline["close_ts"] - DAY_SECONDS)
    prior = pd.merge_asof(prior, prices, on="close_ts", direction="backward")
    result = timeline.copy()
    result["upstream_ret_24h"] = now["close_price"].div(prior["close_price"]).sub(1.0).to_numpy()
    return result


def _add_upstream_features(
    frame: pd.DataFrame,
    source: pd.DataFrame,
    relations: list[Mapping[str, Any]],
    upstream_bars: pd.DataFrame,
) -> pd.DataFrame:
    frame = frame.copy()
    frame["upstream_ret_24h"] = np.nan
    source = upstream_bars if not upstream_bars.empty else source
    if not relations:
        return frame
    for target_id in frame["item_id"].dropna().unique():
        target_relations = [
            r for r in relations if int(r.get("target_item_id", -1)) == int(target_id)
        ]
        if not target_relations:
            continue
        timeline = frame.loc[frame["item_id"].eq(target_id), ["close_ts"]]
        timeline = timeline.drop_duplicates().sort_values("close_ts")
        weighted: list[pd.Series] = []
        weights: list[float] = []
        for relation in target_relations:
            upstream_id = relation.get("upstream_item_id")
            if upstream_id is None:
                continue
            series = _return_series(source, int(upstream_id))
            if series.empty:
                continue
            joined = pd.merge_asof(timeline, series, on="close_ts", direction="backward")
            weight = pd.to_numeric(
                pd.Series([relation.get("weight", 1.0)]), errors="coerce"
            ).iloc[0]
            weight = float(weight)
            if not isfinite(weight):
                continue
            weighted.append(joined["upstream_ret_24h"] * weight)
            weights.append(weight)
        if weighted:
            value = pd.concat(weighted, axis=1).sum(axis=1, min_count=1)
            if weights and sum(abs(weight) for weight in weights) > 0:
                value = value / sum(abs(weight) for weight in weights)
            lookup = pd.DataFrame({"close_ts": timeline["close_ts"], "__up": value.to_numpy()})
            selected = frame["item_id"].eq(target_id)
            frame.loc[selected, "upstream_ret_24h"] = frame.loc[selected, ["close_ts"]].merge(
                lookup, on="close_ts", how="left"
            )["__up"].to_numpy()
    return frame


def _add_manual_features(frame: pd.DataFrame, manual_bars: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    manual_columns = (
        "manual_1d_close_price",
        "manual_1d_p10_price",
        "manual_1d_listed_qty",
        "manual_1d_price_gap",
    )
    for column in manual_columns:
        frame[column] = np.nan
    if manual_bars.empty:
        return frame
    manual = manual_bars.copy()
    if "close_price" not in manual.columns and "price" in manual.columns:
        manual["close_price"] = manual["price"]
    if "p10_price" not in manual.columns:
        manual["p10_price"] = np.nan
    if "total_listed_qty" not in manual.columns and "listed_qty" in manual.columns:
        manual["total_listed_qty"] = manual["listed_qty"]
    for item_id in frame["item_id"].dropna().unique():
        left_index = frame.index[frame["item_id"].eq(item_id)]
        left = frame.loc[left_index, ["close_ts"]].sort_values("close_ts")
        right_columns = ["close_ts", "close_price", "p10_price", "total_listed_qty"]
        right = manual.loc[manual["item_id"].eq(item_id), right_columns]
        right = right.dropna(subset=["close_ts"]).sort_values("close_ts")
        right = right.drop_duplicates("close_ts", keep="last")
        if right.empty:
            continue
        joined = pd.merge_asof(left, right, on="close_ts", direction="backward")
        frame.loc[left_index, "manual_1d_close_price"] = joined["close_price"].to_numpy()
        frame.loc[left_index, "manual_1d_p10_price"] = joined["p10_price"].to_numpy()
        frame.loc[left_index, "manual_1d_listed_qty"] = joined["total_listed_qty"].to_numpy()
        frame.loc[left_index, "manual_1d_price_gap"] = joined["close_price"].div(
            frame.loc[left_index, "close_price"].to_numpy()
        ).sub(1.0).to_numpy()
    return frame


def _events_frame(value: Any) -> pd.DataFrame:
    events = _as_frame(value)
    if events.empty:
        return events
    for column in ("announced_at", "start_ts", "end_ts"):
        if column not in events.columns:
            events[column] = np.nan
        events[column] = _number(events[column])
    return events.dropna(subset=["announced_at", "start_ts", "end_ts"]).sort_values("start_ts")


def _add_event_features(frame: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["is_in_event"] = 0
    frame["days_to_next_event"] = -1.0
    frame["days_since_event_end"] = -1.0
    if events.empty:
        return frame
    event_records = events.to_dict("records")
    for index, timestamp in frame["close_ts"].items():
        known = [event for event in event_records if event["announced_at"] <= timestamp]
        if not known:
            continue
        frame.at[index, "is_in_event"] = int(
            any(event["start_ts"] <= timestamp < event["end_ts"] for event in known)
        )
        future = [event["start_ts"] for event in known if event["start_ts"] >= timestamp]
        if future:
            frame.at[index, "days_to_next_event"] = (min(future) - timestamp) / DAY_SECONDS
        ended = [event["end_ts"] for event in known if event["end_ts"] <= timestamp]
        if ended:
            frame.at[index, "days_since_event_end"] = (timestamp - max(ended)) / DAY_SECONDS
    return frame


def build_feature_frame(
    bars: Any = None,
    *,
    item_bars: Any = None,
    rp_bars: Any = None,
    upstream_bars: Any = None,
    manual_bars: Any = None,
    market_events: Any = None,
    events: Any = None,
    config: Any = None,
    item_relations: Mapping[str, Any] | None = None,
    relations: Mapping[str, Any] | None = None,
    grid_start: int | None = None,
    grid_end: int | None = None,
) -> pd.DataFrame:
    """Build a causal feature panel from 4h item bars.

    ``bars`` may be a DataFrame, a list of records or a mapping of item IDs to
    records.  When a table contains multiple timeframes only its ``4h`` rows
    are used as the primary panel; pass ``manual_bars`` for separate ``1d``
    manual quotes.  Missing optional tables simply produce NaN feature columns.
    """

    primary_input = item_bars if item_bars is not None else bars
    primary = _normalise_bars(primary_input, timeframe="4h")
    if primary.empty:
        return pd.DataFrame(columns=["item_id", "close_ts", *FEATURE_COLUMNS])
    manual_input = manual_bars
    if manual_input is None and "timeframe" in _as_frame(primary_input).columns:
        all_bars = _normalise_bars(primary_input)
        if "timeframe" in all_bars:
            manual_input = all_bars.loc[all_bars["timeframe"].eq("1d")]
    manual = (
        _normalise_bars(manual_input, timeframe="1d")
        if manual_input is not None
        else pd.DataFrame()
    )
    rp = _normalise_bars(rp_bars, timeframe="4h") if rp_bars is not None else pd.DataFrame()
    upstream = (
        _normalise_bars(upstream_bars, timeframe="4h")
        if upstream_bars is not None
        else pd.DataFrame()
    )
    frame = _with_grid(primary, grid_start=grid_start, grid_end=grid_end)
    frame = _technical_features(frame)

    if item_relations is not None:
        relation_data = item_relations
    elif relations is not None:
        relation_data = relations
    else:
        relation_data = getattr(config, "item_relations", None)
    rp_relations = _relation_mapping(relation_data, "rp_vouchers")
    upstream_relations = _relation_mapping(relation_data, "upstream_relations")
    frame = _add_rp_features(frame, primary, rp_relations, rp)
    frame = _add_upstream_features(frame, primary, upstream_relations, upstream)
    frame = _add_manual_features(frame, manual)
    if market_events is not None:
        event_input = market_events
    elif events is not None:
        event_input = events
    else:
        event_input = getattr(config, "events", None)
    frame = _add_event_features(frame, _events_frame(event_input))

    for column in FEATURE_COLUMNS:
        if column not in frame.columns:
            frame[column] = np.nan
    frame = frame.drop(columns=["__observed"], errors="ignore")
    frame = frame.sort_values(["item_id", "close_ts"], kind="mergesort").reset_index(drop=True)
    return frame


def _config_value(config: Any, name: str, default: Any) -> Any:
    if config is None:
        return default
    if isinstance(config, Mapping):
        return config.get(name, default)
    return getattr(config, name, default)


def _target_value(
    row: pd.Series,
    future: Mapping[tuple[int, int], pd.Series],
    horizon_seconds: int,
    name: str,
) -> Any:
    key = (int(row["item_id"]), int(row["close_ts"]) + horizon_seconds)
    target = future.get(key)
    return target.get(name) if target is not None else np.nan


def add_targets(
    df: pd.DataFrame,
    horizon_hours: int,
    fees_cfg: Any,
    *,
    ref_notional: int = REF_NOTIONAL,
    max_stack_size: int | None = None,
) -> pd.DataFrame:
    """Append execution-friction-aware regression and classification labels.

    The entry is the current close and the exit is the future maker price
    ``min(close_price, p10_price)``.  Rows without an exact future grid point
    retain NaN targets, which avoids silently shortening a holding period.
    """

    if not isinstance(horizon_hours, int) or isinstance(horizon_hours, bool) or horizon_hours <= 0:
        raise ValueError("horizon_hours must be a positive integer")
    result = df.copy(deep=True)
    required = {"item_id", "close_ts", "close_price"}
    missing = required.difference(result.columns)
    if missing:
        raise ValueError(f"missing target columns: {', '.join(sorted(missing))}")
    horizon_seconds = horizon_hours * 3600
    future: dict[tuple[int, int], pd.Series] = {}
    target_columns = [column for column in ("close_price", "p10_price") if column in result.columns]
    for _, row in result.iterrows():
        future[(int(row["item_id"]), int(row["close_ts"]))] = row

    target_close = result.apply(
        lambda row: _target_value(row, future, horizon_seconds, "close_price"), axis=1
    )
    target_p10 = result.apply(
        lambda row: (
            _target_value(row, future, horizon_seconds, "p10_price")
            if "p10_price" in target_columns
            else np.nan
        ),
        axis=1,
    )
    entry = _number(result["close_price"])
    exit_price = target_close.where(target_p10.isna(), np.minimum(target_close, target_p10))
    valid = entry.gt(0) & exit_price.gt(0)
    result["target_close_price"] = target_close
    result["target_p10_price"] = target_p10
    result["p_exit"] = exit_price
    result["y_reg"] = np.nan
    result["y_cls"] = np.nan
    result["ref_qty"] = np.nan
    result["net_profit"] = np.nan
    result["breakeven_sell_price"] = np.nan

    item_min_fee = int(_config_value(fees_cfg, "item_min_fee", 100))
    pokemon_min_fee = int(_config_value(fees_cfg, "pokemon_min_fee", item_min_fee))
    rate = _config_value(fees_cfg, "rate", "0.05")
    relist_rate = _config_value(fees_cfg, "default_relist_rate", "0.15")
    max_fee = int(_config_value(fees_cfg, "max_fee_per_slot", 25_000))
    notional = int(ref_notional)

    for index in result.index[valid.fillna(False)]:
        row = result.loc[index]
        default_stack = max_stack_size if max_stack_size is not None else 99
        stack_value = row.get("max_stack_size", default_stack)
        try:
            stack = max(1, int(stack_value))
        except (TypeError, ValueError):
            stack = max_stack_size if max_stack_size is not None else 99
        k_value = row.get("K_t", row.get("k_t", np.nan))
        if pd.isna(k_value):
            for candidate in ("depth_within_5pct", "listing_count", "total_listed_qty"):
                if candidate in row and pd.notna(row[candidate]):
                    k_value = row[candidate]
                    break
        try:
            k = max(1, int(k_value)) if pd.notna(k_value) else 1
        except (TypeError, ValueError):
            k = 1
        qty = min(stack, k, max(1, floor(notional / max(float(entry.loc[index]), 1.0))))
        asset_class = str(row.get("asset_class", ""))
        is_pokemon = asset_class in {"pokemon", "pokemon_category"}
        is_synthetic_pokemon = int(row["item_id"]) >= 900000
        min_fee = pokemon_min_fee if is_pokemon or is_synthetic_pokemon else item_min_fee
        buy = float(entry.loc[index])
        sell = float(exit_price.loc[index])
        profit = net_profit(
            buy,
            sell,
            qty,
            stack,
            min_fee=min_fee,
            rate=rate,
            relist_rate=relist_rate,
            max_fee_per_slot=max_fee,
        )
        break_even = breakeven_sell_price(
            int(round(buy)),
            qty,
            stack,
            min_fee=min_fee,
            rate=rate,
            relist_rate=relist_rate,
            max_fee_per_slot=max_fee,
        )
        result.at[index, "ref_qty"] = qty
        result.at[index, "net_profit"] = float(profit)
        result.at[index, "breakeven_sell_price"] = break_even
        result.at[index, "y_reg"] = log(sell / buy)
        result.at[index, "y_cls"] = int(profit > Decimal("0"))
    return result


__all__ = ["FEATURE_COLUMNS", "build_feature_frame", "add_targets"]
