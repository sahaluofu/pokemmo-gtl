"""M4 adversarial tests for strict close-time feature causality."""

from __future__ import annotations

from math import isclose, log
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pokemmo_gtl.config import Fees
from pokemmo_gtl.quant.features import FEATURE_COLUMNS, add_targets, build_feature_frame
from pokemmo_gtl.quant.fees import net_profit

STEP = 14_400
BASE = 1_700_006_400
ITEM_RELATIONS = {
    "rp_vouchers": [
        {"item_id": 1190, "denomination": 100},
        {"item_id": 1191, "denomination": 500},
    ],
    "upstream_relations": [
        {"target_item_id": 148, "upstream_item_id": 201, "weight": 1.0},
    ],
}


@pytest.fixture
def tmp_path(request):
    """Keep the repository guard valid with the task's alternate basetemp."""

    path = Path.cwd() / ".pytest_tmp" / f"m4_{request.node.name}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def make_bars() -> pd.DataFrame:
    rows = []
    item_ids = (148, 1190, 1191, 201)
    for item_id in item_ids:
        for offset in range(60):
            close_ts = BASE + offset * STEP
            price = 100 + item_id % 17 + offset * (0.2 + item_id % 3 * 0.05)
            rows.append(
                {
                    "item_id": item_id,
                    "timeframe": "4h",
                    "close_ts": close_ts,
                    "data_source": "snapshot",
                    "open_price": price - 0.5,
                    "high_price": price + 1.0,
                    "low_price": price - 1.0,
                    "close_price": price,
                    "robust_best_ask": price + 1,
                    "best_ask_min": price - 0.2,
                    "p10_price": price - 0.5,
                    "total_listed_qty": 80 + offset,
                    "listing_count": 12 + offset % 5,
                    "depth_within_5pct": 20 + offset,
                    "spread_top5_pct": 0.025,
                    "est_sold_qty_sum": 10 + offset % 7,
                    "new_listed_qty_sum": 12 + offset % 6,
                    "expired_qty_sum": 2 + offset % 3,
                    "sample_count": 4,
                    "valid_flow_samples": 3,
                }
            )
    return pd.DataFrame(rows)


def make_manual_bars() -> pd.DataFrame:
    rows = []
    for day in range(10):
        close_ts = BASE + day * 6 * STEP
        rows.append(
            {
                "item_id": 148,
                "timeframe": "1d",
                "close_ts": close_ts,
                "data_source": "manual",
                "close_price": 130 + day,
                "p10_price": 128 + day,
                "total_listed_qty": 40 + day,
            }
        )
    return pd.DataFrame(rows)


def scale_future_market_values(frame: pd.DataFrame, threshold: int) -> pd.DataFrame:
    changed = frame.copy(deep=True)
    future = changed["close_ts"].gt(threshold)
    for column in (
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "robust_best_ask",
        "best_ask_min",
        "p10_price",
    ):
        if column in changed:
            changed.loc[future, column] = changed.loc[future, column] * 100
    for column in (
        "total_listed_qty",
        "listing_count",
        "depth_within_5pct",
        "est_sold_qty_sum",
        "new_listed_qty_sum",
        "expired_qty_sum",
    ):
        if column in changed:
            changed.loc[future, column] = changed.loc[future, column] * 100
    return changed


def build_panel(bars, manual, events):
    return build_feature_frame(
        bars,
        rp_bars=bars.loc[bars["item_id"].isin([1190, 1191])],
        upstream_bars=bars.loc[bars["item_id"].eq(201)],
        manual_bars=manual,
        market_events=events,
        item_relations=ITEM_RELATIONS,
    )


def test_three_way_future_perturbation_leaves_all_prior_features_identical():
    bars = make_bars()
    manual = make_manual_bars()
    events = pd.DataFrame()
    threshold = BASE + 42 * STEP
    before = build_panel(bars, manual, events)

    # Mutate every item's future prices and quantities, including RP and
    # upstream series, then mutate the separately supplied 1d manual table.
    changed_bars = scale_future_market_values(bars, threshold)
    changed_manual = scale_future_market_values(manual, threshold)
    surprise = pd.DataFrame(
        [
            {
                "name": "unannounced raid",
                "event_type": "patch",
                "announced_at": threshold + 3_600,
                "start_ts": threshold + 86_400,
                "end_ts": threshold + 90_000,
                "impact_scope": "all",
            }
        ]
    )
    changed_events = pd.concat([events, surprise], ignore_index=True)
    after = build_panel(changed_bars, changed_manual, changed_events)

    before_prefix = before.loc[before["close_ts"].le(threshold)].reset_index(drop=True)
    after_prefix = after.loc[after["close_ts"].le(threshold)].reset_index(drop=True)
    assert_frame_equal(before_prefix, after_prefix, check_exact=True, check_dtype=True)
    assert set(FEATURE_COLUMNS).issubset(before.columns)
    threshold_row = before_prefix.loc[before_prefix["close_ts"].eq(threshold)]
    assert threshold_row["days_to_next_event"].iloc[0] == -1.0


def test_no_rp_and_manual_null_order_book_are_supported():
    manual_only = pd.DataFrame(
        [
            {
                "item_id": 77,
                "timeframe": "4h",
                "close_ts": BASE + offset * STEP,
                "data_source": "manual",
                "close_price": 50 + offset,
                "robust_best_ask": np.nan,
                "best_ask_min": np.nan,
                "p10_price": np.nan,
                "total_listed_qty": np.nan,
                "listing_count": np.nan,
                "depth_within_5pct": np.nan,
                "spread_top5_pct": np.nan,
            }
            for offset in (0, 2, 4, 6)
        ]
    )
    frame = build_feature_frame(manual_only, item_relations=None)
    assert len(frame) == 7
    assert frame["rp_index"].isna().all()
    assert frame["upstream_ret_24h"].isna().all()
    assert frame["robust_best_ask"].isna().all()
    assert frame["ask_spread_pct"].isna().all()
    assert frame["bars_since_last_obs"].tolist() == [0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0]
    assert frame["est_sold_qty_24h"].isna().all()
    assert frame["new_listed_qty_24h"].isna().all()

    sparse_snapshot = pd.DataFrame(
        [
            {
                "item_id": 78,
                "close_ts": BASE + offset * STEP,
                "data_source": "snapshot",
                "close_price": price,
            }
            for offset, price in ((0, 10), (8, 20))
        ]
    )
    snapshot_frame = build_feature_frame(sparse_snapshot)
    assert snapshot_frame.loc[:6, "close_price"].eq(10).all()
    assert pd.isna(snapshot_frame.loc[7, "close_price"])
    assert snapshot_frame.loc[8, "close_price"] == 20
    assert snapshot_frame.loc[7, "bars_since_last_obs"] == 7

    sparse_manual = pd.DataFrame(
        [
            {
                "item_id": 79,
                "close_ts": BASE + offset * STEP,
                "data_source": "manual",
                "close_price": price,
                "robust_best_ask": np.nan,
                "p10_price": np.nan,
            }
            for offset, price in ((0, 10), (20, 20))
        ]
    )
    manual_frame = build_feature_frame(sparse_manual)
    assert manual_frame.loc[:18, "close_price"].eq(10).all()
    assert pd.isna(manual_frame.loc[19, "close_price"])
    assert manual_frame.loc[20, "close_price"] == 20


def test_targets_use_future_maker_price_reference_quantity_and_fees():
    bars = make_bars().loc[lambda frame: frame["item_id"].eq(148)].copy()
    features = build_feature_frame(bars)
    fees = Fees(
        rate="0.05",
        item_min_fee=100,
        pokemon_min_fee=1000,
        max_fee_per_slot=25_000,
        default_relist_rate="0.15",
    )
    targets = add_targets(features, 24, fees)
    entry_index = 10
    exit_index = entry_index + 6
    entry = features.loc[entry_index, "close_price"]
    exit_price = min(features.loc[exit_index, "close_price"], features.loc[exit_index, "p10_price"])
    ref_qty = min(
        99,
        int(features.loc[entry_index, "depth_within_5pct"]),
        max(1, int(500_000 // entry)),
    )
    expected_profit = net_profit(
        entry,
        exit_price,
        ref_qty,
        99,
        min_fee=100,
        rate="0.05",
        relist_rate="0.15",
        max_fee_per_slot=25_000,
    )
    assert targets.loc[entry_index, "ref_qty"] == ref_qty
    assert isclose(targets.loc[entry_index, "p_exit"], exit_price)
    assert isclose(targets.loc[entry_index, "y_reg"], log(exit_price / entry))
    assert targets.loc[entry_index, "net_profit"] == float(expected_profit)
    assert targets.loc[entry_index, "y_cls"] == int(expected_profit > 0)
    assert pd.isna(targets.loc[len(targets) - 1, "y_reg"])
