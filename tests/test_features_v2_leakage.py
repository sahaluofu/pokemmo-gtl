"""V2 regression tests for causal constituents and stocked-only quote joins."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pokemmo_gtl.quant.features import (
    FEATURE_COLUMNS,
    _asof_price,
    _return_series,
    _source_for_item,
    add_targets,
    build_feature_frame,
    compute_recipe_floor_cost,
    compute_rp_yen_per_unit,
)

STEP = 14_400
DAY = 86_400
BASE = 1_700_006_400
TARGET_ID = 138
PRICE_COLUMNS = [
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "robust_best_ask",
    "best_ask_min",
    "p10_price",
    "vwap_top5_qty",
]


def _bar(
    item_id: int,
    close_ts: int,
    price: float,
    book_state: str = "stocked",
) -> dict:
    stocked = book_state == "stocked"
    return {
        "item_id": item_id,
        "timeframe": "4h",
        "close_ts": close_ts,
        "data_source": "snapshot",
        "book_state": book_state,
        "open_price": price,
        "high_price": price,
        "low_price": price,
        "close_price": price,
        "robust_best_ask": price if stocked else np.nan,
        "best_ask_min": price,
        "p10_price": price if stocked else np.nan,
        "vwap_top5_qty": price if stocked else np.nan,
        "total_listed_qty": 20 if stocked else 0,
        "listing_count": 5 if stocked else 0,
        "depth_within_5pct": 10 if stocked else 0,
    }


def _target_bars(last_offset: int = 14) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _bar(TARGET_ID, BASE + offset * STEP, 100.0 + offset)
            for offset in range(last_offset + 1)
        ]
    )


def _target_row(frame: pd.DataFrame, timestamp: int) -> pd.Series:
    return frame.loc[
        frame["item_id"].eq(TARGET_ID) & frame["close_ts"].eq(timestamp)
    ].iloc[0]


def test_future_first_upstream_quotes_do_not_dilute_historical_return_bitwise():
    """Exact Final P1-5 repro: the historical return must never become 0.5."""

    timestamp = BASE + DAY
    primary = _target_bars()
    relations = {
        "upstream_relations": [
            {
                "target_item_id": TARGET_ID,
                "upstream_item_id": 1031,
                "weight": 0.5,
            },
            {
                "target_item_id": TARGET_ID,
                "upstream_item_id": 1034,
                "weight": 0.5,
            },
        ]
    }
    upstream = pd.DataFrame(
        [
            _bar(1031, BASE, 100.0),
            _bar(1031, timestamp, 200.0),
        ]
    )
    before = build_feature_frame(
        primary,
        upstream_bars=upstream,
        item_relations=relations,
    )
    original = np.float64(_target_row(before, timestamp)["upstream_ret_24h"])
    assert original == 1.0

    future_first_quotes = pd.DataFrame(
        [
            _bar(1034, timestamp + 14_400, 100.0),
            _bar(1034, timestamp + 100_800, 200.0),
        ]
    )
    changed_upstream = pd.concat(
        [upstream, future_first_quotes], ignore_index=True
    )
    after = build_feature_frame(
        primary,
        upstream_bars=changed_upstream,
        item_relations=relations,
    )
    recomputed = np.float64(_target_row(after, timestamp)["upstream_ret_24h"])
    assert recomputed == 1.0
    assert recomputed.tobytes() == original.tobytes()

    before_prefix = before.loc[before["close_ts"].le(timestamp)].reset_index(drop=True)
    after_prefix = after.loc[after["close_ts"].le(timestamp)].reset_index(drop=True)
    assert_frame_equal(
        before_prefix,
        after_prefix,
        check_exact=True,
        check_dtype=True,
    )


def test_rp_quotes_expire_individually_and_all_stale_vouchers_yield_nan():
    vouchers = pd.DataFrame(
        [
            _bar(1190, BASE, 10_000.0),
            _bar(1191, BASE + STEP, 60_000.0),
        ]
    )
    relations = [
        {"item_id": 1190, "denomination": 100},
        {"item_id": 1191, "denomination": 500},
    ]
    result = compute_rp_yen_per_unit(
        vouchers,
        relations,
        timeline_ts=np.array(
            [BASE + STEP, BASE + 2 * STEP, BASE + 2 * STEP + 1],
            dtype="int64",
        ),
        max_quote_age_sec=STEP,
    )
    assert result.columns.tolist() == ["close_ts", "rp_index", "rp_ret_24h"]
    # At the inclusive age boundary both vouchers are fresh: median(100, 120).
    assert result.loc[0, "rp_index"] == 110.0
    # The first voucher has expired; the second remains at its age boundary.
    assert result.loc[1, "rp_index"] == 120.0
    assert pd.isna(result.loc[2, "rp_index"])
    assert result["rp_ret_24h"].isna().all()


def test_rp_return_uses_actual_24h_prices_and_yen_per_rp_units():
    timestamp = BASE + DAY
    vouchers = pd.DataFrame(
        [
            _bar(1190, BASE, 10_000.0),
            _bar(1190, timestamp, 20_000.0),
            _bar(1191, BASE, 50_000.0),
            _bar(1191, timestamp, 100_000.0),
        ]
    )
    result = compute_rp_yen_per_unit(
        vouchers,
        [
            {"item_id": 1190, "denomination": 100},
            {"item_id": 1191, "denomination": 500},
        ],
        timeline_ts=pd.Series([timestamp]),
    )
    assert len(result) == 1
    assert result.loc[0, "rp_index"] == 200.0
    assert result.loc[0, "rp_ret_24h"] == 1.0


def test_asof_price_enforces_default_age_and_preserves_timeline_order():
    timeline = pd.DataFrame(
        {"close_ts": [BASE + DAY + 1, BASE, BASE + DAY]},
        index=[8, 3, 9],
    )
    source = pd.DataFrame({"close_ts": [BASE], "close_price": [10.0]})
    fresh = _asof_price(timeline, source)
    assert fresh.index.tolist() == [8, 3, 9]
    assert pd.isna(fresh.loc[8])
    assert fresh.loc[3] == 10.0
    assert fresh.loc[9] == 10.0

    unlimited = _asof_price(timeline, source, max_age_sec=None)
    assert unlimited.index.tolist() == [8, 3, 9]
    assert unlimited.eq(10.0).all()


@pytest.mark.parametrize("separate_sources", [False, True])
@pytest.mark.parametrize("empty_state", ["mixed_ended_empty", "empty_confirmed"])
def test_nonstocked_rp_and_upstream_quotes_invalidate_prior_live_quotes(
    separate_sources: bool,
    empty_state: str,
):
    timestamp = BASE + DAY
    empty_ts = timestamp + STEP
    restock_ts = timestamp + 3 * STEP
    empty_price = 5_000.0 if empty_state == "mixed_ended_empty" else np.nan
    primary = _target_bars(last_offset=9)
    rp = pd.DataFrame(
        [
            _bar(1190, BASE, 10_000.0),
            _bar(1190, timestamp, 20_000.0),
            _bar(1190, empty_ts, empty_price, empty_state),
            _bar(1190, restock_ts, 30_000.0),
        ]
    )
    upstream = pd.DataFrame(
        [
            _bar(1031, BASE, 100.0),
            _bar(1031, timestamp, 200.0),
            _bar(1031, empty_ts, empty_price, empty_state),
            _bar(1031, restock_ts, 300.0),
        ]
    )
    relations = {
        "rp_vouchers": [{"item_id": 1190, "denomination": 100}],
        "upstream_relations": [
            {
                "target_item_id": TARGET_ID,
                "upstream_item_id": 1031,
                "weight": 1.0,
            }
        ],
    }
    if separate_sources:
        result = build_feature_frame(
            primary,
            rp_bars=rp,
            upstream_bars=upstream,
            item_relations=relations,
        )
    else:
        result = build_feature_frame(
            pd.concat([primary, rp, upstream], ignore_index=True),
            item_relations=relations,
        )

    assert _target_row(result, timestamp)["rp_index"] == 200.0
    assert _target_row(result, timestamp)["upstream_ret_24h"] == 1.0
    for query_ts in (empty_ts, empty_ts + STEP):
        row = _target_row(result, query_ts)
        assert pd.isna(row["rp_index"])
        assert pd.isna(row["upstream_ret_24h"])
    assert _target_row(result, restock_ts)["rp_index"] == 300.0
    assert _target_row(result, restock_ts)["upstream_ret_24h"] == 2.0

    for quotes, item_id in ((rp, 1190), (upstream, 1031)):
        live = _source_for_item(quotes, item_id)
        assert live["book_state"].eq("stocked").all()
        assert not live["close_ts"].eq(empty_ts).any()


@pytest.mark.parametrize("empty_state", ["empty_confirmed", "mixed_ended_empty"])
def test_grid_price_fills_stop_at_empty_books_until_restock(empty_state: str):
    empty_price = 5_000.0 if empty_state == "mixed_ended_empty" else np.nan
    source = pd.DataFrame(
        [
            _bar(TARGET_ID, BASE, 100.0),
            _bar(TARGET_ID, BASE + 2 * STEP, empty_price, empty_state),
            _bar(TARGET_ID, BASE + 5 * STEP, 200.0),
        ]
    )
    original = source.copy(deep=True)
    result = build_feature_frame(source)

    # The ordinary missing row before the empty observation can still fill.
    assert result.loc[1, PRICE_COLUMNS].eq(100.0).all()
    # Neither the pre-empty quote nor mixed-bar historical prices may fill
    # into the unobserved rows after an explicit empty-book reset.
    assert result.loc[[3, 4], PRICE_COLUMNS].isna().all().all()
    assert result.loc[5, PRICE_COLUMNS].eq(200.0).all()
    assert result.loc[2, "bars_since_last_obs"] == 0.0
    assert result.loc[3, "bars_since_last_obs"] == 1.0
    if empty_state == "empty_confirmed":
        assert result.loc[2, PRICE_COLUMNS].isna().all()
    else:
        # Preserve mixed-bar recorded history without treating it as live.
        assert result.loc[2, "close_price"] == 5_000.0
        assert pd.isna(result.loc[2, "robust_best_ask"])
        assert pd.isna(result.loc[2, "p10_price"])
    assert_frame_equal(source, original, check_exact=True)


def test_last_duplicate_empty_observation_does_not_expose_earlier_stocked_quote():
    source = pd.DataFrame(
        [
            _bar(1190, BASE, 10_000.0),
            _bar(1190, BASE, 5_000.0, "mixed_ended_empty"),
        ]
    )
    assert _source_for_item(source, 1190).empty
    result = compute_rp_yen_per_unit(
        source,
        [{"item_id": 1190, "denomination": 100}],
        timeline_ts=np.array([BASE], dtype="int64"),
    )
    assert pd.isna(result.loc[0, "rp_index"])


def test_upstream_effective_from_and_signed_weights_are_normalized_per_row():
    timestamp = BASE + DAY
    upstream = pd.DataFrame(
        [
            _bar(1031, BASE, 100.0),
            _bar(1031, timestamp, 200.0),
            _bar(1034, BASE, 100.0),
            _bar(1034, timestamp, 150.0),
        ]
    )
    relations = {
        "upstream_relations": [
            {
                "target_item_id": TARGET_ID,
                "upstream_item_id": 1031,
                "weight": 0.5,
            },
            {
                "target_item_id": TARGET_ID,
                "upstream_item_id": 1034,
                "weight": -0.25,
                "effective_from": timestamp + STEP,
            },
        ]
    }
    result = build_feature_frame(
        _target_bars(last_offset=7),
        upstream_bars=upstream,
        item_relations=relations,
    )
    assert pd.isna(_target_row(result, BASE)["upstream_ret_24h"])
    # The second relation exists in source but is not yet effective.
    assert _target_row(result, timestamp)["upstream_ret_24h"] == 1.0
    # (0.5 * 1.0 - 0.25 * 0.5) / (abs(0.5) + abs(-0.25)) == 0.5.
    assert _target_row(result, timestamp + STEP)["upstream_ret_24h"] == 0.5


def test_build_feature_frame_respects_configured_rp_and_upstream_quote_ages():
    timestamp = BASE + DAY
    rp = pd.DataFrame(
        [
            _bar(1190, BASE, 10_000.0),
            _bar(1190, timestamp, 20_000.0),
        ]
    )
    upstream = pd.DataFrame(
        [
            _bar(1031, BASE, 100.0),
            _bar(1031, timestamp, 200.0),
        ]
    )
    result = build_feature_frame(
        _target_bars(last_offset=8),
        rp_bars=rp,
        upstream_bars=upstream,
        config={
            "max_rp_quote_age_sec": STEP,
            "max_upstream_quote_age_sec": STEP,
        },
        item_relations={
            "rp_vouchers": [{"item_id": 1190, "denomination": 100}],
            "upstream_relations": [
                {
                    "target_item_id": TARGET_ID,
                    "upstream_item_id": 1031,
                    "weight": 1.0,
                }
            ],
        },
    )
    at_boundary = _target_row(result, timestamp + STEP)
    assert at_boundary["rp_index"] == 200.0
    assert at_boundary["upstream_ret_24h"] == 1.0
    expired = _target_row(result, timestamp + 2 * STEP)
    assert pd.isna(expired["rp_index"])
    assert pd.isna(expired["upstream_ret_24h"])


def test_return_series_checks_freshness_of_the_24h_prior_quote():
    source = pd.DataFrame(
        [
            _bar(1031, BASE, 100.0),
            _bar(1031, BASE + DAY + STEP, 200.0),
        ]
    )
    expired = _return_series(source, 1031, max_age_sec=STEP - 1)
    assert expired["upstream_ret_24h"].isna().all()

    at_boundary = _return_series(source, 1031, max_age_sec=STEP)
    assert at_boundary.iloc[-1]["upstream_ret_24h"] == 1.0


def test_recipe_missing_one_of_three_seeds_is_unknown_and_future_quote_cannot_fix_it():
    timestamp = BASE + DAY
    recipe = [
        {"item_id": 1031, "qty": 1},
        {"item_id": 1034, "qty": 2},
        {"item_id": 1036, "qty": 1},
    ]
    source = pd.DataFrame(
        [
            _bar(1031, timestamp, 10.0),
            _bar(1034, timestamp, 20.0),
        ]
    )
    before = compute_recipe_floor_cost(source, recipe, [timestamp])
    assert pd.isna(before.iloc[0])

    future_source = pd.concat(
        [
            source,
            pd.DataFrame([_bar(1036, timestamp + STEP, 30.0)]),
        ],
        ignore_index=True,
    )
    after = compute_recipe_floor_cost(future_source, recipe, [timestamp])
    assert pd.isna(after.iloc[0])

    complete_source = pd.concat(
        [
            source,
            pd.DataFrame([_bar(1036, timestamp, 30.0)]),
        ],
        ignore_index=True,
    )
    complete = compute_recipe_floor_cost(complete_source, recipe, [timestamp])
    assert complete.iloc[0] == 80.0


@pytest.mark.parametrize(
    "failure_kind",
    ["missing", "empty_confirmed", "mixed_ended_empty", "stale"],
)
def test_recipe_requires_every_active_ingredient_to_have_a_fresh_live_quote(
    failure_kind: str,
):
    timestamp = BASE + 2 * STEP
    rows = [
        _bar(1031, timestamp, 10.0),
        _bar(1034, timestamp, 20.0),
    ]
    if failure_kind == "stale":
        rows.append(_bar(1036, timestamp - STEP - 1, 30.0))
    elif failure_kind != "missing":
        # A still-fresh older quote must not survive the observed empty book.
        rows.append(_bar(1036, timestamp - STEP, 30.0))
        empty_price = 5_000.0 if failure_kind == "mixed_ended_empty" else np.nan
        rows.append(_bar(1036, timestamp, empty_price, failure_kind))
    result = compute_recipe_floor_cost(
        pd.DataFrame(rows),
        [
            {"item_id": 1031, "quantity": 1},
            {"item_id": 1034, "quantity": 2},
            {"item_id": 1036, "quantity": 1},
        ],
        [timestamp],
        max_age_sec=STEP,
    )
    assert pd.isna(result.iloc[0])


def test_recipe_effective_from_is_causal_and_preserves_requested_timeline_order():
    timestamp = BASE + DAY
    timeline = [timestamp + STEP, timestamp, timestamp - STEP, timestamp + STEP]
    source = pd.DataFrame(
        [
            _bar(1031, timestamp, 10.0),
            _bar(1034, timestamp, 20.0),
        ]
    )
    recipe = [
        {"item_id": 1031, "qty": 1, "effective_from": timestamp},
        {"item_id": 1034, "quantity": 2, "effective_from": timestamp},
        {"item_id": 1036, "qty": 1, "effective_from": timestamp + STEP},
    ]
    before = compute_recipe_floor_cost(source, recipe, timeline)
    assert before.index.tolist() == timeline
    assert before.index.name == "close_ts"
    assert before.name == "recipe_floor_cost"
    assert pd.isna(before.iloc[0])
    assert before.iloc[1] == 50.0
    assert pd.isna(before.iloc[2])
    assert pd.isna(before.iloc[3])

    future_source = pd.concat(
        [
            source,
            pd.DataFrame([_bar(1036, timestamp + STEP, 30.0)]),
        ],
        ignore_index=True,
    )
    after = compute_recipe_floor_cost(future_source, recipe, timeline)
    assert after.index.tolist() == timeline
    assert after.iloc[0] == 80.0
    assert after.iloc[1] == before.iloc[1] == 50.0
    assert pd.isna(after.iloc[2])
    assert after.iloc[3] == 80.0


def test_empty_rp_source_and_empty_recipe_return_unknown():
    timeline = np.array([BASE, BASE + STEP], dtype="int64")
    rp = compute_rp_yen_per_unit(
        pd.DataFrame(),
        [{"item_id": 1190, "denomination": 100}],
        timeline_ts=timeline,
    )
    assert rp["close_ts"].tolist() == timeline.tolist()
    assert rp[["rp_index", "rp_ret_24h"]].isna().all().all()

    recipe = compute_recipe_floor_cost(pd.DataFrame(), [], timeline)
    assert recipe.index.tolist() == timeline.tolist()
    assert recipe.isna().all()


def test_feature_columns_and_add_targets_remain_compatible():
    assert len(FEATURE_COLUMNS) == 53
    assert len(set(FEATURE_COLUMNS)) == 53
    features = build_feature_frame(_target_bars(last_offset=8))
    original = features.copy(deep=True)
    assert set(FEATURE_COLUMNS).issubset(features.columns)
    targets = add_targets(
        features,
        24,
        {
            "item_min_fee": 100,
            "pokemon_min_fee": 1_000,
            "rate": "0.05",
            "default_relist_rate": "0.15",
            "max_fee_per_slot": 25_000,
        },
    )
    assert len(targets) == len(features)
    assert targets.loc[0, "target_close_price"] == features.loc[6, "close_price"]
    assert targets.loc[0, "p_exit"] == min(
        features.loc[6, "close_price"],
        features.loc[6, "p10_price"],
    )
    assert np.isfinite(targets.loc[0, "y_reg"])
    assert pd.isna(targets.iloc[-1]["y_reg"])
    assert_frame_equal(features, original, check_exact=True)
