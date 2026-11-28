"""Tests for exact batch entry decisions and retained orderbook evidence."""

import json
from decimal import Decimal
from itertools import combinations

import pytest

from pokemmo_gtl.quant.entry_calc import (
    OrderLot,
    assign_opportunity_group_id,
    build_orderbook_observation,
    compare_batch_candidates,
    evaluate_entry_scenarios,
    feasible_bundle_quantities,
    max_acceptable_total_buy_cost,
    solve_buy_cost,
    solve_buy_lots,
)


@pytest.mark.parametrize(
    ("quantities", "fillable", "cost"),
    [
        ([60, 39], True, 390_000),
        ([50, 49], False, None),
    ],
)
def test_bundle_39_requires_actual_lot_combination(quantities, fillable, cost):
    lots = [
        OrderLot(auction_id, 10_000, qty)
        for auction_id, qty in enumerate(quantities, start=1)
    ]

    plan = solve_buy_cost(lots, 39, purchase_mode="bundle_only")

    assert plan.fillable is fillable
    assert plan.total_cost == cost
    assert plan.max_fillable_qty == (39 if fillable else 0)
    assert plan.filled_qty == plan.max_fillable_qty
    assert (39 in feasible_bundle_quantities(lots, 99)) is fillable
    if fillable:
        assert plan.fills == [OrderLot(2, 10_000, 39)]
    else:
        assert plan.fills == []


@pytest.mark.parametrize(
    ("quantities", "expected"),
    [
        ([60, 39], {39: 390_000, 60: 600_000, 99: 990_000}),
        ([50, 49], {49: 490_000, 50: 500_000, 99: 990_000}),
    ],
)
def test_feasible_quantities_preserve_exact_bundle_boundaries(quantities, expected):
    lots = [
        OrderLot(auction_id, 10_000, qty)
        for auction_id, qty in enumerate(quantities, start=1)
    ]

    plans = feasible_bundle_quantities(lots, 99)

    assert {qty: plan.total_cost for qty, plan in plans.items()} == expected
    for qty, plan in plans.items():
        assert plan.fillable
        assert plan.target_qty == qty
        assert plan.filled_qty == qty
        assert plan.filled_cost == plan.total_cost
        assert plan.purchase_mode == "bundle_only"


def test_bundle_solver_minimizes_total_cost_not_greedy_lot_order():
    lots = [
        OrderLot(1, 100, 3),
        OrderLot(2, 70, 2),
        OrderLot(3, 90, 4),
        OrderLot(4, 120, 1),
    ]

    plan = solve_buy_cost(lots, 5, purchase_mode="bundle_only")

    assert plan.fillable
    assert plan.total_cost == 440
    assert plan.fills == [OrderLot(2, 70, 2), OrderLot(1, 100, 3)]


def test_equal_cost_bundle_ties_use_sorted_auction_id_tuple():
    lots = [
        OrderLot(8, 10, 2),
        OrderLot(7, 10, 1),
        OrderLot(2, 10, 2),
        OrderLot(3, 10, 1),
    ]

    forward = solve_buy_cost(lots, 3, purchase_mode="bundle_only")
    backward = solve_buy_cost(list(reversed(lots)), 3, purchase_mode="bundle_only")

    assert forward == backward
    assert forward.total_cost == 30
    assert forward.auction_ids == (2, 3)


def test_all_bundle_dp_results_match_exhaustive_subsets():
    lots = [
        OrderLot(9, 4, 1),
        OrderLot(3, 3, 2),
        OrderLot(1, 5, 1),
        OrderLot(7, 2, 3),
        OrderLot(2, 3, 2),
    ]
    max_qty = 12
    expected: dict[int, tuple[int, tuple[int, ...]]] = {}
    for size in range(len(lots) + 1):
        for chosen in combinations(lots, size):
            qty = sum(lot.qty for lot in chosen)
            if not 1 <= qty <= max_qty:
                continue
            candidate = (
                sum(lot.price * lot.qty for lot in chosen),
                tuple(sorted(lot.auction_id for lot in chosen)),
            )
            if qty not in expected or candidate < expected[qty]:
                expected[qty] = candidate

    plans = feasible_bundle_quantities(lots, max_qty)

    assert set(plans) == set(expected)
    for qty, (cost, auction_ids) in expected.items():
        assert plans[qty].total_cost == cost
        assert plans[qty].auction_ids == auction_ids
        assert plans[qty] == solve_buy_cost(
            lots, qty, purchase_mode="bundle_only"
        )


def test_bundle_solver_never_reuses_a_duplicate_auction():
    lot = OrderLot(1, 100, 2)

    plan = solve_buy_cost([lot, lot], 4, purchase_mode="bundle_only")

    assert not plan.fillable
    assert plan.total_cost is None
    assert plan.max_fillable_qty == 2
    assert plan.fills == [lot]
    assert set(feasible_bundle_quantities([lot, lot], 4)) == {2}


def test_unfillable_bundle_reports_best_partial_quantity():
    lots = [OrderLot(1, 100, 50), OrderLot(2, 100, 49)]

    plan = solve_buy_cost(lots, 90, purchase_mode="bundle_only")

    assert not plan.fillable
    assert plan.total_cost is None
    assert plan.max_fillable_qty == 50
    assert plan.fills == [lots[0]]
    assert plan.filled_cost == 5_000


def test_per_unit_ladder_keeps_exact_989000_total():
    lots = [OrderLot(2, 10_000, 99), OrderLot(1, 9_000, 1)]

    plan = solve_buy_cost(lots, 99, purchase_mode="per_unit")

    assert plan.fillable
    assert plan.total_cost == 989_000
    assert plan.filled_qty == 99
    assert plan.max_fillable_qty == 99
    assert plan.fills == [
        OrderLot(1, 9_000, 1),
        OrderLot(2, 10_000, 98),
    ]
    assert solve_buy_lots(lots, 99, purchase_mode="per_unit") == plan


def test_per_unit_price_ties_are_ordered_by_auction_id():
    lots = [OrderLot(3, 100, 2), OrderLot(1, 100, 2)]

    plan = solve_buy_cost(lots, 3, purchase_mode="per_unit")

    assert plan.fills == [OrderLot(1, 100, 2), OrderLot(3, 100, 1)]
    assert plan.total_cost == 300


def test_per_unit_insufficient_depth_returns_partial_fills_without_total():
    lots = [OrderLot(1, 100, 2), OrderLot(2, 80, 3)]

    plan = solve_buy_cost(lots, 8, purchase_mode="per_unit")

    assert not plan.fillable
    assert plan.max_fillable_qty == 5
    assert plan.filled_qty == 5
    assert plan.total_cost is None
    assert plan.filled_cost == 440
    assert plan.fills == [OrderLot(2, 80, 3), OrderLot(1, 100, 2)]


@pytest.mark.parametrize("purchase_mode", ["per_unit", "bundle_only"])
def test_duplicate_rows_do_not_double_count_stock(purchase_mode):
    lot = OrderLot(1, 100, 2)

    plan = solve_buy_cost([lot, lot], 3, purchase_mode=purchase_mode)

    assert not plan.fillable
    assert plan.max_fillable_qty == 2
    assert plan.fills == [lot]


def test_conflicting_duplicate_rows_have_input_order_independent_policy():
    lots = [
        OrderLot(10, 100, 4),
        OrderLot(10, 90, 2),
        OrderLot(11, 100, 4),
    ]

    forward = solve_buy_cost(lots, 6, purchase_mode="per_unit")
    backward = solve_buy_cost(list(reversed(lots)), 6, purchase_mode="per_unit")

    assert forward == backward
    assert forward.total_cost == 580
    assert forward.fills == [OrderLot(10, 90, 2), OrderLot(11, 100, 4)]


@pytest.mark.parametrize("purchase_mode", ["per_unit", "bundle_only"])
def test_zero_quantity_is_a_successful_no_buy(purchase_mode):
    plan = solve_buy_cost([], 0, purchase_mode=purchase_mode)

    assert plan.fillable
    assert plan.total_cost == 0
    assert plan.max_fillable_qty == 0
    assert plan.fills == []
    assert feasible_bundle_quantities([], 0) == {}


@pytest.mark.parametrize("purchase_mode", ["per_unit", "bundle_only"])
def test_empty_book_cannot_fill_positive_quantity(purchase_mode):
    plan = solve_buy_cost([], 1, purchase_mode=purchase_mode)

    assert not plan.fillable
    assert plan.total_cost is None
    assert plan.max_fillable_qty == 0
    assert plan.fills == []


@pytest.mark.parametrize(
    "lot",
    [
        OrderLot(1, 0, 1),
        OrderLot(1, -1, 1),
        OrderLot(1, 100, 0),
        OrderLot(1, 100, -1),
        OrderLot(1, True, 1),
        OrderLot(1, 100, 1.5),
    ],
)
def test_invalid_lots_raise_even_when_not_needed(lot):
    with pytest.raises(ValueError, match="price|qty"):
        solve_buy_cost([OrderLot(1, 10, 2), lot], 0, purchase_mode="per_unit")


@pytest.mark.parametrize("target_qty", [-1, 1.5, True])
def test_invalid_target_quantity_raises(target_qty):
    with pytest.raises(ValueError, match="target_qty"):
        solve_buy_cost([], target_qty, purchase_mode="per_unit")


def test_unknown_purchase_mode_raises():
    with pytest.raises(ValueError, match="purchase_mode"):
        solve_buy_cost([], 0, purchase_mode="unknown")


def test_max_acceptable_cost_uses_total_cash_and_net_residual_once():
    result = max_acceptable_total_buy_cost(
        100,
        25,
        75,
        conservative_cash_in_by_deadline=250_000,
        conservative_residual_net_value=674_000,
        unpaid_future_fees=30_000,
        required_min_net_profit=10_000,
    )

    assert result == 884_000


def test_max_acceptable_cost_checks_inventory_conservation():
    with pytest.raises(ValueError, match="must equal planned_qty"):
        max_acceptable_total_buy_cost(
            100,
            25,
            74,
            conservative_cash_in_by_deadline=250_000,
            conservative_residual_net_value=674_000,
            unpaid_future_fees=30_000,
            required_min_net_profit=10_000,
        )


def test_max_acceptable_cost_preserves_negative_threshold():
    assert max_acceptable_total_buy_cost(
        1,
        0,
        1,
        conservative_cash_in_by_deadline=0,
        conservative_residual_net_value=0,
        unpaid_future_fees=100,
        required_min_net_profit=50,
    ) == -150


def test_default_scenario_bands_floor_sales_and_account_for_tail_fees():
    bands = evaluate_entry_scenarios(
        99,
        10_000,
        9_000,
        first_listing_fee=25_000,
        planned_edit_or_relist_fee=5_000,
        residual_disposal_fee_fn=lambda qty: 1_000 if qty else 0,
        required_min_net_profit=10_000,
    )

    assert list(bands) == ["stagnant", "low_fill", "partial_fill"]
    expected = {
        "stagnant": (0, 99, 0, 890_000, 850_000),
        "low_fill": (24, 75, 240_000, 674_000, 874_000),
        "partial_fill": (49, 50, 490_000, 449_000, 899_000),
    }
    for name, values in expected.items():
        band = bands[name]
        assert (
            band.sold_qty,
            band.residual_qty,
            band.cash_in,
            band.residual_net,
            band.max_acceptable_buy_cost,
        ) == values
        assert band.scenario == name
        assert band.planned_qty == 99
        assert band.sold_qty + band.residual_qty == band.planned_qty
        assert band.unpaid_fees == 30_000
        assert band.required_min_net_profit == 10_000


def test_residual_value_is_clamped_and_disposal_fee_receives_zero_quantity():
    disposal_quantities = []

    def disposal_fee(qty):
        disposal_quantities.append(qty)
        return 1_000

    bands = evaluate_entry_scenarios(
        3,
        100,
        10,
        first_listing_fee=20,
        planned_edit_or_relist_fee=5,
        residual_disposal_fee_fn=disposal_fee,
        required_min_net_profit=10,
        scenario_sold_fractions={"none": Decimal("0"), "all": Decimal("1")},
    )

    assert disposal_quantities == [3, 0]
    assert bands["none"].residual_net == 0
    assert bands["none"].max_acceptable_buy_cost == -35
    assert bands["all"].sold_qty == 3
    assert bands["all"].residual_qty == 0
    assert bands["all"].residual_net == 0
    assert bands["all"].max_acceptable_buy_cost == 265


@pytest.mark.parametrize(
    ("planned_qty", "fraction", "expected_sold"),
    [
        (100, 0.29, 29),
        (3, Decimal("0.50"), 1),
        (1, Decimal("0.999999999999999999999999999999999999"), 0),
    ],
)
def test_scenario_floor_avoids_binary_float_and_decimal_rounding(
    planned_qty, fraction, expected_sold
):
    bands = evaluate_entry_scenarios(
        planned_qty,
        100,
        0,
        first_listing_fee=0,
        planned_edit_or_relist_fee=0,
        residual_disposal_fee_fn=lambda qty: 0,
        required_min_net_profit=0,
        scenario_sold_fractions={"custom": fraction},
    )

    assert bands["custom"].sold_qty == expected_sold
    assert bands["custom"].residual_qty == planned_qty - expected_sold


@pytest.mark.parametrize(
    "fraction",
    [-0.1, 1.01, float("nan"), float("inf"), Decimal("NaN"), True],
)
def test_invalid_scenario_fraction_raises(fraction):
    with pytest.raises(ValueError, match="fraction"):
        evaluate_entry_scenarios(
            10,
            100,
            50,
            first_listing_fee=0,
            planned_edit_or_relist_fee=0,
            residual_disposal_fee_fn=lambda qty: 0,
            required_min_net_profit=0,
            scenario_sold_fractions={"bad": fraction},
        )


def test_negative_residual_disposal_fee_is_rejected():
    with pytest.raises(ValueError, match="residual_disposal_fee"):
        evaluate_entry_scenarios(
            10,
            100,
            50,
            first_listing_fee=0,
            planned_edit_or_relist_fee=0,
            residual_disposal_fee_fn=lambda qty: -1,
            required_min_net_profit=0,
        )


def test_batch_candidates_gate_budget_with_immediate_fee_reservation():
    evaluated = []

    def evaluate(qty):
        evaluated.append(qty)
        return qty * 100 - (1 if qty == 5 else 0), 50

    comparisons = compare_batch_candidates(
        [OrderLot(1, 100, 20)],
        [10, 5, 1, 5, 0, 30],
        purchase_mode="per_unit",
        budget_available_cash=1_049,
        scenario_evaluator=evaluate,
    )
    by_qty = {comparison.planned_qty: comparison for comparison in comparisons}

    assert list(by_qty) == [0, 1, 5, 10, 30]
    assert evaluated == [1, 5, 10, 30]
    assert by_qty[0].accepted
    assert by_qty[0].total_buy_cost == 0
    assert by_qty[0].required_cash == 0
    assert by_qty[0].immediate_fee_reserve == 0
    assert by_qty[1].accepted
    assert by_qty[5].budget_ok
    assert not by_qty[5].entry_cost_ok
    assert not by_qty[5].accepted
    assert by_qty[10].entry_cost_ok
    assert by_qty[10].required_cash == 1_050
    assert not by_qty[10].budget_ok
    assert not by_qty[10].accepted
    assert not by_qty[30].buy_plan.fillable
    assert by_qty[30].total_buy_cost is None
    assert by_qty[30].required_cash is None
    assert not by_qty[30].accepted


def test_budget_and_max_cost_equalities_are_accepted_without_double_fee_charge():
    comparisons = compare_batch_candidates(
        [OrderLot(1, 100, 10)],
        [10],
        purchase_mode="per_unit",
        budget_available_cash=1_050,
        scenario_evaluator=lambda qty: (1_000, 50),
    )

    assert comparisons[1].total_buy_cost == 1_000
    assert comparisons[1].required_cash == 1_050
    assert comparisons[1].accepted


def test_bundle_candidate_comparison_uses_feasible_exact_quantities():
    comparisons = compare_batch_candidates(
        [OrderLot(1, 10_000, 60), OrderLot(2, 10_000, 39)],
        [39, 49, 50, 60, 99],
        purchase_mode="bundle_only",
        budget_available_cash=1_000_000,
        scenario_evaluator=lambda qty: (qty * 10_000, 0),
    )
    by_qty = {comparison.planned_qty: comparison for comparison in comparisons}

    assert [qty for qty, value in by_qty.items() if value.accepted] == [
        0,
        39,
        60,
        99,
    ]
    assert by_qty[49].buy_plan.max_fillable_qty == 39
    assert by_qty[50].total_buy_cost is None


def test_empty_candidate_list_still_includes_no_buy_without_evaluation():
    def unexpected_evaluation(qty):
        pytest.fail(f"no-buy must not evaluate a purchase scenario: {qty}")

    comparisons = compare_batch_candidates(
        [],
        [],
        purchase_mode="bundle_only",
        budget_available_cash=0,
        scenario_evaluator=unexpected_evaluation,
    )

    assert len(comparisons) == 1
    assert comparisons[0].planned_qty == 0
    assert comparisons[0].accepted


@pytest.mark.parametrize("invalid_reserve", [-1, 1.5, True])
def test_invalid_immediate_fee_reserve_is_rejected(invalid_reserve):
    with pytest.raises(ValueError, match="immediate_fee_reserve"):
        compare_batch_candidates(
            [OrderLot(1, 100, 1)],
            [1],
            purchase_mode="per_unit",
            budget_available_cash=1_000,
            scenario_evaluator=lambda qty: (1_000, invalid_reserve),
        )


def test_ladder_json_preserves_lots_after_raw_listings_are_removed():
    raw_listings = [
        {"auction_id": 103, "price": 10_000, "qty": 60, "expires_at": 10_000},
        {"auction_id": 101, "price": 10_000, "qty": 39, "expires_at": None},
    ]
    observation = build_orderbook_observation(
        raw_listings,
        current_ts=1_000,
        purchase_mode="bundle_only",
        coverage_hours=48,
    )
    raw_listings.clear()

    retained = json.loads(observation.ladder_json)
    recovered_lots = [OrderLot(**row) for row in retained]
    plan = solve_buy_cost(recovered_lots, 39, purchase_mode="bundle_only")

    assert raw_listings == []
    assert retained == [
        {"auction_id": 101, "price": 10_000, "qty": 39, "expires_at": None},
        {"auction_id": 103, "price": 10_000, "qty": 60, "expires_at": 10_000},
    ]
    assert observation.purchase_mode == "bundle_only"
    assert observation.coverage_hours == 48.0
    assert not observation.ladder_truncated
    assert plan.fillable
    assert plan.total_cost == 390_000

    other = build_orderbook_observation(
        [OrderLot(1, 10_000, 50), OrderLot(2, 10_000, 49)],
        current_ts=1_000,
        purchase_mode="bundle_only",
        coverage_hours=48,
    )
    other_lots = [OrderLot(**row) for row in json.loads(other.ladder_json)]
    assert not solve_buy_cost(
        other_lots, 39, purchase_mode="bundle_only"
    ).fillable


@pytest.mark.parametrize("max_ladder_lots", [3, 50])
def test_ladder_limit_does_not_truncate_wall_or_depth_metrics(max_ladder_lots):
    lots = [OrderLot(auction_id, 100, 1) for auction_id in range(60, 0, -1)]

    observation = build_orderbook_observation(
        lots,
        current_ts=1_000,
        purchase_mode="per_unit",
        coverage_hours=1,
        reference_price=101,
        max_ladder_lots=max_ladder_lots,
    )
    retained = json.loads(observation.ladder_json)

    assert len(retained) == max_ladder_lots
    assert [row["auction_id"] for row in retained] == list(
        range(1, max_ladder_lots + 1)
    )
    assert observation.total_listed_qty == 60
    assert observation.total_lot_count == 60
    assert observation.visible_lower_qty == 60
    assert observation.wall_qty == 60
    assert observation.ladder_truncated


def test_default_ladder_limit_retains_top_fifty_lots():
    observation = build_orderbook_observation(
        [OrderLot(auction_id, 100, 1) for auction_id in range(60)],
        current_ts=1_000,
        purchase_mode="bundle_only",
        coverage_hours=1,
    )

    assert len(json.loads(observation.ladder_json)) == 50


def test_listing_changes_exclude_expired_and_near_expiry_disappearances():
    previous = [
        OrderLot(1, 100, 10, 5_000),
        OrderLot(2, 100, 4, 1_001),
        OrderLot(3, 100, 5, 1_005),
        OrderLot(4, 100, 6, 1_006),
        OrderLot(4, 100, 6, 1_006),
        OrderLot(5, 100, 8),
        OrderLot(6, 100, 9, 900),
        OrderLot(7, 110, 10, 5_000),
    ]
    current = [
        OrderLot(1, 100, 7, 5_000),
        OrderLot(7, 110, 12, 5_000),
        OrderLot(8, 90, 20, 5_000),
    ]

    observation = build_orderbook_observation(
        current,
        previous,
        current_ts=1_000,
        purchase_mode="per_unit",
        coverage_hours=2,
        reference_price=110,
        grace_sec=5,
    )

    assert observation.same_id_qty_drop == 3
    assert observation.full_disappear_qty == 14
    assert observation.visible_lower_qty == 27
    assert observation.total_listed_qty == 39


def test_wall_aggregates_price_levels_and_breaks_ties_at_lower_price():
    observation = build_orderbook_observation(
        [
            OrderLot(1, 100, 2),
            OrderLot(2, 110, 4),
            OrderLot(3, 110, 6),
            OrderLot(4, 120, 10),
            OrderLot(5, 126, 100),
        ],
        current_ts=1_000,
        purchase_mode="bundle_only",
        coverage_hours=2,
        reference_price=120,
        prev_wall_price=110,
        prev_wall_persist_frames=4,
    )

    assert observation.best_ask == 100
    assert observation.wall_price == 110
    assert observation.wall_qty == 10
    assert observation.wall_persist_frames == 5
    assert observation.visible_lower_qty == 12


def test_wall_includes_exact_twenty_five_percent_boundary():
    observation = build_orderbook_observation(
        [
            OrderLot(1, 100, 1),
            OrderLot(2, 125, 9),
            OrderLot(3, 126, 100),
        ],
        current_ts=1_000,
        purchase_mode="per_unit",
        coverage_hours=1,
    )

    assert observation.wall_price == 125
    assert observation.wall_qty == 9


def test_wall_persistence_survives_id_changes_and_resets_on_wall_or_empty_book():
    first = build_orderbook_observation(
        [OrderLot(1, 100, 1), OrderLot(2, 110, 9)],
        current_ts=1_000,
        purchase_mode="per_unit",
        coverage_hours=1,
    )
    second = build_orderbook_observation(
        [OrderLot(3, 100, 2), OrderLot(4, 110, 8)],
        current_ts=1_100,
        purchase_mode="per_unit",
        coverage_hours=1,
        prev_wall_price=first.wall_price,
        prev_wall_persist_frames=first.wall_persist_frames,
    )
    third = build_orderbook_observation(
        [OrderLot(5, 100, 2), OrderLot(6, 120, 10)],
        current_ts=1_200,
        purchase_mode="per_unit",
        coverage_hours=1,
        prev_wall_price=second.wall_price,
        prev_wall_persist_frames=second.wall_persist_frames,
    )
    empty = build_orderbook_observation(
        [],
        current_ts=1_300,
        purchase_mode="per_unit",
        coverage_hours=1,
        prev_wall_price=third.wall_price,
        prev_wall_persist_frames=third.wall_persist_frames,
    )
    restocked = build_orderbook_observation(
        [OrderLot(7, 120, 10)],
        current_ts=1_400,
        purchase_mode="per_unit",
        coverage_hours=1,
        prev_wall_price=empty.wall_price,
        prev_wall_persist_frames=empty.wall_persist_frames,
    )

    assert (first.wall_price, first.wall_persist_frames) == (110, 1)
    assert (second.wall_price, second.wall_persist_frames) == (110, 2)
    assert (third.wall_price, third.wall_persist_frames) == (120, 1)
    assert (empty.wall_price, empty.wall_persist_frames) == (None, 0)
    assert (restocked.wall_price, restocked.wall_persist_frames) == (120, 1)


def test_empty_confirmed_book_retains_flow_but_clears_current_quote():
    observation = build_orderbook_observation(
        [],
        [OrderLot(1, 100, 20, 5_000)],
        current_ts=1_000,
        purchase_mode="bundle_only",
        coverage_hours=24,
        reference_price=100,
        prev_wall_price=100,
        prev_wall_persist_frames=8,
        grace_sec=0,
    )

    assert observation.book_state == "empty_confirmed"
    assert observation.best_ask is None
    assert observation.total_listed_qty == 0
    assert observation.same_id_qty_drop == 0
    assert observation.full_disappear_qty == 20
    assert observation.visible_lower_qty == 0
    assert observation.wall_price is None
    assert observation.wall_qty is None
    assert observation.wall_persist_frames == 0
    assert observation.ladder_json == "[]"
    assert observation.total_lot_count == 0
    assert not observation.ladder_truncated


def test_missing_reference_is_distinguished_from_zero_lower_quantity():
    observation = build_orderbook_observation(
        [OrderLot(1, 100, 2), OrderLot(1, 100, 2)],
        current_ts=1_000,
        purchase_mode="per_unit",
        coverage_hours=0,
    )

    assert observation.visible_lower_qty is None
    assert observation.same_id_qty_drop == 0
    assert observation.full_disappear_qty == 0
    assert observation.total_listed_qty == 2
    assert observation.wall_qty == 2
    assert len(json.loads(observation.ladder_json)) == 1


@pytest.mark.parametrize("coverage_hours", [-1, float("nan"), float("inf"), True])
def test_invalid_observation_coverage_is_rejected(coverage_hours):
    with pytest.raises(ValueError, match="coverage_hours"):
        build_orderbook_observation(
            [],
            current_ts=1_000,
            purchase_mode="per_unit",
            coverage_hours=coverage_hours,
        )


def test_zero_ladder_limit_is_rejected():
    with pytest.raises(ValueError, match="max_ladder_lots"):
        build_orderbook_observation(
            [],
            current_ts=1_000,
            purchase_mode="per_unit",
            coverage_hours=1,
            max_ladder_lots=0,
        )


def test_opportunity_group_merges_consecutive_updates_at_inclusive_gap():
    first = assign_opportunity_group_id(
        None, None, 1_000, item_id=25, window_id=1
    )
    same_second = assign_opportunity_group_id(
        first, 1_000, 1_000, item_id=25, window_id=1
    )
    second = assign_opportunity_group_id(
        same_second, 1_000, 2_000, item_id=25, window_id=1
    )
    boundary = assign_opportunity_group_id(
        second, 2_000, 5_600, item_id=25, window_id=1
    )
    new_group = assign_opportunity_group_id(
        boundary, 5_600, 9_201, item_id=25, window_id=1
    )

    observations = [first, same_second, second, boundary, new_group]
    assert len(observations) == 5
    assert len(set(observations)) == 2
    assert first == same_second == second == boundary
    assert new_group != first
    assert first == assign_opportunity_group_id(
        None, None, 1_000, item_id=25, window_id=1
    )


def test_generated_opportunity_groups_do_not_merge_across_window_or_item():
    first = assign_opportunity_group_id(
        None, None, 1_000, item_id=25, window_id=1
    )

    other_window = assign_opportunity_group_id(
        first, 1_000, 1_001, item_id=25, window_id=2
    )
    other_item = assign_opportunity_group_id(
        first, 1_000, 1_001, item_id=26, window_id=1
    )

    assert len({first, other_window, other_item}) == 3


def test_opaque_previous_group_is_preserved_within_callers_scope():
    assert assign_opportunity_group_id(
        "existing-group",
        1_000,
        1_001,
        item_id=25,
        window_id=1,
    ) == "existing-group"


def test_opportunity_group_does_not_merge_backwards_or_without_previous_time():
    first = assign_opportunity_group_id(
        None, None, 1_000, item_id=25, window_id=1
    )

    backwards = assign_opportunity_group_id(
        first, 1_000, 999, item_id=25, window_id=1
    )
    missing_time = assign_opportunity_group_id(
        first, None, 1_001, item_id=25, window_id=1
    )

    assert backwards != first
    assert missing_time != first


def test_negative_opportunity_merge_gap_is_rejected():
    with pytest.raises(ValueError, match="merge_gap_sec"):
        assign_opportunity_group_id(
            None,
            None,
            1_000,
            item_id=25,
            window_id=1,
            merge_gap_sec=-1,
        )
