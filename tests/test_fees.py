"""M1 fee and integer breakeven boundary contracts."""

from decimal import Decimal

import pytest

from pokemmo_gtl.quant.fees import breakeven_sell_price, listing_fee, net_profit


def test_low_price_order_uses_minimum_fee():
    assert listing_fee(1, 1, 99, min_fee=100, rate="0.05") == 100


def test_regular_five_percent_fee():
    assert listing_fee(1_000, 10, 99, min_fee=100, rate="0.05") == 500


def test_single_stack_slot_fee_is_capped_at_25000():
    assert listing_fee(200_000, 99, 99, min_fee=100, rate="0.05") == 25_000


def test_250_berries_are_charged_as_99_99_and_52_slots():
    assert listing_fee(500, 250, 99, min_fee=100, rate="0.05") == 6_250
    assert listing_fee(101, 250, 99) == 499 + 499 + 262
    assert listing_fee(10_000, 250, 99) == 75_000


def test_five_single_stack_pokemon_or_cosmetics_have_five_fee_caps():
    assert listing_fee(1_000_000, 5, 1, min_fee=1_000, rate="0.05") == 125_000


def test_decimal_relist_boundary_has_exact_minimum_integer_breakeven():
    breakeven = breakeven_sell_price(
        1_000, 1, 99, min_fee=100, rate="0.05", relist_rate=0.10
    )
    assert breakeven == 1_110
    assert net_profit(1_000, breakeven, 1, 99, min_fee=100, relist_rate=0.10) >= Decimal("0")
    assert net_profit(1_000, breakeven - 1, 1, 99, min_fee=100, relist_rate=0.10) < Decimal("0")


def test_low_price_pokemon_minimum_fee_breakeven():
    breakeven = breakeven_sell_price(
        50, 1, 1, min_fee=1_000, rate="0.05", relist_rate="0.15"
    )
    assert net_profit(50, breakeven, 1, 1, min_fee=1_000, relist_rate="0.15") >= 0
    assert net_profit(50, breakeven - 1, 1, 1, min_fee=1_000, relist_rate="0.15") < 0


def test_breakeven_doubles_initial_upper_bound_until_feasible():
    price = breakeven_sell_price(50, 1, 1, min_fee=1_000, relist_rate=4)
    assert price > 50 + 2000
    assert net_profit(50, price, 1, 1, min_fee=1_000, relist_rate=4) >= 0
    assert net_profit(50, price - 1, 1, 1, min_fee=1_000, relist_rate=4) < 0


@pytest.mark.parametrize(
    ("qty", "stack"), [(0, 99), (-1, 99), (1, 0), (1, -1)],
)
def test_slot_inputs_must_be_positive_integers(qty, stack):
    with pytest.raises(ValueError):
        listing_fee(100, qty, stack)
