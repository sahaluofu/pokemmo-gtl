"""Decimal-based discrete GTL fees and exact batch-cost breakeven prices."""

from collections.abc import Sequence
from decimal import (
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_UP,
    Decimal,
    InvalidOperation,
    localcontext,
)
from typing import Literal

FeeBasis = Literal["per_slot", "per_unit"]
RoundingMode = Literal["floor", "half_up", "ceil"]

_ROUNDING_MODES = {
    "floor": ROUND_FLOOR,
    "half_up": ROUND_HALF_UP,
    "ceil": ROUND_CEILING,
}
_DIRECT_EDIT_MODES = (
    "full_relist_fee",
    "PositiveDiffOnly",
    "flat_min_fee",
    "free",
)


def listing_fee(
    sell_price: int | float | str | Decimal,
    qty: int,
    max_stack_size: int,
    *,
    min_fee: int = 100,
    rate: int | float | str | Decimal = "0.05",
    max_fee_per_slot: int = 25_000,
    fee_basis: FeeBasis = "per_slot",
    rounding_mode: RoundingMode = "floor",
) -> int:
    """Round and clamp each occupied slot's fee, or each individual unit's fee.

    In per-unit mode, max_fee_per_slot is retained as the compatible parameter
    name but applies independently to each unit.
    """
    _validate(qty, max_stack_size, min_fee, max_fee_per_slot)
    _validate_choices(fee_basis, rounding_mode)
    price = _non_negative_decimal(sell_price, "sell_price")
    commission = _non_negative_decimal(rate, "rate")

    def slot_fee(slot_qty: int) -> int:
        # Allocate enough significant digits for an exact Decimal product.
        with localcontext() as context:
            context.prec = max(
                context.prec,
                len(price.as_tuple().digits)
                + len(commission.as_tuple().digits)
                + len(str(slot_qty))
                + 2,
            )
            proportional = price * slot_qty * commission
            if proportional <= min_fee:
                return min_fee
            if proportional >= max_fee_per_slot:
                return max_fee_per_slot
            rounded = int(
                proportional.to_integral_value(
                    rounding=_ROUNDING_MODES[rounding_mode]
                )
            )
        return min(max_fee_per_slot, max(min_fee, rounded))

    if fee_basis == "per_unit":
        return slot_fee(1) * qty

    full_slots, remainder = divmod(qty, max_stack_size)
    return full_slots * slot_fee(max_stack_size) + (slot_fee(remainder) if remainder else 0)


def effective_fee(
    sell_price: int | float | str | Decimal,
    qty: int,
    max_stack_size: int,
    *,
    min_fee: int = 100,
    rate: int | float | str | Decimal = "0.05",
    relist_rate: int | float | str | Decimal = "0.15",
    max_fee_per_slot: int = 25_000,
    fee_basis: FeeBasis = "per_slot",
    rounding_mode: RoundingMode = "floor",
) -> Decimal:
    """Return expected fees including the expected relist multiplier."""
    relists = _non_negative_decimal(relist_rate, "relist_rate")
    fee = listing_fee(
        sell_price,
        qty,
        max_stack_size,
        min_fee=min_fee,
        rate=rate,
        max_fee_per_slot=max_fee_per_slot,
        fee_basis=fee_basis,
        rounding_mode=rounding_mode,
    )
    return Decimal(fee) * (Decimal("1") + relists)


def net_profit(
    buy_price: int | float | str | Decimal,
    sell_price: int | float | str | Decimal,
    qty: int,
    max_stack_size: int,
    *,
    min_fee: int = 100,
    rate: int | float | str | Decimal = "0.05",
    relist_rate: int | float | str | Decimal = "0.15",
    max_fee_per_slot: int = 25_000,
    fee_basis: FeeBasis = "per_slot",
    rounding_mode: RoundingMode = "floor",
) -> Decimal:
    """Return expected net profit with the compatible default relist rate."""
    buy = _decimal(buy_price)
    sell = _decimal(sell_price)
    if buy < 0 or sell < 0:
        raise ValueError("prices must be non-negative")
    return (sell - buy) * qty - effective_fee(
        sell,
        qty,
        max_stack_size,
        min_fee=min_fee,
        rate=rate,
        relist_rate=relist_rate,
        max_fee_per_slot=max_fee_per_slot,
        fee_basis=fee_basis,
        rounding_mode=rounding_mode,
    )


def breakeven_sell_price(
    buy_price: int,
    qty: int,
    max_stack_size: int,
    *,
    min_fee: int = 100,
    rate: int | float | str | Decimal = "0.05",
    relist_rate: int | float | str | Decimal = "0.15",
    max_fee_per_slot: int = 25_000,
    fee_basis: FeeBasis = "per_slot",
    rounding_mode: RoundingMode = "floor",
) -> int:
    """Wrap the batch-cost solver while preserving legacy fee defaults."""
    _validate_non_negative_integer(buy_price, "buy_price")
    _validate(qty, max_stack_size, min_fee, max_fee_per_slot)
    return breakeven_sell_price_for_cost(
        buy_price * qty,
        qty,
        max_stack_size,
        min_fee=min_fee,
        rate=rate,
        relist_rate=relist_rate,
        max_fee_per_slot=max_fee_per_slot,
        fee_basis=fee_basis,
        rounding_mode=rounding_mode,
    )


def direct_edit_fee(
    old_price: int,
    new_price: int,
    remaining_qty: int,
    max_stack_size: int,
    *,
    mode: str = "full_relist_fee",
    min_fee: int = 100,
    rate: Decimal | str = "0.05",
    max_fee_per_slot: int = 25_000,
    fee_basis: str = "per_slot",
    rounding_mode: str = "floor",
) -> int:
    """Calculate one direct-edit charge using only the remaining quantity.

    PositiveDiffOnly applies the configured minimum even to a non-increase;
    setting min_fee=0 makes non-increases free. Flat minimum fees apply once
    per occupied slot or per remaining unit according to fee_basis.
    """
    if mode not in _DIRECT_EDIT_MODES:
        raise ValueError(f"Unsupported direct edit mode: {mode}")
    _validate_non_negative_integer(old_price, "old_price")
    _validate_non_negative_integer(new_price, "new_price")
    _validate(remaining_qty, max_stack_size, min_fee, max_fee_per_slot)
    _validate_choices(fee_basis, rounding_mode)
    _non_negative_decimal(rate, "rate")

    if mode == "free":
        return 0
    if mode == "flat_min_fee":
        charge_price = 0
        charge_rate = Decimal("0")
    elif mode == "PositiveDiffOnly":
        charge_price = max(0, new_price - old_price)
        charge_rate = rate
    else:
        charge_price = new_price
        charge_rate = rate

    return listing_fee(
        charge_price,
        remaining_qty,
        max_stack_size,
        min_fee=min_fee,
        rate=charge_rate,
        max_fee_per_slot=max_fee_per_slot,
        fee_basis=fee_basis,
        rounding_mode=rounding_mode,
    )


def relist_fee(
    new_price: int,
    remaining_qty: int,
    max_stack_size: int,
    *,
    min_fee: int = 100,
    rate: Decimal | str = "0.05",
    max_fee_per_slot: int = 25_000,
    fee_basis: str = "per_slot",
    rounding_mode: str = "floor",
) -> int:
    """Charge a fresh listing fee for the quantity remaining after cancellation."""
    _validate_non_negative_integer(new_price, "new_price")
    return listing_fee(
        new_price,
        remaining_qty,
        max_stack_size,
        min_fee=min_fee,
        rate=rate,
        max_fee_per_slot=max_fee_per_slot,
        fee_basis=fee_basis,
        rounding_mode=rounding_mode,
    )


def discrete_total_fees(
    sell_price: int,
    qty: int,
    max_stack_size: int,
    *,
    edit_prices: Sequence[int] = (),
    relist_prices: Sequence[int] = (),
    min_fee: int = 100,
    rate: Decimal | str = "0.05",
    max_fee_per_slot: int = 25_000,
    fee_basis: str = "per_slot",
    rounding_mode: str = "floor",
    direct_edit_mode: str = "full_relist_fee",
    direct_edit_rate: Decimal | str | None = None,
    direct_edit_min_fee: int | None = None,
) -> int:
    """Sum an initial listing, sequential edits, and subsequent fresh listings.

    All actions here use qty, which can be the remaining quantity for a
    partial-sale scenario. Histories with changing quantities should sum
    individual direct_edit_fee/relist_fee calls using each action's quantity.
    Edit-specific rates and minimums default to the listing parameters.
    """
    _validate_non_negative_integer(sell_price, "sell_price")
    if direct_edit_mode not in _DIRECT_EDIT_MODES:
        raise ValueError(f"Unsupported direct edit mode: {direct_edit_mode}")

    listing_options = {
        "min_fee": min_fee,
        "rate": rate,
        "max_fee_per_slot": max_fee_per_slot,
        "fee_basis": fee_basis,
        "rounding_mode": rounding_mode,
    }
    total = listing_fee(sell_price, qty, max_stack_size, **listing_options)
    previous_price = sell_price
    for new_price in edit_prices:
        total += direct_edit_fee(
            previous_price,
            new_price,
            qty,
            max_stack_size,
            mode=direct_edit_mode,
            min_fee=(
                min_fee if direct_edit_min_fee is None else direct_edit_min_fee
            ),
            rate=rate if direct_edit_rate is None else direct_edit_rate,
            max_fee_per_slot=max_fee_per_slot,
            fee_basis=fee_basis,
            rounding_mode=rounding_mode,
        )
        previous_price = new_price

    for new_price in relist_prices:
        total += relist_fee(new_price, qty, max_stack_size, **listing_options)
    return total


def net_profit_for_cost(
    total_buy_cost: int,
    sell_price: int,
    qty: int,
    max_stack_size: int,
    *,
    discrete_relists: int = 0,
    edit_prices: Sequence[int] = (),
    relist_rate: Decimal | str | int | float | None = None,
    fee_basis: str = "per_slot",
    rounding_mode: str = "floor",
    min_fee: int = 100,
    rate: Decimal | str = "0.05",
    max_fee_per_slot: int = 25_000,
) -> Decimal:
    """Return batch revenue minus exact total cost and action fees.

    Each discrete relist repeats the sell_price listing charge. Edit prices
    add full-relist direct-edit charges and do not replace the final revenue
    price. An optional relist_rate adds expected extra listings on top of
    explicitly counted relists; it never multiplies the edit charges.
    """
    _validate_non_negative_integer(total_buy_cost, "total_buy_cost")
    _validate_non_negative_integer(sell_price, "sell_price")
    _validate_non_negative_integer(discrete_relists, "discrete_relists")
    expected_relists = (
        Decimal("0")
        if relist_rate is None
        else _non_negative_decimal(relist_rate, "relist_rate")
    )
    fee_options = {
        "min_fee": min_fee,
        "rate": rate,
        "max_fee_per_slot": max_fee_per_slot,
        "fee_basis": fee_basis,
        "rounding_mode": rounding_mode,
    }
    integer_fees = discrete_total_fees(
        sell_price,
        qty,
        max_stack_size,
        edit_prices=edit_prices,
        **fee_options,
    )
    initial_fee = 0
    if discrete_relists or expected_relists:
        initial_fee = listing_fee(sell_price, qty, max_stack_size, **fee_options)
        integer_fees += initial_fee * discrete_relists

    # Keep discrete batch arithmetic in integers until the final conversion.
    profit = Decimal(sell_price * qty - total_buy_cost - integer_fees)
    if expected_relists:
        profit -= Decimal(initial_fee) * expected_relists
    return profit


def breakeven_sell_price_for_cost(
    total_buy_cost: int,
    qty: int,
    max_stack_size: int,
    *,
    discrete_relists: int = 0,
    edit_prices: Sequence[int] = (),
    relist_rate: Decimal | str | int | float | None = None,
    fee_basis: str = "per_slot",
    rounding_mode: str = "floor",
    min_fee: int = 100,
    rate: Decimal | str = "0.05",
    max_fee_per_slot: int = 25_000,
) -> int:
    """Find the globally first profitable integer price for an integer batch cost.

    A single 5% listing has non-decreasing profit, but repeated or expected
    listing charges can make profit drop at rounding boundaries. This search
    relies only on non-decreasing fees, rather than assuming monotone profit.

    At a loss of D, an increase smaller than ceil(D / qty) cannot recover D:
    revenue grows by qty per yen and fees cannot fall. Every jump therefore
    skips only infeasible prices. Finite fee caps guarantee termination.
    """
    _validate_non_negative_integer(total_buy_cost, "total_buy_cost")
    _validate(qty, max_stack_size, min_fee, max_fee_per_slot)

    def profit(price: int) -> Decimal:
        return net_profit_for_cost(
            total_buy_cost,
            price,
            qty,
            max_stack_size,
            discrete_relists=discrete_relists,
            edit_prices=edit_prices,
            relist_rate=relist_rate,
            fee_basis=fee_basis,
            rounding_mode=rounding_mode,
            min_fee=min_fee,
            rate=rate,
            max_fee_per_slot=max_fee_per_slot,
        )

    candidate = _ceil_per_unit(-profit(0), qty)
    while True:
        current_profit = profit(candidate)
        if current_profit >= 0:
            break
        candidate += _ceil_per_unit(-current_profit, qty)

    if profit(candidate) < 0:
        raise ArithmeticError("breakeven candidate has negative profit")
    if candidate > 0 and profit(candidate - 1) >= 0:
        raise ArithmeticError("breakeven candidate is not minimal")
    return candidate


def _ceil_per_unit(amount: Decimal, qty: int) -> int:
    """Divide an exact Decimal by qty and ceil without context rounding."""
    numerator, denominator = amount.as_integer_ratio()
    return -(-numerator // (denominator * qty))


def _decimal(value: int | float | str | Decimal) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("fee inputs must be finite numbers") from exc
    if not parsed.is_finite():
        raise ValueError("fee inputs must be finite numbers")
    return parsed


def _non_negative_decimal(
    value: int | float | str | Decimal, name: str
) -> Decimal:
    parsed = _decimal(value)
    if parsed < 0:
        raise ValueError(f"{name} must be non-negative")
    return parsed


def _validate_non_negative_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _validate_choices(fee_basis: str, rounding_mode: str) -> None:
    if fee_basis not in ("per_slot", "per_unit"):
        raise ValueError(f"Unsupported fee basis: {fee_basis}")
    if rounding_mode not in _ROUNDING_MODES:
        raise ValueError(f"Unsupported rounding mode: {rounding_mode}")


def _validate(qty: int, max_stack_size: int, min_fee: int, max_fee_per_slot: int) -> None:
    for name, value in (("qty", qty), ("max_stack_size", max_stack_size)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if type(min_fee) is not int or type(max_fee_per_slot) is not int:
        raise ValueError("fee bounds must be integers")
    if min_fee < 0 or max_fee_per_slot <= 0 or min_fee > max_fee_per_slot:
        raise ValueError("fee bounds are invalid")
