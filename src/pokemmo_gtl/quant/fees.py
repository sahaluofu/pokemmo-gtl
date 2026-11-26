"""Decimal-based PokeMMO GTL listing fees and breakeven prices."""

from decimal import Decimal


def listing_fee(
    sell_price: int | float | str | Decimal,
    qty: int,
    max_stack_size: int,
    *,
    min_fee: int = 100,
    rate: int | float | str | Decimal = "0.05",
    max_fee_per_slot: int = 25_000,
) -> int:
    """Calculate listing fees independently for each occupied stack slot."""
    _validate(qty, max_stack_size, min_fee, max_fee_per_slot)
    price = _decimal(sell_price)
    if price < 0:
        raise ValueError("sell_price must be non-negative")
    commission = _decimal(rate)
    if commission < 0:
        raise ValueError("rate must be non-negative")

    full_slots, remainder = divmod(qty, max_stack_size)

    def slot_fee(slot_qty: int) -> int:
        proportional = int(Decimal(str(price * slot_qty)) * commission)
        return min(max_fee_per_slot, max(min_fee, proportional))

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
) -> Decimal:
    """Return expected fees including the expected relist multiplier."""
    relists = _decimal(relist_rate)
    if relists < 0:
        raise ValueError("relist_rate must be non-negative")
    fee = listing_fee(
        sell_price,
        qty,
        max_stack_size,
        min_fee=min_fee,
        rate=rate,
        max_fee_per_slot=max_fee_per_slot,
    )
    return Decimal(str(fee)) * (Decimal("1") + relists)


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
) -> Decimal:
    """Return expected net profit after per-slot listing and relist fees."""
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
) -> int:
    """Find the minimum integer sale price whose expected net profit is nonnegative."""
    if isinstance(buy_price, bool) or not isinstance(buy_price, int) or buy_price < 0:
        raise ValueError("buy_price must be a non-negative integer")
    _validate(qty, max_stack_size, min_fee, max_fee_per_slot)

    def profit(price: int) -> Decimal:
        return net_profit(
            buy_price,
            price,
            qty,
            max_stack_size,
            min_fee=min_fee,
            rate=rate,
            relist_rate=relist_rate,
            max_fee_per_slot=max_fee_per_slot,
        )

    lo = buy_price
    hi = max(buy_price + 2000, buy_price * 2)
    while profit(hi) < 0:
        hi *= 2
    while lo < hi:
        mid = (lo + hi) // 2
        if profit(mid) >= 0:
            hi = mid
        else:
            lo = mid + 1
    return lo


def _decimal(value: int | float | str | Decimal) -> Decimal:
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("fee inputs must be finite numbers")
    return parsed


def _validate(qty: int, max_stack_size: int, min_fee: int, max_fee_per_slot: int) -> None:
    for name, value in (("qty", qty), ("max_stack_size", max_stack_size)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if type(min_fee) is not int or type(max_fee_per_slot) is not int:
        raise ValueError("fee bounds must be integers")
    if min_fee < 0 or max_fee_per_slot <= 0 or min_fee > max_fee_per_slot:
        raise ValueError("fee bounds are invalid")
