"""Exact lot-level entry calculations and retained orderbook observations.

All monetary amounts are integer yen. These functions calculate alternatives
and observations only; they do not place or modify orders.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from math import isfinite
from types import MappingProxyType
from typing import Literal

PurchaseMode = Literal["per_unit", "bundle_only"]


@dataclass(frozen=True)
class OrderLot:
    """One auction, with a per-unit price and its available quantity."""

    auction_id: int
    price: int
    qty: int
    expires_at: int | None = None

    @property
    def total_cost(self) -> int:
        """Return the cost of buying this entire lot."""
        return self.price * self.qty


@dataclass(frozen=True)
class BuyExecutionPlan:
    """An exact requested purchase, or a diagnostic partial execution.

    ``max_fillable_qty`` is the largest executable quantity not exceeding the
    requested quantity. For an unfillable request, ``fills`` describes that
    partial execution, but ``total_cost`` is None: it is never presented as the
    cost of successfully buying the requested quantity.
    """

    target_qty: int
    purchase_mode: PurchaseMode
    fillable: bool
    max_fillable_qty: int
    total_cost: int | None
    fills: list[OrderLot]

    @property
    def filled_qty(self) -> int:
        """Return the quantity represented by the execution details."""
        return sum(lot.qty for lot in self.fills)

    @property
    def filled_cost(self) -> int:
        """Return the cost of the execution details, including partial plans."""
        return sum(lot.total_cost for lot in self.fills)

    @property
    def auction_ids(self) -> tuple[int, ...]:
        """Return the canonical ascending ID tuple used for bundle ties."""
        return tuple(sorted(lot.auction_id for lot in self.fills))


@dataclass(frozen=True)
class EntryScenarioBand:
    """One conservative deadline scenario for an entire planned batch."""

    scenario: str
    planned_qty: int
    sold_fraction: Decimal
    sold_qty: int
    residual_qty: int
    cash_in: int
    residual_net: int
    unpaid_fees: int
    required_min_net_profit: int
    max_acceptable_buy_cost: int


@dataclass(frozen=True)
class BatchCandidateComparison:
    """One mutually exclusive batch alternative, including the no-buy baseline."""

    planned_qty: int
    buy_plan: BuyExecutionPlan
    max_acceptable_buy_cost: int
    immediate_fee_reserve: int
    budget_available_cash: int
    budget_ok: bool
    entry_cost_ok: bool

    @property
    def total_buy_cost(self) -> int | None:
        """Return the exact requested purchase cost, if executable."""
        return self.buy_plan.total_cost

    @property
    def required_cash(self) -> int | None:
        """Return immediately required cash, including the fee reserve."""
        cost = self.total_buy_cost
        return None if cost is None else cost + self.immediate_fee_reserve

    @property
    def accepted(self) -> bool:
        """Whether this alternative satisfies execution, cash, and cost gates."""
        return self.buy_plan.fillable and self.budget_ok and self.entry_cost_ok


@dataclass(frozen=True)
class OrderbookObservation:
    """Lot-level retained evidence and descriptive listing-change metrics.

    Disappearance and quantity drops are observations, not confirmed sales.
    All aggregate metrics use the complete supplied book, even when the
    retained ladder is truncated.
    """

    current_ts: int
    book_state: Literal["stocked", "empty_confirmed"]
    purchase_mode: PurchaseMode
    coverage_hours: float
    best_ask: int | None
    total_listed_qty: int
    same_id_qty_drop: int
    full_disappear_qty: int
    visible_lower_qty: int | None
    wall_price: int | None
    wall_qty: int | None
    wall_persist_frames: int
    ladder_json: str
    total_lot_count: int
    ladder_truncated: bool


@dataclass(frozen=True)
class _BundleState:
    total_cost: int
    auction_ids: tuple[int, ...]


ListingInput = OrderLot | Mapping[str, object]

_DEFAULT_SCENARIO_SOLD_FRACTIONS: Mapping[str, Decimal | float] = MappingProxyType(
    {
        "stagnant": Decimal("0"),
        "low_fill": Decimal("0.25"),
        "partial_fill": Decimal("0.50"),
    }
)


def solve_buy_cost(
    lots: Sequence[OrderLot],
    target_qty: int,
    *,
    purchase_mode: PurchaseMode,
) -> BuyExecutionPlan:
    """Find the cheapest executable purchase of exactly ``target_qty`` units.

    Splittable lots use a price/auction-ID ordered greedy walk. Whole-lot
    purchases use a 0/1 minimum-cost subset-sum DP, with ascending auction-ID
    tuples breaking equal-cost ties. Every auction is counted at most once.

    Zero quantity is a successful no-buy plan. Invalid quantities, prices, or
    purchase modes raise ValueError.
    """
    _integer("target_qty", target_qty, minimum=0)
    _validate_purchase_mode(purchase_mode)
    ordered = _normalise_lots(lots)
    if purchase_mode == "per_unit":
        return _per_unit_plan(ordered, target_qty)
    states = _bundle_states(ordered, target_qty)
    return _bundle_plan(ordered, states, target_qty)


def solve_buy_lots(
    lots: Sequence[OrderLot],
    target_qty: int,
    *,
    purchase_mode: PurchaseMode,
) -> BuyExecutionPlan:
    """Expose the lot solver under the operation's alternative public name."""
    return solve_buy_cost(lots, target_qty, purchase_mode=purchase_mode)


def feasible_bundle_quantities(
    lots: Sequence[OrderLot],
    max_qty: int,
) -> dict[int, BuyExecutionPlan]:
    """Return the cheapest whole-lot plan for each achievable exact quantity.

    Keys cover only achievable quantities in ``1..max_qty``. One descending
    quantity DP evaluates all quantities, with O(len(lots) * max_qty) state
    transitions. ID tuples retain deterministic witnesses for cost ties.
    """
    _integer("max_qty", max_qty, minimum=0)
    ordered = _normalise_lots(lots)
    states = _bundle_states(ordered, max_qty)
    return {
        qty: _bundle_plan(ordered, states, qty)
        for qty in range(1, max_qty + 1)
        if states[qty] is not None
    }


def max_acceptable_total_buy_cost(
    planned_qty: int,
    planned_sold_qty: int,
    residual_qty: int,
    *,
    conservative_cash_in_by_deadline: int,
    conservative_residual_net_value: int,
    unpaid_future_fees: int,
    required_min_net_profit: int,
) -> int:
    """Apply the batch-level maximum buy-cost formula without rounding.

    Residual value must already be net of its disposal fees. Unpaid future
    fees must exclude fees already deducted from cash-in or residual value.
    A negative result is retained: no positive purchase meets that scenario.
    """
    for name, value in (
        ("planned_qty", planned_qty),
        ("planned_sold_qty", planned_sold_qty),
        ("residual_qty", residual_qty),
        ("conservative_cash_in_by_deadline", conservative_cash_in_by_deadline),
        ("conservative_residual_net_value", conservative_residual_net_value),
        ("unpaid_future_fees", unpaid_future_fees),
        ("required_min_net_profit", required_min_net_profit),
    ):
        _integer(name, value, minimum=0)
    if planned_sold_qty + residual_qty != planned_qty:
        raise ValueError("planned_sold_qty + residual_qty must equal planned_qty")
    return (
        conservative_cash_in_by_deadline
        + conservative_residual_net_value
        - unpaid_future_fees
        - required_min_net_profit
    )


def evaluate_entry_scenarios(
    planned_qty: int,
    target_sell_price: int,
    residual_liquidation_unit_price: int,
    *,
    first_listing_fee: int,
    planned_edit_or_relist_fee: int,
    residual_disposal_fee_fn: Callable[[int], int],
    required_min_net_profit: int,
    scenario_sold_fractions: Mapping[
        str, Decimal | float
    ] = _DEFAULT_SCENARIO_SOLD_FRACTIONS,
) -> dict[str, EntryScenarioBand]:
    """Calculate conservative total buy-cost limits for deadline scenarios.

    Fractions must be finite and within [0, 1]. Floats are interpreted through
    their decimal string representation. Integer-ratio arithmetic floors sold
    quantities exactly, independently of the active Decimal precision.
    """
    for name, value in (
        ("planned_qty", planned_qty),
        ("target_sell_price", target_sell_price),
        ("residual_liquidation_unit_price", residual_liquidation_unit_price),
        ("first_listing_fee", first_listing_fee),
        ("planned_edit_or_relist_fee", planned_edit_or_relist_fee),
        ("required_min_net_profit", required_min_net_profit),
    ):
        _integer(name, value, minimum=0)

    unpaid_fees = first_listing_fee + planned_edit_or_relist_fee
    result: dict[str, EntryScenarioBand] = {}
    for name, value in scenario_sold_fractions.items():
        if not isinstance(name, str) or not name:
            raise ValueError("scenario names must be non-empty strings")
        fraction = _nonnegative_decimal("scenario sold fraction", value)
        if fraction > 1:
            raise ValueError("scenario sold fraction must be within [0, 1]")
        numerator, denominator = fraction.as_integer_ratio()
        sold_qty = planned_qty * numerator // denominator
        residual_qty = planned_qty - sold_qty
        cash_in = sold_qty * target_sell_price
        disposal_fee = _integer(
            "residual_disposal_fee",
            residual_disposal_fee_fn(residual_qty),
            minimum=0,
        )
        residual_net = max(
            0,
            residual_qty * residual_liquidation_unit_price - disposal_fee,
        )
        max_cost = max_acceptable_total_buy_cost(
            planned_qty,
            sold_qty,
            residual_qty,
            conservative_cash_in_by_deadline=cash_in,
            conservative_residual_net_value=residual_net,
            unpaid_future_fees=unpaid_fees,
            required_min_net_profit=required_min_net_profit,
        )
        result[name] = EntryScenarioBand(
            scenario=name,
            planned_qty=planned_qty,
            sold_fraction=fraction,
            sold_qty=sold_qty,
            residual_qty=residual_qty,
            cash_in=cash_in,
            residual_net=residual_net,
            unpaid_fees=unpaid_fees,
            required_min_net_profit=required_min_net_profit,
            max_acceptable_buy_cost=max_cost,
        )
    return result


def compare_batch_candidates(
    lots: Sequence[OrderLot],
    candidate_quantities: Sequence[int],
    *,
    purchase_mode: PurchaseMode,
    budget_available_cash: int,
    scenario_evaluator: Callable[[int], tuple[int, int]],
) -> list[BatchCandidateComparison]:
    """Compare independent batch alternatives in ascending quantity order.

    ``scenario_evaluator(qty)`` returns
    ``(max_acceptable_buy_cost, immediate_fee_reserve)``. Each positive
    alternative must satisfy both ``cost + reserve <= available_cash`` and
    ``cost <= max_acceptable_buy_cost``.

    The no-buy quantity 0 is always included with zero cost and fees; its
    scenario evaluator is never called. Alternatives share the same input
    book and budget and are not treated as simultaneous purchases.
    """
    _validate_purchase_mode(purchase_mode)
    _integer("budget_available_cash", budget_available_cash, minimum=0)
    ordered = _normalise_lots(lots)
    quantities = sorted(
        {0}
        | {
            _integer("candidate quantity", qty, minimum=0)
            for qty in candidate_quantities
        }
    )
    states = (
        _bundle_states(ordered, quantities[-1])
        if purchase_mode == "bundle_only"
        else None
    )
    result: list[BatchCandidateComparison] = []
    for qty in quantities:
        if states is None:
            plan = _per_unit_plan(ordered, qty)
        else:
            plan = _bundle_plan(ordered, states, qty)

        if qty == 0:
            max_cost, fee_reserve = 0, 0
        else:
            max_cost, fee_reserve = scenario_evaluator(qty)
            _integer("max_acceptable_buy_cost", max_cost)
            _integer("immediate_fee_reserve", fee_reserve, minimum=0)

        cost = plan.total_cost
        result.append(
            BatchCandidateComparison(
                planned_qty=qty,
                buy_plan=plan,
                max_acceptable_buy_cost=max_cost,
                immediate_fee_reserve=fee_reserve,
                budget_available_cash=budget_available_cash,
                budget_ok=(
                    cost is not None
                    and cost + fee_reserve <= budget_available_cash
                ),
                entry_cost_ok=cost is not None and cost <= max_cost,
            )
        )
    return result


def build_orderbook_observation(
    current_listings: Sequence[ListingInput],
    previous_listings: Sequence[ListingInput] | None = None,
    *,
    current_ts: int,
    purchase_mode: PurchaseMode,
    coverage_hours: Decimal | float,
    reference_price: int | None = None,
    prev_wall_price: int | None = None,
    prev_wall_persist_frames: int = 0,
    grace_sec: int = 300,
    max_ladder_lots: int = 50,
) -> OrderbookObservation:
    """Extract evidence from completed, covered snapshots for one item.

    Listings may be OrderLot instances or mappings with auction_id, price,
    qty, and optional expires_at. The caller must establish collection
    coverage before treating an empty input as ``empty_confirmed``.

    Disappeared auctions expiring at or before ``current_ts + grace_sec``
    are excluded from premature disappearance. Unknown expiry is retained
    as an observed disappearance, without claiming it was a sale.

    The JSON ladder retains the first ``max_ladder_lots`` unique auctions
    in price/ID order. Its explicit truncation flag prevents interpreting
    retained depth as evidence about the rest of the original book.
    """
    _validate_purchase_mode(purchase_mode)
    _integer("current_ts", current_ts, minimum=0)
    _integer("grace_sec", grace_sec, minimum=0)
    _integer("max_ladder_lots", max_ladder_lots, minimum=1)
    _integer("prev_wall_persist_frames", prev_wall_persist_frames, minimum=0)
    if reference_price is not None:
        _integer("reference_price", reference_price, minimum=0)
    if prev_wall_price is not None:
        _integer("prev_wall_price", prev_wall_price, minimum=1)
    coverage = float(_nonnegative_decimal("coverage_hours", coverage_hours))
    if not isfinite(coverage):
        raise ValueError("coverage_hours must fit a finite float")

    current = _normalise_lots(
        [_as_order_lot(listing) for listing in current_listings]
    )
    previous = _normalise_lots(
        [_as_order_lot(listing) for listing in previous_listings]
        if previous_listings is not None
        else []
    )
    current_by_id = {lot.auction_id: lot for lot in current}
    same_id_qty_drop = 0
    full_disappear_qty = 0
    expiry_cutoff = current_ts + grace_sec
    for old in previous:
        new = current_by_id.get(old.auction_id)
        if new is not None:
            same_id_qty_drop += max(0, old.qty - new.qty)
        elif old.expires_at is None or old.expires_at > expiry_cutoff:
            full_disappear_qty += old.qty

    visible_lower_qty = (
        None
        if reference_price is None
        else sum(lot.qty for lot in current if lot.price < reference_price)
    )
    wall_price: int | None = None
    wall_qty: int | None = None
    wall_persist_frames = 0
    best_ask = current[0].price if current else None
    if best_ask is not None:
        level_quantities: dict[int, int] = {}
        for lot in current:
            if lot.price * 4 > best_ask * 5:
                break
            level_quantities[lot.price] = (
                level_quantities.get(lot.price, 0) + lot.qty
            )
        wall_price = min(
            level_quantities,
            key=lambda price: (-level_quantities[price], price),
        )
        wall_qty = level_quantities[wall_price]
        wall_persist_frames = (
            prev_wall_persist_frames + 1
            if prev_wall_price == wall_price
            else 1
        )

    retained = current[:max_ladder_lots]
    ladder_json = json.dumps(
        [
            {
                "auction_id": lot.auction_id,
                "price": lot.price,
                "qty": lot.qty,
                "expires_at": lot.expires_at,
            }
            for lot in retained
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return OrderbookObservation(
        current_ts=current_ts,
        book_state="stocked" if current else "empty_confirmed",
        purchase_mode=purchase_mode,
        coverage_hours=coverage,
        best_ask=best_ask,
        total_listed_qty=sum(lot.qty for lot in current),
        same_id_qty_drop=same_id_qty_drop,
        full_disappear_qty=full_disappear_qty,
        visible_lower_qty=visible_lower_qty,
        wall_price=wall_price,
        wall_qty=wall_qty,
        wall_persist_frames=wall_persist_frames,
        ladder_json=ladder_json,
        total_lot_count=len(current),
        ladder_truncated=len(retained) < len(current),
    )


def assign_opportunity_group_id(
    prev_group_id: str | None,
    prev_available_at: int | None,
    curr_available_at: int,
    *,
    item_id: int,
    window_id: int,
    merge_gap_sec: int = 3600,
) -> str:
    """Group consecutive updates without discarding individual observations.

    The merge interval is inclusive and measured from the previous update,
    not the group's first update. Generated IDs encode window, item, and
    first availability time, making retries deterministic and preventing
    generated IDs from merging across scopes.

    An opaque externally assigned previous ID is accepted as belonging to
    the caller's supplied scope; callers must partition those IDs by item
    and window before invoking this function.
    """
    _integer("curr_available_at", curr_available_at, minimum=0)
    _integer("item_id", item_id, minimum=0)
    _integer("window_id", window_id, minimum=0)
    _integer("merge_gap_sec", merge_gap_sec, minimum=0)
    if prev_available_at is not None:
        _integer("prev_available_at", prev_available_at, minimum=0)
    if prev_group_id is not None and (
        not isinstance(prev_group_id, str) or not prev_group_id
    ):
        raise ValueError("prev_group_id must be a non-empty string or None")

    prefix = f"opportunity:{window_id}:{item_id}:"
    if prev_group_id is not None and prev_available_at is not None:
        same_scope = (
            not prev_group_id.startswith("opportunity:")
            or prev_group_id.startswith(prefix)
        )
        gap = curr_available_at - prev_available_at
        if same_scope and 0 <= gap <= merge_gap_sec:
            return prev_group_id
    return f"{prefix}{curr_available_at}"


def _integer(name: str, value: object, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _nonnegative_decimal(name: str, value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, float)):
        raise ValueError(f"{name} must be a finite non-negative number")
    parsed = Decimal(str(value))
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return parsed


def _validate_purchase_mode(purchase_mode: PurchaseMode) -> None:
    if purchase_mode not in ("per_unit", "bundle_only"):
        raise ValueError("purchase_mode must be 'per_unit' or 'bundle_only'")


def _normalise_lots(lots: Sequence[OrderLot]) -> list[OrderLot]:
    """Validate all rows, then deterministically retain one row per auction.

    Identical duplicates collapse naturally. Conflicting duplicate IDs retain
    the lowest price, then lowest quantity, then earliest known expiry, with
    unknown expiry last. This deterministic policy never adds their stock
    together. All rows are validated, including rows that will be discarded.
    """
    validated: list[OrderLot] = []
    for lot in lots:
        if not isinstance(lot, OrderLot):
            raise ValueError("lots must contain OrderLot instances")
        _integer("auction_id", lot.auction_id)
        _integer("price", lot.price, minimum=1)
        _integer("qty", lot.qty, minimum=1)
        if lot.expires_at is not None:
            _integer("expires_at", lot.expires_at)
        validated.append(lot)

    validated.sort(
        key=lambda lot: (
            lot.price,
            lot.auction_id,
            lot.qty,
            lot.expires_at is None,
            lot.expires_at if lot.expires_at is not None else 0,
        )
    )
    seen: set[int] = set()
    ordered: list[OrderLot] = []
    for lot in validated:
        if lot.auction_id not in seen:
            seen.add(lot.auction_id)
            ordered.append(lot)
    return ordered


def _as_order_lot(listing: ListingInput) -> OrderLot:
    if isinstance(listing, OrderLot):
        return listing
    if not isinstance(listing, Mapping):
        raise ValueError("listings must be OrderLot instances or mappings")
    try:
        auction_id = listing["auction_id"]
        price = listing["price"]
        qty = listing["qty"]
    except KeyError as exc:
        raise ValueError(f"listing is missing required field {exc.args[0]}") from exc
    expires_at = listing.get("expires_at")
    return OrderLot(
        auction_id=_integer("auction_id", auction_id),
        price=_integer("price", price, minimum=1),
        qty=_integer("qty", qty, minimum=1),
        expires_at=(
            None
            if expires_at is None
            else _integer("expires_at", expires_at)
        ),
    )


def _per_unit_plan(
    ordered: Sequence[OrderLot],
    target_qty: int,
) -> BuyExecutionPlan:
    needed = target_qty
    fills: list[OrderLot] = []
    cost = 0
    for lot in ordered:
        if needed == 0:
            break
        take = min(lot.qty, needed)
        fills.append(OrderLot(lot.auction_id, lot.price, take, lot.expires_at))
        cost += take * lot.price
        needed -= take
    return BuyExecutionPlan(
        target_qty=target_qty,
        purchase_mode="per_unit",
        fillable=needed == 0,
        max_fillable_qty=target_qty - needed,
        total_cost=cost if needed == 0 else None,
        fills=fills,
    )


def _bundle_states(
    ordered: Sequence[OrderLot],
    max_qty: int,
) -> list[_BundleState | None]:
    states: list[_BundleState | None] = [None] * (max_qty + 1)
    states[0] = _BundleState(0, ())
    # Descending IDs let each witness prepend the current ID in sorted order.
    # Descending quantities ensure each auction can be used only once.
    for lot in sorted(ordered, key=lambda value: value.auction_id, reverse=True):
        lot_cost = lot.total_cost
        for qty in range(max_qty, lot.qty - 1, -1):
            previous = states[qty - lot.qty]
            if previous is None:
                continue
            candidate_cost = previous.total_cost + lot_cost
            existing = states[qty]
            if existing is not None and candidate_cost > existing.total_cost:
                continue
            candidate_ids = (lot.auction_id,) + previous.auction_ids
            if (
                existing is None
                or candidate_cost < existing.total_cost
                or candidate_ids < existing.auction_ids
            ):
                states[qty] = _BundleState(candidate_cost, candidate_ids)
    return states


def _bundle_plan(
    ordered: Sequence[OrderLot],
    states: Sequence[_BundleState | None],
    target_qty: int,
) -> BuyExecutionPlan:
    selected_qty = target_qty
    state = states[selected_qty]
    fillable = state is not None
    while state is None:
        selected_qty -= 1
        state = states[selected_qty]
    selected_ids = set(state.auction_ids)
    return BuyExecutionPlan(
        target_qty=target_qty,
        purchase_mode="bundle_only",
        fillable=fillable,
        max_fillable_qty=selected_qty,
        total_cost=state.total_cost if fillable else None,
        fills=[lot for lot in ordered if lot.auction_id in selected_ids],
    )
