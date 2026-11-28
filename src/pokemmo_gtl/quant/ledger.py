"""Event-sourced, per-arm accounting and equity-anchored loss budgets.

The event stream is authoritative. Appends, receipt checks, full replay, and
projection replacement share one BEGIN IMMEDIATE transaction.

All money is integer yen. Inventory capital-days use remaining acquisition
cost plus confirmed, unclaimed proceeds. An unknown inventory cost makes
capital-days unknown while that inventory is held.

ACCOUNT_OPEN supplies opening cash and receivables. Known-cost legacy
inventory adds its carrying cost to opening equity. Unknown-cost legacy
inventory never acquires an invented historical cost or historical profit.
Settlement equity can still be evaluated with explicit residual valuations.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields, replace

from pokemmo_gtl.db.connection import write_transaction


class LedgerValidationError(ValueError):
    """An event, receipt, or resulting ledger state is invalid."""


class LedgerIdempotencyConflictError(LedgerValidationError):
    """An idempotency key was reused for a different canonical payload."""


EVENT_TYPES = frozenset(
    {
        "ACCOUNT_OPEN",
        "LEGACY_POSITION_INIT",
        "LEGACY_ORDER_INIT",
        "BUY",
        "LIST",
        "ORDER_PUBLIC",
        "DIRECT_EDIT",
        "SALE_CONFIRMED",
        "CLAIM_CASH",
        "CANCEL",
        "EXPIRE",
        "EXTERNAL_TRANSFER",
    }
)

_BASELINE_TYPES = frozenset(
    {"ACCOUNT_OPEN", "LEGACY_POSITION_INIT", "LEGACY_ORDER_INIT"}
)
_LEGACY_TYPES = frozenset({"LEGACY_POSITION_INIT", "LEGACY_ORDER_INIT"})
_LIVE_ORDER_STATUSES = frozenset({"PENDING", "ACTIVE", "PARTIAL"})
_SECONDS_PER_DAY = 86_400
_SQLITE_MIN_INT = -(2**63)
_SQLITE_MAX_INT = 2**63 - 1


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    """One immutable event; database identity and digest are derived metadata."""

    idempotency_key: str
    arm_id: str
    event_type: str
    event_ts: int | None = None
    event_order: int = 0
    item_id: int | None = None
    position_id: int | None = None
    order_id: int | None = None
    parent_order_id: int | None = None
    exit_arm: str | None = None
    settlement_due_at: int | None = None
    batch_claim_id: str | None = None
    slot_limit: int | None = None
    max_loss_budget: int | None = None
    new_listed_price: int | None = None
    new_expires_at: int | None = None
    new_public_visible_at: int | None = None
    total_amount: int | None = None
    unit_price: int | None = None
    qty_delta: int = 0
    available_cash_delta: int = 0
    unclaimed_delta: int = 0
    fee_paid: int = 0
    slot_delta: int = 0
    fills_detail_json: str | None = None
    voucher_note: str | None = None
    unknown_reason: str | None = None
    event_id: int | None = None
    payload_sha256: str | None = None


EventInput = LedgerEvent | Mapping[str, object] | sqlite3.Row

_PAYLOAD_FIELDS = tuple(
    member.name
    for member in fields(LedgerEvent)
    if member.name not in {"event_id", "payload_sha256"}
)
_EVENT_COLUMNS = ("event_id",) + _PAYLOAD_FIELDS + ("payload_sha256",)
_EVENT_FIELD_NAMES = frozenset(member.name for member in fields(LedgerEvent))
_NULLABLE_INTEGER_FIELDS = frozenset(
    {
        "event_id",
        "event_ts",
        "item_id",
        "position_id",
        "order_id",
        "parent_order_id",
        "settlement_due_at",
        "slot_limit",
        "max_loss_budget",
        "new_listed_price",
        "new_expires_at",
        "new_public_visible_at",
        "total_amount",
        "unit_price",
    }
)
_REQUIRED_INTEGER_FIELDS = (
    "event_order",
    "qty_delta",
    "available_cash_delta",
    "unclaimed_delta",
    "fee_paid",
    "slot_delta",
)
_OPTIONAL_TEXT_FIELDS = (
    "exit_arm",
    "batch_claim_id",
    "fills_detail_json",
    "voucher_note",
    "unknown_reason",
    "payload_sha256",
)


@dataclass(slots=True)
class PositionState:
    """Position projection, including non-persisted historical-profit details."""

    position_id: int
    arm_id: str
    item_id: int
    is_legacy_inventory: int
    opened_at: int | None
    initial_qty: int
    remaining_qty: int
    unallocated_qty: int
    total_cost: int | None
    remaining_cost: int | None
    unknown_cost_reason: str | None
    settlement_due_at: int | None
    status: str
    realized_proceeds: int = 0
    fees_paid: int = 0
    fee_refunds: int = 0

    @property
    def historical_profit(self) -> int | None:
        """Realized proceeds less released cost and net fees; None if unknown."""
        if self.total_cost is None or self.remaining_cost is None:
            return None
        released_cost = self.total_cost - self.remaining_cost
        return (
            self.realized_proceeds
            - released_cost
            - self.fees_paid
            + self.fee_refunds
        )

    @property
    def historical_profit_status(self) -> str:
        return "UNKNOWN" if self.historical_profit is None else "KNOWN"


@dataclass(slots=True)
class OrderState:
    """Order projection; fills retain their original confirmation timestamps."""

    order_id: int
    parent_order_id: int | None
    arm_id: str
    position_id: int
    item_id: int
    exit_arm: str | None
    submitted_at: int | None
    public_visible_at: int | None
    slot_closed_at: int | None
    listed_price: int
    initial_qty: int
    sold_qty: int
    remaining_qty: int
    first_fill_at: int | None
    listing_fee_paid: int
    edit_fee_paid: int
    edit_count: int
    expires_at: int | None
    status: str
    _fills: list[tuple[int, int]] = field(default_factory=list, repr=False)

    @property
    def first_fill_delay_seconds(self) -> int | None:
        if self.public_visible_at is None or self.first_fill_at is None:
            return None
        return self.first_fill_at - self.public_visible_at


@dataclass(slots=True)
class LedgerState:
    """A selected arm or the aggregate ledger.

    Aggregate results expose independent states in ``arms``. Selected-arm
    results expose their own balances, positions, orders, and history directly.

    ``remaining_loss_budget`` is a carrying-cost diagnostic, not an opening
    approval: an actual opening decision must supply conservative valuations
    to ``check_open_loss_budget``. It is None when historical opening
    inventory cost is unknown.
    """

    arm_id: str | None = None
    opening_equity: int = 0
    net_external_transfer: int = 0
    available_cash: int = 0
    unclaimed_proceeds: int = 0
    occupied_slots: int = 0
    total_fees_paid: int = 0
    total_fee_refunds: int = 0
    slot_limit: int | None = None
    max_loss_budget: int | None = None
    account_opened_at: int | None = None
    has_account: bool = False
    positions: dict[int, PositionState] = field(default_factory=dict)
    orders: dict[int, OrderState] = field(default_factory=dict)
    arms: dict[str, LedgerState] = field(default_factory=dict, repr=False)
    _history: list[tuple[int | None, int | None, int]] = field(
        default_factory=list, repr=False
    )
    _as_of_ts: int | None = field(default=None, repr=False)
    _observed_at: int | None = field(default=None, repr=False)

    @property
    def base_equity(self) -> int:
        return self.opening_equity + self.net_external_transfer

    @property
    def historical_profit(self) -> int | None:
        profits = [position.historical_profit for position in self.positions.values()]
        if any(profit is None for profit in profits):
            return None
        return sum(profit for profit in profits if profit is not None)

    @property
    def historical_profit_status(self) -> str:
        return "UNKNOWN" if self.historical_profit is None else "KNOWN"

    @property
    def remaining_loss_budget(self) -> int | None:
        if self.max_loss_budget is None:
            return None
        if any(position.total_cost is None for position in self.positions.values()):
            return None
        carrying_value = sum(
            position.remaining_cost or 0 for position in self.positions.values()
        )
        equity = self.available_cash + self.unclaimed_proceeds + carrying_value
        return self.max_loss_budget - max(0, self.base_equity - equity)

    def capital_days(self, t0: int, t1: int) -> float | None:
        """Integrate remaining inventory cost plus unclaimed proceeds over time.

        Null-timestamp legacy baselines apply at the start of the interval.
        An as-of snapshot clips the requested end to its observation boundary.
        """
        return self._integrate(t0, t1, value_index=1)

    def slot_days(self, t0: int, t1: int) -> float:
        """Integrate occupied slots, including Pending submission time."""
        result = self._integrate(t0, t1, value_index=2)
        assert result is not None
        return result

    def _integrate(
        self, t0: int, t1: int, *, value_index: int
    ) -> float | None:
        _integer(t0, "t0")
        _integer(t1, "t1")
        _require(t1 >= t0, "t1 must not precede t0")
        end = min(t1, self._as_of_ts) if self._as_of_ts is not None else t1
        if end <= t0:
            return 0.0

        current: int | None = 0
        cursor = t0
        accumulated = 0
        for frame in self._history:
            timestamp = frame[0]
            value = frame[value_index]
            if timestamp is None or timestamp <= t0:
                current = value
                continue
            if timestamp >= end:
                break
            if current is None and timestamp > cursor:
                return None
            accumulated += (current or 0) * (timestamp - cursor)
            cursor = timestamp
            current = value

        if current is None and end > cursor:
            return None
        accumulated += (current or 0) * (end - cursor)
        return accumulated / _SECONDS_PER_DAY

    def sold_qty_ratio_by_deadline(
        self,
        deadline_ts: int | None = None,
        *,
        horizon_seconds: int | None = None,
    ) -> float | None:
        """Quantity-weighted fill ratio for orders with known public starts.

        ``deadline_ts`` is an absolute cutoff. ``horizon_seconds`` instead
        supplies an order-relative deadline measured from public_visible_at.
        When both are supplied, only horizons matured by the absolute cutoff
        are included. Snapshot boundaries always apply. Unknown public starts
        are excluded rather than assigned a fabricated start time.
        """
        observations = self._exit_observations(deadline_ts, horizon_seconds)
        denominator = sum(order.initial_qty for order, _ in observations)
        if not denominator:
            return None
        return sum(sold for _, sold in observations) / denominator

    def full_sellout_order_ratio(
        self,
        deadline_ts: int | None = None,
        *,
        horizon_seconds: int | None = None,
    ) -> float | None:
        """Fraction of eligible orders fully sold by their public-start deadline."""
        observations = self._exit_observations(deadline_ts, horizon_seconds)
        if not observations:
            return None
        return sum(
            sold == order.initial_qty for order, sold in observations
        ) / len(observations)

    def _exit_observations(
        self, deadline_ts: int | None, horizon_seconds: int | None
    ) -> list[tuple[OrderState, int]]:
        if deadline_ts is not None:
            _integer(deadline_ts, "deadline_ts")
        if horizon_seconds is not None:
            _integer(horizon_seconds, "horizon_seconds", minimum=0)

        limit = deadline_ts
        if limit is None:
            limit = (
                self._as_of_ts
                if self._as_of_ts is not None
                else self._observed_at
            )
        elif self._as_of_ts is not None:
            limit = min(limit, self._as_of_ts)
        if limit is None:
            return []

        observations = []
        for order in self.orders.values():
            public_at = order.public_visible_at
            if public_at is None or public_at > limit:
                continue
            cutoff = limit
            if horizon_seconds is not None:
                cutoff = public_at + horizon_seconds
                if cutoff > limit:
                    continue
            sold = sum(
                quantity
                for timestamp, quantity in order._fills
                if public_at <= timestamp <= cutoff
            )
            observations.append((order, sold))
        return observations


@dataclass(frozen=True, slots=True)
class EquitySummary:
    available_cash: int
    unclaimed_proceeds: int
    residual_net_value: int
    unpaid_future_fees: int
    opening_equity: int
    net_external_transfer: int
    base_equity: int
    end_net_equity: int
    delta_net_equity: int
    historical_profit: int | None


@dataclass(frozen=True, slots=True)
class LossBudgetCheck:
    allowed: bool
    post_cash: int
    base_equity: int
    e_cons_post: int
    projected_total_loss: int
    current_conservative_loss: int
    remaining_loss_budget: int
    max_loss_budget: int


_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS user_ledger_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        idempotency_key TEXT NOT NULL UNIQUE,
        payload_sha256 TEXT NOT NULL,
        arm_id TEXT NOT NULL,
        event_ts INTEGER,
        event_order INTEGER NOT NULL DEFAULT 0,
        event_type TEXT NOT NULL,
        item_id INTEGER,
        position_id INTEGER,
        order_id INTEGER,
        parent_order_id INTEGER,
        exit_arm TEXT,
        settlement_due_at INTEGER,
        batch_claim_id TEXT,
        slot_limit INTEGER,
        max_loss_budget INTEGER,
        new_listed_price INTEGER,
        new_expires_at INTEGER,
        new_public_visible_at INTEGER,
        total_amount INTEGER,
        unit_price INTEGER,
        qty_delta INTEGER NOT NULL DEFAULT 0,
        available_cash_delta INTEGER NOT NULL DEFAULT 0,
        unclaimed_delta INTEGER NOT NULL DEFAULT 0,
        fee_paid INTEGER NOT NULL DEFAULT 0,
        slot_delta INTEGER NOT NULL DEFAULT 0,
        fills_detail_json TEXT,
        voucher_note TEXT,
        unknown_reason TEXT
    ) STRICT
    """,
    """
    CREATE TABLE IF NOT EXISTS user_positions (
        position_id INTEGER PRIMARY KEY,
        arm_id TEXT NOT NULL,
        item_id INTEGER NOT NULL,
        is_legacy_inventory INTEGER NOT NULL DEFAULT 0,
        opened_at INTEGER,
        initial_qty INTEGER NOT NULL,
        remaining_qty INTEGER NOT NULL,
        unallocated_qty INTEGER NOT NULL,
        total_cost INTEGER,
        remaining_cost INTEGER,
        unknown_cost_reason TEXT,
        settlement_due_at INTEGER,
        status TEXT NOT NULL
    ) STRICT
    """,
    """
    CREATE TABLE IF NOT EXISTS user_orders (
        order_id INTEGER PRIMARY KEY,
        parent_order_id INTEGER,
        arm_id TEXT NOT NULL,
        position_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        exit_arm TEXT,
        submitted_at INTEGER,
        public_visible_at INTEGER,
        slot_closed_at INTEGER,
        listed_price INTEGER NOT NULL,
        initial_qty INTEGER NOT NULL,
        sold_qty INTEGER NOT NULL DEFAULT 0,
        remaining_qty INTEGER NOT NULL,
        first_fill_at INTEGER,
        listing_fee_paid INTEGER NOT NULL DEFAULT 0,
        edit_fee_paid INTEGER NOT NULL DEFAULT 0,
        edit_count INTEGER NOT NULL DEFAULT 0,
        expires_at INTEGER,
        status TEXT NOT NULL
    ) STRICT
    """,
)


def ensure_ledger_tables(conn: sqlite3.Connection) -> None:
    """Create the exact migration-002 tables without committing caller work."""
    for statement in _SCHEMA_STATEMENTS:
        conn.execute(statement)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise LedgerValidationError(message)


def _integer(value: object, name: str, *, minimum: int | None = None) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LedgerValidationError(f"{name} must be an integer")
    _require(
        _SQLITE_MIN_INT <= value <= _SQLITE_MAX_INT,
        f"{name} must fit a SQLite INTEGER",
    )
    if minimum is not None:
        _require(value >= minimum, f"{name} must be >= {minimum}")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def _payload_digest(event: LedgerEvent) -> str:
    payload = {name: getattr(event, name) for name in _PAYLOAD_FIELDS}
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalise_event(raw: EventInput) -> LedgerEvent:
    if isinstance(raw, LedgerEvent):
        event = raw
    else:
        _require(
            isinstance(raw, (Mapping, sqlite3.Row)),
            "events must be LedgerEvent instances or mappings",
        )
        values = dict(raw)
        extra = set(values) - _EVENT_FIELD_NAMES
        _require(not extra, f"unknown event fields: {sorted(extra)}")
        try:
            event = LedgerEvent(**values)
        except TypeError as exc:
            raise LedgerValidationError(f"invalid event fields: {exc}") from exc

    for name in ("idempotency_key", "arm_id", "event_type"):
        value = getattr(event, name)
        _require(
            isinstance(value, str) and bool(value.strip()),
            f"{name} must be non-empty text",
        )
    for name in _OPTIONAL_TEXT_FIELDS:
        value = getattr(event, name)
        _require(
            value is None or isinstance(value, str),
            f"{name} must be text or None",
        )
    for name in ("exit_arm", "batch_claim_id"):
        value = getattr(event, name)
        _require(value is None or bool(value.strip()), f"{name} cannot be empty")

    for name in _REQUIRED_INTEGER_FIELDS:
        _integer(getattr(event, name), name)
    for name in _NULLABLE_INTEGER_FIELDS:
        value = getattr(event, name)
        if value is not None:
            _integer(value, name)
    for name in ("event_id", "item_id", "position_id", "order_id", "parent_order_id"):
        value = getattr(event, name)
        if value is not None:
            _integer(value, name, minimum=1)
    _integer(event.event_order, "event_order", minimum=0)

    if event.fills_detail_json is not None:
        try:
            detail = json.loads(
                event.fills_detail_json, parse_constant=_reject_json_constant
            )
            _require(
                isinstance(detail, (dict, list)),
                "fills_detail_json must encode an object or array",
            )
            canonical_detail = json.dumps(
                detail,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise LedgerValidationError(f"invalid fills_detail_json: {exc}") from exc
        event = replace(event, fills_detail_json=canonical_detail)

    digest = _payload_digest(event)
    if event.payload_sha256 is not None:
        _require(
            event.payload_sha256 == digest,
            f"payload_sha256 mismatch for {event.idempotency_key}",
        )
    return replace(event, payload_sha256=digest)


def canonical_payload_sha256(event: EventInput) -> str:
    """Hash all default-filled event fields except event_id and the digest.

    JSON object key order and insignificant fills-detail whitespace do not
    change the hash. Array order remains significant.
    """
    return _payload_digest(_normalise_event(event))


def _required_fields(event: LedgerEvent, *names: str) -> None:
    for name in names:
        _require(
            getattr(event, name) is not None,
            f"{event.event_type} requires {name}",
        )


def _validate_event(
    event: LedgerEvent, *, allow_null_account_open: bool = False
) -> None:
    kind = event.event_type
    _require(kind in EVENT_TYPES, f"unsupported event_type: {kind}")
    null_open = kind == "ACCOUNT_OPEN" and allow_null_account_open
    if kind not in _LEGACY_TYPES and not null_open:
        _required_fields(event, "event_ts")

    _integer(event.fee_paid, "fee_paid", minimum=0)
    for name in ("total_amount", "unit_price", "slot_limit", "max_loss_budget"):
        value = getattr(event, name)
        if value is not None:
            _integer(value, name, minimum=0)
    if event.new_listed_price is not None:
        _integer(event.new_listed_price, "new_listed_price", minimum=1)

    if kind != "ACCOUNT_OPEN":
        _require(
            event.slot_limit is None and event.max_loss_budget is None,
            "slot_limit and max_loss_budget belong to ACCOUNT_OPEN",
        )
    if kind != "CLAIM_CASH":
        _require(event.batch_claim_id is None, "batch_claim_id requires CLAIM_CASH")
    if kind not in {"LIST", "LEGACY_ORDER_INIT"}:
        _require(
            event.parent_order_id is None,
            "parent_order_id belongs to a listing event",
        )
    if kind not in {
        "LEGACY_POSITION_INIT",
        "LEGACY_ORDER_INIT",
        "BUY",
        "LIST",
        "SALE_CONFIRMED",
    }:
        _require(event.qty_delta == 0, f"{kind} requires qty_delta == 0")
    if kind not in {"LIST", "DIRECT_EDIT"}:
        _require(event.fee_paid == 0, f"{kind} requires fee_paid == 0")
    if kind not in {"ACCOUNT_OPEN", "SALE_CONFIRMED", "CLAIM_CASH"}:
        _require(event.unclaimed_delta == 0, f"{kind} cannot change receivables")
    if kind in _LEGACY_TYPES or kind in {"ORDER_PUBLIC", "SALE_CONFIRMED"}:
        _require(event.available_cash_delta == 0, f"{kind} cannot change cash")

    fixed_slots = {
        "ACCOUNT_OPEN": 0,
        "LEGACY_POSITION_INIT": 0,
        "LEGACY_ORDER_INIT": 1,
        "BUY": 0,
        "LIST": 1,
        "ORDER_PUBLIC": 0,
        "DIRECT_EDIT": 0,
        "CLAIM_CASH": 0,
        "CANCEL": -1,
        "EXPIRE": -1,
        "EXTERNAL_TRANSFER": 0,
    }
    if kind in fixed_slots:
        _require(
            event.slot_delta == fixed_slots[kind],
            f"{kind} requires slot_delta == {fixed_slots[kind]}",
        )

    if kind == "ACCOUNT_OPEN":
        _require(event.available_cash_delta >= 0, "opening cash cannot be negative")
        _require(event.unclaimed_delta >= 0, "opening receivables cannot be negative")
    elif kind == "LEGACY_POSITION_INIT":
        _required_fields(event, "item_id", "position_id")
        _require(event.qty_delta > 0, "legacy inventory quantity must be positive")
        if event.total_amount is None or event.event_ts is None:
            _require(
                bool(event.unknown_reason and event.unknown_reason.strip()),
                "unknown legacy cost or timestamp requires unknown_reason",
            )
    elif kind == "LEGACY_ORDER_INIT":
        _required_fields(
            event, "item_id", "position_id", "order_id", "new_listed_price"
        )
        _require(event.qty_delta > 0, "legacy order quantity must be positive")
        if event.event_ts is None or event.new_public_visible_at is None:
            _require(
                bool(event.unknown_reason and event.unknown_reason.strip()),
                "unknown legacy order timing requires unknown_reason",
            )
        if event.event_ts is not None and event.new_public_visible_at is not None:
            _require(
                event.new_public_visible_at <= event.event_ts,
                "a legacy order must already be publicly visible",
            )
    elif kind == "BUY":
        _required_fields(
            event, "item_id", "position_id", "total_amount", "settlement_due_at"
        )
        _require(event.qty_delta > 0, "BUY quantity must be positive")
        _require(event.total_amount is not None and event.total_amount > 0,
                 "BUY total_amount must be positive")
        _require(
            event.available_cash_delta == -event.total_amount,
            "BUY cash delta must equal -total_amount",
        )
    elif kind == "LIST":
        _required_fields(
            event,
            "item_id",
            "position_id",
            "order_id",
            "new_listed_price",
            "new_expires_at",
        )
        _require(event.qty_delta > 0, "LIST quantity must be positive")
        _require(
            event.available_cash_delta == -event.fee_paid,
            "LIST cash delta must equal -fee_paid",
        )
    elif kind == "ORDER_PUBLIC":
        _required_fields(event, "order_id", "new_public_visible_at")
        _require(
            event.new_public_visible_at <= event.event_ts,
            "ORDER_PUBLIC visibility cannot follow its confirmation timestamp",
        )
    elif kind == "DIRECT_EDIT":
        _required_fields(event, "order_id", "new_listed_price")
        _require(
            event.available_cash_delta == -event.fee_paid,
            "DIRECT_EDIT cash delta must equal -fee_paid",
        )
    elif kind == "SALE_CONFIRMED":
        _required_fields(event, "order_id", "position_id", "total_amount")
        _require(event.qty_delta < 0, "SALE_CONFIRMED quantity must be negative")
        _require(
            event.total_amount is not None and event.total_amount > 0,
            "SALE_CONFIRMED total_amount must be positive",
        )
        _require(
            event.unclaimed_delta == event.total_amount,
            "sale receivables must equal total_amount",
        )
        _require(event.slot_delta in {-1, 0}, "sale slot delta must be -1 or 0")
    elif kind == "CLAIM_CASH":
        _require(event.unclaimed_delta < 0, "CLAIM_CASH must reduce receivables")
        _require(
            event.available_cash_delta == -event.unclaimed_delta,
            "CLAIM_CASH cash must equal claimed receivables",
        )
    elif kind in {"CANCEL", "EXPIRE"}:
        _required_fields(event, "order_id")
        _require(event.available_cash_delta >= 0, "a close refund cannot be negative")
    elif kind == "EXTERNAL_TRANSFER":
        _require(
            event.available_cash_delta != 0,
            "EXTERNAL_TRANSFER requires a nonzero cash delta",
        )


def _position_for_event(
    arm: LedgerState, event: LedgerEvent
) -> PositionState:
    position = arm.positions.get(event.position_id)
    _require(
        position is not None,
        f"unknown position_id {event.position_id} in arm {event.arm_id}",
    )
    assert position is not None
    if event.item_id is not None:
        _require(position.item_id == event.item_id, "position item_id mismatch")
    return position


def _order_for_event(arm: LedgerState, event: LedgerEvent) -> OrderState:
    order = arm.orders.get(event.order_id)
    _require(
        order is not None,
        f"unknown order_id {event.order_id} in arm {event.arm_id}",
    )
    assert order is not None
    if event.position_id is not None:
        _require(
            order.position_id == event.position_id, "order position_id mismatch"
        )
    if event.item_id is not None:
        _require(order.item_id == event.item_id, "order item_id mismatch")
    return order


def _create_position(
    aggregate: LedgerState, arm: LedgerState, event: LedgerEvent, *, legacy: bool
) -> None:
    assert event.position_id is not None
    assert event.item_id is not None
    _require(
        event.position_id not in aggregate.positions,
        f"position_id {event.position_id} already exists",
    )
    position = PositionState(
        position_id=event.position_id,
        arm_id=event.arm_id,
        item_id=event.item_id,
        is_legacy_inventory=int(legacy),
        opened_at=event.event_ts,
        initial_qty=event.qty_delta,
        remaining_qty=event.qty_delta,
        unallocated_qty=event.qty_delta,
        total_cost=event.total_amount,
        remaining_cost=event.total_amount,
        unknown_cost_reason=(
            event.unknown_reason if event.total_amount is None else None
        ),
        settlement_due_at=event.settlement_due_at,
        status="OPEN",
    )
    aggregate.positions[position.position_id] = position
    arm.positions[position.position_id] = position
    if legacy and event.total_amount is not None:
        arm.opening_equity += event.total_amount


def _create_order(
    aggregate: LedgerState, arm: LedgerState, event: LedgerEvent, *, legacy: bool
) -> None:
    assert event.order_id is not None
    assert event.new_listed_price is not None
    _require(
        event.order_id not in aggregate.orders,
        f"order_id {event.order_id} already exists",
    )
    position = _position_for_event(arm, event)
    _require(position.status == "OPEN", "cannot list a closed position")
    if legacy:
        _require(
            bool(position.is_legacy_inventory),
            "LEGACY_ORDER_INIT requires a legacy position",
        )
    _require(
        event.qty_delta <= position.unallocated_qty,
        "listing quantity exceeds unallocated inventory",
    )
    if event.parent_order_id is not None:
        parent = arm.orders.get(event.parent_order_id)
        _require(parent is not None, "unknown parent_order_id")
        assert parent is not None
        _require(
            parent.position_id == position.position_id,
            "relist parent must belong to the same position",
        )
        _require(
            parent.status in {"CANCELLED", "EXPIRED"},
            "relist parent must already be cancelled or expired",
        )

    visible_now = (
        event.new_public_visible_at is not None
        and event.event_ts is not None
        and event.new_public_visible_at <= event.event_ts
    )
    order = OrderState(
        order_id=event.order_id,
        parent_order_id=event.parent_order_id,
        arm_id=event.arm_id,
        position_id=position.position_id,
        item_id=position.item_id,
        exit_arm=event.exit_arm,
        submitted_at=event.event_ts,
        public_visible_at=event.new_public_visible_at,
        slot_closed_at=None,
        listed_price=event.new_listed_price,
        initial_qty=event.qty_delta,
        sold_qty=0,
        remaining_qty=event.qty_delta,
        first_fill_at=None,
        listing_fee_paid=event.fee_paid,
        edit_fee_paid=0,
        edit_count=0,
        expires_at=event.new_expires_at,
        status="ACTIVE" if legacy or visible_now else "PENDING",
    )
    position.unallocated_qty -= event.qty_delta
    position.fees_paid += event.fee_paid
    aggregate.orders[order.order_id] = order
    arm.orders[order.order_id] = order


def _apply_sale(arm: LedgerState, event: LedgerEvent) -> None:
    order = _order_for_event(arm, event)
    _require(
        order.status in {"ACTIVE", "PARTIAL"},
        "sales require an active or partially filled order",
    )
    position = _position_for_event(arm, event)
    sold_qty = -event.qty_delta
    _require(sold_qty <= order.remaining_qty, "sale exceeds order.remaining_qty")
    _require(
        sold_qty <= position.remaining_qty,
        "sale exceeds position.remaining_qty",
    )
    assert event.event_ts is not None
    assert event.total_amount is not None
    if order.public_visible_at is not None:
        _require(
            event.event_ts >= order.public_visible_at,
            "sale cannot precede public visibility",
        )

    new_order_remaining = order.remaining_qty - sold_qty
    expected_slot_delta = -1 if new_order_remaining == 0 else 0
    _require(
        event.slot_delta == expected_slot_delta,
        "sale slot_delta does not match the resulting order quantity",
    )

    old_remaining = position.remaining_qty
    position.remaining_qty -= sold_qty
    if position.remaining_cost is not None:
        position.remaining_cost = (
            position.remaining_cost * position.remaining_qty
        ) // old_remaining
    position.realized_proceeds += event.total_amount

    order.sold_qty += sold_qty
    order.remaining_qty = new_order_remaining
    if order.first_fill_at is None:
        order.first_fill_at = event.event_ts
    order._fills.append((event.event_ts, sold_qty))
    if new_order_remaining == 0:
        order.status = "SOLD_OUT"
        order.slot_closed_at = event.event_ts
    else:
        order.status = "PARTIAL"

    if position.remaining_qty == 0:
        has_live_order = any(
            candidate.position_id == position.position_id
            and candidate.status in _LIVE_ORDER_STATUSES
            for candidate in arm.orders.values()
        )
        if not has_live_order:
            position.status = "CLOSED"


def _apply_close(arm: LedgerState, event: LedgerEvent) -> None:
    order = _order_for_event(arm, event)
    _require(order.status in _LIVE_ORDER_STATUSES, "order is already closed")
    position = arm.positions[order.position_id]
    if event.available_cash_delta:
        _require(order.status == "PENDING", "only Pending orders support refunds")
        _require(
            event.available_cash_delta
            <= order.listing_fee_paid + order.edit_fee_paid,
            "Pending refund exceeds fees paid",
        )
        position.fee_refunds += event.available_cash_delta
        arm.total_fee_refunds += event.available_cash_delta
    position.unallocated_qty += order.remaining_qty
    order.status = "CANCELLED" if event.event_type == "CANCEL" else "EXPIRED"
    order.slot_closed_at = event.event_ts


def _validate_arm(arm: LedgerState) -> None:
    _require(arm.available_cash >= 0, f"negative available_cash in {arm.arm_id}")
    _require(
        arm.unclaimed_proceeds >= 0,
        f"negative unclaimed_proceeds in {arm.arm_id}",
    )
    _require(arm.occupied_slots >= 0, "occupied slots cannot be negative")
    if arm.slot_limit is not None:
        _require(arm.occupied_slots <= arm.slot_limit, "slot limit exceeded")

    live_orders = [
        order
        for order in arm.orders.values()
        if order.status in _LIVE_ORDER_STATUSES
    ]
    _require(
        arm.occupied_slots == len(live_orders),
        "occupied slots do not match live orders",
    )
    allocated: dict[int, int] = {}
    for order in arm.orders.values():
        _require(
            order.initial_qty == order.sold_qty + order.remaining_qty,
            "order quantity conservation failed",
        )
        _require(
            order.sold_qty >= 0 and order.remaining_qty >= 0,
            "negative order quantity",
        )
        if order.status in _LIVE_ORDER_STATUSES:
            _require(order.remaining_qty > 0, "live order has no remaining quantity")
            allocated[order.position_id] = (
                allocated.get(order.position_id, 0) + order.remaining_qty
            )
        if order.status == "SOLD_OUT":
            _require(order.remaining_qty == 0, "SOLD_OUT order retains inventory")

    for position in arm.positions.values():
        _require(
            0
            <= position.unallocated_qty
            <= position.remaining_qty
            <= position.initial_qty,
            "position quantity bounds failed",
        )
        _require(
            position.unallocated_qty + allocated.get(position.position_id, 0)
            == position.remaining_qty,
            "position allocation conservation failed",
        )
        if position.total_cost is None:
            _require(
                position.remaining_cost is None,
                "unknown total cost requires unknown remaining cost",
            )
        else:
            _require(
                position.remaining_cost is not None
                and 0 <= position.remaining_cost <= position.total_cost,
                "position cost bounds failed",
            )
        expected_status = "CLOSED" if position.remaining_qty == 0 else "OPEN"
        _require(position.status == expected_status, "position status mismatch")


def _apply_event(
    aggregate: LedgerState, arm: LedgerState, event: LedgerEvent
) -> None:
    kind = event.event_type
    if kind == "ACCOUNT_OPEN":
        _require(not arm.has_account, f"arm {event.arm_id} is already open")
        arm.has_account = True
        arm.account_opened_at = event.event_ts
        arm.slot_limit = event.slot_limit
        arm.max_loss_budget = event.max_loss_budget
        arm.opening_equity += event.available_cash_delta + event.unclaimed_delta
    elif kind not in _LEGACY_TYPES:
        _require(arm.has_account, f"arm {event.arm_id} requires ACCOUNT_OPEN")

    if kind == "LEGACY_POSITION_INIT":
        _create_position(aggregate, arm, event, legacy=True)
    elif kind == "BUY":
        if event.position_id not in aggregate.positions:
            _create_position(aggregate, arm, event, legacy=False)
        else:
            position = _position_for_event(arm, event)
            _require(not position.is_legacy_inventory, "cannot add BUY to legacy cost")
            _require(position.status == "OPEN", "cannot add BUY to a closed position")
            _require(
                position.settlement_due_at == event.settlement_due_at,
                "BUY settlement_due_at differs within the same position",
            )
            assert event.total_amount is not None
            assert position.total_cost is not None
            assert position.remaining_cost is not None
            position.initial_qty += event.qty_delta
            position.remaining_qty += event.qty_delta
            position.unallocated_qty += event.qty_delta
            position.total_cost += event.total_amount
            position.remaining_cost += event.total_amount
    elif kind in {"LIST", "LEGACY_ORDER_INIT"}:
        _create_order(
            aggregate, arm, event, legacy=kind == "LEGACY_ORDER_INIT"
        )
    elif kind == "ORDER_PUBLIC":
        order = _order_for_event(arm, event)
        _require(order.status == "PENDING", "ORDER_PUBLIC requires a Pending order")
        order.public_visible_at = event.new_public_visible_at
        order.status = "ACTIVE"
    elif kind == "DIRECT_EDIT":
        order = _order_for_event(arm, event)
        _require(
            order.status in _LIVE_ORDER_STATUSES,
            "cannot edit a closed order",
        )
        assert event.new_listed_price is not None
        order.listed_price = event.new_listed_price
        order.edit_fee_paid += event.fee_paid
        order.edit_count += 1
        if event.new_expires_at is not None:
            order.expires_at = event.new_expires_at
        arm.positions[order.position_id].fees_paid += event.fee_paid
    elif kind == "SALE_CONFIRMED":
        _apply_sale(arm, event)
    elif kind in {"CANCEL", "EXPIRE"}:
        _apply_close(arm, event)
    elif kind == "CLAIM_CASH":
        _require(
            -event.unclaimed_delta <= arm.unclaimed_proceeds,
            f"claim exceeds unclaimed proceeds in arm {event.arm_id}",
        )
    elif kind == "EXTERNAL_TRANSFER":
        arm.net_external_transfer += event.available_cash_delta

    arm.available_cash += event.available_cash_delta
    arm.unclaimed_proceeds += event.unclaimed_delta
    arm.occupied_slots += event.slot_delta
    arm.total_fees_paid += event.fee_paid
    if event.event_ts is not None:
        arm._observed_at = (
            event.event_ts
            if arm._observed_at is None
            else max(arm._observed_at, event.event_ts)
        )
    _validate_arm(arm)


def _capital_value(state: LedgerState) -> int | None:
    value = state.unclaimed_proceeds
    for position in state.positions.values():
        if position.remaining_qty == 0:
            continue
        if position.remaining_cost is None:
            return None
        value += position.remaining_cost
    return value


def _refresh_aggregate(aggregate: LedgerState) -> None:
    arms = list(aggregate.arms.values())
    for name in (
        "opening_equity",
        "net_external_transfer",
        "available_cash",
        "unclaimed_proceeds",
        "occupied_slots",
        "total_fees_paid",
        "total_fee_refunds",
    ):
        setattr(aggregate, name, sum(getattr(arm, name) for arm in arms))
    for name in ("slot_limit", "max_loss_budget"):
        values = [getattr(arm, name) for arm in arms]
        total = (
            sum(value for value in values if value is not None)
            if values and all(value is not None for value in values)
            else None
        )
        setattr(aggregate, name, total)
    aggregate.has_account = bool(arms) and all(arm.has_account for arm in arms)
    timestamps = [
        arm._observed_at for arm in arms if arm._observed_at is not None
    ]
    aggregate._observed_at = max(timestamps) if timestamps else None


def _read_event_rows(conn: sqlite3.Connection) -> list[dict[str, object]]:
    columns = ", ".join(_EVENT_COLUMNS)
    cursor = conn.execute(
        f"SELECT {columns} FROM user_ledger_events ORDER BY event_id"
    )
    return [
        dict(zip(_EVENT_COLUMNS, row, strict=True)) for row in cursor.fetchall()
    ]


def replay_ledger(
    events_or_conn: Iterable[EventInput] | sqlite3.Connection,
    *,
    as_of_ts: int | None = None,
    arm_id: str | None = None,
) -> LedgerState:
    """Pure replay ordered by null-baseline, timestamp, event_order, event_id.

    Missing event IDs in an iterable receive deterministic, one-based input
    ordinals. Identical duplicate receipts are skipped; conflicting receipts
    fail. Historical null-timestamp ACCOUNT_OPEN baselines are readable, but
    new ACCOUNT_OPEN appends must have a timestamp.

    No projection tables are read or written. Baseline-only as-of snapshots
    may precede their arm's dated ACCOUNT_OPEN.
    """
    if as_of_ts is not None:
        _integer(as_of_ts, "as_of_ts")
    if arm_id is not None:
        _require(
            isinstance(arm_id, str) and bool(arm_id.strip()),
            "arm_id must be non-empty text",
        )
    raw_events = (
        _read_event_rows(events_or_conn)
        if isinstance(events_or_conn, sqlite3.Connection)
        else events_or_conn
    )
    events = []
    seen_keys: dict[str, str] = {}
    seen_ids: set[int] = set()
    for ordinal, raw in enumerate(raw_events, start=1):
        event = _normalise_event(raw)
        if arm_id is not None and event.arm_id != arm_id:
            continue
        if as_of_ts is not None:
            if event.event_ts is None:
                if event.event_type not in _BASELINE_TYPES:
                    continue
            elif event.event_ts > as_of_ts:
                continue
        digest = _payload_digest(event)
        previous = seen_keys.get(event.idempotency_key)
        if previous is not None:
            if previous != digest:
                raise LedgerIdempotencyConflictError(
                    f"conflicting payload for {event.idempotency_key}"
                )
            continue
        seen_keys[event.idempotency_key] = digest
        event_id = event.event_id if event.event_id is not None else ordinal
        _require(event_id not in seen_ids, f"duplicate event_id {event_id}")
        seen_ids.add(event_id)
        events.append(replace(event, event_id=event_id))

    events.sort(
        key=lambda event: (
            0 if event.event_ts is None else 1,
            event.event_ts or 0,
            event.event_order,
            event.event_id,
        )
    )
    aggregate = LedgerState(_as_of_ts=as_of_ts)
    for event in events:
        _validate_event(event, allow_null_account_open=True)
        arm = aggregate.arms.setdefault(
            event.arm_id,
            LedgerState(arm_id=event.arm_id, _as_of_ts=as_of_ts),
        )
        _apply_event(aggregate, arm, event)
        arm._history.append(
            (event.event_ts, _capital_value(arm), arm.occupied_slots)
        )
        _refresh_aggregate(aggregate)
        aggregate._history.append(
            (event.event_ts, _capital_value(aggregate), aggregate.occupied_slots)
        )

    if arm_id is not None:
        return aggregate.arms.get(
            arm_id, LedgerState(arm_id=arm_id, _as_of_ts=as_of_ts)
        )
    return aggregate


def _validate_batch_receipts(
    events: Iterable[LedgerEvent], expected: Mapping[str, int]
) -> None:
    grouped: dict[str, list[LedgerEvent]] = {batch_id: [] for batch_id in expected}
    for event in events:
        if event.batch_claim_id in grouped:
            grouped[event.batch_claim_id].append(event)
    for batch_id, receipt_total in expected.items():
        claims = grouped[batch_id]
        _require(bool(claims), f"batch claim {batch_id} has no events")
        _require(
            all(event.event_type == "CLAIM_CASH" for event in claims),
            "batch receipts may contain only CLAIM_CASH events",
        )
        _require(
            len({event.arm_id for event in claims}) == len(claims),
            "a batch claim must contain exactly one allocation per arm",
        )
        _require(
            sum(-event.unclaimed_delta for event in claims) == receipt_total,
            f"batch claim {batch_id} does not match its receipt total",
        )


def _rebuild_projections(
    conn: sqlite3.Connection, state: LedgerState
) -> None:
    conn.execute("DELETE FROM user_orders")
    conn.execute("DELETE FROM user_positions")
    conn.executemany(
        """
        INSERT INTO user_positions (
            position_id, arm_id, item_id, is_legacy_inventory, opened_at,
            initial_qty, remaining_qty, unallocated_qty, total_cost,
            remaining_cost, unknown_cost_reason, settlement_due_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                position.position_id,
                position.arm_id,
                position.item_id,
                position.is_legacy_inventory,
                position.opened_at,
                position.initial_qty,
                position.remaining_qty,
                position.unallocated_qty,
                position.total_cost,
                position.remaining_cost,
                position.unknown_cost_reason,
                position.settlement_due_at,
                position.status,
            )
            for position in sorted(
                state.positions.values(), key=lambda value: value.position_id
            )
        ),
    )
    conn.executemany(
        """
        INSERT INTO user_orders (
            order_id, parent_order_id, arm_id, position_id, item_id, exit_arm,
            submitted_at, public_visible_at, slot_closed_at, listed_price,
            initial_qty, sold_qty, remaining_qty, first_fill_at,
            listing_fee_paid, edit_fee_paid, edit_count, expires_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                order.order_id,
                order.parent_order_id,
                order.arm_id,
                order.position_id,
                order.item_id,
                order.exit_arm,
                order.submitted_at,
                order.public_visible_at,
                order.slot_closed_at,
                order.listed_price,
                order.initial_qty,
                order.sold_qty,
                order.remaining_qty,
                order.first_fill_at,
                order.listing_fee_paid,
                order.edit_fee_paid,
                order.edit_count,
                order.expires_at,
                order.status,
            )
            for order in sorted(
                state.orders.values(), key=lambda value: value.order_id
            )
        ),
    )


def append_ledger_events(
    conn: sqlite3.Connection,
    events: Iterable[EventInput],
    *,
    expected_batch_claim_totals: Mapping[str, int] | None = None,
) -> int:
    """Atomically append a batch and rebuild both projections; return new count.

    Every touched batch_claim_id requires an explicit receipt total, including
    an idempotent retry. A committed receipt cannot later be extended with new
    allocations. Retry subsets are safe because the entire persisted receipt
    is rechecked.

    The connection must not already be inside a transaction.
    """
    candidates = tuple(_normalise_event(event) for event in events)
    expected = dict(expected_batch_claim_totals or {})
    for batch_id, amount in expected.items():
        _require(
            isinstance(batch_id, str) and bool(batch_id.strip()),
            "receipt batch IDs must be non-empty text",
        )
        _integer(amount, f"receipt total for {batch_id}", minimum=0)
    touched_batches = {
        event.batch_claim_id
        for event in candidates
        if event.batch_claim_id is not None
    }
    _require(
        touched_batches == set(expected),
        "expected_batch_claim_totals must exactly cover touched batch claims",
    )

    inserted = 0
    try:
        with write_transaction(conn):
            ensure_ledger_tables(conn)
            existing = [
                _normalise_event(row) for row in _read_event_rows(conn)
            ]
            known = {event.idempotency_key: event for event in existing}
            committed_batches = {
                event.batch_claim_id
                for event in existing
                if event.batch_claim_id is not None
            }
            insert_columns = _PAYLOAD_FIELDS + ("payload_sha256",)
            placeholders = ", ".join("?" for _ in insert_columns)
            statement = (
                f"INSERT INTO user_ledger_events ({', '.join(insert_columns)}) "
                f"VALUES ({placeholders})"
            )

            for event in candidates:
                previous = known.get(event.idempotency_key)
                if previous is not None:
                    if previous.payload_sha256 != event.payload_sha256:
                        raise LedgerIdempotencyConflictError(
                            f"conflicting payload for {event.idempotency_key}"
                        )
                    continue
                _validate_event(event)
                _require(
                    event.batch_claim_id is None
                    or event.batch_claim_id not in committed_batches,
                    "cannot extend an already committed batch claim",
                )
                cursor = conn.execute(
                    statement,
                    tuple(getattr(event, name) for name in insert_columns),
                )
                stored = replace(event, event_id=cursor.lastrowid)
                known[event.idempotency_key] = stored
                inserted += 1

            _validate_batch_receipts(known.values(), expected)
            state = replay_ledger(conn)
            _require(
                all(arm.has_account for arm in state.arms.values()),
                "every appended arm requires ACCOUNT_OPEN",
            )
            _rebuild_projections(conn, state)
    except (sqlite3.IntegrityError, sqlite3.DataError, OverflowError) as exc:
        raise LedgerValidationError(f"ledger storage validation failed: {exc}") from exc
    return inserted


def build_relist_events(
    *,
    idempotency_key: str,
    arm_id: str,
    event_ts: int,
    old_order_id: int,
    new_order_id: int,
    position_id: int,
    item_id: int,
    new_listed_price: int,
    new_expires_at: int,
    fee_paid: int,
    qty_delta: int | None = None,
    qty: int | None = None,
    event_order: int = 0,
    exit_arm: str | None = None,
    new_public_visible_at: int | None = None,
    cancel_refund: int = 0,
) -> tuple[LedgerEvent, LedgerEvent]:
    """Build CANCEL(k), LIST(k+1) for one atomic append.

    ``qty`` is an alias for the event-native ``qty_delta``; supply exactly one.
    The old order's remaining quantity is still validated during replay.
    """
    _require(
        isinstance(idempotency_key, str) and bool(idempotency_key.strip()),
        "relist idempotency_key must be non-empty text",
    )
    _require(
        (qty_delta is None) != (qty is None),
        "supply exactly one of qty_delta or qty",
    )
    _require(old_order_id != new_order_id, "relist requires a new order_id")
    quantity = qty_delta if qty_delta is not None else qty
    assert quantity is not None
    cancel = LedgerEvent(
        idempotency_key=f"{idempotency_key}:cancel",
        arm_id=arm_id,
        event_type="CANCEL",
        event_ts=event_ts,
        event_order=event_order,
        order_id=old_order_id,
        slot_delta=-1,
        available_cash_delta=cancel_refund,
    )
    listing = LedgerEvent(
        idempotency_key=f"{idempotency_key}:list",
        arm_id=arm_id,
        event_type="LIST",
        event_ts=event_ts,
        event_order=event_order + 1,
        item_id=item_id,
        position_id=position_id,
        order_id=new_order_id,
        parent_order_id=old_order_id,
        exit_arm=exit_arm,
        qty_delta=quantity,
        new_listed_price=new_listed_price,
        new_expires_at=new_expires_at,
        new_public_visible_at=new_public_visible_at,
        fee_paid=fee_paid,
        available_cash_delta=-fee_paid,
        slot_delta=1,
    )
    result = (_normalise_event(cancel), _normalise_event(listing))
    for event in result:
        _validate_event(event)
    return result


def evaluate_net_equity(
    arm_state: LedgerState,
    residual_valuations: Mapping[int, int],
    unpaid_future_fees: int = 0,
) -> EquitySummary:
    """Evaluate settlement equity using explicit, already-net residual marks."""
    _integer(unpaid_future_fees, "unpaid_future_fees", minimum=0)
    for position_id, valuation in residual_valuations.items():
        _integer(position_id, "residual position_id", minimum=1)
        _integer(valuation, "residual valuation")
    residual_value = sum(residual_valuations.values())
    base_equity = arm_state.opening_equity + arm_state.net_external_transfer
    end_equity = (
        arm_state.available_cash
        + arm_state.unclaimed_proceeds
        + residual_value
        - unpaid_future_fees
    )
    return EquitySummary(
        available_cash=arm_state.available_cash,
        unclaimed_proceeds=arm_state.unclaimed_proceeds,
        residual_net_value=residual_value,
        unpaid_future_fees=unpaid_future_fees,
        opening_equity=arm_state.opening_equity,
        net_external_transfer=arm_state.net_external_transfer,
        base_equity=base_equity,
        end_net_equity=end_equity,
        delta_net_equity=end_equity - base_equity,
        historical_profit=arm_state.historical_profit,
    )


def check_open_loss_budget(
    arm_state: LedgerState,
    *,
    max_loss_budget: int,
    open_positions_conservative_net_value: int,
    new_order_buy_cost: int,
    new_order_immediate_fee: int,
    new_order_conservative_net_value: int,
    unpaid_future_fees: int = 0,
) -> LossBudgetCheck:
    """Apply the opening-equity-anchored gate, including prior realized losses."""
    for name, value in (
        ("max_loss_budget", max_loss_budget),
        ("new_order_buy_cost", new_order_buy_cost),
        ("new_order_immediate_fee", new_order_immediate_fee),
        ("unpaid_future_fees", unpaid_future_fees),
    ):
        _integer(value, name, minimum=0)
    _integer(
        open_positions_conservative_net_value,
        "open_positions_conservative_net_value",
    )
    _integer(
        new_order_conservative_net_value,
        "new_order_conservative_net_value",
    )

    base_equity = arm_state.opening_equity + arm_state.net_external_transfer
    post_cash = (
        arm_state.available_cash - new_order_buy_cost - new_order_immediate_fee
    )
    e_cons_post = (
        post_cash
        + arm_state.unclaimed_proceeds
        + open_positions_conservative_net_value
        + new_order_conservative_net_value
        - unpaid_future_fees
    )
    projected_total_loss = max(0, base_equity - e_cons_post)
    current_conservative_loss = max(
        0,
        base_equity
        - (
            arm_state.available_cash
            + arm_state.unclaimed_proceeds
            + open_positions_conservative_net_value
        ),
    )
    return LossBudgetCheck(
        allowed=post_cash >= 0 and projected_total_loss <= max_loss_budget,
        post_cash=post_cash,
        base_equity=base_equity,
        e_cons_post=e_cons_post,
        projected_total_loss=projected_total_loss,
        current_conservative_loss=current_conservative_loss,
        remaining_loss_budget=max_loss_budget - current_conservative_loss,
        max_loss_budget=max_loss_budget,
    )
