"""Acceptance tests for per-arm ledger replay and unified equity risk."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import replace

import pytest

from pokemmo_gtl.quant.fees import listing_fee
from pokemmo_gtl.quant.ledger import (
    LedgerEvent,
    LedgerIdempotencyConflictError,
    LedgerState,
    LedgerValidationError,
    append_ledger_events,
    build_relist_events,
    canonical_payload_sha256,
    check_open_loss_budget,
    ensure_ledger_tables,
    evaluate_net_equity,
    replay_ledger,
)


@pytest.fixture(params=[False, True], ids=["tuple_rows", "sqlite_rows"])
def conn(request: pytest.FixtureRequest) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    if request.param:
        connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    ensure_ledger_tables(connection)
    try:
        yield connection
    finally:
        connection.close()


def event(
    key: str,
    kind: str,
    timestamp: int | None,
    *,
    arm: str = "SIMPLE_LIMIT",
    **values: object,
) -> LedgerEvent:
    return LedgerEvent(
        idempotency_key=key,
        arm_id=arm,
        event_type=kind,
        event_ts=timestamp,
        **values,
    )


def opening(
    *,
    arm: str = "SIMPLE_LIMIT",
    cash: int = 1_000_000,
    slots: int = 2,
    budget: int = 100_000,
    timestamp: int = 0,
) -> LedgerEvent:
    return event(
        f"{arm}:open",
        "ACCOUNT_OPEN",
        timestamp,
        arm=arm,
        available_cash_delta=cash,
        slot_limit=slots,
        max_loss_budget=budget,
    )


def buy(
    key: str,
    timestamp: int,
    *,
    arm: str = "SIMPLE_LIMIT",
    position_id: int = 1,
    item_id: int = 101,
    quantity: int = 1,
    cost: int = 9_000,
) -> LedgerEvent:
    return event(
        key,
        "BUY",
        timestamp,
        arm=arm,
        item_id=item_id,
        position_id=position_id,
        qty_delta=quantity,
        total_amount=cost,
        available_cash_delta=-cost,
        settlement_due_at=10_000,
    )


def listing(
    key: str,
    timestamp: int,
    *,
    arm: str = "SIMPLE_LIMIT",
    position_id: int = 1,
    order_id: int = 10,
    item_id: int = 101,
    quantity: int = 1,
    price: int = 10_000,
    fee: int = 100,
    public_at: int | None = None,
) -> LedgerEvent:
    return event(
        key,
        "LIST",
        timestamp,
        arm=arm,
        item_id=item_id,
        position_id=position_id,
        order_id=order_id,
        qty_delta=quantity,
        new_listed_price=price,
        fee_paid=fee,
        available_cash_delta=-fee,
        slot_delta=1,
        new_expires_at=20_000,
        new_public_visible_at=public_at,
    )


def sale(
    key: str,
    timestamp: int,
    *,
    arm: str = "SIMPLE_LIMIT",
    position_id: int = 1,
    order_id: int = 10,
    quantity: int = 1,
    proceeds: int = 10_000,
    sold_out: bool = True,
) -> LedgerEvent:
    return event(
        key,
        "SALE_CONFIRMED",
        timestamp,
        arm=arm,
        position_id=position_id,
        order_id=order_id,
        qty_delta=-quantity,
        total_amount=proceeds,
        unclaimed_delta=proceeds,
        slot_delta=-1 if sold_out else 0,
    )


def row_counts(conn: sqlite3.Connection) -> tuple[int, int, int]:
    return (
        conn.execute("SELECT COUNT(*) FROM user_ledger_events").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM user_positions").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM user_orders").fetchone()[0],
    )


def projection_snapshot(conn: sqlite3.Connection) -> tuple[list, list]:
    positions = [
        tuple(row)
        for row in conn.execute(
            """
            SELECT position_id, arm_id, item_id, initial_qty, remaining_qty,
                   unallocated_qty, total_cost, remaining_cost, status
            FROM user_positions ORDER BY position_id
            """
        )
    ]
    orders = [
        tuple(row)
        for row in conn.execute(
            """
            SELECT order_id, parent_order_id, arm_id, position_id, listed_price,
                   sold_qty, remaining_qty, listing_fee_paid, edit_fee_paid,
                   edit_count, status
            FROM user_orders ORDER BY order_id
            """
        )
    ]
    return positions, orders


def test_exact_schema_and_repeatable_standalone_creation(
    conn: sqlite3.Connection,
) -> None:
    expected_columns = {
        "user_ledger_events": (
            "event_id",
            "idempotency_key",
            "payload_sha256",
            "arm_id",
            "event_ts",
            "event_order",
            "event_type",
            "item_id",
            "position_id",
            "order_id",
            "parent_order_id",
            "exit_arm",
            "settlement_due_at",
            "batch_claim_id",
            "slot_limit",
            "max_loss_budget",
            "new_listed_price",
            "new_expires_at",
            "new_public_visible_at",
            "total_amount",
            "unit_price",
            "qty_delta",
            "available_cash_delta",
            "unclaimed_delta",
            "fee_paid",
            "slot_delta",
            "fills_detail_json",
            "voucher_note",
            "unknown_reason",
        ),
        "user_positions": (
            "position_id",
            "arm_id",
            "item_id",
            "is_legacy_inventory",
            "opened_at",
            "initial_qty",
            "remaining_qty",
            "unallocated_qty",
            "total_cost",
            "remaining_cost",
            "unknown_cost_reason",
            "settlement_due_at",
            "status",
        ),
        "user_orders": (
            "order_id",
            "parent_order_id",
            "arm_id",
            "position_id",
            "item_id",
            "exit_arm",
            "submitted_at",
            "public_visible_at",
            "slot_closed_at",
            "listed_price",
            "initial_qty",
            "sold_qty",
            "remaining_qty",
            "first_fill_at",
            "listing_fee_paid",
            "edit_fee_paid",
            "edit_count",
            "expires_at",
            "status",
        ),
    }
    ensure_ledger_tables(conn)
    strict_tables = {
        row[1]: row[5] for row in conn.execute("PRAGMA table_list")
    }
    for table_name, column_names in expected_columns.items():
        info = list(conn.execute(f"PRAGMA table_info({table_name})"))
        assert tuple(row[1] for row in info) == column_names
        assert strict_tables[table_name] == 1
        assert all(row[2] in {"INTEGER", "TEXT"} for row in info)
    assert append_ledger_events(conn, [opening()]) == 1
    ensure_ledger_tables(conn)
    assert row_counts(conn) == (1, 0, 0)


def test_p1_2_same_second_edits_and_atomic_relist(
    conn: sqlite3.Connection,
) -> None:
    history = [
        opening(cash=1_000_000, slots=1, timestamp=100),
        buy("buy", 110, quantity=2, cost=18_000),
        listing("list", 120, quantity=2, price=11_000),
        event(
            "public",
            "ORDER_PUBLIC",
            130,
            order_id=10,
            new_public_visible_at=125,
        ),
    ]
    append_ledger_events(conn, history)
    assert replay_ledger(conn, as_of_ts=119).occupied_slots == 0
    pending = replay_ledger(conn, as_of_ts=129)
    assert pending.orders[10].status == "PENDING"
    assert pending.orders[10].public_visible_at is None
    active = replay_ledger(conn, as_of_ts=130)
    assert active.orders[10].status == "ACTIVE"
    assert active.orders[10].public_visible_at == 125

    first_edit = event(
        "edit-1",
        "DIRECT_EDIT",
        150,
        event_order=1,
        order_id=10,
        fee_paid=100,
        available_cash_delta=-100,
        new_listed_price=10_500,
    )
    second_edit = event(
        "edit-2",
        "DIRECT_EDIT",
        150,
        event_order=2,
        order_id=10,
        fee_paid=100,
        available_cash_delta=-100,
        new_listed_price=9_900,
        new_expires_at=30_000,
    )
    relist = build_relist_events(
        idempotency_key="relist",
        arm_id="SIMPLE_LIMIT",
        event_ts=150,
        event_order=3,
        old_order_id=10,
        new_order_id=11,
        position_id=1,
        item_id=101,
        qty_delta=2,
        new_listed_price=9_800,
        new_expires_at=40_000,
        fee_paid=100,
        exit_arm="REVIEW_PRICE",
    )
    assert relist[0].event_order == 3
    assert relist[1].event_order == 4
    assert relist[0].event_ts == relist[1].event_ts == first_edit.event_ts

    append_ledger_events(conn, [first_edit])
    first = replay_ledger(conn, as_of_ts=150)
    assert first.orders[10].listed_price == 10_500
    assert first.orders[10].edit_fee_paid == 100
    assert first.orders[10].edit_count == 1

    append_ledger_events(conn, [second_edit])
    second = replay_ledger(conn, as_of_ts=150)
    assert second.orders[10].listed_price == 9_900
    assert second.orders[10].edit_fee_paid == 200
    assert second.orders[10].edit_count == 2
    assert second.orders[10].expires_at == 30_000

    # A timestamp alone cannot select an ordinal inside the same second.
    # Replaying the exact prefix verifies the intermediate cancellation.
    cancelled_prefix = replay_ledger(
        history + [first_edit, second_edit, relist[0]], as_of_ts=150
    )
    assert cancelled_prefix.orders[10].status == "CANCELLED"
    assert cancelled_prefix.occupied_slots == 0
    assert cancelled_prefix.positions[1].unallocated_qty == 2

    assert append_ledger_events(conn, relist) == 2
    before_edits = replay_ledger(conn, as_of_ts=149)
    assert before_edits.orders[10].listed_price == 11_000
    assert before_edits.orders[10].status == "ACTIVE"
    assert 11 not in before_edits.orders

    after_relist = replay_ledger(conn, as_of_ts=150)
    assert after_relist.orders[10].listed_price == 9_900
    assert after_relist.orders[10].status == "CANCELLED"
    assert after_relist.orders[10].slot_closed_at == 150
    assert after_relist.orders[11].listed_price == 9_800
    assert after_relist.orders[11].parent_order_id == 10
    assert after_relist.orders[11].exit_arm == "REVIEW_PRICE"
    assert after_relist.orders[11].status == "PENDING"
    assert after_relist.occupied_slots == 1
    assert after_relist.available_cash == 981_600
    assert after_relist.total_fees_paid == 400
    assert after_relist.positions[1].unallocated_qty == 0

    append_ledger_events(
        conn,
        [
            event(
                "relist-public",
                "ORDER_PUBLIC",
                155,
                order_id=11,
                new_public_visible_at=155,
            ),
            sale(
                "relist-sold",
                160,
                order_id=11,
                quantity=2,
                proceeds=19_600,
            ),
        ],
    )
    final = replay_ledger(conn, as_of_ts=160)
    assert final.orders[11].status == "SOLD_OUT"
    assert final.orders[11].first_fill_delay_seconds == 5
    assert final.orders[11].slot_closed_at == 160
    assert final.positions[1].status == "CLOSED"
    assert final.occupied_slots == 0
    assert final.slot_days(120, 160) == pytest.approx(40 / 86_400)
    assert final.full_sellout_order_ratio(160) == 0.5
    assert final.historical_profit == 1_200
    persisted = conn.execute(
        """
        SELECT listed_price, parent_order_id, status
        FROM user_orders WHERE order_id = 11
        """
    ).fetchone()
    assert tuple(persisted) == (9_800, 10, "SOLD_OUT")


def test_p1_5_cross_arm_claim_receipt_and_exact_balances(
    conn: sqlite3.Connection,
) -> None:
    simple = "SIMPLE_LIMIT"
    shock = "B_SHOCK"
    append_ledger_events(
        conn,
        [
            opening(arm=simple, cash=500_000, slots=1),
            opening(arm=shock, cash=500_000, slots=1),
            buy("simple-buy", 10, quantity=2, cost=200_000),
            buy(
                "shock-buy",
                10,
                arm=shock,
                position_id=2,
                cost=100_000,
            ),
            listing(
                "simple-list",
                20,
                quantity=2,
                price=90_000,
                fee=1_000,
                public_at=20,
            ),
            listing(
                "shock-list",
                20,
                arm=shock,
                position_id=2,
                order_id=20,
                price=90_000,
                fee=500,
                public_at=20,
            ),
            sale("simple-sale", 30, quantity=2, proceeds=180_000),
            sale(
                "shock-sale",
                30,
                arm=shock,
                position_id=2,
                order_id=20,
                proceeds=90_000,
            ),
        ],
    )
    before_simple = replay_ledger(conn, arm_id=simple)
    before_shock = replay_ledger(conn, arm_id=shock)
    assert (before_simple.available_cash, before_simple.unclaimed_proceeds) == (
        299_000,
        180_000,
    )
    assert (before_shock.available_cash, before_shock.unclaimed_proceeds) == (
        399_500,
        90_000,
    )
    assert before_simple.remaining_loss_budget == 79_000
    assert before_shock.remaining_loss_budget == 89_500

    claims = [
        event(
            "simple-claim",
            "CLAIM_CASH",
            40,
            arm=simple,
            batch_claim_id="mail-001",
            unclaimed_delta=-180_000,
            available_cash_delta=180_000,
        ),
        event(
            "shock-claim",
            "CLAIM_CASH",
            40,
            arm=shock,
            batch_claim_id="mail-001",
            unclaimed_delta=-90_000,
            available_cash_delta=90_000,
        ),
    ]
    before_counts = row_counts(conn)
    before_projections = projection_snapshot(conn)
    with pytest.raises(LedgerValidationError, match="receipt total"):
        append_ledger_events(
            conn,
            claims,
            expected_batch_claim_totals={"mail-001": 269_999},
        )
    assert row_counts(conn) == before_counts
    assert projection_snapshot(conn) == before_projections

    excessive_allocation = [
        replace(
            claims[0],
            unclaimed_delta=-180_001,
            available_cash_delta=180_001,
        ),
        replace(
            claims[1],
            unclaimed_delta=-89_999,
            available_cash_delta=89_999,
        ),
    ]
    with pytest.raises(LedgerValidationError, match="claim exceeds"):
        append_ledger_events(
            conn,
            excessive_allocation,
            expected_batch_claim_totals={"mail-001": 270_000},
        )
    assert row_counts(conn) == before_counts

    assert append_ledger_events(
        conn,
        claims,
        expected_batch_claim_totals={"mail-001": 270_000},
    ) == 2
    assert append_ledger_events(
        conn,
        claims,
        expected_batch_claim_totals={"mail-001": 270_000},
    ) == 0

    simple_state = replay_ledger(conn, arm_id=simple)
    shock_state = replay_ledger(conn, arm_id=shock)
    assert simple_state.available_cash == 479_000
    assert simple_state.unclaimed_proceeds == 0
    assert simple_state.remaining_loss_budget == 79_000
    assert shock_state.available_cash == 489_500
    assert shock_state.unclaimed_proceeds == 0
    assert shock_state.remaining_loss_budget == 89_500
    assert evaluate_net_equity(simple_state, {}).delta_net_equity == -21_000
    assert evaluate_net_equity(shock_state, {}).delta_net_equity == -10_500

    aggregate = replay_ledger(conn)
    assert set(aggregate.arms) == {simple, shock}
    assert aggregate.available_cash == 968_500
    assert aggregate.unclaimed_proceeds == 0
    assert aggregate.remaining_loss_budget == 168_500
    assert aggregate.occupied_slots == 0
    assert aggregate.opening_equity == 1_000_000

    missing_receipt = replace(
        claims[0],
        idempotency_key="another-claim",
        batch_claim_id="mail-002",
    )
    with pytest.raises(LedgerValidationError, match="exactly cover"):
        append_ledger_events(conn, [missing_receipt])


@pytest.mark.parametrize("split", [False, True], ids=["one_batch_buy", "two_buys"])
def test_p2_3_exact_multilot_integer_cost(
    conn: sqlite3.Connection, split: bool
) -> None:
    purchases = (
        [
            buy("buy-cheap", 10, quantity=1, cost=9_000),
            buy("buy-rest", 11, quantity=98, cost=980_000),
        ]
        if split
        else [buy("buy-all", 10, quantity=99, cost=989_000)]
    )
    append_ledger_events(conn, [opening(cash=2_000_000), *purchases])
    state = replay_ledger(conn, arm_id="SIMPLE_LIMIT")
    position = state.positions[1]
    assert position.initial_qty == 99
    assert position.remaining_qty == 99
    assert position.unallocated_qty == 99
    assert position.total_cost == 989_000
    assert position.remaining_cost == 989_000
    assert state.available_cash == 1_011_000
    assert state.available_cash + position.total_cost == 2_000_000
    assert state.remaining_loss_budget == 100_000

    stored = conn.execute(
        """
        SELECT initial_qty, remaining_qty, unallocated_qty, total_cost,
               remaining_cost, settlement_due_at
        FROM user_positions WHERE position_id = 1
        """
    ).fetchone()
    assert tuple(stored) == (99, 99, 99, 989_000, 989_000, 10_000)

    # At the supplied fee schedule, 10,243 is the integer total-cost breakeven.
    # A truncated average purchase price would lose the 989,000-yen basis.
    break_even = 10_243
    assert (
        break_even * 99 - listing_fee(break_even, 99, 99)
        >= position.total_cost
    )
    assert (
        (break_even - 1) * 99 - listing_fee(break_even - 1, 99, 99)
        < position.total_cost
    )


def test_p1_3_prior_realized_loss_rejects_new_trade(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(cash=500_000, budget=100_000),
            buy("loss-buy", 10, cost=200_000),
            listing(
                "loss-list", 20, price=120_000, fee=0, public_at=20
            ),
            sale("loss-sale", 30, proceeds=120_000),
            event(
                "loss-claim",
                "CLAIM_CASH",
                40,
                unclaimed_delta=-120_000,
                available_cash_delta=120_000,
            ),
        ],
    )
    state = replay_ledger(conn, arm_id="SIMPLE_LIMIT")
    assert state.opening_equity == 500_000
    assert state.available_cash == 420_000
    assert state.positions[1].status == "CLOSED"
    assert state.remaining_loss_budget == 20_000

    result = check_open_loss_budget(
        state,
        max_loss_budget=100_000,
        open_positions_conservative_net_value=0,
        new_order_buy_cost=200_000,
        new_order_immediate_fee=0,
        new_order_conservative_net_value=150_000,
    )
    assert result.base_equity == 500_000
    assert result.post_cash == 220_000
    assert result.e_cons_post == 370_000
    assert result.current_conservative_loss == 80_000
    assert result.remaining_loss_budget == 20_000
    assert result.projected_total_loss == 130_000
    assert result.allowed is False


def test_unknown_legacy_cost_tracks_exits_without_inventing_profit(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(cash=5_000, timestamp=100),
            event(
                "legacy-position",
                "LEGACY_POSITION_INIT",
                None,
                item_id=909,
                position_id=7,
                qty_delta=3,
                total_amount=None,
                unknown_reason="pre-v2 inventory",
            ),
            event(
                "legacy-order",
                "LEGACY_ORDER_INIT",
                None,
                event_order=1,
                item_id=909,
                position_id=7,
                order_id=70,
                qty_delta=3,
                new_listed_price=1_000,
                new_public_visible_at=90,
                slot_delta=1,
                unknown_reason="pre-v2 inventory",
            ),
            sale(
                "legacy-first-fill",
                120,
                position_id=7,
                order_id=70,
                proceeds=1_000,
                sold_out=False,
            ),
        ],
    )
    state = replay_ledger(conn, as_of_ts=120, arm_id="SIMPLE_LIMIT")
    position = state.positions[7]
    order = state.orders[70]
    assert position.is_legacy_inventory == 1
    assert position.opened_at is None
    assert position.total_cost is None
    assert position.remaining_cost is None
    assert position.unknown_cost_reason == "pre-v2 inventory"
    assert position.remaining_qty == 2
    assert position.historical_profit is None
    assert position.historical_profit_status == "UNKNOWN"
    assert state.historical_profit is None
    assert state.historical_profit_status == "UNKNOWN"
    assert state.remaining_loss_budget is None
    assert state.capital_days(100, 120) is None
    assert order.status == "PARTIAL"
    assert order.first_fill_at == 120
    assert order.first_fill_delay_seconds == 30
    assert state.sold_qty_ratio_by_deadline(120) == pytest.approx(1 / 3)
    assert state.full_sellout_order_ratio(120) == 0.0
    assert state.unclaimed_proceeds == 1_000

    summary = evaluate_net_equity(state, {7: 1_500})
    assert summary.end_net_equity == 7_500
    assert summary.historical_profit is None

    append_ledger_events(
        conn,
        [
            sale(
                "legacy-last-fill",
                130,
                position_id=7,
                order_id=70,
                quantity=2,
                proceeds=2_000,
            )
        ],
    )
    closed = replay_ledger(conn)
    assert closed.positions[7].status == "CLOSED"
    assert closed.positions[7].remaining_cost is None
    assert closed.historical_profit is None
    assert closed.orders[70].status == "SOLD_OUT"


def test_unknown_legacy_public_start_is_not_fabricated(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(cash=0),
            event(
                "legacy-position",
                "LEGACY_POSITION_INIT",
                None,
                position_id=1,
                item_id=101,
                qty_delta=1,
                unknown_reason="pre-v2 inventory",
            ),
            event(
                "legacy-order",
                "LEGACY_ORDER_INIT",
                None,
                event_order=1,
                position_id=1,
                item_id=101,
                order_id=10,
                qty_delta=1,
                new_listed_price=100,
                slot_delta=1,
                unknown_reason="pre-v2 public time unknown",
            ),
            sale("legacy-sale", 10, proceeds=100),
        ],
    )
    state = replay_ledger(conn)
    assert state.orders[10].sold_qty == 1
    assert state.orders[10].first_fill_at == 10
    assert state.orders[10].first_fill_delay_seconds is None
    assert state.sold_qty_ratio_by_deadline(20) is None
    assert state.full_sellout_order_ratio(20) is None


def test_idempotency_same_payload_skip_conflict_rolls_back(
    conn: sqlite3.Connection,
) -> None:
    original_buy = replace(
        buy("buy", 10),
        fills_detail_json=(
            '{"lots": [{"price": 9000, "qty": 1}], "note": "receipt"}'
        ),
    )
    assert append_ledger_events(conn, [opening(), original_buy]) == 2
    equivalent = replace(
        original_buy,
        event_id=999,
        fills_detail_json=(
            '{"note":"receipt","lots":[{"qty":1,"price":9000}]}'
        ),
    )
    assert canonical_payload_sha256(original_buy) == canonical_payload_sha256(
        equivalent
    )
    assert append_ledger_events(conn, [equivalent]) == 0
    assert replay_ledger(conn).available_cash == 991_000

    before_counts = row_counts(conn)
    before_projections = projection_snapshot(conn)
    conflict = replace(
        original_buy,
        total_amount=9_001,
        available_cash_delta=-9_001,
    )
    with pytest.raises(LedgerIdempotencyConflictError):
        append_ledger_events(
            conn,
            [
                event(
                    "would-have-been-new",
                    "EXTERNAL_TRANSFER",
                    20,
                    available_cash_delta=10,
                ),
                conflict,
            ],
        )
    assert row_counts(conn) == before_counts
    assert projection_snapshot(conn) == before_projections
    assert replay_ledger(conn).net_external_transfer == 0
    assert not conn.in_transaction


def test_same_payload_duplicate_inside_batch_is_applied_once(
    conn: sqlite3.Connection,
) -> None:
    account = opening()
    assert append_ledger_events(conn, [account, account]) == 1
    assert replay_ledger(conn).available_cash == 1_000_000
    assert row_counts(conn) == (1, 0, 0)


def test_mid_batch_failure_leaves_no_partial_rows(
    conn: sqlite3.Connection,
) -> None:
    with pytest.raises(LedgerValidationError, match="unallocated"):
        append_ledger_events(
            conn,
            [
                opening(),
                buy("buy", 10, quantity=1),
                listing("too-many", 20, quantity=2),
            ],
        )
    assert row_counts(conn) == (0, 0, 0)
    assert not conn.in_transaction
    assert append_ledger_events(conn, [opening()]) == 1


@pytest.mark.parametrize(
    "invalid",
    [
        buy("unaffordable", 30, position_id=2, cost=2_000_000),
        listing("overallocated", 30, order_id=11, quantity=2),
        event(
            "unknown-order",
            "DIRECT_EDIT",
            30,
            order_id=999,
            new_listed_price=10_000,
        ),
        sale("oversell", 30, quantity=2, proceeds=20_000),
        sale("wrong-close-delta", 30, sold_out=False),
        event(
            "claim-too-much",
            "CLAIM_CASH",
            30,
            unclaimed_delta=-1,
            available_cash_delta=1,
        ),
        event(
            "foreign-arm-edit",
            "DIRECT_EDIT",
            30,
            arm="B_SHOCK",
            order_id=10,
            new_listed_price=10_000,
        ),
    ],
    ids=[
        "negative_cash",
        "negative_unallocated",
        "unknown_order",
        "oversell",
        "wrong_slot_delta",
        "negative_receivables",
        "cross_arm_ownership",
    ],
)
def test_state_constraint_failure_preserves_stream_and_projections(
    conn: sqlite3.Connection, invalid: LedgerEvent
) -> None:
    append_ledger_events(
        conn,
        [
            opening(),
            buy("buy", 10),
            listing("list", 20, public_at=20),
        ],
    )
    counts = row_counts(conn)
    projections = projection_snapshot(conn)
    with pytest.raises(LedgerValidationError):
        append_ledger_events(
            conn,
            [
                event(
                    "rollback-transfer",
                    "EXTERNAL_TRANSFER",
                    25,
                    available_cash_delta=1,
                ),
                invalid,
            ],
        )
    assert row_counts(conn) == counts
    assert projection_snapshot(conn) == projections
    assert replay_ledger(conn).net_external_transfer == 0


def test_slot_limit_is_checked_at_every_event(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(slots=1),
            buy("buy", 10, quantity=2, cost=18_000),
            listing("list", 20, quantity=1),
        ],
    )
    before = row_counts(conn)
    with pytest.raises(LedgerValidationError, match="slot limit"):
        append_ledger_events(
            conn,
            [listing("second-list", 30, order_id=11, quantity=1)],
        )
    assert row_counts(conn) == before
    assert replay_ledger(conn).positions[1].unallocated_qty == 1


def test_partial_sale_floor_cost_and_cancel_return_allocation(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(cash=1_000),
            buy("buy", 10, quantity=3, cost=100),
            listing(
                "list", 20, quantity=2, price=50, fee=0, public_at=20
            ),
            sale("partial", 30, proceeds=50, sold_out=False),
        ],
    )
    partial = replay_ledger(conn)
    assert partial.positions[1].remaining_qty == 2
    assert partial.positions[1].remaining_cost == 66
    assert partial.positions[1].unallocated_qty == 1
    assert partial.orders[10].remaining_qty == 1
    assert partial.orders[10].status == "PARTIAL"
    assert partial.occupied_slots == 1

    append_ledger_events(
        conn,
        [
            event("cancel", "CANCEL", 40, order_id=10, slot_delta=-1),
            listing(
                "reallocated",
                50,
                order_id=11,
                quantity=2,
                price=50,
                fee=0,
                public_at=50,
            ),
            sale(
                "last-sale",
                60,
                order_id=11,
                quantity=2,
                proceeds=100,
            ),
        ],
    )
    final = replay_ledger(conn)
    assert final.positions[1].remaining_qty == 0
    assert final.positions[1].remaining_cost == 0
    assert final.positions[1].unallocated_qty == 0
    assert final.positions[1].status == "CLOSED"
    assert final.positions[1].historical_profit == 50
    assert final.orders[10].remaining_qty == 1
    assert final.orders[10].status == "CANCELLED"
    assert final.occupied_slots == 0


def test_capital_days_slot_days_and_public_start_exit_horizons(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(cash=1_000),
            buy("buy", 10, quantity=2, cost=200),
            listing(
                "list", 20, quantity=2, price=150, fee=0, public_at=20
            ),
            sale("first", 30, proceeds=150, sold_out=False),
            sale("second", 40, proceeds=150),
            event(
                "claim",
                "CLAIM_CASH",
                50,
                unclaimed_delta=-300,
                available_cash_delta=300,
            ),
        ],
    )
    state = replay_ledger(conn)
    assert state.capital_days(0, 60) == pytest.approx(9_500 / 86_400)
    assert state.slot_days(0, 60) == pytest.approx(20 / 86_400)
    assert state.orders[10].first_fill_delay_seconds == 10
    assert state.sold_qty_ratio_by_deadline(35) == 0.5
    assert state.full_sellout_order_ratio(35) == 0.0
    assert state.sold_qty_ratio_by_deadline(horizon_seconds=15) == 0.5
    assert state.full_sellout_order_ratio(horizon_seconds=15) == 0.0
    assert state.sold_qty_ratio_by_deadline(horizon_seconds=20) == 1.0
    assert state.full_sellout_order_ratio(horizon_seconds=20) == 1.0
    assert state.full_sellout_order_ratio(horizon_seconds=40) is None
    assert evaluate_net_equity(state, {}).delta_net_equity == 100

    snapshot = replay_ledger(conn, as_of_ts=35)
    assert snapshot.orders[10].status == "PARTIAL"
    assert snapshot.capital_days(0, 60) == pytest.approx(5_250 / 86_400)
    assert snapshot.slot_days(0, 60) == pytest.approx(15 / 86_400)
    assert snapshot.full_sellout_order_ratio(60) == 0.0
    assert snapshot.full_sellout_order_ratio(horizon_seconds=20) is None


def test_pending_refunds_and_expiration_are_replayed(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(cash=1_000),
            buy("buy", 10, cost=100),
            listing("pending", 20, fee=100),
            event(
                "cancel-pending",
                "CANCEL",
                30,
                order_id=10,
                slot_delta=-1,
                available_cash_delta=100,
            ),
            listing("pending-again", 40, order_id=11, fee=50),
            event(
                "expire-pending",
                "EXPIRE",
                50,
                order_id=11,
                slot_delta=-1,
                available_cash_delta=50,
            ),
        ],
    )
    state = replay_ledger(conn)
    assert state.available_cash == 900
    assert state.total_fees_paid == 150
    assert state.total_fee_refunds == 150
    assert state.positions[1].unallocated_qty == 1
    assert state.positions[1].historical_profit == 0
    assert state.orders[10].status == "CANCELLED"
    assert state.orders[11].status == "EXPIRED"
    assert state.orders[11].slot_closed_at == 50
    assert state.occupied_slots == 0


def test_active_order_cannot_receive_pending_refund(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(),
            buy("buy", 10),
            listing("active", 20, public_at=20),
        ],
    )
    before = row_counts(conn)
    with pytest.raises(LedgerValidationError, match="Pending"):
        append_ledger_events(
            conn,
            [
                event(
                    "invalid-refund",
                    "CANCEL",
                    30,
                    order_id=10,
                    slot_delta=-1,
                    available_cash_delta=100,
                )
            ],
        )
    assert row_counts(conn) == before
    assert replay_ledger(conn).orders[10].status == "ACTIVE"


def test_relist_failure_restores_cancelled_parent_atomically(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(slots=1),
            buy("buy", 10),
            listing("list", 20, public_at=20),
        ],
    )
    relist = build_relist_events(
        idempotency_key="bad-relist",
        arm_id="SIMPLE_LIMIT",
        event_ts=30,
        old_order_id=10,
        new_order_id=11,
        position_id=1,
        item_id=101,
        qty=2,
        new_listed_price=9_900,
        new_expires_at=30_000,
        fee_paid=100,
    )
    before = projection_snapshot(conn)
    with pytest.raises(LedgerValidationError, match="unallocated"):
        append_ledger_events(conn, relist)
    assert projection_snapshot(conn) == before
    state = replay_ledger(conn)
    assert state.orders[10].status == "ACTIVE"
    assert 11 not in state.orders
    assert state.occupied_slots == 1


@pytest.mark.parametrize(
    "invalid",
    [
        event("no-time", "ACCOUNT_OPEN", None, available_cash_delta=1),
        event(
            "no-buy-total",
            "BUY",
            10,
            item_id=101,
            position_id=1,
            qty_delta=1,
            settlement_due_at=100,
        ),
        event(
            "zero-buy-total",
            "BUY",
            10,
            item_id=101,
            position_id=1,
            qty_delta=1,
            total_amount=0,
            settlement_due_at=100,
        ),
        event(
            "no-edit-price",
            "DIRECT_EDIT",
            10,
            order_id=10,
            fee_paid=100,
            available_cash_delta=-100,
        ),
        event(
            "no-expiry",
            "LIST",
            10,
            item_id=101,
            position_id=1,
            order_id=10,
            qty_delta=1,
            new_listed_price=100,
            slot_delta=1,
        ),
        event("no-public-time", "ORDER_PUBLIC", 10, order_id=10),
        event(
            "unknown-legacy",
            "LEGACY_POSITION_INIT",
            None,
            item_id=101,
            position_id=1,
            qty_delta=1,
        ),
        event("zero-transfer", "EXTERNAL_TRANSFER", 10),
        event(
            "bad-claim-delta",
            "CLAIM_CASH",
            10,
            unclaimed_delta=-100,
            available_cash_delta=99,
        ),
        event(
            "bool-amount",
            "ACCOUNT_OPEN",
            10,
            available_cash_delta=True,
        ),
    ],
)
def test_strict_required_fields_and_integer_validation(
    conn: sqlite3.Connection, invalid: LedgerEvent
) -> None:
    with pytest.raises(LedgerValidationError):
        append_ledger_events(conn, [invalid])
    assert row_counts(conn) == (0, 0, 0)


def test_null_baselines_and_exact_event_order_sorting() -> None:
    account = event(
        "old-open", "ACCOUNT_OPEN", None, available_cash_delta=500
    )
    legacy = event(
        "old-inventory",
        "LEGACY_POSITION_INIT",
        None,
        item_id=101,
        position_id=1,
        qty_delta=1,
        total_amount=100,
        unknown_reason="acquisition timestamp unknown",
    )
    state = replay_ledger([account, legacy], as_of_ts=-100)
    assert state.available_cash == 500
    assert state.opening_equity == 600
    assert state.positions[1].remaining_cost == 100

    # Input arrival order does not override the event timestamp/order keys.
    ordered = replay_ledger(
        [
            event(
                "withdraw",
                "EXTERNAL_TRANSFER",
                10,
                event_order=2,
                available_cash_delta=-100,
                event_id=1,
            ),
            event(
                "deposit",
                "EXTERNAL_TRANSFER",
                10,
                event_order=1,
                available_cash_delta=100,
                event_id=2,
            ),
            replace(opening(cash=0), event_id=3),
        ],
        as_of_ts=10,
    )
    assert ordered.available_cash == 0
    assert ordered.net_external_transfer == 0


def test_external_transfers_equity_fees_and_unaffordable_gate(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(cash=500_000),
            event(
                "deposit",
                "EXTERNAL_TRANSFER",
                10,
                available_cash_delta=50_000,
            ),
            event(
                "withdrawal",
                "EXTERNAL_TRANSFER",
                20,
                available_cash_delta=-10_000,
            ),
        ],
    )
    state = replay_ledger(conn)
    equity = evaluate_net_equity(state, {}, unpaid_future_fees=1_000)
    assert equity.base_equity == 540_000
    assert equity.end_net_equity == 539_000
    assert equity.delta_net_equity == -1_000

    gate = check_open_loss_budget(
        state,
        max_loss_budget=100_000,
        open_positions_conservative_net_value=0,
        new_order_buy_cost=600_000,
        new_order_immediate_fee=1_000,
        new_order_conservative_net_value=700_000,
    )
    assert gate.post_cash == -61_000
    assert gate.allowed is False

    with_fees = check_open_loss_budget(
        state,
        max_loss_budget=100_000,
        open_positions_conservative_net_value=0,
        new_order_buy_cost=200_000,
        new_order_immediate_fee=5_000,
        new_order_conservative_net_value=110_000,
        unpaid_future_fees=6_000,
    )
    assert with_fees.e_cons_post == 439_000
    assert with_fees.projected_total_loss == 101_000
    assert with_fees.allowed is False


def test_risk_boundary_allows_exact_budget_but_preserves_negative_remaining() -> None:
    state = LedgerState(opening_equity=500_000, available_cash=420_000)
    exact = check_open_loss_budget(
        state,
        max_loss_budget=100_000,
        open_positions_conservative_net_value=0,
        new_order_buy_cost=100_000,
        new_order_immediate_fee=0,
        new_order_conservative_net_value=80_000,
    )
    assert exact.projected_total_loss == 100_000
    assert exact.allowed is True

    breached = replace(state, available_cash=390_000)
    result = check_open_loss_budget(
        breached,
        max_loss_budget=100_000,
        open_positions_conservative_net_value=0,
        new_order_buy_cost=0,
        new_order_immediate_fee=0,
        new_order_conservative_net_value=0,
    )
    assert result.current_conservative_loss == 110_000
    assert result.remaining_loss_budget == -10_000
    assert result.allowed is False


def test_projection_drift_is_repaired_from_events(
    conn: sqlite3.Connection,
) -> None:
    append_ledger_events(
        conn,
        [
            opening(),
            buy("buy", 10),
            listing("list", 20, public_at=20),
        ],
    )
    conn.execute(
        "UPDATE user_orders SET listed_price = 1 WHERE order_id = 10"
    )
    conn.execute(
        "UPDATE user_positions SET remaining_cost = 0 WHERE position_id = 1"
    )
    assert replay_ledger(conn).orders[10].listed_price == 10_000
    assert replay_ledger(conn).positions[1].remaining_cost == 9_000
    assert append_ledger_events(conn, []) == 0
    assert conn.execute(
        "SELECT listed_price FROM user_orders WHERE order_id = 10"
    ).fetchone()[0] == 10_000
    assert conn.execute(
        "SELECT remaining_cost FROM user_positions WHERE position_id = 1"
    ).fetchone()[0] == 9_000


def test_payload_hash_covers_price_but_excludes_database_event_identity() -> None:
    original = event(
        "edit",
        "DIRECT_EDIT",
        10,
        order_id=10,
        new_listed_price=10_500,
        fee_paid=100,
        available_cash_delta=-100,
    )
    assert canonical_payload_sha256(original) == canonical_payload_sha256(
        replace(original, event_id=123)
    )
    assert canonical_payload_sha256(original) != canonical_payload_sha256(
        replace(original, new_listed_price=9_900)
    )
    with pytest.raises(LedgerValidationError, match="payload_sha256"):
        replay_ledger([replace(original, payload_sha256="0" * 64)])


def test_pure_replay_duplicate_receipts_and_unknown_arm() -> None:
    account = opening()
    state = replay_ledger([account, account], arm_id="SIMPLE_LIMIT")
    assert state.available_cash == 1_000_000
    with pytest.raises(LedgerIdempotencyConflictError):
        replay_ledger(
            [account, replace(account, available_cash_delta=999_999)]
        )
    absent = replay_ledger([account], arm_id="B_SHOCK")
    assert absent.arm_id == "B_SHOCK"
    assert absent.available_cash == 0
    assert absent.positions == {}
    assert absent.orders == {}
