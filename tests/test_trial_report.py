"""Standalone acceptance tests for the frozen paired-window trial protocol."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from dataclasses import asdict, replace
from typing import Any

import pytest

from pokemmo_gtl.quant.trial_report import (
    TrialProtocol,
    compute_stage2_trial_report,
    ensure_trial_tables,
    load_trial_protocol,
    rank_window_candidates,
    register_paired_window_block,
)


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.execute("PRAGMA foreign_keys = ON")
    ensure_trial_tables(connection)
    try:
        yield connection
    finally:
        connection.close()


def _register(
    conn: sqlite3.Connection,
    block_id: str,
    *,
    start: int = 1000,
) -> list[dict[str, Any]]:
    return register_paired_window_block(
        conn,
        block_id,
        (start, start + 1800),
        (start + 3600, start + 5400),
        planned_review_min=5.0,
        item_pool_version="pool-v1",
        rule_version="rule-v1",
        seed=20261126,
        registered_at=0,
    )


def _completed_block(
    conn: sqlite3.Connection,
    block_id: str,
    *,
    start: int = 1000,
) -> dict[str, dict[str, Any]]:
    windows = _register(conn, block_id, start=start)
    conn.execute(
        """
        UPDATE review_windows
        SET window_status = 'COMPLETED',
            actual_start_at = planned_start_at,
            actual_end_at = planned_start_at + 300,
            actual_review_min = 5.0
        WHERE pair_block_id = ?
        """,
        (block_id,),
    )
    return {window["assigned_arm"]: window for window in windows}


def _observation(
    conn: sqlite3.Connection,
    window_id: str,
    group_id: str,
    *,
    arm: str,
    reason: str | None = "CONFIRMED_OK",
    fillable: int | None = 2,
    cost: int | None = 800,
    checked_offset: int | None = 80,
    alert_offset: int | None = 20,
    expires_offset: int = 500,
    rank: int | None = 1,
) -> int:
    start = conn.execute(
        "SELECT planned_start_at FROM review_windows WHERE window_id = ?",
        (window_id,),
    ).fetchone()[0]
    cursor = conn.execute(
        """
        INSERT INTO opportunity_observations (
            window_id, opportunity_group_id, arm, rule_version,
            fee_rule_version, source_snapshot_id, read_publish_seq, item_id,
            collect_start_at, available_at, quote_expires_at, rank_order,
            planned_qty, max_total_buy_cost, snapshot_ladder_json,
            alert_seen_at, client_checked_at, client_fillable_qty,
            client_total_cost, outcome_reason
        ) VALUES (
            ?, ?, ?, 'rule-v1', 'fee-v1', 1, 10, 1,
            ?, ?, ?, ?, 2, 1000, '[]', ?, ?, ?, ?, ?
        )
        """,
        (
            window_id,
            group_id,
            arm,
            start,
            start + 10,
            start + expires_offset,
            rank,
            start + alert_offset if alert_offset is not None else None,
            start + checked_offset if checked_offset is not None else None,
            fillable,
            cost,
            reason,
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _candidate(item_id: int = 1, **updates: Any) -> dict[str, Any]:
    result = {
        "item_id": item_id,
        "opportunity_group_id": f"item-{item_id}-episode",
        "fee_verified": True,
        "fee_rule_version": "fee-v1",
        "rule_version": "rule-v1",
        "book_state": "stocked",
        "frame_complete": True,
        "total_listed_qty": 20,
        "source_snapshot_id": item_id,
        "read_publish_seq": 10,
        "collect_start_at": 1100,
        "available_at": 1200,
        "quote_expires_at": 2400,
        "planned_qty": 2,
        "planned_buy_cost": 800,
        "max_total_buy_cost": 1000,
        "lot_feasible": True,
        "history_coverage_hours": 168.0,
        "price_zscore": -2.0,
        "depth_sufficient": True,
        "snapshot_ladder": [{"price": 400, "qty": 2}],
    }
    result.update(updates)
    return result


def _trial(
    conn: sqlite3.Connection, differences: Sequence[int]
) -> dict[str, Any]:
    for index, difference in enumerate(differences):
        block_id = f"block-{index:03d}"
        windows = _completed_block(
            conn, block_id, start=1000 + index * 10000
        )
        for arm, count in (
            ("B_SHOCK", max(difference, 0)),
            ("SIMPLE_LIMIT", max(-difference, 0)),
        ):
            for opportunity in range(count):
                _observation(
                    conn,
                    windows[arm]["window_id"],
                    f"{block_id}-{arm}-{opportunity}",
                    arm=arm,
                )
    return compute_stage2_trial_report(conn)


def test_frozen_yaml_matches_complete_protocol() -> None:
    assert asdict(load_trial_protocol()) == asdict(TrialProtocol())


def test_standalone_schema_has_exact_trial_columns(
    conn: sqlite3.Connection,
) -> None:
    window_columns = [
        row[1] for row in conn.execute("PRAGMA table_info(review_windows)")
    ]
    observation_columns = [
        row[1]
        for row in conn.execute("PRAGMA table_info(opportunity_observations)")
    ]
    assert window_columns == [
        "window_id",
        "pair_block_id",
        "planned_start_at",
        "planned_end_at",
        "planned_review_min",
        "item_pool_version",
        "rule_version",
        "assigned_arm",
        "window_status",
        "actual_start_at",
        "actual_end_at",
        "actual_review_min",
        "miss_or_abort_reason",
    ]
    assert observation_columns == [
        "obs_id",
        "window_id",
        "opportunity_group_id",
        "arm",
        "rule_version",
        "fee_rule_version",
        "source_snapshot_id",
        "read_publish_seq",
        "item_id",
        "collect_start_at",
        "available_at",
        "quote_expires_at",
        "rank_order",
        "planned_qty",
        "max_total_buy_cost",
        "snapshot_ladder_json",
        "alert_seen_at",
        "client_checked_at",
        "client_fillable_qty",
        "client_total_cost",
        "outcome_reason",
    ]
    ensure_trial_tables(conn)
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_registration_is_deterministic_and_frozen(
    conn: sqlite3.Connection,
) -> None:
    windows = _register(conn, "registered")
    assert {window["assigned_arm"] for window in windows} == {
        "SIMPLE_LIMIT",
        "B_SHOCK",
    }
    assert all(window["window_status"] is None for window in windows)
    assert all(window["planned_review_min"] == 5.0 for window in windows)
    other = sqlite3.connect(":memory:", isolation_level=None)
    try:
        ensure_trial_tables(other)
        assert _register(other, "registered") == windows
    finally:
        other.close()
    with pytest.raises(ValueError, match="already registered"):
        _register(conn, "registered")
    assert conn.execute("SELECT COUNT(*) FROM review_windows").fetchone()[0] == 2


def test_registration_rejects_started_or_incomparable_slots(
    conn: sqlite3.Connection,
) -> None:
    with pytest.raises(ValueError, match="before it begins"):
        register_paired_window_block(
            conn,
            "late",
            (1000, 2800),
            (4600, 6400),
            5.0,
            "pool-v1",
            "rule-v1",
            7,
            registered_at=1000,
        )
    with pytest.raises(ValueError, match="equal duration"):
        register_paired_window_block(
            conn,
            "unequal",
            (1000, 2800),
            (4600, 6500),
            5.0,
            "pool-v1",
            "rule-v1",
            7,
            registered_at=0,
        )
    assert conn.execute("SELECT COUNT(*) FROM review_windows").fetchone()[0] == 0


def test_helpers_do_not_commit_a_callers_transaction(
    conn: sqlite3.Connection,
) -> None:
    conn.execute("BEGIN IMMEDIATE")
    _register(conn, "rollback")
    ensure_trial_tables(conn)
    assert conn.in_transaction
    conn.execute("ROLLBACK")
    assert conn.execute("SELECT COUNT(*) FROM review_windows").fetchone()[0] == 0


def test_double_zero_and_missed_windows_preserve_paired_denominator(
    conn: sqlite3.Connection,
) -> None:
    _completed_block(conn, "double-zero")
    conn.execute(
        """
        UPDATE review_windows SET window_status = 'ZERO_OPPORTUNITY'
        WHERE pair_block_id = 'double-zero'
        """
    )
    missed = _completed_block(conn, "missed", start=10000)
    conn.execute(
        """
        UPDATE review_windows
        SET window_status = 'MISSED_BY_USER',
            actual_start_at = NULL,
            actual_end_at = NULL,
            actual_review_min = NULL,
            miss_or_abort_reason = 'User unavailable'
        WHERE window_id = ?
        """,
        (missed["B_SHOCK"]["window_id"],),
    )
    report = compute_stage2_trial_report(conn)
    assert report["window_count"] == 4
    assert report["paired_window_count"] == 4
    assert report["K"] == 2
    assert report["paired_differences"] == [0, 0]
    assert all(window["Y"] == 0 for window in report["windows"])
    assert all(window["included_in_pair"] for window in report["windows"])
    assert report["diagnostics"]["window_status_counts"] == {
        "COMPLETED": 1,
        "MISSED_BY_USER": 1,
        "ZERO_OPPORTUNITY": 2,
    }
    b_metrics = report["per_arm"]["B_SHOCK"]
    assert b_metrics["paired_window_count"] == 2
    assert b_metrics["missed_count"] == 1
    assert b_metrics["expired_or_missed_denominator"] == 1
    assert b_metrics["client_confirmation_rate"] == 0.0
    assert report["b_client_confirmation_rate"] == 0.0
    assert b_metrics["unknown_actual_review_windows"] == 1


def test_audit_controls_and_unassigned_arm_never_inflate_primary(
    conn: sqlite3.Connection,
) -> None:
    windows = _completed_block(conn, "isolation")
    for arm in ("B_SHOCK", "SIMPLE_LIMIT"):
        _observation(
            conn,
            windows[arm]["window_id"],
            f"primary-{arm}",
            arm=arm,
        )
    for index in range(3):
        _observation(
            conn,
            windows["B_SHOCK"]["window_id"],
            f"audit-{index}",
            arm="AUDIT_SAMPLE",
        )
        _observation(
            conn,
            windows["B_SHOCK"]["window_id"],
            f"shadow-{index}",
            arm="SIMPLE_LIMIT",
        )
    for index in range(2):
        _observation(
            conn,
            windows["SIMPLE_LIMIT"]["window_id"],
            f"control-{index}",
            arm="NO_ALERT_CONTROL",
        )
    report = compute_stage2_trial_report(conn)
    assert report["paired_blocks"][0]["Y_B"] == 1
    assert report["paired_blocks"][0]["Y_S"] == 1
    assert report["paired_differences"] == [0]
    for arm in ("B_SHOCK", "SIMPLE_LIMIT"):
        assert report["per_arm"][arm]["expired_or_missed_denominator"] == 1
        assert report["per_arm"][arm]["client_confirmation_rate"] == 1.0
    assert report["diagnostics"]["AUDIT_SAMPLE"]["confirmed_opportunities"] == 3
    assert (
        report["diagnostics"]["NO_ALERT_CONTROL"]["confirmed_opportunities"]
        == 2
    )
    assert report["diagnostics"]["unassigned_arm"]["confirmed_opportunities"] == 3


def test_three_price_ticks_count_as_one_independent_opportunity(
    conn: sqlite3.Connection,
) -> None:
    windows = _completed_block(conn, "ticks")
    window_id = windows["B_SHOCK"]["window_id"]
    for cost in (900, 850, 800):
        _observation(
            conn, window_id, "same-opportunity", arm="B_SHOCK", cost=cost
        )
    _observation(
        conn, window_id, "insufficient-quantity", arm="B_SHOCK", fillable=1
    )
    _observation(
        conn, window_id, "above-ceiling", arm="B_SHOCK", cost=1001
    )
    _observation(
        conn, window_id, "unknown-cost", arm="B_SHOCK", cost=None
    )
    report = compute_stage2_trial_report(conn)
    assert report["paired_blocks"][0]["Y_B"] == 1
    assert report["per_arm"]["B_SHOCK"]["confirmed_opportunities"] == 1
    assert report["per_arm"]["B_SHOCK"]["expired_or_missed_denominator"] == 4
    assert report["per_arm"]["B_SHOCK"]["client_confirmation_rate"] == 0.25
    assert report["b_client_confirmation_rate"] == 0.25
    assert report["diagnostics"]["raw_observation_rows"] == 6
    assert conn.execute(
        """
        SELECT COUNT(*) FROM opportunity_observations
        WHERE opportunity_group_id = 'same-opportunity'
        """
    ).fetchone()[0] == 3


@pytest.mark.parametrize("status", ["ZERO_OPPORTUNITY", "MISSED_BY_USER"])
def test_forced_zero_status_overrides_even_saved_confirmations(
    conn: sqlite3.Connection, status: str
) -> None:
    windows = _completed_block(conn, "forced-zero")
    window_id = windows["B_SHOCK"]["window_id"]
    _observation(conn, window_id, "saved-confirmation", arm="B_SHOCK")
    conn.execute(
        "UPDATE review_windows SET window_status = ? WHERE window_id = ?",
        (status, window_id),
    )
    report = compute_stage2_trial_report(conn)
    assert report["K"] == 1
    assert report["paired_blocks"][0]["Y_B"] == 0
    assert report["per_arm"]["B_SHOCK"]["confirmed_opportunities"] == 0
    assert report["per_arm"]["B_SHOCK"]["client_confirmation_rate"] == 0.0
    assert report["b_client_confirmation_rate"] == 0.0


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"fee_verified": False}, "FEE_UNKNOWN"),
        ({"book_state": "mixed_ended_empty"}, "BOOK_NOT_STOCKED"),
        ({"book_state": "empty_confirmed"}, "BOOK_NOT_STOCKED"),
        ({"quote_expires_at": 1299}, "QUOTE_EXPIRED"),
        ({"available_at": 1301}, "NOT_YET_AVAILABLE"),
        ({"planned_buy_cost": 1001}, "COST_ABOVE_LIMIT"),
        ({"lot_feasible": False}, "LOT_INFEASIBLE"),
        ({"total_listed_qty": 0}, "BOOK_NOT_STOCKED"),
        ({"frame_complete": False}, "INCOMPLETE_FRAME"),
    ],
)
def test_hard_gates_block_both_actionable_rankings(
    conn: sqlite3.Connection,
    updates: dict[str, Any],
    reason: str,
) -> None:
    window_id = _register(conn, "eligibility")[0]["window_id"]
    result = rank_window_candidates(
        conn,
        window_id,
        [_candidate(1, **updates), _candidate(2)],
        1300,
    )
    for arm in ("SIMPLE_LIMIT", "B_SHOCK"):
        assert [row["item_id"] for row in result[arm]] == [2]
        assert all(row["actionable"] for row in result[arm])
    assert len(result["rejected"]) == 1
    assert result["rejected"][0]["outcome_reason"] == reason
    blocked = conn.execute(
        """
        SELECT arm, rank_order, outcome_reason
        FROM opportunity_observations WHERE item_id = 1 ORDER BY arm
        """
    ).fetchall()
    assert len(blocked) == 2
    assert all(row[1] is None and row[2] == reason for row in blocked)


def test_unknown_fee_version_can_stay_in_observation(
    conn: sqlite3.Connection,
) -> None:
    window_id = _register(conn, "unknown-fees")[0]["window_id"]
    result = rank_window_candidates(
        conn,
        window_id,
        [_candidate(fee_verified=False, fee_rule_version=None)],
        1300,
    )
    assert result["SIMPLE_LIMIT"] == []
    assert result["B_SHOCK"] == []
    assert result["rejected"][0]["outcome_reason"] == "FEE_UNKNOWN"
    assert result["rejected"][0]["fee_rule_version"] == "unknown"


@pytest.mark.parametrize(
    ("decision_ts", "eligible"),
    [(1200, True), (2400, True), (2401, False)],
)
def test_quote_boundaries_are_inclusive_and_ttl_cannot_be_extended(
    conn: sqlite3.Connection, decision_ts: int, eligible: bool
) -> None:
    window_id = _register(conn, "ttl")[0]["window_id"]
    result = rank_window_candidates(
        conn,
        window_id,
        [_candidate(quote_expires_at=9999)],
        decision_ts,
    )
    assert bool(result["SIMPLE_LIMIT"]) is eligible
    observed_expiry = conn.execute(
        "SELECT quote_expires_at FROM opportunity_observations LIMIT 1"
    ).fetchone()[0]
    assert observed_expiry == 2400


def test_b_prioritizes_only_qualified_shocks_and_preserves_lineage(
    conn: sqlite3.Connection,
) -> None:
    window_id = _register(conn, "ranking")[0]["window_id"]
    result = rank_window_candidates(
        conn,
        window_id,
        [
            _candidate(4, planned_buy_cost=800, depth_sufficient=False),
            _candidate(3, planned_buy_cost=600, history_coverage_hours=167.99),
            _candidate(1, planned_buy_cost=700, price_zscore=None),
            _candidate(2, planned_buy_cost=800),
        ],
        1300,
        audit_sample_size=1,
        no_alert_controls=[_candidate(9)],
    )
    assert [row["item_id"] for row in result["SIMPLE_LIMIT"]] == [3, 1, 2, 4]
    assert [row["item_id"] for row in result["B_SHOCK"]] == [2, 3, 1, 4]
    assert len(result["AUDIT_SAMPLE"]) == 1
    assert result["AUDIT_SAMPLE"][0]["item_id"] == 4
    assert result["AUDIT_SAMPLE"][0]["actionable"] is False
    assert len(result["NO_ALERT_CONTROL"]) == 1
    assert result["NO_ALERT_CONTROL"][0]["actionable"] is False
    assert result["NO_ALERT_CONTROL"][0]["alert_seen_at"] is None
    for arm in ("SIMPLE_LIMIT", "B_SHOCK"):
        for row in result[arm]:
            assert row["flow_metrics"] == {"volume": None, "flow": None}
            assert row["source_snapshot_id"] == row["item_id"]
            assert row["read_publish_seq"] == 10
            assert row["rule_version"] == "rule-v1"
            assert row["fee_rule_version"] == "fee-v1"
    assert conn.execute(
        "SELECT COUNT(*) FROM opportunity_observations"
    ).fetchone()[0] == 10


def test_insufficient_history_falls_back_to_simple_order(
    conn: sqlite3.Connection,
) -> None:
    window_id = _register(conn, "fallback")[0]["window_id"]
    candidates = [
        _candidate(
            2,
            planned_buy_cost=900,
            history_coverage_hours=167.999,
            price_zscore=-10.0,
        ),
        _candidate(
            1,
            planned_buy_cost=600,
            history_coverage_hours=None,
            price_zscore=None,
            depth_sufficient=False,
        ),
    ]
    result = rank_window_candidates(conn, window_id, candidates, 1300)
    assert [row["item_id"] for row in result["SIMPLE_LIMIT"]] == [1, 2]
    assert [row["item_id"] for row in result["B_SHOCK"]] == [1, 2]
    assert all(not row["shock_qualified"] for row in result["B_SHOCK"])


def test_latest_empty_frame_blocks_an_older_stocked_frame(
    conn: sqlite3.Connection,
) -> None:
    window_id = _register(conn, "latest-frame")[0]["window_id"]
    result = rank_window_candidates(
        conn,
        window_id,
        [
            _candidate(1, source_snapshot_id=1),
            _candidate(
                1,
                source_snapshot_id=2,
                read_publish_seq=11,
                collect_start_at=1240,
                available_at=1250,
                book_state="mixed_ended_empty",
                total_listed_qty=0,
            ),
        ],
        1300,
    )
    assert result["SIMPLE_LIMIT"] == []
    assert result["B_SHOCK"] == []
    assert {row["outcome_reason"] for row in result["rejected"]} == {
        "SUPERSEDED_FRAME",
        "BOOK_NOT_STOCKED",
    }


def test_future_frame_does_not_supersede_visible_frame(
    conn: sqlite3.Connection,
) -> None:
    window_id = _register(conn, "future-frame")[0]["window_id"]
    result = rank_window_candidates(
        conn,
        window_id,
        [
            _candidate(1),
            _candidate(
                1,
                source_snapshot_id=2,
                collect_start_at=1290,
                available_at=1400,
                book_state="stocked",
            ),
        ],
        1300,
    )
    assert len(result["SIMPLE_LIMIT"]) == 1
    assert result["SIMPLE_LIMIT"][0]["source_snapshot_id"] == 1
    assert result["rejected"][0]["outcome_reason"] == "NOT_YET_AVAILABLE"


def test_invalid_candidate_batch_is_atomic(conn: sqlite3.Connection) -> None:
    window_id = _register(conn, "atomic-ranking")[0]["window_id"]
    with pytest.raises(ValueError, match="rule_version"):
        rank_window_candidates(
            conn,
            window_id,
            [_candidate(1), _candidate(2, rule_version="unregistered-rule")],
            1300,
        )
    assert conn.execute(
        "SELECT COUNT(*) FROM opportunity_observations"
    ).fetchone()[0] == 0


def test_reproducible_b_gate_passes_with_consistent_paired_gain(
    conn: sqlite3.Connection,
) -> None:
    first = _trial(conn, [1, 1, 1, 1, 1, 1])
    second = compute_stage2_trial_report(conn)
    assert first == second
    assert first["K"] == 6
    assert first["mean_gain"] == 1
    assert first["bootstrap_lower_bound"] == 1
    assert first["b_client_confirmation_rate"] == 1.0
    assert first["b_expired_or_missed_rate"] == 0
    assert first["b_passes_gate"] is True
    assert all(first["gate_checks"].values())
    assert first["per_arm"]["B_SHOCK"]["client_confirmation_rate"] == 1.0
    assert first["per_arm"]["SIMPLE_LIMIT"]["client_confirmation_rate"] == 0.0
    for arm in ("SIMPLE_LIMIT", "B_SHOCK"):
        assert first["per_arm"][arm]["actual_review_min"] == 30.0
    assert (
        first["per_arm"]["B_SHOCK"]["median_alert_to_check_delay_sec"]
        == 60
    )
    assert (
        first["per_arm"]["SIMPLE_LIMIT"]["median_alert_to_check_delay_sec"]
        is None
    )


@pytest.mark.parametrize(
    ("reason", "fillable", "cost"),
    [
        ("INSUFFICIENT_QTY", 1, 800),
        ("OVER_MAX_COST", 2, 1001),
    ],
)
def test_b_gate_rejects_a2_timely_unconfirmable_alert_flood(
    conn: sqlite3.Connection,
    reason: str,
    fillable: int,
    cost: int,
) -> None:
    _trial(conn, [1, 1, 1, 1, 1, 1])
    b_windows = conn.execute(
        """
        SELECT window_id FROM review_windows
        WHERE assigned_arm = 'B_SHOCK' ORDER BY window_id
        """
    ).fetchall()
    assert len(b_windows) == 6
    for index, (window_id,) in enumerate(b_windows):
        for opportunity in range(99):
            _observation(
                conn,
                window_id,
                f"unconfirmable-{index}-{opportunity}",
                arm="B_SHOCK",
                reason=reason,
                fillable=fillable,
                cost=cost,
                checked_offset=80,
                expires_offset=500,
                rank=opportunity + 2,
            )

    report = compute_stage2_trial_report(conn)
    b_metrics = report["per_arm"]["B_SHOCK"]
    assert report["K"] == 6
    assert all(
        pair["Y_B"] == 1 and pair["Y_S"] == 0
        for pair in report["paired_blocks"]
    )
    assert report["paired_differences"] == [1, 1, 1, 1, 1, 1]
    assert report["mean_gain"] == 1.0
    assert report["bootstrap_lower_bound"] == 1.0
    assert b_metrics["confirmed_opportunities"] == 6
    assert b_metrics["expired_or_missed_denominator"] == 600
    assert b_metrics["expired_or_missed_count"] == 0
    assert report["b_expired_or_missed_rate"] == 0.0
    assert b_metrics["client_confirmation_rate"] == pytest.approx(0.01)
    assert report["b_client_confirmation_rate"] == pytest.approx(0.01)
    assert report["gate_checks"]["client_confirmation_rate_meets_minimum"] is False
    assert all(
        passed
        for name, passed in report["gate_checks"].items()
        if name != "client_confirmation_rate_meets_minimum"
    )
    assert report["b_passes_gate"] is False


@pytest.mark.parametrize(
    ("minimum_rate", "passes_gate"),
    [(0.50, True), (0.75, False)],
)
def test_client_confirmation_redline_is_configurable_and_inclusive(
    conn: sqlite3.Connection,
    minimum_rate: float,
    passes_gate: bool,
) -> None:
    _trial(conn, [1, 1, 1, 1, 1, 1])
    b_windows = conn.execute(
        """
        SELECT window_id FROM review_windows
        WHERE assigned_arm = 'B_SHOCK' ORDER BY window_id
        """
    ).fetchall()
    for index, (window_id,) in enumerate(b_windows):
        _observation(
            conn,
            window_id,
            f"insufficient-{index}",
            arm="B_SHOCK",
            reason="INSUFFICIENT_QTY",
            fillable=1,
        )
    report = compute_stage2_trial_report(
        conn,
        protocol=replace(
            TrialProtocol(),
            min_client_confirmation_rate=minimum_rate,
        ),
    )
    assert report["b_client_confirmation_rate"] == 0.5
    assert report["b_expired_or_missed_rate"] == 0.0
    assert (
        report["gate_checks"]["client_confirmation_rate_meets_minimum"]
        is passes_gate
    )
    assert report["b_passes_gate"] is passes_gate


@pytest.mark.parametrize(
    ("differences", "failed_check"),
    [
        ([1, 1, 1, 1, 1], "minimum_paired_blocks"),
        ([0, 0, 0, 0, 0, 1], "minimum_mean_gain"),
        ([6, 0, 0, 0, 0, 0], "positive_bootstrap_lower_bound"),
    ],
)
def test_b_gate_rejects_insufficient_or_single_block_evidence(
    conn: sqlite3.Connection,
    differences: list[int],
    failed_check: str,
) -> None:
    report = _trial(conn, differences)
    assert report["b_passes_gate"] is False
    assert report["gate_checks"][failed_check] is False
    if differences == [6, 0, 0, 0, 0, 0]:
        assert report["mean_gain"] == 1
        assert report["bootstrap_lower_bound"] == 0


def test_expired_rate_is_group_based_and_can_fail_an_otherwise_good_gate(
    conn: sqlite3.Connection,
) -> None:
    _trial(conn, [1, 1, 1, 1, 1, 1])
    b_windows = conn.execute(
        """
        SELECT window_id FROM review_windows
        WHERE assigned_arm = 'B_SHOCK' ORDER BY window_id
        """
    ).fetchall()
    for index, (window_id,) in enumerate(b_windows):
        for _ in range(3):
            _observation(
                conn,
                window_id,
                f"expired-{index}",
                arm="B_SHOCK",
                reason="EXPIRED",
                fillable=None,
                cost=None,
                checked_offset=None,
            )
        _observation(
            conn,
            window_id,
            f"diagnostic-{index}",
            arm="NO_ALERT_CONTROL",
        )
    report = compute_stage2_trial_report(conn)
    assert report["mean_gain"] == 1
    assert report["bootstrap_lower_bound"] == 1
    assert report["per_arm"]["B_SHOCK"]["expired_count"] == 6
    assert report["per_arm"]["B_SHOCK"]["expired_or_missed_denominator"] == 12
    assert report["b_client_confirmation_rate"] == 0.5
    assert report["gate_checks"]["client_confirmation_rate_meets_minimum"] is True
    assert report["b_expired_or_missed_rate"] == 0.5
    assert report["gate_checks"]["expired_or_missed_rate_within_limit"] is False
    assert report["b_passes_gate"] is False


def test_successful_later_tick_resolves_group_without_expiry_double_count(
    conn: sqlite3.Connection,
) -> None:
    windows = _completed_block(conn, "resolved")
    window_id = windows["B_SHOCK"]["window_id"]
    _observation(
        conn,
        window_id,
        "one-episode",
        arm="B_SHOCK",
        reason="EXPIRED",
        fillable=None,
        cost=None,
        checked_offset=None,
    )
    _observation(conn, window_id, "one-episode", arm="B_SHOCK")
    report = compute_stage2_trial_report(conn)
    assert report["paired_blocks"][0]["Y_B"] == 1
    assert report["per_arm"]["B_SHOCK"]["expired_or_missed_denominator"] == 1
    assert report["per_arm"]["B_SHOCK"]["expired_count"] == 0
    assert report["per_arm"]["B_SHOCK"]["client_confirmation_rate"] == 1.0
    assert report["b_client_confirmation_rate"] == 1.0


def test_unfinished_pair_remains_visible_but_cannot_pass_gate(
    conn: sqlite3.Connection,
) -> None:
    _register(conn, "unfinished")
    report = compute_stage2_trial_report(conn)
    assert report["window_count"] == 2
    assert len(report["windows"]) == 2
    assert report["K"] == 0
    assert report["mean_gain"] is None
    assert report["bootstrap_lower_bound"] is None
    assert report["b_client_confirmation_rate"] == 0.0
    assert report["gate_checks"]["client_confirmation_rate_meets_minimum"] is False
    assert report["b_passes_gate"] is False
    assert report["diagnostics"]["incomplete_or_invalid_pairs"] == [
        {"pair_block_id": "unfinished", "reason": "unfinished_windows"}
    ]


def test_aborted_window_is_not_silently_dropped(
    conn: sqlite3.Connection,
) -> None:
    windows = _completed_block(conn, "aborted")
    conn.execute(
        """
        UPDATE review_windows
        SET window_status = 'ABORTED', miss_or_abort_reason = 'Interrupted'
        WHERE window_id = ?
        """,
        (windows["B_SHOCK"]["window_id"],),
    )
    report = compute_stage2_trial_report(conn)
    assert report["window_count"] == 2
    assert report["K"] == 1
    assert report["paired_blocks"][0]["Y_B"] == 0
    assert report["per_arm"]["B_SHOCK"]["missed_count"] == 1
    assert report["b_client_confirmation_rate"] == 0.0


def test_empty_database_reports_no_evidence(conn: sqlite3.Connection) -> None:
    report = compute_stage2_trial_report(conn)
    assert report["window_count"] == 0
    assert report["K"] == 0
    assert report["paired_differences"] == []
    assert report["mean_gain"] is None
    assert report["bootstrap_lower_bound"] is None
    assert report["b_client_confirmation_rate"] == 0.0
    for arm in ("SIMPLE_LIMIT", "B_SHOCK"):
        assert report["per_arm"][arm]["expired_or_missed_denominator"] == 0
        assert report["per_arm"][arm]["client_confirmation_rate"] == 0.0
    assert report["gate_checks"]["client_confirmation_rate_meets_minimum"] is False
    assert report["b_passes_gate"] is False


@pytest.mark.parametrize("minimum_rate", [0.0, 0.5, 1.0])
def test_protocol_accepts_client_confirmation_rate_boundaries(
    minimum_rate: float,
) -> None:
    protocol = replace(
        TrialProtocol(),
        min_client_confirmation_rate=minimum_rate,
    )
    assert protocol.min_client_confirmation_rate == minimum_rate


@pytest.mark.parametrize(
    "updates",
    [
        {"quote_ttl_sec": 0},
        {"min_paired_blocks": 0},
        {"bootstrap_iterations": 0},
        {"bootstrap_confidence_level": 1.0},
        {"min_client_confirmation_rate": -0.1},
        {"min_client_confirmation_rate": 1.1},
        {"min_client_confirmation_rate": float("nan")},
        {"min_client_confirmation_rate": float("inf")},
        {"min_client_confirmation_rate": float("-inf")},
        {"max_expired_or_missed_rate": 1.1},
        {"min_history_coverage_hours": -1.0},
        {"shock_zscore_threshold": float("nan")},
    ],
)
def test_protocol_rejects_invalid_parameters(updates: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(TrialProtocol(), **updates)
