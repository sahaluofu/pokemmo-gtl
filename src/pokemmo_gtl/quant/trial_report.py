"""Preregistered paired entry trials and an isolated, reproducible B gate.

Call ``ensure_trial_tables`` only when standalone trial tables are needed.
Production connections can use the tables created by migration 002 directly.
Connection configuration, including WAL, belongs to ``db.connection``.

A NULL window_status represents a preregistered, unfinished window. All four
terminal statuses remain in the window denominator. Only complete, comparable
two-window blocks enter K; unfinished or malformed blocks are disclosed.

Candidate inputs describe complete latest-frame evidence, including feasible
integer lot costs. This module does not infer purchase feasibility from pooled
price-level quantities. Unknown flow metrics remain None and are not invented.

Client-confirmation and expired/missed rates and bootstrap definitions are
frozen in the protocol YAML. Neither shadow lists nor diagnostic samples can
enlarge the primary endpoint or dilute the assigned-arm gate rates.
"""

from __future__ import annotations

import json
import math
import random
import sqlite3
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields
from fractions import Fraction
from pathlib import Path
from statistics import mean, median
from typing import Any

import yaml

from pokemmo_gtl.db.connection import write_transaction

MAIN_ARMS = ("SIMPLE_LIMIT", "B_SHOCK")
FINAL_STATUSES = frozenset(
    {"COMPLETED", "ZERO_OPPORTUNITY", "MISSED_BY_USER", "ABORTED"}
)
EXPIRED_REASONS = frozenset(
    {"EXPIRED", "QUOTE_EXPIRED", "EXPIRED_QUOTE", "EXPIRED_OR_MISSED"}
)
MISSED_REASONS = frozenset(
    {
        "MISSED",
        "MISSED_BY_USER",
        "MISSED_OPPORTUNITY",
        "ALERT_MISSED",
        "NOT_CHECKED",
        "EXPIRED_OR_MISSED",
    }
)
REJECTION_REASONS = frozenset(
    {
        "FEE_UNKNOWN",
        "BOOK_NOT_STOCKED",
        "INCOMPLETE_FRAME",
        "NOT_YET_AVAILABLE",
        "QUOTE_EXPIRED",
        "SUPERSEDED_FRAME",
        "INVALID_LOT",
        "LOT_INFEASIBLE",
        "COST_ABOVE_LIMIT",
    }
)

REVIEW_WINDOWS_DDL = """
CREATE TABLE IF NOT EXISTS review_windows (
    window_id TEXT PRIMARY KEY,
    pair_block_id TEXT NOT NULL,
    planned_start_at INTEGER NOT NULL,
    planned_end_at INTEGER NOT NULL,
    planned_review_min REAL NOT NULL,
    item_pool_version TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    assigned_arm TEXT NOT NULL
        CHECK (assigned_arm IN ('SIMPLE_LIMIT', 'B_SHOCK')),
    window_status TEXT
        CHECK (window_status IN (
            'COMPLETED', 'ZERO_OPPORTUNITY', 'MISSED_BY_USER', 'ABORTED'
        )),
    actual_start_at INTEGER,
    actual_end_at INTEGER,
    actual_review_min REAL,
    miss_or_abort_reason TEXT
) STRICT
"""

OPPORTUNITY_OBSERVATIONS_DDL = """
CREATE TABLE IF NOT EXISTS opportunity_observations (
    obs_id INTEGER PRIMARY KEY AUTOINCREMENT,
    window_id TEXT NOT NULL REFERENCES review_windows(window_id),
    opportunity_group_id TEXT NOT NULL,
    arm TEXT NOT NULL CHECK (arm IN (
        'SIMPLE_LIMIT', 'B_SHOCK', 'AUDIT_SAMPLE', 'NO_ALERT_CONTROL'
    )),
    rule_version TEXT NOT NULL,
    fee_rule_version TEXT,
    source_snapshot_id INTEGER,
    read_publish_seq INTEGER,
    item_id INTEGER NOT NULL,
    collect_start_at INTEGER,
    available_at INTEGER,
    quote_expires_at INTEGER,
    rank_order INTEGER,
    planned_qty INTEGER,
    max_total_buy_cost INTEGER,
    snapshot_ladder_json TEXT,
    alert_seen_at INTEGER,
    client_checked_at INTEGER,
    client_fillable_qty INTEGER,
    client_total_cost INTEGER,
    outcome_reason TEXT
) STRICT
"""

OBSERVATION_INSERT_SQL = """
INSERT INTO opportunity_observations (
    window_id, opportunity_group_id, arm, rule_version, fee_rule_version,
    source_snapshot_id, read_publish_seq, item_id, collect_start_at,
    available_at, quote_expires_at, rank_order, planned_qty,
    max_total_buy_cost, snapshot_ladder_json, alert_seen_at,
    client_checked_at, client_fillable_qty, client_total_cost, outcome_reason
) VALUES (
    :window_id, :opportunity_group_id, :arm, :rule_version, :fee_rule_version,
    :source_snapshot_id, :read_publish_seq, :item_id, :collect_start_at,
    :available_at, :quote_expires_at, :rank_order, :planned_qty,
    :max_total_buy_cost, :snapshot_ladder_json, :alert_seen_at,
    :client_checked_at, :client_fillable_qty, :client_total_cost, :outcome_reason
)
"""

TRIAL_QUERY = """
SELECT
    w.window_id AS w_window_id,
    w.pair_block_id AS w_pair_block_id,
    w.planned_start_at AS w_planned_start_at,
    w.planned_end_at AS w_planned_end_at,
    w.planned_review_min AS w_planned_review_min,
    w.item_pool_version AS w_item_pool_version,
    w.rule_version AS w_rule_version,
    w.assigned_arm AS w_assigned_arm,
    w.window_status AS w_window_status,
    w.actual_start_at AS w_actual_start_at,
    w.actual_end_at AS w_actual_end_at,
    w.actual_review_min AS w_actual_review_min,
    w.miss_or_abort_reason AS w_miss_or_abort_reason,
    o.obs_id AS o_obs_id,
    o.window_id AS o_window_id,
    o.opportunity_group_id AS o_opportunity_group_id,
    o.arm AS o_arm,
    o.rule_version AS o_rule_version,
    o.fee_rule_version AS o_fee_rule_version,
    o.source_snapshot_id AS o_source_snapshot_id,
    o.read_publish_seq AS o_read_publish_seq,
    o.item_id AS o_item_id,
    o.collect_start_at AS o_collect_start_at,
    o.available_at AS o_available_at,
    o.quote_expires_at AS o_quote_expires_at,
    o.rank_order AS o_rank_order,
    o.planned_qty AS o_planned_qty,
    o.max_total_buy_cost AS o_max_total_buy_cost,
    o.snapshot_ladder_json AS o_snapshot_ladder_json,
    o.alert_seen_at AS o_alert_seen_at,
    o.client_checked_at AS o_client_checked_at,
    o.client_fillable_qty AS o_client_fillable_qty,
    o.client_total_cost AS o_client_total_cost,
    o.outcome_reason AS o_outcome_reason
FROM review_windows AS w
LEFT JOIN opportunity_observations AS o ON o.window_id = w.window_id
ORDER BY w.pair_block_id, w.planned_start_at, w.window_id, o.obs_id
"""


@dataclass(frozen=True, slots=True)
class TrialProtocol:
    """Immutable protocol values; defaults match the frozen YAML."""

    protocol_version: str = "v2.0-stage2-frozen"
    quote_ttl_sec: int = 1200
    min_history_coverage_hours: float = 168.0
    shock_zscore_threshold: float = -2.0
    min_paired_blocks: int = 6
    min_mean_opportunity_gain_per_window: float = 0.5
    bootstrap_confidence_level: float = 0.90
    bootstrap_iterations: int = 2000
    bootstrap_seed: int = 20261126
    min_client_confirmation_rate: float = 0.50
    max_expired_or_missed_rate: float = 0.35

    def __post_init__(self) -> None:
        _text(self.protocol_version, "protocol_version")
        for name in (
            "quote_ttl_sec",
            "min_paired_blocks",
            "bootstrap_iterations",
        ):
            _integer(getattr(self, name), name, minimum=1)
        _integer(self.bootstrap_seed, "bootstrap_seed")
        for name in (
            "min_history_coverage_hours",
            "shock_zscore_threshold",
            "min_mean_opportunity_gain_per_window",
            "bootstrap_confidence_level",
            "min_client_confirmation_rate",
            "max_expired_or_missed_rate",
        ):
            if _optional_number(getattr(self, name)) is None:
                raise ValueError(f"{name} must be finite")
        if self.min_history_coverage_hours < 0:
            raise ValueError("min_history_coverage_hours must be nonnegative")
        if self.min_mean_opportunity_gain_per_window < 0:
            raise ValueError("minimum mean gain must be nonnegative")
        if not 0 < self.bootstrap_confidence_level < 1:
            raise ValueError("bootstrap_confidence_level must be between 0 and 1")
        if not 0 <= self.min_client_confirmation_rate <= 1:
            raise ValueError("min_client_confirmation_rate must be between 0 and 1")
        if not 0 <= self.max_expired_or_missed_rate <= 1:
            raise ValueError("max_expired_or_missed_rate must be between 0 and 1")


def load_trial_protocol(path: Path | str | None = None) -> TrialProtocol:
    """Read the complete frozen UTF-8 YAML without silently missing a field."""
    protocol_path = (
        Path(path)
        if path is not None
        else Path(__file__).resolve().parents[3] / "config" / "trial_protocol.yaml"
    )
    data = yaml.safe_load(protocol_path.read_text(encoding="utf-8"))
    expected = {field.name for field in fields(TrialProtocol)}
    if not isinstance(data, dict) or set(data) != expected:
        raise ValueError("Trial protocol YAML must contain exactly the protocol keys")
    return TrialProtocol(**data)


def _protocol(
    value: TrialProtocol | Mapping[str, Any] | None,
) -> TrialProtocol:
    if value is None:
        return load_trial_protocol()
    if isinstance(value, TrialProtocol):
        return value
    return TrialProtocol(**dict(value))


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _optional_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _true(value: Any) -> bool:
    """Accept booleans and SQLite's integer boolean representation only."""
    return value is True or (type(value) is int and value == 1)


@contextmanager
def _write_scope(conn: sqlite3.Connection) -> Iterator[None]:
    """Preserve caller transactions while using BEGIN IMMEDIATE otherwise."""
    if not conn.in_transaction:
        with write_transaction(conn):
            yield
        return

    conn.execute("SAVEPOINT trial_report_scope")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK TO SAVEPOINT trial_report_scope")
        conn.execute("RELEASE SAVEPOINT trial_report_scope")
        raise
    else:
        conn.execute("RELEASE SAVEPOINT trial_report_scope")


def ensure_trial_tables(conn: sqlite3.Connection) -> None:
    """Create only the two standalone STRICT trial-table contracts.

    Existing migration-created tables and connection pragmas are left intact.
    Individual execute calls avoid executescript's implicit transaction commit.
    """
    with _write_scope(conn):
        conn.execute(REVIEW_WINDOWS_DDL)
        conn.execute(OPPORTUNITY_OBSERVATIONS_DDL)


def _slot(value: Sequence[int], name: str) -> tuple[int, int]:
    if len(value) != 2:
        raise ValueError(f"{name} must contain start and end timestamps")
    start = _integer(value[0], f"{name}.start")
    end = _integer(value[1], f"{name}.end")
    if end <= start:
        raise ValueError(f"{name} must end after it starts")
    return start, end


def register_paired_window_block(
    conn: sqlite3.Connection,
    pair_block_id: str,
    slot_a: Sequence[int],
    slot_b: Sequence[int],
    planned_review_min: float,
    item_pool_version: str,
    rule_version: str,
    seed: int,
    *,
    registered_at: int | None = None,
) -> list[dict[str, Any]]:
    """Atomically register two comparable slots before either slot begins.

    Slots are ``(start_at, end_at)`` Unix-second pairs. They must be equally
    long and nonoverlapping, with enough time for the common review budget.
    The explicit clock argument supports deterministic replay and testing;
    ordinary callers should use the current-time default.

    Mixing the block identity into the seed avoids assigning every block in
    the same slot order when a trial uses a common randomization seed.
    Existing block assignments can never be replaced by this function.
    """
    block_id = _text(pair_block_id, "pair_block_id")
    pool_version = _text(item_pool_version, "item_pool_version")
    frozen_rule = _text(rule_version, "rule_version")
    random_seed = _integer(seed, "seed")
    first = _slot(slot_a, "slot_a")
    second = _slot(slot_b, "slot_b")
    review_min = _optional_number(planned_review_min)
    if review_min is None or review_min <= 0:
        raise ValueError("planned_review_min must be positive and finite")
    if first[1] - first[0] != second[1] - second[0]:
        raise ValueError("Paired slots must have equal duration")
    if max(first[0], second[0]) < min(first[1], second[1]):
        raise ValueError("Paired manual review slots must not overlap")
    if review_min * 60 > first[1] - first[0]:
        raise ValueError("The review budget exceeds the available slot duration")
    now = _integer(
        int(time.time()) if registered_at is None else registered_at,
        "registered_at",
    )
    if now >= min(first[0], second[0]):
        raise ValueError("A paired block must be registered before it begins")

    arms = list(MAIN_ARMS)
    random.Random(f"paired-window:{random_seed}:{block_id}").shuffle(arms)
    windows = [
        {
            "window_id": f"{block_id}:{suffix}",
            "pair_block_id": block_id,
            "planned_start_at": slot[0],
            "planned_end_at": slot[1],
            "planned_review_min": review_min,
            "item_pool_version": pool_version,
            "rule_version": frozen_rule,
            "assigned_arm": arm,
            "window_status": None,
            "actual_start_at": None,
            "actual_end_at": None,
            "actual_review_min": None,
            "miss_or_abort_reason": None,
        }
        for suffix, slot, arm in zip(
            ("a", "b"), (first, second), arms, strict=True
        )
    ]
    with _write_scope(conn):
        exists = conn.execute(
            "SELECT window_id FROM review_windows WHERE pair_block_id = ? LIMIT 1",
            (block_id,),
        ).fetchone()
        if exists is not None:
            raise ValueError(f"Paired block is already registered: {block_id}")
        conn.executemany(
            """
            INSERT INTO review_windows (
                window_id, pair_block_id, planned_start_at, planned_end_at,
                planned_review_min, item_pool_version, rule_version,
                assigned_arm, window_status, actual_start_at, actual_end_at,
                actual_review_min, miss_or_abort_reason
            ) VALUES (
                :window_id, :pair_block_id, :planned_start_at, :planned_end_at,
                :planned_review_min, :item_pool_version, :rule_version,
                :assigned_arm, :window_status, :actual_start_at, :actual_end_at,
                :actual_review_min, :miss_or_abort_reason
            )
            """,
            windows,
        )
    return windows


def _dict_rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [column[0] for column in cursor.description or ()]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _normalize_candidate(
    raw: Mapping[str, Any],
    window: Mapping[str, Any],
    protocol: TrialProtocol,
) -> dict[str, Any]:
    record = dict(raw)
    available = _integer(raw.get("available_at"), "available_at")
    collect_start = _integer(
        raw.get("collect_start_at", available), "collect_start_at"
    )
    if collect_start > available:
        raise ValueError("collect_start_at cannot be after available_at")
    expiry = available + protocol.quote_ttl_sec
    explicit_expiry = raw.get("quote_expires_at")
    if explicit_expiry is not None:
        expiry = min(expiry, _integer(explicit_expiry, "quote_expires_at"))

    rule = _text(raw.get("rule_version", window["rule_version"]), "rule_version")
    if rule != window["rule_version"]:
        raise ValueError("Candidate rule_version differs from the frozen window")
    fee_version = raw.get("fee_rule_version")
    if fee_version is None and not _true(raw.get("fee_verified")):
        fee_version = "unknown"
    fee_version = _text(fee_version, "fee_rule_version")

    ladder = raw.get("snapshot_ladder_json", raw.get("snapshot_ladder", []))
    if isinstance(ladder, str):
        ladder = json.loads(ladder)
    ladder_json = json.dumps(
        ladder, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    raw_metrics = raw.get("flow_metrics")
    metrics = dict(raw_metrics) if isinstance(raw_metrics, Mapping) else {}
    metrics["volume"] = raw.get("volume", metrics.get("volume"))
    metrics["flow"] = raw.get("flow", metrics.get("flow"))
    flow_metrics = {
        str(name): _optional_number(value) for name, value in metrics.items()
    }

    coverage = _optional_number(raw.get("history_coverage_hours"))
    zscore = _optional_number(raw.get("price_zscore"))
    planned_cost = _integer(raw.get("planned_buy_cost"), "planned_buy_cost")
    ceiling = _integer(raw.get("max_total_buy_cost"), "max_total_buy_cost")
    record.update(
        window_id=window["window_id"],
        opportunity_group_id=_text(
            raw.get("opportunity_group_id"), "opportunity_group_id"
        ),
        arm=None,
        rule_version=rule,
        fee_rule_version=fee_version,
        source_snapshot_id=_integer(
            raw.get("source_snapshot_id"), "source_snapshot_id"
        ),
        read_publish_seq=_integer(raw.get("read_publish_seq"), "read_publish_seq"),
        item_id=_integer(raw.get("item_id"), "item_id", minimum=1),
        collect_start_at=collect_start,
        available_at=available,
        quote_expires_at=expiry,
        rank_order=None,
        planned_qty=_integer(raw.get("planned_qty"), "planned_qty", minimum=1),
        planned_buy_cost=planned_cost,
        max_total_buy_cost=ceiling,
        snapshot_ladder_json=ladder_json,
        history_coverage_hours=coverage,
        price_zscore=zscore,
        flow_metrics=flow_metrics,
        shock_qualified=(
            coverage is not None
            and coverage >= protocol.min_history_coverage_hours
            and zscore is not None
            and zscore <= protocol.shock_zscore_threshold
            and _true(raw.get("depth_sufficient"))
        ),
        discount_ratio=(
            float(Fraction(ceiling - planned_cost, ceiling))
            if ceiling > 0
            else None
        ),
        actionable=False,
        alert_seen_at=None,
        client_checked_at=None,
        client_fillable_qty=None,
        client_total_cost=None,
        outcome_reason=None,
    )
    return record


def _frame_key(record: Mapping[str, Any]) -> tuple[int, int, int]:
    """Use nominal collection order, not the arrival order of delayed frames."""
    return (
        record["collect_start_at"],
        record["source_snapshot_id"],
        record["read_publish_seq"],
    )


def _rejection_reason(
    record: Mapping[str, Any],
    decision_ts: int,
    latest_frames: Mapping[int, tuple[int, int, int]],
) -> str | None:
    if not _true(record.get("fee_verified")):
        return "FEE_UNKNOWN"
    if record.get("book_state") != "stocked":
        return "BOOK_NOT_STOCKED"
    if not _true(record.get("frame_complete", True)):
        return "INCOMPLETE_FRAME"
    quantity = record.get("total_listed_qty")
    if quantity is not None:
        number = _optional_number(quantity)
        if number is None or number <= 0:
            return "BOOK_NOT_STOCKED"
    if record["available_at"] > decision_ts:
        return "NOT_YET_AVAILABLE"
    if decision_ts > record["quote_expires_at"]:
        return "QUOTE_EXPIRED"
    if _frame_key(record) != latest_frames.get(record["item_id"]):
        return "SUPERSEDED_FRAME"
    if record["planned_buy_cost"] <= 0 or record["max_total_buy_cost"] <= 0:
        return "INVALID_LOT"
    if not _true(record.get("lot_feasible", True)):
        return "LOT_INFEASIBLE"
    if record["planned_buy_cost"] > record["max_total_buy_cost"]:
        return "COST_ABOVE_LIMIT"
    return None


def _simple_key(record: Mapping[str, Any]) -> tuple[Fraction, int, str]:
    # Exact integer ratios avoid unstable ordering for large monetary amounts.
    discount = Fraction(
        record["max_total_buy_cost"] - record["planned_buy_cost"],
        record["max_total_buy_cost"],
    )
    return -discount, record["item_id"], record["opportunity_group_id"]


def _persist_observation(
    conn: sqlite3.Connection, record: dict[str, Any]
) -> dict[str, Any]:
    result = dict(record)
    result["obs_id"] = conn.execute(
        OBSERVATION_INSERT_SQL, result
    ).lastrowid
    return result


def rank_window_candidates(
    conn: sqlite3.Connection,
    window_id: str,
    candidates: Iterable[Mapping[str, Any]],
    decision_ts: int,
    *,
    protocol: TrialProtocol | Mapping[str, Any] | None = None,
    audit_sample_size: int = 0,
    no_alert_controls: Iterable[Mapping[str, Any]] = (),
) -> dict[str, list[dict[str, Any]]]:
    """Persist both shadow rankings and return actionable lists separately.

    Required candidate fields are item_id, opportunity_group_id, fee_verified,
    book_state, available_at, planned_qty, planned_buy_cost,
    max_total_buy_cost, source_snapshot_id, read_publish_seq, and, for verified
    fees, fee_rule_version. Optional quote_expires_at can shorten but never
    extend the frozen TTL. Missing history/depth/flow evidence never qualifies
    an item as a shock.

    Inputs may contain multiple frames for an item. Only its latest frame
    visible at decision_ts can be actionable; an empty latest frame blocks
    an older stocked frame. Rejections remain raw observations for both arms
    with rank_order NULL, including FEE_UNKNOWN.

    AUDIT_SAMPLE contains the assigned ranking's lowest selected ranks.
    Explicit NO_ALERT_CONTROL records are diagnostic and never actionable.
    No function here emits an alert or supplies client confirmation.
    """
    settings = _protocol(protocol)
    timestamp = _integer(decision_ts, "decision_ts")
    sample_size = _integer(audit_sample_size, "audit_sample_size")
    result: dict[str, list[dict[str, Any]]] = {
        "SIMPLE_LIMIT": [],
        "B_SHOCK": [],
        "AUDIT_SAMPLE": [],
        "NO_ALERT_CONTROL": [],
        "rejected": [],
    }
    with _write_scope(conn):
        windows = _dict_rows(
            conn.execute(
                """
                SELECT window_id, assigned_arm, rule_version
                FROM review_windows WHERE window_id = ?
                """,
                (_text(window_id, "window_id"),),
            )
        )
        if not windows:
            raise ValueError(f"Unknown review window: {window_id}")
        window = windows[0]
        normalized = [
            _normalize_candidate(candidate, window, settings)
            for candidate in candidates
        ]
        controls = [
            _normalize_candidate(candidate, window, settings)
            for candidate in no_alert_controls
        ]
        latest_frames: dict[int, tuple[int, int, int]] = {}
        for record in normalized + controls:
            if record["available_at"] <= timestamp:
                item_id = record["item_id"]
                key = _frame_key(record)
                if item_id not in latest_frames or key > latest_frames[item_id]:
                    latest_frames[item_id] = key

        eligible = []
        for record in normalized:
            reason = _rejection_reason(record, timestamp, latest_frames)
            if reason is None:
                eligible.append(record)
                continue
            rejected = dict(record, outcome_reason=reason)
            observation_ids = {}
            for arm in MAIN_ARMS:
                stored = _persist_observation(conn, dict(rejected, arm=arm))
                observation_ids[arm] = stored["obs_id"]
            rejected["observation_ids"] = observation_ids
            result["rejected"].append(rejected)

        rankings = {
            "SIMPLE_LIMIT": sorted(eligible, key=_simple_key),
            "B_SHOCK": sorted(
                eligible,
                key=lambda record: (
                    not record["shock_qualified"],
                    _simple_key(record),
                ),
            ),
        }
        for arm, ranking in rankings.items():
            for rank, record in enumerate(ranking, start=1):
                result[arm].append(
                    _persist_observation(
                        conn,
                        dict(record, arm=arm, rank_order=rank, actionable=True),
                    )
                )

        if sample_size:
            assigned_ranking = result[window["assigned_arm"]]
            for record in assigned_ranking[-sample_size:]:
                result["AUDIT_SAMPLE"].append(
                    _persist_observation(
                        conn,
                        dict(
                            record,
                            arm="AUDIT_SAMPLE",
                            actionable=False,
                            source_arm=window["assigned_arm"],
                            source_rank_order=record["rank_order"],
                        ),
                    )
                )
        for rank, record in enumerate(controls, start=1):
            reason = _rejection_reason(record, timestamp, latest_frames)
            result["NO_ALERT_CONTROL"].append(
                _persist_observation(
                    conn,
                    dict(
                        record,
                        arm="NO_ALERT_CONTROL",
                        rank_order=rank if reason is None else None,
                        outcome_reason=reason,
                        actionable=False,
                    ),
                )
            )
    return result


def _is_confirmed(row: Mapping[str, Any]) -> bool:
    planned = row["planned_qty"]
    fillable = row["client_fillable_qty"]
    ceiling = row["max_total_buy_cost"]
    cost = row["client_total_cost"]
    return (
        row["outcome_reason"] == "CONFIRMED_OK"
        and planned is not None
        and planned > 0
        and fillable is not None
        and fillable >= planned
        and ceiling is not None
        and cost is not None
        and 0 <= cost <= ceiling
    )


def _is_exposure(rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(
        row["rank_order"] is not None
        or row["outcome_reason"] == "CONFIRMED_OK"
        or (
            row["outcome_reason"] in EXPIRED_REASONS | MISSED_REASONS
            and row["outcome_reason"] not in REJECTION_REASONS
        )
        for row in rows
    )


def _alert_delay(rows: Sequence[Mapping[str, Any]]) -> int | None:
    alerts = [
        row["alert_seen_at"]
        for row in rows
        if row["alert_seen_at"] is not None
    ]
    if not alerts:
        return None
    first_alert = min(alerts)
    checks = [
        row["client_checked_at"]
        for row in rows
        if row["client_checked_at"] is not None
        and row["client_checked_at"] >= first_alert
    ]
    return min(checks) - first_alert if checks else None


def _window_summary(
    window: Mapping[str, Any], observations: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    assigned = [
        row for row in observations if row["arm"] == window["assigned_arm"]
    ]
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in assigned:
        groups[row["opportunity_group_id"]].append(row)
    confirmed = {
        group_id
        for group_id, rows in groups.items()
        if any(_is_confirmed(row) for row in rows)
    }
    status = window["window_status"]
    forced_zero = status in {"ZERO_OPPORTUNITY", "MISSED_BY_USER"}
    summary = dict(window)
    summary.update(
        Y=(
            len(confirmed)
            if status in FINAL_STATUSES and not forced_zero
            else 0
        ),
        included_in_pair=False,
        assigned_arm_raw_rows=len(assigned),
        expired_count=0,
        missed_count=0,
        expired_or_missed_count=0,
        expired_or_missed_denominator=0,
        _delays=[],
    )
    if status not in FINAL_STATUSES or status == "ZERO_OPPORTUNITY":
        return summary

    for group_id, rows in groups.items():
        if not _is_exposure(rows):
            continue
        summary["expired_or_missed_denominator"] += 1
        delay = _alert_delay(rows)
        if delay is not None:
            summary["_delays"].append(delay)
        if status == "MISSED_BY_USER":
            summary["missed_count"] += 1
            continue
        if group_id in confirmed:
            continue
        expired = any(
            row["outcome_reason"] in EXPIRED_REASONS
            or (
                row["client_checked_at"] is not None
                and row["quote_expires_at"] is not None
                and row["client_checked_at"] > row["quote_expires_at"]
            )
            for row in rows
        )
        missed = any(
            row["outcome_reason"] in MISSED_REASONS for row in rows
        ) or all(row["client_checked_at"] is None for row in rows)
        if expired:
            summary["expired_count"] += 1
        elif missed:
            summary["missed_count"] += 1

    if (
        status in {"MISSED_BY_USER", "ABORTED"}
        and summary["expired_or_missed_denominator"] == 0
    ):
        summary["expired_or_missed_denominator"] = 1
        summary["missed_count"] = 1
    summary["expired_or_missed_count"] = (
        summary["expired_count"] + summary["missed_count"]
    )
    return summary


def _percentile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] + weight * (ordered[upper] - ordered[lower])


def _bootstrap_lower_bound(
    differences: Sequence[int], protocol: TrialProtocol
) -> float | None:
    if not differences:
        return None
    rng = random.Random(protocol.bootstrap_seed)
    size = len(differences)
    bootstrap_means = [
        sum(rng.choice(differences) for _ in range(size)) / size
        for _ in range(protocol.bootstrap_iterations)
    ]
    return _percentile(
        bootstrap_means, (1 - protocol.bootstrap_confidence_level) / 2
    )


def _review_minutes(windows: Sequence[Mapping[str, Any]]) -> float | None:
    known = [
        window["actual_review_min"]
        for window in windows
        if window["actual_review_min"] is not None
    ]
    return float(sum(known)) if known else None


def _diagnostic(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    confirmed = {
        (row["window_id"], row["opportunity_group_id"])
        for row in rows
        if _is_confirmed(row)
    }
    return {
        "raw_rows": len(rows),
        "confirmed_opportunities": len(confirmed),
        "expired_rows": sum(
            row["outcome_reason"] in EXPIRED_REASONS for row in rows
        ),
        "missed_rows": sum(
            row["outcome_reason"] in MISSED_REASONS for row in rows
        ),
    }


def compute_stage2_trial_report(
    conn: sqlite3.Connection,
    *,
    protocol: TrialProtocol | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Read one LEFT JOIN snapshot and compute the intention-to-treat gate.

    ``window_count`` and ``windows`` include every registered window.
    ``K`` counts comparable blocks with two terminal windows, one per arm.
    Primary Y uses only the assigned arm's distinct confirmed groups.

    ``per_arm`` exposes both all-window and paired-window counts and actual
    minutes. Gate rates and delay medians use only assigned observations in
    completed paired blocks. Both gate rates share the assigned-arm exposure
    denominator. Missing actual minutes and delays remain None.
    The report has no wall-clock field, making identical reads reproducible.
    """
    settings = _protocol(protocol)
    window_data: dict[str, dict[str, Any]] = {}
    observation_data: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for joined in _dict_rows(conn.execute(TRIAL_QUERY)):
        window = {
            key[2:]: value
            for key, value in joined.items()
            if key.startswith("w_")
        }
        window_id = window["window_id"]
        window_data[window_id] = window
        if joined["o_obs_id"] is not None:
            observation_data[window_id].append(
                {
                    key[2:]: value
                    for key, value in joined.items()
                    if key.startswith("o_")
                }
            )

    windows = [
        _window_summary(window, observation_data[window_id])
        for window_id, window in window_data.items()
    ]
    blocks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for window in windows:
        blocks[window["pair_block_id"]].append(window)

    pairs = []
    excluded = []
    for block_id, block_windows in sorted(blocks.items()):
        reason = None
        if len(block_windows) != 2:
            reason = "expected_two_windows"
        elif {w["assigned_arm"] for w in block_windows} != set(MAIN_ARMS):
            reason = "expected_one_window_per_arm"
        elif any(w["window_status"] not in FINAL_STATUSES for w in block_windows):
            reason = "unfinished_windows"
        elif len(
            {
                (
                    w["item_pool_version"],
                    w["rule_version"],
                    w["planned_review_min"],
                    w["planned_end_at"] - w["planned_start_at"],
                )
                for w in block_windows
            }
        ) != 1:
            reason = "incomparable_registered_windows"
        if reason is not None:
            excluded.append({"pair_block_id": block_id, "reason": reason})
            continue
        by_arm = {window["assigned_arm"]: window for window in block_windows}
        for window in block_windows:
            window["included_in_pair"] = True
        b_window = by_arm["B_SHOCK"]
        simple_window = by_arm["SIMPLE_LIMIT"]
        pairs.append(
            {
                "pair_block_id": block_id,
                "b_window_id": b_window["window_id"],
                "simple_window_id": simple_window["window_id"],
                "Y_B": b_window["Y"],
                "Y_S": simple_window["Y"],
                "d_k": b_window["Y"] - simple_window["Y"],
            }
        )

    per_arm = {}
    for arm in MAIN_ARMS:
        all_arm_windows = [w for w in windows if w["assigned_arm"] == arm]
        paired_windows = [
            w for w in all_arm_windows if w["included_in_pair"]
        ]
        denominator = sum(
            w["expired_or_missed_denominator"] for w in paired_windows
        )
        confirmed = sum(w["Y"] for w in paired_windows)
        failures = sum(w["expired_or_missed_count"] for w in paired_windows)
        delays = [
            delay for window in paired_windows for delay in window["_delays"]
        ]
        per_arm[arm] = {
            "window_count": len(all_arm_windows),
            "paired_window_count": len(paired_windows),
            "confirmed_opportunities": confirmed,
            "all_window_confirmed_opportunities": sum(
                w["Y"] for w in all_arm_windows
            ),
            "actual_review_min": _review_minutes(all_arm_windows),
            "paired_actual_review_min": _review_minutes(paired_windows),
            "unknown_actual_review_windows": sum(
                w["actual_review_min"] is None for w in all_arm_windows
            ),
            "median_alert_to_check_delay_sec": median(delays) if delays else None,
            "expired_count": sum(w["expired_count"] for w in paired_windows),
            "missed_count": sum(w["missed_count"] for w in paired_windows),
            "expired_or_missed_count": failures,
            "expired_or_missed_denominator": denominator,
            "client_confirmation_rate": (
                confirmed / denominator if denominator > 0 else 0.0
            ),
            "expired_or_missed_rate": failures / denominator if denominator else 0.0,
        }

    differences = [pair["d_k"] for pair in pairs]
    mean_gain = mean(differences) if differences else None
    lower_bound = _bootstrap_lower_bound(differences, settings)
    b_confirmation_rate = per_arm["B_SHOCK"]["client_confirmation_rate"]
    b_rate = per_arm["B_SHOCK"]["expired_or_missed_rate"]
    checks = {
        "minimum_paired_blocks": len(pairs) >= settings.min_paired_blocks,
        "minimum_mean_gain": (
            mean_gain is not None
            and mean_gain >= settings.min_mean_opportunity_gain_per_window
        ),
        "positive_bootstrap_lower_bound": (
            lower_bound is not None and lower_bound > 0
        ),
        "client_confirmation_rate_meets_minimum": (
            b_confirmation_rate >= settings.min_client_confirmation_rate
        ),
        "expired_or_missed_rate_within_limit": (
            b_rate <= settings.max_expired_or_missed_rate
        ),
    }
    raw_rows = [
        row for rows in observation_data.values() for row in rows
    ]
    unassigned_rows = [
        row
        for window in windows
        for row in observation_data[window["window_id"]]
        if row["arm"] in MAIN_ARMS and row["arm"] != window["assigned_arm"]
    ]
    status_counts = Counter(
        window["window_status"] or "REGISTERED" for window in windows
    )
    diagnostics = {
        "raw_observation_rows": len(raw_rows),
        "blocked_observation_rows": sum(
            row["rank_order"] is None
            and row["outcome_reason"] in REJECTION_REASONS
            for row in raw_rows
        ),
        "window_status_counts": dict(sorted(status_counts.items())),
        "incomplete_or_invalid_pairs": excluded,
        "AUDIT_SAMPLE": _diagnostic(
            [row for row in raw_rows if row["arm"] == "AUDIT_SAMPLE"]
        ),
        "NO_ALERT_CONTROL": _diagnostic(
            [row for row in raw_rows if row["arm"] == "NO_ALERT_CONTROL"]
        ),
        "unassigned_arm": _diagnostic(unassigned_rows),
    }
    return {
        "protocol_version": settings.protocol_version,
        "protocol": asdict(settings),
        "window_count": len(windows),
        "paired_window_count": 2 * len(pairs),
        "K": len(pairs),
        "paired_blocks": pairs,
        "paired_differences": differences,
        "mean_gain": mean_gain,
        "bootstrap_lower_bound": lower_bound,
        "b_client_confirmation_rate": b_confirmation_rate,
        "b_expired_or_missed_rate": b_rate,
        "b_passes_gate": all(checks.values()),
        "gate_checks": checks,
        "per_arm": per_arm,
        "windows": [
            {key: value for key, value in window.items() if key != "_delays"}
            for window in windows
        ],
        "diagnostics": diagnostics,
    }
