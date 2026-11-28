"""Versioned fee rules, discrete action charges, and exact batch breakevens."""

from decimal import Decimal

import pytest
import yaml

from pokemmo_gtl.config import FeeRuleVersion, Fees, load_config
from pokemmo_gtl.quant.fees import (
    breakeven_sell_price,
    breakeven_sell_price_for_cost,
    direct_edit_fee,
    discrete_total_fees,
    effective_fee,
    listing_fee,
    net_profit,
    net_profit_for_cost,
    relist_fee,
)

_LEGACY_FEES = {
    "rate": "0.05",
    "item_min_fee": 100,
    "pokemon_min_fee": 1000,
    "max_fee_per_slot": 25_000,
    "default_relist_rate": "0.15",
}


@pytest.mark.parametrize(
    ("fee_basis", "expected_fee", "expected_profit"),
    [
        ("per_slot", 25_000, 14_600),
        ("per_unit", 49_500, -9_900),
    ],
)
def test_codex_audit_fee_basis_flips_profit(
    fee_basis, expected_fee, expected_profit
):
    total_buy_cost = 99 * 9_600
    assert total_buy_cost == 950_400
    assert 10_000 * 99 - total_buy_cost == 39_600
    assert listing_fee(10_000, 99, 99, fee_basis=fee_basis) == expected_fee
    assert net_profit_for_cost(
        total_buy_cost,
        10_000,
        99,
        99,
        discrete_relists=0,
        fee_basis=fee_basis,
    ) == Decimal(expected_profit)


@pytest.mark.parametrize(
    ("fee_basis", "expected_price"),
    [("per_slot", 10_243), ("per_unit", 10_515)],
)
def test_non_divisible_batch_cost_has_exact_breakeven(fee_basis, expected_price):
    total_buy_cost = 1 * 9_000 + 98 * 10_000
    assert total_buy_cost == 989_000
    assert total_buy_cost % 99 != 0
    price = breakeven_sell_price_for_cost(
        total_buy_cost,
        99,
        99,
        discrete_relists=0,
        fee_basis=fee_basis,
    )
    assert price == expected_price
    assert net_profit_for_cost(
        total_buy_cost, price, 99, 99, fee_basis=fee_basis
    ) >= 0
    assert net_profit_for_cost(
        total_buy_cost, price - 1, 99, 99, fee_basis=fee_basis
    ) < 0


@pytest.mark.parametrize("fee_basis", ["per_slot", "per_unit"])
@pytest.mark.parametrize("rounding_mode", ["floor", "half_up", "ceil"])
def test_legacy_wrappers_forward_new_fee_options(fee_basis, rounding_mode):
    options = {
        "fee_basis": fee_basis,
        "rounding_mode": rounding_mode,
    }
    assert net_profit(9_600, 10_000, 99, 99, **options) == net_profit_for_cost(
        950_400,
        10_000,
        99,
        99,
        relist_rate="0.15",
        **options,
    )
    assert breakeven_sell_price(
        9_600, 99, 99, **options
    ) == breakeven_sell_price_for_cost(
        950_400,
        99,
        99,
        relist_rate="0.15",
        **options,
    )
    assert effective_fee(10_000, 99, 99, **options) == Decimal(
        listing_fee(10_000, 99, 99, **options)
    ) * Decimal("1.15")


@pytest.mark.parametrize(
    ("price", "rounding_mode", "expected"),
    [
        (1, "floor", 0),
        (1, "half_up", 0),
        (1, "ceil", 1),
        (10, "floor", 0),
        (10, "half_up", 1),
        (10, "ceil", 1),
    ],
)
def test_rounding_modes_use_exact_decimal_values(price, rounding_mode, expected):
    assert listing_fee(
        price,
        1,
        99,
        min_fee=0,
        rate=Decimal("0.05"),
        rounding_mode=rounding_mode,
    ) == expected


@pytest.mark.parametrize(
    ("rounding_mode", "expected"),
    [("floor", 1_260), ("half_up", 1_263), ("ceil", 1_263)],
)
def test_each_occupied_slot_is_rounded_independently(rounding_mode, expected):
    assert listing_fee(
        101, 250, 99, rounding_mode=rounding_mode
    ) == expected


def test_per_unit_minimum_and_cap_apply_to_each_unit():
    assert listing_fee(1, 250, 99, fee_basis="per_unit") == 25_000
    assert listing_fee(
        1_000_000, 250, 99, fee_basis="per_unit"
    ) == 6_250_000
    assert listing_fee(
        10_000, 250, 1, fee_basis="per_unit"
    ) == listing_fee(10_000, 250, 99, fee_basis="per_unit")


@pytest.mark.parametrize(
    ("mode", "new_price", "remaining_qty", "expected"),
    [
        ("full_relist_fee", 10_000, 99, 25_000),
        ("full_relist_fee", 10_000, 40, 20_000),
        ("PositiveDiffOnly", 11_000, 40, 2_000),
        ("PositiveDiffOnly", 9_000, 40, 100),
        ("PositiveDiffOnly", 10_000, 40, 100),
        ("flat_min_fee", 11_000, 250, 300),
        ("free", 11_000, 250, 0),
    ],
)
def test_direct_edit_modes_and_partial_sale_quantities(
    mode, new_price, remaining_qty, expected
):
    assert direct_edit_fee(
        10_000,
        new_price,
        remaining_qty,
        99,
        mode=mode,
    ) == expected


@pytest.mark.parametrize("new_price", [9_000, 10_000])
def test_positive_difference_non_increase_can_be_free(new_price):
    assert direct_edit_fee(
        10_000,
        new_price,
        40,
        99,
        mode="PositiveDiffOnly",
        min_fee=0,
    ) == 0


def test_direct_edit_per_unit_rules_use_remaining_units():
    assert direct_edit_fee(
        10_000,
        11_000,
        40,
        99,
        mode="PositiveDiffOnly",
        fee_basis="per_unit",
    ) == 4_000
    assert direct_edit_fee(
        10_000,
        9_000,
        40,
        99,
        mode="flat_min_fee",
        fee_basis="per_unit",
    ) == 4_000


@pytest.mark.parametrize(
    ("remaining_qty", "expected"), [(99, 25_000), (40, 20_000)]
)
def test_relisting_charges_a_fresh_fee_for_remaining_quantity(
    remaining_qty, expected
):
    assert relist_fee(10_000, remaining_qty, 99) == expected
    assert relist_fee(10_000, remaining_qty, 99) == listing_fee(
        10_000, remaining_qty, 99
    )


def test_discrete_total_includes_each_edit_and_relist():
    assert discrete_total_fees(
        10_000,
        40,
        99,
        edit_prices=(9_000, 11_000),
        relist_prices=(8_000,),
    ) == 76_000


def test_positive_difference_edits_use_the_previous_edit_price():
    assert discrete_total_fees(
        10_000,
        40,
        99,
        edit_prices=(9_000, 11_000),
        relist_prices=(8_000,),
        direct_edit_mode="PositiveDiffOnly",
    ) == 40_100


def test_discrete_total_supports_separate_direct_edit_parameters():
    assert discrete_total_fees(
        10_000,
        40,
        99,
        edit_prices=(9_000, 11_000),
        relist_prices=(8_000,),
        direct_edit_mode="PositiveDiffOnly",
        direct_edit_rate="0.10",
        direct_edit_min_fee=1_000,
    ) == 45_000


def test_batch_profit_counts_exact_relists_and_edit_charges():
    assert net_profit_for_cost(
        90_000,
        10_000,
        10,
        99,
        discrete_relists=2,
        edit_prices=(9_000, 8_000),
    ) == Decimal("-13500")


def test_expected_relists_add_to_discrete_relists_without_scaling_edits():
    assert net_profit_for_cost(
        90_000,
        10_000,
        10,
        99,
        discrete_relists=2,
        edit_prices=(9_000, 8_000),
        relist_rate=0.10,
    ) == Decimal("-14000")


@pytest.mark.parametrize("fee_basis", ["per_slot", "per_unit"])
@pytest.mark.parametrize("rounding_mode", ["floor", "half_up", "ceil"])
@pytest.mark.parametrize("qty", [1, 2, 99, 250])
def test_single_five_percent_listing_profit_is_monotone(
    fee_basis, rounding_mode, qty
):
    profits = [
        net_profit_for_cost(
            0,
            price,
            qty,
            99,
            min_fee=3,
            max_fee_per_slot=7,
            fee_basis=fee_basis,
            rounding_mode=rounding_mode,
        )
        for price in range(201)
    ]
    assert all(after >= before for before, after in zip(profits, profits[1:]))


@pytest.mark.parametrize(
    ("discrete_relists", "relist_rate"),
    [(1, None), (0, "0.15")],
)
def test_breakeven_handles_non_monotone_profit_at_rounding_jumps(
    discrete_relists, relist_rate
):
    options = {
        "discrete_relists": discrete_relists,
        "relist_rate": relist_rate,
        "min_fee": 0,
    }
    assert net_profit_for_cost(19, 19, 1, 99, **options) == 0
    assert net_profit_for_cost(19, 20, 1, 99, **options) < 0
    assert breakeven_sell_price_for_cost(19, 1, 99, **options) == 19


@pytest.mark.parametrize("fee_basis", ["per_slot", "per_unit"])
@pytest.mark.parametrize("rounding_mode", ["floor", "half_up", "ceil"])
@pytest.mark.parametrize("discrete_relists", [0, 2])
@pytest.mark.parametrize("qty", [1, 3])
def test_breakeven_matches_exhaustive_search_across_discrete_boundaries(
    fee_basis, rounding_mode, discrete_relists, qty
):
    options = {
        "fee_basis": fee_basis,
        "rounding_mode": rounding_mode,
        "discrete_relists": discrete_relists,
        "edit_prices": (12, 23),
        "rate": "0.45",
        "min_fee": 2,
        "max_fee_per_slot": 7,
    }
    price = breakeven_sell_price_for_cost(37, qty, 2, **options)
    expected = next(
        candidate
        for candidate in range(price + 1)
        if net_profit_for_cost(37, candidate, qty, 2, **options) >= 0
    )
    assert price == expected
    assert net_profit_for_cost(37, price, qty, 2, **options) >= 0
    if price > 0:
        assert net_profit_for_cost(37, price - 1, qty, 2, **options) < 0


@pytest.mark.parametrize("rate", ["1", "1.25"])
def test_finite_caps_allow_breakeven_even_for_large_rates(rate):
    assert breakeven_sell_price_for_cost(
        1,
        1,
        99,
        min_fee=0,
        rate=rate,
        max_fee_per_slot=10,
    ) == 11


def test_zero_cost_and_zero_fees_have_zero_breakeven():
    assert breakeven_sell_price_for_cost(
        0, 99, 99, min_fee=0, rate="0"
    ) == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("total_buy_cost", -1),
        ("total_buy_cost", True),
        ("total_buy_cost", 1.5),
        ("sell_price", -1),
        ("sell_price", 1.5),
        ("qty", 0),
        ("qty", True),
        ("max_stack_size", 0),
        ("discrete_relists", -1),
        ("discrete_relists", 0.5),
        ("fee_basis", "per_lot"),
        ("rounding_mode", "bankers"),
        ("rate", "-0.05"),
        ("relist_rate", "-0.1"),
        ("relist_rate", "NaN"),
    ],
)
def test_batch_profit_rejects_invalid_inputs(field, value):
    arguments = {
        "total_buy_cost": 1,
        "sell_price": 2,
        "qty": 1,
        "max_stack_size": 99,
    }
    arguments[field] = value
    with pytest.raises(ValueError):
        net_profit_for_cost(**arguments)


def test_direct_edit_rejects_unknown_modes():
    with pytest.raises(ValueError, match="Unsupported direct edit mode"):
        direct_edit_fee(100, 200, 1, 99, mode="unknown")


def test_legacy_five_key_config_has_safe_unverified_defaults():
    fees = Fees.model_validate(_LEGACY_FEES)
    assert fees.verified is False
    assert fees.fee_basis == "per_slot"
    assert fees.rounding_mode == "floor"
    assert fees.rules_schedule == []

    rule = fees.resolve_at(123)
    assert rule.version_id == "legacy"
    assert rule.verified is False
    assert rule.rate == Decimal("0.05")
    assert rule.item_min_fee == 100
    assert rule.pokemon_min_fee == 1_000
    assert rule.max_fee == 25_000


def test_synthesized_rule_preserves_explicit_top_level_options():
    fees = Fees.model_validate(
        {
            **_LEGACY_FEES,
            "verified": True,
            "fee_basis": "per_unit",
            "rounding_mode": "ceil",
            "rate": "0.04",
        }
    )
    rule = fees.resolve_at(123)
    assert rule.verified is True
    assert rule.fee_basis == "per_unit"
    assert rule.rounding_mode == "ceil"
    assert rule.rate == Decimal("0.04")
    assert rule.direct_edit_rate == Decimal("0.04")


def test_fee_rule_version_defaults_cover_all_action_fields():
    rule = FeeRuleVersion(version_id="unverified-v1", effective_from=100)
    assert rule.verified is False
    assert rule.fee_basis == "per_slot"
    assert rule.rate == Decimal("0.05")
    assert rule.item_min_fee == 100
    assert rule.pokemon_min_fee == 1_000
    assert rule.max_fee == 25_000
    assert rule.rounding_mode == "floor"
    assert rule.direct_edit_mode == "full_relist_fee"
    assert rule.direct_edit_rate == Decimal("0.05")
    assert rule.direct_edit_min_fee == 100
    assert rule.partial_edit_uses_remaining_qty is True
    assert rule.pending_cancel_refund_rate == Decimal("0")


@pytest.fixture
def scheduled_fees():
    return Fees.model_validate(
        {
            **_LEGACY_FEES,
            "rules_schedule": [
                {
                    "version_id": "v2",
                    "effective_from": 200,
                    "verified": False,
                    "fee_basis": "per_unit",
                },
                {
                    "version_id": "v1",
                    "effective_from": 100,
                    "verified": False,
                    "fee_basis": "per_slot",
                },
            ],
        }
    )


@pytest.mark.parametrize(
    ("timestamp", "version_id", "fee_basis"),
    [
        (100, "v1", "per_slot"),
        (199, "v1", "per_slot"),
        (200, "v2", "per_unit"),
        (999, "v2", "per_unit"),
    ],
)
def test_rule_resolution_uses_effective_time_and_preserves_unverified_flag(
    scheduled_fees, timestamp, version_id, fee_basis
):
    rule = scheduled_fees.resolve_at(timestamp)
    assert rule.version_id == version_id
    assert rule.fee_basis == fee_basis
    assert rule.verified is False
    assert [rule.version_id for rule in scheduled_fees.rules_schedule] == [
        "v2",
        "v1",
    ]


def test_rule_resolution_never_uses_a_future_version(scheduled_fees):
    with pytest.raises(ValueError, match="No fee rule is effective"):
        scheduled_fees.resolve_at(99)


def test_rule_resolution_defaults_to_current_utc_seconds(
    scheduled_fees, monkeypatch
):
    monkeypatch.setattr("pokemmo_gtl.config.time", lambda: 199.9)
    assert scheduled_fees.resolve_at().version_id == "v1"


@pytest.mark.parametrize("timestamp", [True, 100.5, "100"])
def test_rule_resolution_requires_integer_timestamp(scheduled_fees, timestamp):
    with pytest.raises(ValueError, match="integer UTC timestamp"):
        scheduled_fees.resolve_at(timestamp)


def test_fee_schedule_defaults_are_independent():
    first = Fees.model_validate(_LEGACY_FEES)
    second = Fees.model_validate(_LEGACY_FEES)
    first.rules_schedule.append(
        FeeRuleVersion(version_id="v1", effective_from=100)
    )
    assert second.rules_schedule == []


@pytest.fixture
def legacy_config_root(tmp_path, monkeypatch):
    monkeypatch.setenv("POKEMMO_API_TOKEN", "")
    directory = tmp_path / "config"
    directory.mkdir()
    files = {
        "settings.yaml": {
            "poll_interval_sec": 60,
            "rate_limit_tokens": 10,
            "rate_limit_window_sec": 60,
            "retention_hours": 48,
            "expiry_grace_sec": 0,
            "max_diff_gap_sec": 600,
            "alpha_near": 0.6,
            "alpha_mid": 0.3,
            "alpha_far": 0.1,
            "ref_notional_yen": 100_000,
            "simulation_default_end_ts": 1_700_000_000,
        },
        "fees.yaml": _LEGACY_FEES,
        "events.yaml": {"events": []},
        "item_aliases_zh.yaml": {"items": []},
        "item_relations.yaml": {},
    }
    for filename, contents in files.items():
        (directory / filename).write_text(
            yaml.safe_dump(contents, allow_unicode=True),
            encoding="utf-8",
        )
    return tmp_path


def test_load_config_accepts_legacy_fees_without_trial_protocol(
    legacy_config_root,
):
    config = load_config(root=legacy_config_root)
    assert config.trial_protocol == {}
    assert config.fees.verified is False
    assert config.fees.resolve_at(123).verified is False
    assert config.db_path == legacy_config_root / "data" / "gtl_live.db"


def test_load_config_reads_optional_trial_protocol(legacy_config_root):
    protocol = {
        "k_min": 6,
        "delta_min": 1,
        "paired_interval": {"method": "bootstrap", "confidence": "0.90"},
    }
    (legacy_config_root / "config" / "trial_protocol.yaml").write_text(
        yaml.safe_dump(protocol),
        encoding="utf-8",
    )
    assert load_config(root=legacy_config_root).trial_protocol == protocol


def test_missing_trial_protocol_returns_independent_mappings(
    legacy_config_root,
):
    first = load_config(root=legacy_config_root)
    second = load_config(root=legacy_config_root)
    first.trial_protocol["k_min"] = 6
    assert second.trial_protocol == {}
