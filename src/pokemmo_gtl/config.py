"""UTF-8 YAML configuration and explicit live/sim database paths."""

import os
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from time import time
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Environment(StrEnum):
    live = "live"
    sim = "sim"


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Settings(ConfigModel):
    poll_interval_sec: int = Field(gt=0)
    rate_limit_tokens: int = Field(gt=0)
    rate_limit_window_sec: int = Field(gt=0)
    retention_hours: int = Field(gt=0)
    expiry_grace_sec: int = Field(ge=0)
    max_diff_gap_sec: int = Field(gt=0)
    alpha_near: float = Field(ge=0, le=1)
    alpha_mid: float = Field(ge=0, le=1)
    alpha_far: float = Field(ge=0, le=1)
    ref_notional_yen: int = Field(gt=0)
    simulation_default_end_ts: int = Field(gt=0)


class FeeRuleVersion(ConfigModel):
    """A fee schedule effective from an explicit UTC-second timestamp."""

    version_id: str = Field(min_length=1)
    effective_from: int
    verified: bool = False
    fee_basis: Literal["per_slot", "per_unit"] = "per_slot"
    rate: Decimal = Field(default=Decimal("0.05"), ge=0, le=1)
    item_min_fee: int = Field(default=100, ge=0)
    pokemon_min_fee: int = Field(default=1000, ge=0)
    max_fee: int = Field(default=25_000, gt=0)
    rounding_mode: Literal["floor", "half_up", "ceil"] = "floor"
    direct_edit_mode: Literal[
        "full_relist_fee", "PositiveDiffOnly", "flat_min_fee", "free"
    ] = "full_relist_fee"
    direct_edit_rate: Decimal = Field(default=Decimal("0.05"), ge=0, le=1)
    direct_edit_min_fee: int = Field(default=100, ge=0)
    partial_edit_uses_remaining_qty: bool = True
    pending_cancel_refund_rate: Decimal = Field(default=Decimal("0"), ge=0, le=1)


class Fees(ConfigModel):
    rate: Decimal = Field(ge=0, le=1)
    item_min_fee: int = Field(ge=0)
    pokemon_min_fee: int = Field(ge=0)
    max_fee_per_slot: int = Field(gt=0)
    default_relist_rate: Decimal = Field(ge=0)
    verified: bool = False
    fee_basis: Literal["per_slot", "per_unit"] = "per_slot"
    rounding_mode: Literal["floor", "half_up", "ceil"] = "floor"
    rules_schedule: list[FeeRuleVersion] = Field(default_factory=list)

    def resolve_at(self, decision_ts: int | None = None) -> FeeRuleVersion:
        """Resolve the latest effective rule without using future versions.

        With no schedule, synthesize a rule from the legacy top-level fields.
        Equal effective timestamps use the last declared matching version.
        An explicit schedule with no effective version raises instead of
        silently substituting a future rule or the legacy assumptions.
        """
        timestamp = int(time()) if decision_ts is None else decision_ts
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            raise ValueError("decision_ts must be an integer UTC timestamp")

        if not self.rules_schedule:
            return FeeRuleVersion(
                version_id="legacy",
                effective_from=0,
                verified=self.verified,
                fee_basis=self.fee_basis,
                rate=self.rate,
                item_min_fee=self.item_min_fee,
                pokemon_min_fee=self.pokemon_min_fee,
                max_fee=self.max_fee_per_slot,
                rounding_mode=self.rounding_mode,
                direct_edit_rate=self.rate,
                direct_edit_min_fee=self.item_min_fee,
            )

        resolved: FeeRuleVersion | None = None
        for rule in self.rules_schedule:
            if rule.effective_from <= timestamp and (
                resolved is None or rule.effective_from >= resolved.effective_from
            ):
                resolved = rule

        if resolved is None:
            raise ValueError(f"No fee rule is effective at decision_ts={timestamp}")
        return resolved


@dataclass(frozen=True)
class AppConfig:
    root: Path
    env: Environment
    settings: Settings
    fees: Fees
    events: list[dict[str, Any]]
    item_aliases: list[dict[str, Any]]
    item_relations: dict[str, list[dict[str, Any]]]
    api_token: SecretStr | None = field(default=None, repr=False)
    trial_protocol: dict[str, Any] = field(default_factory=dict)

    @property
    def db_path(self) -> Path:
        return self.root / "data" / f"gtl_{self.env.value}.db"


def load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping: {path.name}")
    return data


def _load_token(root: Path) -> SecretStr | None:
    # Environment wins, including an explicitly empty value. Never log file contents.
    token = os.environ.get("POKEMMO_API_TOKEN")
    dotenv = root / ".env"
    if token is None and dotenv.exists():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.strip().partition("=")
            if separator and key.strip() == "POKEMMO_API_TOKEN":
                token = value.strip()
                if len(token) >= 2 and token[0] == token[-1] and token[0] in "\"'":
                    token = token[1:-1]
    return SecretStr(token) if token else None


def load_config(
    env: Environment | str = Environment.live, *, root: Path | None = None
) -> AppConfig:
    environment = Environment(env)
    root = (root if root is not None else PROJECT_ROOT).resolve()
    directory = root / "config"
    trial_protocol_path = directory / "trial_protocol.yaml"
    return AppConfig(
        root=root,
        env=environment,
        settings=Settings.model_validate(load_yaml(directory / "settings.yaml")),
        fees=Fees.model_validate(load_yaml(directory / "fees.yaml")),
        events=load_yaml(directory / "events.yaml")["events"],
        item_aliases=load_yaml(directory / "item_aliases_zh.yaml")["items"],
        item_relations=load_yaml(directory / "item_relations.yaml"),
        api_token=_load_token(root),
        trial_protocol=(
            load_yaml(trial_protocol_path) if trial_protocol_path.exists() else {}
        ),
    )
