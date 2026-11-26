"""UTF-8 YAML configuration and explicit live/sim database paths."""

import os
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

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


class Fees(ConfigModel):
    rate: Decimal = Field(ge=0, le=1)
    item_min_fee: int = Field(ge=0)
    pokemon_min_fee: int = Field(ge=0)
    max_fee_per_slot: int = Field(gt=0)
    default_relist_rate: Decimal = Field(ge=0)


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
    return AppConfig(
        root=root,
        env=environment,
        settings=Settings.model_validate(load_yaml(directory / "settings.yaml")),
        fees=Fees.model_validate(load_yaml(directory / "fees.yaml")),
        events=load_yaml(directory / "events.yaml")["events"],
        item_aliases=load_yaml(directory / "item_aliases_zh.yaml")["items"],
        item_relations=load_yaml(directory / "item_relations.yaml"),
        api_token=_load_token(root),
    )
