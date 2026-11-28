"""REST contracts for architecture §5; app.py will adapt backend data to these.

UTC timestamps are integer seconds. Prices are yen. *_pct values use percentage
points; probability/ROI/backtest returns use ratios (0.05 = 5%).
GET items/events return JSON arrays. Forecast uses {item, forecasts}. Backtest
uses {model_name, metrics} rows and {model_name, points} curves.
BarsQuery.days=0 requests all stored history, including historical SIM data.
CSV bodies are multipart/text, never JSON-wrapped. Origin/Host protection,
streaming CSV limits, formula escaping and the job mutex belong to app.py.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal
from numbers import Integral, Real
from typing import Annotated, Any, Literal, Self

from fastapi import UploadFile
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StrictBool,
    StringConstraints,
    field_validator,
    model_validator,
)

MAX_CSV_BYTES = 5 * 1024 * 1024
MAX_CSV_ROWS = 50_000
CSV_MEDIA_TYPE = "text/csv; charset=utf-8"
PositiveInt = Annotated[int, Field(gt=0, strict=True)]
PositiveId = PositiveInt
PriceInt = PositiveInt
NonNegativeInt = Annotated[int, Field(ge=0, strict=True)]
EpochSeconds = NonNegativeInt
FiniteFloat = Annotated[float, Field(allow_inf_nan=False)]
PositivePrice = Annotated[float, Field(gt=0, allow_inf_nan=False)]
NonNegativeFloat = Annotated[float, Field(ge=0, allow_inf_nan=False)]
Probability = Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]
NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Timeframe = Literal["15m", "4h", "1d"]
Horizon = Literal[24, 72, 168]
Signal = Literal["BUY_WATCH", "NEUTRAL", "SELL_CAUTION"]
Environment = Literal["live", "sim"]
AssetClass = Literal["item", "cosmetic", "berry", "pokemon_category"]
ModelName = Literal["naive", "statistical", "lightgbm"]
AdminAction = Literal["simulate", "collect", "sync_catalog", "forecast"]
JobName = Literal["backtest", "simulate", "collect", "sync_catalog", "forecast"]


def sanitize_non_finite(value: Any) -> Any:
    """Return a cleaned copy, preserving zero, booleans and finite Decimals.

    Supports nested models, mappings, sequences and NumPy arrays/scalars without
    requiring NumPy. Finite Decimals retain precision for Pydantic/FastAPI's
    JSON encoder. Never expose the original data through an error message.
    """
    if isinstance(value, BaseModel):
        return sanitize_non_finite(value.model_dump(mode="python"))
    if isinstance(value, bool):
        return value
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, Real):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, Mapping):
        return {key: sanitize_non_finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [sanitize_non_finite(item) for item in value]
    if type(value).__module__.split(".")[0] == "numpy" and hasattr(value, "tolist"):
        return sanitize_non_finite(value.tolist())
    return value


clean_non_finite = sanitize_non_finite


class APIModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, allow_inf_nan=False,
        validate_default=True,
    )


class APIResponse(APIModel):
    """Clean measurements before validation; missing measurements remain null."""

    @model_validator(mode="before")
    @classmethod
    def clean_measurements(cls, value: Any) -> Any:
        return sanitize_non_finite(value)


class ErrorBody(APIResponse):
    code: NonBlank = Field(max_length=80)
    message: NonBlank = Field(max_length=500)
    details: Any = None


class ErrorEnvelope(APIResponse):
    """All non-2xx responses; app.py must redact secrets and tracebacks."""

    error: ErrorBody


class ItemPath(APIModel):
    item_id: Annotated[int, Field(gt=0)]


class EventPath(APIModel):
    event_id: Annotated[int, Field(gt=0)]


class HealthResponse(APIResponse):
    env: Environment
    db_path: NonBlank
    last_snapshot_at: EpochSeconds | None
    token_configured: bool
    rate_budget_remaining: NonNegativeInt | None
    active_job: JobName | None
    item_count: NonNegativeInt
    bar_count: NonNegativeInt


class OverviewQuery(APIModel):
    horizon: int = 72

    @field_validator("horizon")
    @classmethod
    def allowed_horizon(cls, value: int) -> int:
        if value not in (24, 72, 168):
            raise ValueError("horizon must be 24, 72 or 168")
        return value


class ItemsQuery(APIModel):
    q: str = Field(default="", max_length=200)
    category: str = Field(default="", max_length=80)
    watched_only: bool = False
    limit: Annotated[int, Field(ge=1, le=1000)] = 100


class BarsQuery(APIModel):
    timeframe: Timeframe = "4h"
    days: Annotated[int, Field(ge=0, description="0 = all available history")] = 90


class ForecastQuery(APIModel):
    horizons: str = "24,72,168"

    @field_validator("horizons")
    @classmethod
    def valid_horizons(cls, value: str) -> str:
        parts = [part.strip() for part in value.split(",")]
        if not parts or any(part not in {"24", "72", "168"} for part in parts):
            raise ValueError("horizons must contain only 24,72,168")
        if len(set(parts)) != len(parts):
            raise ValueError("horizons must not contain duplicates")
        return ",".join(parts)

    @property
    def horizon_hours(self) -> tuple[int, ...]:
        return tuple(int(part) for part in self.horizons.split(","))


class CSVExportQuery(APIModel):
    timeframe: Timeframe = "4h"


class EventsQuery(APIModel):
    """The specified GET /api/events route has no query parameters."""


class EventFields(APIModel):
    name: NonBlank = Field(max_length=120)
    event_type: NonBlank = Field(max_length=60)
    announced_at: EpochSeconds
    start_ts: EpochSeconds
    end_ts: EpochSeconds
    impact_scope: NonBlank = Field(default="all", max_length=120)
    notes: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def valid_window(self) -> Self:
        if self.end_ts < self.start_ts:
            raise ValueError("end_ts must be greater than or equal to start_ts")
        return self


class EventCreateRequest(EventFields):
    pass


class MarketEvent(EventFields, APIResponse):
    event_id: PositiveId


class EventCreateResponse(MarketEvent):
    pass


class EventListResponse(RootModel[list[MarketEvent]]):
    pass


class EventDeleteResponse(APIResponse):
    event_id: PositiveId
    deleted: bool


class RPIndex(APIResponse):
    """Yen per RP; level is provided only when a baseline is available."""

    value: PositivePrice | None
    change_24h_pct: FiniteFloat | None = None
    inflation_level: str | None = None
    updated_at: EpochSeconds | None = None


class ItemSummary(APIResponse):
    item_id: PositiveId
    name_en: NonBlank
    name_zh: str | None = None
    category: NonBlank = "other"
    asset_class: AssetClass = "item"
    max_stack_size: PositiveInt = 99
    description: str | None = None
    is_watched: bool = False
    updated_at: EpochSeconds | None = None
    current_price: PositivePrice | None = None
    book_state: Literal["stocked", "mixed_ended_empty", "empty_confirmed"] | None = None
    change_24h_pct: FiniteFloat | None = None
    signal: Signal | None = None
    prob_up: Probability | None = None
    expected_net_roi: FiniteFloat | None = None
    total_listed_qty: NonNegativeInt | None = None
    depth_within_5pct: NonNegativeInt | None = None
    depth_change_24h_pct: FiniteFloat | None = None


class ItemListResponse(RootModel[list[ItemSummary]]):
    """Bare item array, without an invented items envelope."""


class OverviewResponse(APIResponse):
    rp_index: RPIndex | None
    next_event: MarketEvent | None
    top_bullish: list[ItemSummary]
    top_thinning_depth: list[ItemSummary]
    top_movers_24h: list[ItemSummary]
    watchlist: list[ItemSummary]


class WatchRequest(APIModel):
    is_watched: StrictBool


class WatchResponse(APIResponse):
    item_id: PositiveId
    is_watched: bool


class Bar(APIResponse):
    item_id: PositiveId
    timeframe: Timeframe
    bucket_ts: EpochSeconds
    close_ts: EpochSeconds
    data_source: Literal["snapshot", "manual"]
    book_state: Literal["stocked", "mixed_ended_empty", "empty_confirmed"] = "stocked"
    bar_available_at: EpochSeconds | None = None
    latest_publish_seq: PositiveInt | None = None
    open_price: PositivePrice | None
    high_price: PositivePrice | None
    low_price: PositivePrice | None
    close_price: PositivePrice | None
    robust_best_ask: PriceInt | None
    best_ask_min: PriceInt | None
    p10_price: PositivePrice | None = None
    total_listed_qty: NonNegativeInt | None = None
    listing_count: NonNegativeInt | None = None
    depth_within_5pct: NonNegativeInt | None = None
    spread_top5_pct: NonNegativeFloat | None = None
    est_sold_qty_sum: NonNegativeFloat | None = None
    expired_qty_sum: NonNegativeInt | None = None
    new_listed_qty_sum: NonNegativeInt | None = None
    sample_count: PositiveInt = 1
    valid_flow_samples: NonNegativeInt = 0

    @model_validator(mode="after")
    def consistent_bar(self) -> Self:
        duration = {"15m": 900, "4h": 14400, "1d": 86400}[self.timeframe]
        if self.bucket_ts % duration or self.close_ts != self.bucket_ts + duration:
            raise ValueError("bar timestamps must match the UTC timeframe grid")
        prices = (self.open_price, self.high_price, self.low_price, self.close_price)
        if self.book_state == "empty_confirmed":
            all_prices = (
                *prices, self.robust_best_ask, self.best_ask_min, self.p10_price,
            )
            if any(price is not None for price in all_prices):
                raise ValueError("empty_confirmed bars must have null prices")
        elif self.book_state == "mixed_ended_empty":
            if any(price is None for price in prices):
                raise ValueError("mixed_ended_empty bars must retain historical OHLC prices")
        if self.book_state != "stocked":
            if any(value is not None for value in (
                self.robust_best_ask, self.p10_price, self.spread_top5_pct,
            )):
                raise ValueError("empty books must have null current quote measurements")
            if self.total_listed_qty != 0:
                raise ValueError("empty books must have zero total_listed_qty")
            if any(value not in (None, 0) for value in (
                self.listing_count, self.depth_within_5pct,
            )):
                raise ValueError("empty books cannot retain listed inventory or depth")
        if all(price is not None for price in prices):
            if not self.low_price <= min(self.open_price, self.close_price):
                raise ValueError("low_price must not exceed open_price or close_price")
            if not self.high_price >= max(self.open_price, self.close_price):
                raise ValueError("high_price must not be below open_price or close_price")
        if self.valid_flow_samples > self.sample_count:
            raise ValueError("valid_flow_samples must not exceed sample_count")
        return self


class BarsResponse(APIResponse):
    item: ItemSummary
    bars: list[Bar]
    events: list[MarketEvent]


class Driver(APIResponse):
    feature: NonBlank
    contribution: FiniteFloat | None
    value: FiniteFloat | None = None
    direction: Literal["positive", "negative", "neutral"] | None = None


class Forecast(APIResponse):
    prediction_id: PositiveId | None = None
    item_id: PositiveId
    horizon_hours: Horizon
    generated_at: EpochSeconds
    model_name: NonBlank
    current_price: PositivePrice | None
    ref_qty: PositiveInt
    prob_up: Probability | None
    pred_q10: PositivePrice | None
    pred_q50: PositivePrice | None
    pred_q90: PositivePrice | None
    expected_net_roi: FiniteFloat | None
    signal: Signal
    regime_note: str
    top_drivers: list[Driver] = Field(default_factory=list, max_length=5)

    @model_validator(mode="after")
    def ordered_quantiles(self) -> Self:
        values = [value for value in (self.pred_q10, self.pred_q50, self.pred_q90)
                  if value is not None]
        if values != sorted(values):
            raise ValueError("pred_q10, pred_q50 and pred_q90 must be ordered")
        return self


class ForecastResponse(APIResponse):
    item: ItemSummary
    forecasts: list[Forecast] = Field(max_length=3)

    @model_validator(mode="after")
    def unique_horizons(self) -> Self:
        horizons = [forecast.horizon_hours for forecast in self.forecasts]
        if len(set(horizons)) != len(horizons):
            raise ValueError("forecasts must have unique horizons")
        if any(forecast.item_id != self.item.item_id for forecast in self.forecasts):
            raise ValueError("forecasts must match the requested item")
        return self


class FeeSimulationRequest(APIModel):
    item_id: PositiveId | None = None
    asset_class: AssetClass | None = None
    max_stack_size: PositiveInt | None = None
    buy_price: NonNegativeInt
    sell_price: PriceInt
    qty: PositiveInt
    relist_rate: Annotated[Decimal, Field(ge=0, le=1, allow_inf_nan=False)] | None = None


class FeeSimulationResponse(APIResponse):
    slots_used: PositiveInt
    listing_fee: NonNegativeInt
    effective_fee: Annotated[Decimal, Field(ge=0)]
    net_profit: Decimal
    net_roi: Decimal | None = Field(description="null when buy_price is zero")
    breakeven_sell_price: PriceInt


class BacktestRequest(APIModel):
    item_id: PositiveId | None = None
    category: NonBlank | None = Field(default=None, max_length=80)
    horizon_hours: Horizon
    models: list[ModelName] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def valid_scope_and_models(self) -> Self:
        if self.item_id is not None and self.category is not None:
            raise ValueError("choose item_id or category, not both")
        if len(set(self.models)) != len(self.models):
            raise ValueError("models must not contain duplicates")
        return self


class BacktestMetrics(APIResponse):
    """Returns, drawdown and win_rate use ratios; loss/AUC are dimensionless."""

    total_return: FiniteFloat | None = None
    sharpe_ratio: FiniteFloat | None = None
    max_drawdown: FiniteFloat | None = None
    win_rate: Probability | None = None
    trade_count: NonNegativeInt | None = None
    pinball_loss: NonNegativeFloat | None = None
    roc_auc: Probability | None = None
    interval_coverage: Probability | None = None


class BacktestComparison(APIResponse):
    model_name: NonBlank
    metrics: BacktestMetrics


class EquityPoint(APIResponse):
    ts: EpochSeconds
    equity: NonNegativeFloat | None


class EquityCurve(APIResponse):
    model_name: NonBlank
    points: list[EquityPoint]

    @model_validator(mode="after")
    def ordered_points(self) -> Self:
        if any(b.ts <= a.ts for a, b in zip(self.points, self.points[1:])):
            raise ValueError("equity timestamps must be strictly increasing")
        return self


class BacktestResponse(APIResponse):
    run_id: PositiveId
    comparison_table: list[BacktestComparison]
    equity_curves: list[EquityCurve]


class ManualQuoteRequest(APIModel):
    item_id: PositiveId
    price: PriceInt
    listed_qty: NonNegativeInt | None = None
    recorded_at: EpochSeconds | None = None
    note: str | None = Field(default=None, max_length=2000)


class ManualQuoteResponse(APIResponse):
    quote_id: PositiveId
    item_id: PositiveId
    recorded_at: EpochSeconds
    price: PriceInt
    listed_qty: NonNegativeInt | None = None
    note: str | None = None
    source_tag: NonBlank = "manual"
    skipped_snapshot_bars: NonNegativeInt = 0


class CSVImportRequest(APIModel):
    """Multipart file; app.py must ALSO enforce streamed byte and row bounds."""

    file: UploadFile

    @field_validator("file")
    @classmethod
    def bounded_upload(cls, value: UploadFile) -> UploadFile:
        if value.size is not None and value.size > MAX_CSV_BYTES:
            raise ValueError("CSV file exceeds the 5 MiB limit")
        return value


class CSVImportError(APIResponse):
    row: PositiveInt | None = None
    message: NonBlank


class CSVImportResponse(APIResponse):
    imported_rows: Annotated[int, Field(ge=0, le=MAX_CSV_ROWS, strict=True)]
    skipped_snapshot_bars: NonNegativeInt
    errors: list[CSVImportError]


class CSVExportResponse(RootModel[str]):
    """Raw UTF-8 CSV emitted with CSV_MEDIA_TYPE (not a JSON response_model).

    app.py escapes leading =,+,-,@,tab,CR in free-text cells before writing.
    """


class AdminTriggerRequest(APIModel):
    action: AdminAction


class AdminTriggerResponse(APIResponse):
    action: AdminAction
    status: Literal["queued", "running", "completed"]
    active_job: JobName | None = None
