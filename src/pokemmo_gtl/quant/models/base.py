"""Shared forecaster contract and prediction post-processing."""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from ..fees import net_profit


@dataclass(slots=True)
class PredictionOutput:
    """A price forecast with calibrated return bounds and execution costs."""

    prob_up: float
    pred_q10_ret: float
    pred_q50_ret: float
    pred_q90_ret: float
    pred_q10: int
    pred_q50: int
    pred_q90: int
    expected_net_roi: float
    signal: str
    regime_note: str
    top_drivers: list[dict[str, Any]] = field(default_factory=list)
    raw_quantile_crossed: bool = False


class Forecaster(ABC):
    """Common fit/predict contract for cold-start and panel forecasters."""

    @abstractmethod
    def fit(self, X: Any, y: Any = None, **kwargs: Any) -> Forecaster:
        """Fit from feature rows and their forward log-return targets."""

    @abstractmethod
    def predict(self, X: Any, **kwargs: Any) -> PredictionOutput:
        """Predict one observation, or a batch when the model supports it."""


def log_returns_to_prices(
    current_price: float, q10_ret: float, q50_ret: float, q90_ret: float
) -> tuple[int, int, int]:
    """Convert log-return quantiles to positive integer prices in monotone order."""
    if not math.isfinite(current_price) or current_price <= 0:
        raise ValueError("current_price must be a finite positive number")
    prices = [
        min(
            2**62,
            max(1, int(round(current_price * math.exp(min(700.0, max(-700.0, value)))))),
        )
        for value in (q10_ret, q50_ret, q90_ret)
    ]
    return tuple(sorted(prices))  # type: ignore[return-value]


def tukey_expected_price(q10: float, q50: float, q90: float) -> int:
    """Calculate the specified 25/50/25 weighted expected sale price."""
    return min(2**62, max(1, int(round(0.25 * q10 + 0.50 * q50 + 0.25 * q90))))


def signal_for(expected_net_roi: float, prob_up: float) -> str:
    """Map calibrated profitability to the product's three-state signal."""
    if expected_net_roi >= 0.02 and prob_up >= 0.55:
        return "BUY_WATCH"
    if expected_net_roi < 0:
        return "SELL_CAUTION"
    return "NEUTRAL"


def build_prediction_output(
    *,
    current_price: float,
    q10_ret: float,
    q50_ret: float,
    q90_ret: float,
    prob_up: float,
    ref_qty: int = 1,
    max_stack_size: int = 99,
    min_fee: int = 100,
    rate: float | str = "0.05",
    relist_rate: float | str = "0.15",
    max_fee_per_slot: int = 25_000,
    regime_note: str = "",
    top_drivers: list[dict[str, Any]] | None = None,
    raw_quantile_crossed: bool = False,
    force_signal: str | None = None,
) -> PredictionOutput:
    """Restore prices, compute fee-aware ROI, and build the public result."""
    if isinstance(ref_qty, bool) or not isinstance(ref_qty, int) or ref_qty <= 0:
        raise ValueError("ref_qty must be a positive integer")
    if not all(math.isfinite(value) for value in (q10_ret, q50_ret, q90_ret)):
        raise ValueError("return quantiles must be finite")
    if not math.isfinite(prob_up):
        raise ValueError("prob_up must be finite")

    crossed = raw_quantile_crossed or not (q10_ret <= q50_ret <= q90_ret)
    q10_ret, q50_ret, q90_ret = sorted((q10_ret, q50_ret, q90_ret))
    pred_q10, pred_q50, pred_q90 = log_returns_to_prices(
        current_price, q10_ret, q50_ret, q90_ret
    )
    expected_price = tukey_expected_price(pred_q10, pred_q50, pred_q90)
    buy_price = max(1, int(round(current_price)))
    profit = net_profit(
        buy_price,
        expected_price,
        ref_qty,
        max_stack_size,
        min_fee=min_fee,
        rate=rate,
        relist_rate=relist_rate,
        max_fee_per_slot=max_fee_per_slot,
    )
    roi = float(profit / (buy_price * ref_qty))
    probability = min(1.0, max(0.0, float(prob_up)))
    return PredictionOutput(
        prob_up=probability,
        pred_q10_ret=float(q10_ret),
        pred_q50_ret=float(q50_ret),
        pred_q90_ret=float(q90_ret),
        pred_q10=pred_q10,
        pred_q50=pred_q50,
        pred_q90=pred_q90,
        expected_net_roi=roi,
        signal=force_signal if force_signal is not None else signal_for(roi, probability),
        regime_note=regime_note,
        top_drivers=list(top_drivers or [])[:5],
        raw_quantile_crossed=crossed,
    )
