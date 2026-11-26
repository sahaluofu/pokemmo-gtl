"""Historical-distribution baseline used for cold starts and comparisons."""

from __future__ import annotations

from typing import Any

import numpy as np

from .base import Forecaster, PredictionOutput, build_prediction_output


class NaiveForecaster(Forecaster):
    """Predict no median movement and use historical returns for the tails."""

    def __init__(self) -> None:
        self.q10_ret = 0.0
        self.q90_ret = 0.0
        self.positive_rate = 0.5
        self.training_returns = np.empty(0, dtype=float)

    def fit(
        self,
        X: Any = None,
        y: Any = None,
        *,
        historical_returns: Any = None,
        positive_labels: Any = None,
        **_: Any,
    ) -> NaiveForecaster:
        returns = _finite_vector(
            historical_returns if historical_returns is not None else y
        )
        self.training_returns = returns
        if returns.size:
            self.q10_ret, self.q90_ret = np.quantile(returns, [0.1, 0.9]).tolist()
            labels = (
                _finite_vector(positive_labels)
                if positive_labels is not None
                else (returns > 0).astype(float)
            )
            if labels.size:
                self.positive_rate = float(np.mean(labels > 0))
        else:
            self.q10_ret = self.q90_ret = 0.0
            labels = _finite_vector(positive_labels)
            if labels.size:
                self.positive_rate = float(np.mean(labels > 0))

        # The required zero-return median must remain inside the interval.
        self.q10_ret = min(float(self.q10_ret), 0.0)
        self.q90_ret = max(float(self.q90_ret), 0.0)
        return self

    def predict(
        self,
        X: Any = None,
        *,
        current_price: float | None = None,
        ref_qty: int = 1,
        max_stack_size: int = 99,
        min_fee: int = 100,
        rate: float | str = "0.05",
        relist_rate: float | str = "0.15",
        max_fee_per_slot: int = 25_000,
        **_: Any,
    ) -> PredictionOutput:
        price = _resolve_price(X, current_price)
        return build_prediction_output(
            current_price=price,
            q10_ret=self.q10_ret,
            q50_ret=0.0,
            q90_ret=self.q90_ret,
            prob_up=self.positive_rate,
            ref_qty=ref_qty,
            max_stack_size=max_stack_size,
            min_fee=min_fee,
            rate=rate,
            relist_rate=relist_rate,
            max_fee_per_slot=max_fee_per_slot,
            regime_note="naive: historical return distribution; median unchanged",
            force_signal="NEUTRAL",
        )


def _finite_vector(values: Any) -> np.ndarray:
    if values is None:
        return np.empty(0, dtype=float)
    array = np.asarray(values, dtype=float).reshape(-1)
    return array[np.isfinite(array)]


def _resolve_price(X: Any, current_price: float | None) -> float:
    if current_price is not None:
        return float(current_price)
    if isinstance(X, (int, float, np.number)):
        return float(X)
    if isinstance(X, dict) and "current_price" in X:
        return float(X["current_price"])
    raise ValueError("current_price is required")
