"""Rule-based and regularized statistical forecasters for short histories."""

from __future__ import annotations

import math
from statistics import NormalDist
from typing import Any

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from ..fees import breakeven_sell_price
from .base import Forecaster, PredictionOutput, build_prediction_output


class Tier0RuleForecaster(Forecaster):
    """Use sell pressure, flow balance, and price z-score with a volatility band."""

    def __init__(
        self,
        *,
        horizon_hours: int = 72,
        residual_quantiles: tuple[float, float] = (0.1, 0.9),
    ) -> None:
        self.horizon_hours = horizon_hours
        self.residual_quantiles = residual_quantiles
        self.residuals = np.empty(0, dtype=float)
        self.return_mean = 0.0
        self.volatility = 0.03
        self.training_features = pd.DataFrame()

    def fit(
        self,
        X: Any = None,
        y: Any = None,
        *,
        historical_returns: Any = None,
        residuals: Any = None,
        **_: Any,
    ) -> Tier0RuleForecaster:
        self.training_features = _as_frame(X)
        targets = _finite_vector(
            historical_returns if historical_returns is not None else y
        )
        if residuals is not None:
            residual_values = _finite_vector(residuals)
            self.return_mean = float(np.mean(targets)) if targets.size else 0.0
        else:
            self.return_mean = float(np.mean(targets)) if targets.size else 0.0
            residual_values = targets - self.return_mean
        self.residuals = residual_values
        if residual_values.size >= 2:
            self.volatility = float(np.std(residual_values, ddof=1))
        elif residual_values.size == 1:
            self.volatility = abs(float(residual_values[0]))
        if not math.isfinite(self.volatility) or self.volatility <= 1e-8:
            self.volatility = 0.03
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
        row = _last_row(X, self.training_features)
        price = _resolve_price(row, current_price)
        forecast, driver_values = _rule_return(row)
        qlo, qhi = self.residual_quantiles
        if self.residuals.size >= 2:
            low_resid, high_resid = np.quantile(self.residuals, [qlo, qhi])
            median_resid = float(np.median(self.residuals))
        else:
            spread = 1.28155 * self.volatility
            low_resid, median_resid, high_resid = -spread, 0.0, spread
        q10, q50, q90 = forecast + low_resid, forecast + median_resid, forecast + high_resid
        threshold = _breakeven_log_return(
            price,
            ref_qty,
            max_stack_size,
            min_fee,
            rate,
            relist_rate,
            max_fee_per_slot,
        )
        probability = _survival_probability(
            threshold, forecast, self.residuals, self.volatility
        )
        drivers = _top_drivers(driver_values)
        return build_prediction_output(
            current_price=price,
            q10_ret=q10,
            q50_ret=q50,
            q90_ret=q90,
            prob_up=probability,
            ref_qty=ref_qty,
            max_stack_size=max_stack_size,
            min_fee=min_fee,
            rate=rate,
            relist_rate=relist_rate,
            max_fee_per_slot=max_fee_per_slot,
            regime_note=f"tier0: rule forecast with {self.volatility:.4f} residual volatility",
            top_drivers=drivers,
        )


class Tier1RidgeForecaster(Forecaster):
    """Ridge regression with median imputation and empirical residual bounds."""

    def __init__(self, *, horizon_hours: int = 72) -> None:
        self.horizon_hours = horizon_hours
        self.pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
                ("scaler", StandardScaler()),
                ("ridge", Ridge(alpha=10.0)),
            ]
        )
        self.feature_names: list[str] = []
        self.training_features = pd.DataFrame()
        self.residuals = np.empty(0, dtype=float)
        self.volatility = 0.03
        self.fitted = False

    def fit(
        self,
        X: Any,
        y: Any,
        *,
        historical_returns: Any = None,
        **_: Any,
    ) -> Tier1RidgeForecaster:
        frame = _model_frame(X)
        targets = np.asarray(
            historical_returns if historical_returns is not None else y, dtype=float
        ).reshape(-1)
        if len(frame) != targets.size:
            raise ValueError("X and y must contain the same number of rows")
        valid = np.isfinite(targets)
        frame = frame.loc[valid].reset_index(drop=True)
        targets = targets[valid]
        if not len(frame):
            raise ValueError("at least one finite training target is required")
        self.feature_names = list(frame.columns)
        self.training_features = frame.copy()
        self.pipeline.fit(frame, targets)
        self.residuals = targets - self.pipeline.predict(frame)
        if self.residuals.size >= 2:
            self.volatility = float(np.std(self.residuals, ddof=1))
        if not math.isfinite(self.volatility) or self.volatility <= 1e-8:
            self.volatility = 0.03
        self.fitted = True
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
        if not self.fitted:
            raise RuntimeError("Tier1RidgeForecaster must be fitted before prediction")
        row = _last_row(X, self.training_features)
        price = _resolve_price(row, current_price)
        aligned = _align_frame(row, self.feature_names)
        median = float(self.pipeline.predict(aligned)[0])
        qlo_res, qhi_res = np.quantile(self.residuals, [0.1, 0.9])
        q10, q50, q90 = median + qlo_res, median, median + qhi_res
        threshold = _breakeven_log_return(
            price,
            ref_qty,
            max_stack_size,
            min_fee,
            rate,
            relist_rate,
            max_fee_per_slot,
        )
        probability = _survival_probability(
            threshold, median, self.residuals, self.volatility
        )
        return build_prediction_output(
            current_price=price,
            q10_ret=q10,
            q50_ret=q50,
            q90_ret=q90,
            prob_up=probability,
            ref_qty=ref_qty,
            max_stack_size=max_stack_size,
            min_fee=min_fee,
            rate=rate,
            relist_rate=relist_rate,
            max_fee_per_slot=max_fee_per_slot,
            regime_note="tier1: ridge forecast with empirical residual interval",
            top_drivers=self._drivers(aligned),
        )

    def _drivers(self, row: pd.DataFrame) -> list[dict[str, Any]]:
        ridge = self.pipeline.named_steps["ridge"]
        scaler = self.pipeline.named_steps["scaler"]
        imputer = self.pipeline.named_steps["imputer"]
        values = imputer.transform(row)
        scaled = scaler.transform(values)[0]
        contributions = np.asarray(ridge.coef_) * scaled
        return _top_drivers(
            {
                name: float(value)
                for name, value in zip(self.feature_names, contributions, strict=True)
            }
        )


def _as_frame(X: Any) -> pd.DataFrame:
    if X is None:
        return pd.DataFrame()
    if isinstance(X, pd.DataFrame):
        return X.copy()
    if isinstance(X, pd.Series):
        return X.to_frame().T
    if isinstance(X, dict):
        return pd.DataFrame([X])
    array = np.asarray(X)
    if array.ndim == 1:
        array = array.reshape(1, -1)
    return pd.DataFrame(array, columns=[f"feature_{i}" for i in range(array.shape[1])])


def _model_frame(X: Any) -> pd.DataFrame:
    frame = _as_frame(X)
    return frame.drop(columns=["item_id", "close_ts"], errors="ignore").apply(
        pd.to_numeric, errors="coerce"
    )


def _last_row(X: Any, fallback: pd.DataFrame | None = None) -> pd.DataFrame:
    frame = _as_frame(X)
    if frame.empty and fallback is not None and not fallback.empty:
        frame = fallback.tail(1).copy()
    if frame.empty:
        return pd.DataFrame([{}])
    return frame.tail(1).reset_index(drop=True)


def _align_frame(row: pd.DataFrame, names: list[str]) -> pd.DataFrame:
    return row.reindex(columns=names).apply(pd.to_numeric, errors="coerce")


def _finite_vector(values: Any) -> np.ndarray:
    if values is None:
        return np.empty(0, dtype=float)
    array = np.asarray(values, dtype=float).reshape(-1)
    return array[np.isfinite(array)]


def _resolve_price(row: Any, current_price: float | None) -> float:
    if current_price is not None:
        return float(current_price)
    if isinstance(row, (int, float, np.number)):
        return float(row)
    frame = _as_frame(row)
    if not frame.empty:
        for name in ("current_price", "close_price", "price"):
            if name in frame.columns and pd.notna(frame.iloc[-1][name]):
                return float(frame.iloc[-1][name])
    raise ValueError("current_price is required")


def _rule_return(row: pd.DataFrame) -> tuple[float, dict[str, float]]:
    values = row.iloc[-1].to_dict() if not row.empty else {}

    def number(*names: str, default: float = 0.0) -> float:
        for name in names:
            value = values.get(name)
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(parsed):
                return parsed
        return default

    imbalance = number("flow_imbalance_24h")
    if "flow_imbalance_24h" not in values:
        sold = number("est_sold_24h", "est_sold_qty_sum")
        new = number("new_listed_24h", "new_listed_qty_sum")
        imbalance = (sold - new) / (sold + new + 1.0)
    zscore = number("zscore_30d", "price_zscore_30d", "close_zscore_30d", "zscore")
    listings = max(0.0, number("total_listed_qty", "listing_count"))
    sold = max(0.0, number("est_sold_24h", "est_sold_qty_sum"))
    spread = max(0.0, number("spread_top5_pct"))
    depth_ratio = number("depth_ratio_24h", default=1.0)
    pressure = math.log1p(listings / (sold + 1.0))
    components = {
        "flow_imbalance_24h": 0.015 * float(np.clip(imbalance, -1, 1)),
        "sell_pressure": -0.002 * pressure,
        "price_zscore": -0.004 * float(np.clip(zscore, -5, 5)),
        "spread_top5_pct": 0.05 * (0.05 - spread),
        "depth_ratio_24h": 0.003 * float(np.clip(depth_ratio - 1.0, -2, 2)),
    }
    return float(sum(components.values())), components


def _breakeven_log_return(
    price: float,
    qty: int,
    max_stack_size: int,
    min_fee: int,
    rate: float | str,
    relist_rate: float | str,
    max_fee_per_slot: int,
) -> float:
    buy = max(1, int(round(price)))
    breakeven = breakeven_sell_price(
        buy,
        qty,
        max_stack_size,
        min_fee=min_fee,
        rate=rate,
        relist_rate=relist_rate,
        max_fee_per_slot=max_fee_per_slot,
    )
    return math.log(breakeven / buy)


def _survival_probability(
    threshold: float, mean: float, residuals: np.ndarray, volatility: float
) -> float:
    if residuals.size >= 20:
        values = mean + residuals
        return float(np.mean(values >= threshold))
    sigma = max(float(volatility), 1e-8)
    return float(1.0 - NormalDist(mu=mean, sigma=sigma).cdf(threshold))


def _top_drivers(values: dict[str, float]) -> list[dict[str, Any]]:
    ranked = sorted(values.items(), key=lambda item: abs(item[1]), reverse=True)
    return [
        {"feature": name, "contribution": float(value)}
        for name, value in ranked[:5]
        if math.isfinite(value) and value != 0
    ]


def select_forecaster(bar_count: int, *, horizon_hours: int = 72) -> Forecaster:
    """Select a cold-start tier using the item's observed 4h-bar count."""
    if isinstance(bar_count, bool) or not isinstance(bar_count, int) or bar_count < 0:
        raise ValueError("bar_count must be a non-negative integer")
    if bar_count < 84:
        return Tier0RuleForecaster(horizon_hours=horizon_hours)
    if bar_count < 180:
        return Tier1RidgeForecaster(horizon_hours=horizon_hours)
    from .lgbm_model import Tier2LightGBMForecaster

    return Tier2LightGBMForecaster(horizon_hours=horizon_hours)
