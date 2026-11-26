"""Panel LightGBM forecaster with time-purged Platt and CQR calibration."""

from __future__ import annotations

import math
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from .base import Forecaster, PredictionOutput, build_prediction_output


class Tier2LightGBMForecaster(Forecaster):
    """Fit weighted panel direction and quantile trees on log returns."""

    TREE_PARAMS = {
        "max_depth": 4,
        "num_leaves": 15,
        "min_child_samples": 30,
        "random_state": 42,
        "n_estimators": 120,
        "verbosity": -1,
        "n_jobs": 1,
    }

    def __init__(
        self,
        *,
        horizon_hours: int = 72,
        validation_fraction: float = 0.15,
        cqr_coverage: float = 0.80,
    ) -> None:
        if horizon_hours <= 0:
            raise ValueError("horizon_hours must be positive")
        if not 0 < validation_fraction < 0.5:
            raise ValueError("validation_fraction must be between 0 and 0.5")
        if not 0 < cqr_coverage < 1:
            raise ValueError("cqr_coverage must be between 0 and 1")
        self.horizon_hours = horizon_hours
        self.validation_fraction = validation_fraction
        self.cqr_coverage = cqr_coverage
        self.feature_names: list[str] = []
        self.direction_model: Any = None
        self.quantile_models: dict[float, Any] = {}
        self.platt_model: LogisticRegression | None = None
        self.platt_shrinkage = 0.0
        self.training_prior = 0.5
        self.c80 = 0.0
        self.raw_quantile_crossing_rate = 0.0
        self.validation_coverage_before = 0.0
        self.validation_coverage_after = 0.0
        self.validation_start_ts: float | None = None
        self.train_close_ts_max: float | None = None
        self.train_sample_weights = np.empty(0, dtype=float)
        self.validation_targets = np.empty(0, dtype=float)
        self.validation_direction_targets = np.empty(0, dtype=float)
        self.validation_raw_probabilities = np.empty(0, dtype=float)
        self.validation_calibrated_probabilities = np.empty(0, dtype=float)
        self.fitted = False

    def fit(
        self,
        X: Any,
        y: Any,
        y_up: Any = None,
        *,
        close_ts: Any = None,
        item_ids: Any = None,
        sample_delta: Any = None,
        sample_delta_hours: Any = None,
        horizon_hours: int | None = None,
        **_: Any,
    ) -> Tier2LightGBMForecaster:
        """Fit on a panel and reserve a globally time-ordered, purged tail."""
        if horizon_hours is not None:
            if horizon_hours <= 0:
                raise ValueError("horizon_hours must be positive")
            self.horizon_hours = horizon_hours
        frame = _as_frame(X)
        n_rows = len(frame)
        targets = np.asarray(y, dtype=float).reshape(-1)
        if targets.size != n_rows:
            raise ValueError("X and y must contain the same number of rows")
        if y_up is None:
            direction = (targets > 0).astype(int)
        else:
            direction = np.asarray(y_up, dtype=float).reshape(-1)
            if direction.size != n_rows:
                raise ValueError("X and y_up must contain the same number of rows")
        times = _metadata_values(close_ts, frame, "close_ts", n_rows)
        ids = _metadata_values(item_ids, frame, "item_id", n_rows)
        deltas = _metadata_values(sample_delta_hours, frame, "sample_delta_hours", n_rows)
        delta_in_hours = deltas is not None
        if deltas is None:
            deltas = _metadata_values(sample_delta, frame, "sample_delta", n_rows)

        features = frame.drop(
            columns=[
                "item_id",
                "close_ts",
                "sample_delta",
                "sample_delta_hours",
                "target",
                "y",
                "y_up",
            ],
            errors="ignore",
        ).apply(pd.to_numeric, errors="coerce")
        if features.shape[1] == 0:
            raise ValueError("at least one numeric model feature is required")
        valid = np.isfinite(targets) & np.isfinite(direction)
        features = features.loc[valid].reset_index(drop=True)
        targets = targets[valid]
        direction = (direction[valid] > 0).astype(int)
        if times is not None:
            times = times[valid]
        if ids is not None:
            ids = ids[valid]
        if deltas is not None:
            deltas = deltas[valid]
        if len(targets) < 3:
            raise ValueError("at least three finite training rows are required")

        train_idx, validation_idx, time_axis = _purged_split(
            len(targets),
            times,
            self.horizon_hours,
            self.validation_fraction,
        )
        if train_idx.size == 0:
            raise ValueError("purge interval leaves no training rows")
        if validation_idx.size == 0:
            raise ValueError("not enough time-ordered rows for a calibration window")
        self.validation_start_ts = float(np.min(time_axis[validation_idx]))
        self.train_close_ts_max = float(np.max(time_axis[train_idx]))
        if times is not None and self.train_close_ts_max + self.horizon_hours * 3600 > (
            self.validation_start_ts + 1e-9
        ):
            raise RuntimeError("purged split contains an overlapping training target")

        weights = _sample_weights(
            time_axis,
            ids,
            deltas,
            train_idx,
            self.horizon_hours,
            delta_in_hours=delta_in_hours,
        )
        self.train_sample_weights = weights.copy()
        self.feature_names = list(features.columns)
        X_train = features.iloc[train_idx]
        X_valid = features.iloc[validation_idx]
        y_train = targets[train_idx]
        y_valid = targets[validation_idx]
        cls_train = direction[train_idx]
        cls_valid = direction[validation_idx]
        self.training_prior = float(np.average(cls_train, weights=weights))

        self.direction_model = lgb.LGBMClassifier(
            objective="binary",
            **self.TREE_PARAMS,
        )
        if np.unique(cls_train).size > 1:
            self.direction_model.fit(X_train, cls_train, sample_weight=weights)
        else:
            # A constant prior is the well-defined direction model for a one-class fold.
            self.direction_model = _ConstantDirectionModel(self.training_prior)
            self.direction_model.fit(X_train, cls_train, sample_weight=weights)

        self.quantile_models = {}
        for alpha in (0.1, 0.5, 0.9):
            model = lgb.LGBMRegressor(
                objective="quantile",
                alpha=alpha,
                **self.TREE_PARAMS,
            )
            model.fit(X_train, y_train, sample_weight=weights)
            self.quantile_models[alpha] = model

        raw_valid = self._predict_raw_quantiles(X_valid)
        crossing = (raw_valid[:, 0] > raw_valid[:, 1]) | (
            raw_valid[:, 1] > raw_valid[:, 2]
        )
        self.raw_quantile_crossing_rate = float(np.mean(crossing))
        sorted_valid = np.sort(raw_valid, axis=1)
        self.validation_targets = y_valid.copy()
        self.c80 = _conformal_margin(
            sorted_valid[:, 0], sorted_valid[:, 2], y_valid, self.cqr_coverage
        )
        lo_before, hi_before = sorted_valid[:, 0], sorted_valid[:, 2]
        self.validation_coverage_before = float(
            np.mean((y_valid >= lo_before) & (y_valid <= hi_before))
        )
        lo_after, hi_after = lo_before - self.c80, hi_before + self.c80
        self.validation_coverage_after = float(
            np.mean((y_valid >= lo_after) & (y_valid <= hi_after))
        )

        raw_prob = self._predict_raw_probability(X_valid)
        self.validation_direction_targets = cls_valid.copy()
        self.validation_raw_probabilities = raw_prob.copy()
        self._fit_platt(raw_prob, cls_valid)
        self.validation_calibrated_probabilities = self._calibrate(raw_prob)
        self.fitted = True
        return self

    def predict(
        self,
        X: Any,
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
            raise RuntimeError("Tier2LightGBMForecaster must be fitted before prediction")
        row = _as_frame(X).tail(1)
        if row.empty:
            raise ValueError("one feature row is required")
        price = _resolve_price(row, current_price)
        aligned = row.reindex(columns=self.feature_names).apply(pd.to_numeric, errors="coerce")
        raw = self._predict_raw_quantiles(aligned)[0]
        crossed = bool(raw[0] > raw[1] or raw[1] > raw[2])
        q10, q50, q90 = np.sort(raw)
        q10 -= self.c80
        q90 += self.c80
        raw_probability = float(self._predict_raw_probability(aligned)[0])
        probability = float(self._calibrate(np.asarray([raw_probability]))[0])
        return build_prediction_output(
            current_price=price,
            q10_ret=float(q10),
            q50_ret=float(q50),
            q90_ret=float(q90),
            prob_up=probability,
            ref_qty=ref_qty,
            max_stack_size=max_stack_size,
            min_fee=min_fee,
            rate=rate,
            relist_rate=relist_rate,
            max_fee_per_slot=max_fee_per_slot,
            regime_note=(
                f"tier2: panel LightGBM; Platt shrinkage={self.platt_shrinkage:.2f}; "
                f"CQR c80={self.c80:.4f}"
            ),
            top_drivers=self._top_drivers(aligned),
            raw_quantile_crossed=crossed,
        )

    def _predict_raw_quantiles(self, X: pd.DataFrame) -> np.ndarray:
        values = np.column_stack(
            [self.quantile_models[alpha].predict(X) for alpha in (0.1, 0.5, 0.9)]
        )
        return np.asarray(values, dtype=float)

    def _predict_raw_probability(self, X: pd.DataFrame) -> np.ndarray:
        probabilities = self.direction_model.predict_proba(X)
        classes = np.asarray(self.direction_model.classes_)
        positive_column = int(np.flatnonzero(classes == 1)[0]) if 1 in classes else 0
        return np.asarray(probabilities[:, positive_column], dtype=float)

    def _fit_platt(self, raw_prob: np.ndarray, labels: np.ndarray) -> None:
        self.platt_model = None
        self.platt_shrinkage = 0.0
        if raw_prob.size >= 8 and np.unique(labels).size > 1:
            logits = _logit(raw_prob).reshape(-1, 1)
            model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=500)
            model.fit(logits, labels)
            self.platt_model = model
            self.platt_shrinkage = float(raw_prob.size / (raw_prob.size + 50.0))

    def _calibrate(self, raw_prob: np.ndarray) -> np.ndarray:
        probabilities = np.clip(np.asarray(raw_prob, dtype=float), 1e-6, 1 - 1e-6)
        if self.platt_model is None:
            return np.full(probabilities.shape, self.training_prior, dtype=float)
        calibrated = self.platt_model.predict_proba(_logit(probabilities).reshape(-1, 1))[:, 1]
        shrink = self.platt_shrinkage
        return shrink * calibrated + (1.0 - shrink) * self.training_prior

    def _top_drivers(self, row: pd.DataFrame) -> list[dict[str, Any]]:
        gains = np.zeros(len(self.feature_names), dtype=float)
        for model in [*self.quantile_models.values(), self.direction_model]:
            booster = getattr(model, "booster_", None)
            if booster is not None:
                gains += booster.feature_importance(importance_type="gain")
        values = row.iloc[0].to_numpy(dtype=float)
        if np.all(gains <= 0):
            gains = np.nan_to_num(np.abs(values), nan=0.0)
        ranked = np.argsort(gains)[::-1][:5]
        return [
            {
                "feature": self.feature_names[index],
                "importance": float(gains[index]),
                "value": None if not math.isfinite(values[index]) else float(values[index]),
            }
            for index in ranked
            if gains[index] > 0
        ]


class _ConstantDirectionModel:
    """Classifier-compatible prior for a training fold with one observed class."""

    def __init__(self, probability: float) -> None:
        self.probability = min(1.0, max(0.0, float(probability)))
        self.classes_ = np.array([0, 1], dtype=int)

    def fit(self, X: Any, y: Any, sample_weight: Any = None) -> _ConstantDirectionModel:
        return self

    def predict_proba(self, X: Any) -> np.ndarray:
        positive = np.full(len(X), self.probability, dtype=float)
        return np.column_stack((1.0 - positive, positive))


def _as_frame(X: Any) -> pd.DataFrame:
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


def _metadata_values(
    provided: Any, frame: pd.DataFrame, column: str, n_rows: int
) -> np.ndarray | None:
    values = provided
    if values is None and column in frame.columns:
        values = frame[column].to_numpy()
    if values is None:
        return None
    if isinstance(values, pd.Series):
        values = values.to_numpy()
    array = np.asarray(values)
    if array.size != n_rows:
        raise ValueError(f"{column} must contain one value per row")
    if np.issubdtype(array.dtype, np.datetime64):
        array = array.astype("datetime64[ns]").astype(np.int64) / 1e9
    try:
        return array.astype(float).reshape(-1)
    except (TypeError, ValueError):
        return pd.to_numeric(pd.Series(array.reshape(-1)), errors="coerce").to_numpy()


def _purged_split(
    n_rows: int,
    times: np.ndarray | None,
    horizon_hours: int,
    validation_fraction: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if times is None:
        axis = np.arange(n_rows, dtype=float) * 4.0
        unique = axis
        validation_count = max(1, int(math.ceil(n_rows * validation_fraction)))
        start = float(unique[-validation_count])
        train = np.flatnonzero(axis <= start - horizon_hours)
        validation = np.flatnonzero(axis >= start)
        return train, validation, axis

    axis = np.asarray(times, dtype=float)
    if not np.isfinite(axis).all():
        raise ValueError("close_ts values must be finite")
    unique = np.unique(axis)
    validation_count = max(1, int(math.ceil(unique.size * validation_fraction)))
    start = float(unique[-validation_count])
    train = np.flatnonzero(axis + horizon_hours * 3600 <= start + 1e-9)
    validation = np.flatnonzero(axis >= start)
    return train, validation, axis


def _sample_weights(
    times: np.ndarray,
    ids: np.ndarray | None,
    deltas: np.ndarray | None,
    train_idx: np.ndarray,
    horizon_hours: int,
    *,
    delta_in_hours: bool,
) -> np.ndarray:
    if deltas is not None:
        delta_hours = np.asarray(deltas[train_idx], dtype=float)
        if not delta_in_hours:
            delta_hours = delta_hours / 3600.0
        delta_hours = np.where(
            np.isfinite(delta_hours) & (delta_hours > 0), delta_hours, 4.0
        )
    else:
        row_ids = ids if ids is not None else np.zeros(len(times), dtype=int)
        delta_hours_all = np.full(len(times), 4.0, dtype=float)
        for item in np.unique(row_ids):
            positions = np.flatnonzero(row_ids == item)
            ordered = positions[np.argsort(times[positions], kind="stable")]
            diffs = np.diff(times[ordered])
            positive = diffs[diffs > 0]
            default = float(np.median(positive)) if positive.size else 4.0
            delta_hours_all[ordered[0]] = default
            if diffs.size:
                delta_hours_all[ordered[1:]] = np.maximum(diffs, 0.0)
        if np.nanmedian(np.abs(times)) > 1e8 or np.nanmedian(delta_hours_all) > 24:
            delta_hours_all /= 3600.0
        delta_hours = delta_hours_all[train_idx]
    weights = np.maximum(delta_hours / horizon_hours, 0.0)
    return weights.astype(float)


def _conformal_margin(
    q10: np.ndarray, q90: np.ndarray, y: np.ndarray, coverage: float
) -> float:
    scores = np.maximum.reduce((q10 - y, y - q90, np.zeros_like(y)))
    if scores.size == 0:
        return 0.0
    rank = min(scores.size, max(1, int(math.ceil((scores.size + 1) * coverage))))
    return float(np.partition(scores, rank - 1)[rank - 1])


def _logit(probability: np.ndarray) -> np.ndarray:
    clipped = np.clip(np.asarray(probability, dtype=float), 1e-6, 1 - 1e-6)
    return np.log(clipped / (1.0 - clipped))


def _resolve_price(row: Any, current_price: float | None) -> float:
    if current_price is not None:
        return float(current_price)
    if isinstance(row, (int, float, np.number)):
        return float(row)
    frame = _as_frame(row)
    for name in ("current_price", "close_price", "price"):
        if name in frame.columns and pd.notna(frame.iloc[-1][name]):
            return float(frame.iloc[-1][name])
    raise ValueError("current_price is required")


