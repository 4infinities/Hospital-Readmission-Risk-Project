"""
Model training and evaluation utilities for readmission risk.

Responsibilities:
- Build sklearn / LightGBM models from config.
- Train pipelines with scaling and optional cross‑validation.
- Compute probability- and threshold-based metrics.
- Extract model coefficients / feature importances.
- Build per‑threshold confusion metrics for 30d/90d horizons.
"""

import pandas as pd
import numpy as np
import joblib

from sklearn.model_selection import (
    StratifiedKFold,
    train_test_split,
    cross_validate,
)
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    precision_recall_fscore_support,
    brier_score_loss,
)
from pathlib import Path
from config import cv_scoring, proba_metrics, pred_metrics
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from lightgbm import LGBMClassifier
from sklearn.ensemble import RandomForestClassifier


# ---------------------------------------------------------------------
# Basic helpers and model construction
# ---------------------------------------------------------------------


def make_train_test_split(X, y, test_size: float = 0.2, random_state: int = 42):
    """Split features and labels into train and test sets."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    return X_train, X_test, y_train, y_test


def model_config_builder(models: list[dict]) -> dict:
    """Build model instances from the config.models list."""
    model_dict = {
        "logreg": LogisticRegression,
        "rf": RandomForestClassifier,
        "lightgbm": LGBMClassifier,
    }

    models_with_params: dict[str, object] = {}
    for model in models:
        cls = model_dict[model["name"]]
        models_with_params[model["name"]] = cls(**model["params"])

    return models_with_params


def set_name(model_name: str, d30: bool = True) -> str:
    """Append horizon suffix ('_d30' or '_d90') to model name."""
    return model_name + ("_d30" if d30 else "_d90")


def get_cv_columns() -> list[str]:
    """Build column names for per-model cross‑validation metrics."""
    cols: list[str] = []
    for col in cv_scoring:
        cols.append(col)
        cols.append(f"{col}_std")
    return cols


def build_pipeline(model_name: str, model) -> Pipeline:
    """Create a pipeline with standard scaling and the given model."""
    return Pipeline([("scaler", StandardScaler()), (model_name, model)])

def _build_model_path(
        name: str,
        suffix: str = "",
    ) -> Path:
        """
        Build filesystem path for a saved model.

        Examples:
        - models/logreg__readmit_30d.pkl
        - models/logreg__readmit_30d__v1.pkl  (if suffix='v1')
        """
        models_dir = Path("D:\\Python Projects\\Hospital readmission risk\\.secrets\\models")
        models_dir.mkdir(parents=True, exist_ok=True)

        base = f"{name}"
        if suffix != "":
            base = f"{base}_{suffix}"

        filename = f"{base}.pkl"
        return models_dir / filename


def save_model(
        name: str,
        estimator: Pipeline,
        suffix: str = "",
    ) -> Path:
        """
        Save a fitted Pipeline to disk.
        """
        path = _build_model_path(name=name, suffix=suffix)
        joblib.dump(estimator, path)
        return path


# ---------------------------------------------------------------------
# Cross‑validation and training
# ---------------------------------------------------------------------


def evaluate_with_cv(
    pipe: Pipeline,
    X: pd.DataFrame,
    y: pd.Series,
    model_key: str,
    log_df: pd.DataFrame,
) -> pd.DataFrame:
    """Run stratified CV and log per‑fold ROC AUC and average precision."""
    cv_results = cross_validate(
        estimator=pipe,
        X=X,
        y=y,
        cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
        scoring=cv_scoring,
        return_train_score=False,
    )

    scores = cv_results["test_roc_auc"]
    aps = cv_results["test_average_precision"]

    fold_df = pd.DataFrame(
        {
            "model": model_key,
            "fold": range(len(scores)),
            "roc_auc": scores,
            "average_precision": aps,
        }
    )

    return pd.concat([log_df, fold_df], ignore_index=True)


def train_model(
    X: pd.DataFrame,
    y: pd.Series,
    model_name: str,
    model,
    cv_log: pd.DataFrame,
    d30: bool = True,
    skip_cross_val: bool = False,
):
    """Build and fit a pipeline for one model and horizon."""
    model_name_full = set_name(model_name, d30)
    pipe = build_pipeline(model_name_full, model)

    if not skip_cross_val:
        cv_log = evaluate_with_cv(pipe, X, y, model_name_full, cv_log)

    pipe.fit(X, y)
    model_path = save_model(
        name=model_name_full,
        estimator=pipe,
        suffix="old"
        )
    return pipe, model_name_full, cv_log


def get_predictions(
    X: pd.DataFrame,
    pipe: Pipeline,
    model_name: str,
    pred_values: pd.DataFrame,
):
    """Get predicted probabilities and labels for a fitted pipeline."""
    y_proba = pipe.predict_proba(X)[:, 1]
    y_pred = pipe.predict(X)

    pred_values[model_name] = y_proba
    return y_proba, y_pred, pred_values


# ---------------------------------------------------------------------
# Metric helpers and coefficients
# ---------------------------------------------------------------------


def get_continuous_metrics(y: pd.Series, y_proba: np.ndarray) -> dict:
    """Compute probability-based metrics: ROC AUC, PR AUC, Brier."""
    return {
        "roc": roc_auc_score(y, y_proba),
        "pr": average_precision_score(y, y_proba),
        "brier_loss_total": brier_score_loss(y, y_proba),
    }


def get_discrete_metrics(y: pd.Series, y_pred: np.ndarray) -> dict:
    """Compute threshold-based metrics: precision, recall, F1."""
    precision, recall, f1, _ = precision_recall_fscore_support(
        y, y_pred, average="binary"
    )
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def get_normalized_coefs(coefs: np.ndarray) -> np.ndarray:
    """L1-normalise coefficient or importance vector."""
    total = np.sum(np.abs(coefs))
    return coefs / total if total != 0 else coefs


def get_coefs(
    pipe: Pipeline,
    model_name: str,
    coefs: pd.DataFrame,
) -> pd.DataFrame:
    """Extract coefficients or feature importances from a fitted pipeline."""
    est = pipe.named_steps[model_name]

    if isinstance(est, LogisticRegression):
        coefs[model_name] = est.coef_[0]
    elif hasattr(est, "feature_importances_"):
        coefs[model_name] = est.feature_importances_

    norm_name = "norm_" + model_name
    coefs[norm_name] = get_normalized_coefs(coefs[model_name].values)

    return coefs


def evaluate_model(
    X: pd.DataFrame,
    y_true: pd.Series,
    model_name: str,
    pipe: Pipeline,
    coefs: pd.DataFrame,
    metrics: pd.DataFrame,
    pred_values: pd.DataFrame,
):
    """Run prediction, metrics, and coef extraction for one model."""
    y_proba, y_pred, pred_values = get_predictions(
        X, pipe, model_name, pred_values
    )

    new_metrics = get_continuous_metrics(y_true, y_proba)
    new_metrics.update(get_discrete_metrics(y_true, y_pred))

    new_row = pd.DataFrame(new_metrics, index=[model_name])
    metrics = pd.concat([metrics, new_row])

    coefs = get_coefs(pipe, model_name, coefs)

    return coefs, metrics, pred_values


# ---------------------------------------------------------------------
# Training for both horizons + global evaluation
# ---------------------------------------------------------------------


def build_model(
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_test: pd.DataFrame,
    name: str,
    models: dict,
    coefs: pd.DataFrame,
    metrics_log: pd.DataFrame,
    pred_values: pd.DataFrame,
    cv_log: pd.DataFrame,
    d30: bool,
    skip_cross_val: bool,
):
    """Train and evaluate a single model for one horizon (30d or 90d)."""
    col = "readmit_30d" if d30 else "readmit_90d"

    trained_pipe, full_name, cv_log = train_model(
        X_train,
        y_train[col],
        name,
        models[name],
        cv_log,
        d30=d30,
        skip_cross_val=skip_cross_val,
    )

    coefs, metrics_log, pred_values = evaluate_model(
        X_test,
        y_test[col],
        full_name,
        trained_pipe,
        coefs,
        metrics_log,
        pred_values,
    )

    return coefs, metrics_log, pred_values, cv_log


def build_both_models(
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_test: pd.DataFrame,
    name: str,
    models: dict,
    coefs: pd.DataFrame,
    metrics_log: pd.DataFrame,
    pred_values: pd.DataFrame,
    cv_log: pd.DataFrame,
    skip_cross_val: bool,
):
    """Train and evaluate the same model for 30d and 90d horizons."""
    coefs, metrics_log, pred_values, cv_log = build_model(
        X_train,
        y_train,
        X_test,
        y_test,
        name,
        models,
        coefs,
        metrics_log,
        pred_values,
        cv_log,
        d30=True,
        skip_cross_val=skip_cross_val,
    )
    """
    coefs, metrics_log, pred_values, cv_log = build_model(
        X_train,
        y_train,
        X_test,
        y_test,
        name,
        models,
        coefs,
        metrics_log,
        pred_values,
        cv_log,
        d30=False,
        skip_cross_val=skip_cross_val,
    )
    """
    return coefs, metrics_log, pred_values, cv_log


def merge_predictions(source: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate and sort prediction tables from multiple sources."""
    values = pd.DataFrame(columns=source[0].columns)
    for table in source:
        values = pd.concat([values, table])
    values = values.sort_index()
    return values


def build_and_evaluate_models(
    models,
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_test: pd.DataFrame,
    skip_cross_val: bool = False,
):
    """Train and evaluate all models for 30d and 90d horizons."""
    models_built = model_config_builder(models)

    cv_log = pd.DataFrame(columns=get_cv_columns())
    coefs = pd.DataFrame(index=X_train.columns)
    pred_values = y_test.copy()
    metrics_log = pd.DataFrame(columns=pred_metrics + proba_metrics)

    for name in models_built:
        coefs, metrics_log, pred_values, cv_log = build_both_models(
            X_train,
            y_train,
            X_test,
            y_test,
            name,
            models_built,
            coefs,
            metrics_log,
            pred_values,
            cv_log,
            skip_cross_val=skip_cross_val,
        )

    if "rel_readmit_30d" in pred_values.columns:
        pred_values = pred_values.drop(columns=["rel_readmit_30d", "rel_readmit_90d", "readmit_90d"])

    pred_values.to_csv("D:\\Python Projects\\Hospital readmission risk\\scripts\\data\\artifacts\\pred_values_old.csv")

    return {
        "coefs": coefs,
        "metrics_log": metrics_log,
        "pred_values": pred_values,
        "cv_log": cv_log,
    }


# ---------------------------------------------------------------------
# Threshold analysis
# ---------------------------------------------------------------------


def build_thresholds(values: pd.DataFrame) -> pd.DataFrame:
    """Generate binary predictions for a grid of thresholds per model."""
    thresholds = pd.DataFrame(index=values.index)

    for col in values.columns:
        if "_d" in col:
            for t in [round(t, 2) for t in np.arange(0.05, 1, 0.05)]:
                thresholds[col + "_" + str(t)] = (values[col] >= t).astype(int)
        else:
            thresholds[col] = values[col]

    return thresholds


def calc_threshold_metrics(
    thresholds: pd.DataFrame,
    metrics: pd.DataFrame,
) -> pd.DataFrame:
    """Compute confusion counts and discrete metrics for each threshold."""
    for model_threshold in thresholds.columns:
        data: dict[str, float] = {}

        if model_threshold not in ["readmit_30d", "readmit_90d"]:
            
            true_col = "readmit_30d"
            if "_d30" not in model_threshold:
                true_col = "readmit_90d"

            data.update(
                {
                    "TP": (
                        (thresholds[model_threshold] == 1)
                        & (thresholds[true_col] == 1)
                    ).sum(),
                    "FP": (
                        (thresholds[model_threshold] == 1)
                        & (thresholds[true_col] == 0)
                    ).sum(),
                    "FN": (
                        (thresholds[model_threshold] == 0)
                        & (thresholds[true_col] == 1)
                    ).sum(),
                    "TN": (
                        (thresholds[model_threshold] == 0)
                        & (thresholds[true_col] == 0)
                    ).sum(),
                }
            )

            y_true = thresholds[true_col].astype(int)
            data.update(
                get_discrete_metrics(y_true, thresholds[model_threshold].values)
            )

            metrics[model_threshold] = pd.Series(data)

    return metrics


def build_threshold_metrics(values: pd.DataFrame):
    """Build thresholded predictions and metrics for a grid of thresholds."""
    thresholds = build_thresholds(values)

    metrics_index = ["TP", "FP", "FN", "TN", "precision", "recall", "f1"]
    metrics = pd.DataFrame(index=metrics_index)

    metrics = calc_threshold_metrics(thresholds, metrics)

    thresholds.to_csv("D:\\Python Projects\\Hospital readmission risk\\scripts\\data\\artifacts\\thresholds_old.csv")
    metrics.to_csv("D:\\Python Projects\\Hospital readmission risk\\scripts\\data\\artifacts\\threshold_metrics_old.csv")

    return thresholds, metrics
