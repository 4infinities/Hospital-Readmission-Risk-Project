# evaluator.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    precision_recall_fscore_support,
    brier_score_loss,
)
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from .model_registry import ModelRegistry
from .model_config_manager import ModelConfigManager


@dataclass
class Evaluator:
    """
    Evaluate already-trained readmission models.

    Responsibilities:
    - Load final fitted Pipelines from ModelRegistry.
    - Compute test-set probability-based and threshold-based metrics.
    - Extract model coefficients / feature importances.
    - Build per-threshold confusion metrics for 30d/90d horizons.

    No cross-validation, no model construction, no fitting.
    """

    registry: ModelRegistry
    cfg_mgr: ModelConfigManager
    artifacts_dir: Path = Path("data") / "artifacts"

    # ------------------------------------------------------------------
    # Metric helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _continuous_metrics(y: pd.Series, y_proba: np.ndarray) -> Dict[str, float]:
        return {
            "roc": roc_auc_score(y, y_proba),
            "pr": average_precision_score(y, y_proba),
            "brier_loss_total": brier_score_loss(y, y_proba),
        }

    @staticmethod
    def _discrete_metrics(y: pd.Series, y_pred: np.ndarray) -> Dict[str, float]:
        precision, recall, f1, _ = precision_recall_fscore_support(
            y, y_pred, average="binary"
        )
        return {
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }

    @staticmethod
    def _normalize_coefs(coefs: np.ndarray) -> np.ndarray:
        total = np.sum(np.abs(coefs))
        return coefs / total if total != 0 else coefs

    def _extract_coefs(
        self,
        pipe: Pipeline,
        step_name: str,
        coefs: pd.DataFrame,
    ) -> pd.DataFrame:
        est = pipe.named_steps[step_name]

        if isinstance(est, LogisticRegression):
            coefs[step_name] = est.coef_[0]
        elif hasattr(est, "feature_importances_"):
            coefs[step_name] = est.feature_importances_

        norm_name = "norm_" + step_name
        coefs[norm_name] = self._normalize_coefs(coefs[step_name].values)

        return coefs

    # ------------------------------------------------------------------
    # Save path
    # ------------------------------------------------------------------   

    def _ensure_dir(self) -> Path:
        """
        Ensure data/artifacts exists and return its Path.
        """
        path = self.artifacts_dir
        path.mkdir(parents=True, exist_ok=True)
        return path

    def save_predictions_to_csv(
        self,
        pred_values: pd.DataFrame,
        metrics_log: Optional[pd.DataFrame] = None,
    ):
        """
        Save pred_values and metrics_log to CSV in data/artifacts with fixed names:
          - pred_values.csv
          - metrics_log.csv
        Overwrites on each run.
        """
        out_dir = self._ensure_dir()

        pred_path = out_dir / "pred_values.csv"
        pred_values.to_csv(pred_path, index=True)

        if metrics_log is not None:
            metrics_path = out_dir / "metrics_log.csv"
            metrics_log.to_csv(metrics_path, index=True)

    def save_thresholds_to_csv(
        self,
        thresholds: pd.DataFrame,
        threshold_metrics: Optional[pd.DataFrame] = None,
    ):
        """
        Save thresholds and threshold_metrics to CSV in data/artifacts with fixed names:
          - thresholds.csv
          - threshold_metrics.csv
        Overwrites on each run.
        """
        out_dir = self._ensure_dir()

        thr_path = out_dir / "thresholds.csv"
        thresholds.to_csv(thr_path, index=True)

        if threshold_metrics is not None:
            metrics_path = out_dir / "threshold_metrics.csv"
            threshold_metrics.to_csv(metrics_path, index=True)

    # ------------------------------------------------------------------
    # Main evaluation API (no CV)
    # ------------------------------------------------------------------

    def evaluate_models(
        self,
        X: pd.DataFrame,
        y: pd.DataFrame,
        model_names: Optional[List[str]] = None,
        suffix: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Evaluate final models saved by ModelRegistry.

        Uses:
          - registry.load_model(name, target, suffix)
          - cfg_mgr.list_active_models(), is_active()

        Parameters
        ----------
        X : pd.DataFrame
            Feature matrix for evaluation (e.g. held-out test set).
        y : pd.DataFrame
            Targets; must contain 'readmit_30d' and 'readmit_90d'.
        model_names : list[str], optional
            Subset of models to evaluate; if None, uses all active models.
        suffix : str, optional
            Suffix used when saving models via ModelRegistry (e.g. profile/run ID).

        Returns
        -------
        dict with keys:
          - coefs: DataFrame of coefficients/feature importances
          - metrics_log: DataFrame of per-model test metrics
          - pred_values: DataFrame with true labels + per-model probabilities
        """
        if model_names is None:
            model_names = self.cfg_mgr.list_active_models()

        proba_metrics = ["roc", "pr", "brier_loss_total"]
        pred_metrics = ["precision", "recall", "f1"]

        metrics_log = pd.DataFrame(columns=pred_metrics + proba_metrics)
        coefs = pd.DataFrame(index=X.columns)
        pred_values = y.copy()

        for name in model_names:
            if not self.cfg_mgr.is_active(name):
                continue

            # Evaluate both horizons: 30d and 90d
            for is_30d in [True, False]:
                target_col = "readmit_30d" if is_30d else "readmit_90d"
                model_key = f"{name}_{'d30' if is_30d else 'd90'}"

                y_true = y[target_col]

                # Load final fitted model from registry
                pipe = self.registry.load_model(
                    name=name,
                    target=target_col,
                    suffix=suffix,
                )

                y_proba = pipe.predict_proba(X)[:, 1]
                y_pred = pipe.predict(X)

                # store probabilities
                pred_values[model_key] = y_proba

                # metrics
                cont = self._continuous_metrics(y_true, y_proba)
                disc = self._discrete_metrics(y_true, y_pred)
                row = {**cont, **disc}
                metrics_log.loc[model_key] = row

                # coefficients / feature importances
                coefs = self._extract_coefs(pipe, step_name=name, coefs=coefs)

        # drop relative flags if present
        for col in ["rel_readmit_30d", "rel_readmit_90d"]:
            if col in pred_values.columns:
                pred_values = pred_values.drop(columns=[col])

        self.save_predictions_to_csv(
        pred_values=pred_values,
        metrics_log=metrics_log,
        )

        return {
            "coefs": coefs,
            "metrics_log": metrics_log,
            "pred_values": pred_values,
        }

    # ------------------------------------------------------------------
    # Thresholds and threshold metrics
    # ------------------------------------------------------------------

    def build_thresholds(self, values: pd.DataFrame) -> pd.DataFrame:
        """
        Generate binary predictions for a grid of thresholds per model.
        """
        thresholds = pd.DataFrame(index=values.index)

        for col in values.columns:
            if "_d" in col:  # probability columns (e.g. logreg_d30, rf_d90)
                for t in [round(t, 2) for t in np.arange(0.05, 1, 0.05)]:
                    thresholds[f"{col}_{t}"] = (values[col] >= t).astype(int)
            else:
                thresholds[col] = values[col]

        return thresholds

    def build_threshold_metrics(
        self,
        values: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """
        Build thresholded predictions and metrics for a grid of thresholds.

        Returns:
          - thresholds: binary predictions for each model/threshold
          - threshold_metrics: confusion counts + discrete metrics per threshold column
        """
        thresholds = self.build_thresholds(values)

        metrics_index = ["TP", "FP", "FN", "TN", "precision", "recall", "f1"]
        metrics = pd.DataFrame(index=metrics_index)

        for model_threshold in thresholds.columns:
            data: Dict[str, float] = {}

            if model_threshold not in ["readmit_30d", "readmit_90d"]:
                true_col = "readmit_30d"
                if "_d30" not in model_threshold:
                    true_col = "readmit_90d"

                y_true = thresholds[true_col].astype(int)
                y_hat = thresholds[model_threshold].values

                data.update(
                    {
                        "TP": ((y_hat == 1) & (y_true == 1)).sum(),
                        "FP": ((y_hat == 1) & (y_true == 0)).sum(),
                        "FN": ((y_hat == 0) & (y_true == 1)).sum(),
                        "TN": ((y_hat == 0) & (y_true == 0)).sum(),
                    }
                )

                data.update(self._discrete_metrics(y_true, y_hat))
                metrics[model_threshold] = pd.Series(data)

        self.save_thresholds_to_csv(
        thresholds=thresholds,
        threshold_metrics=metrics,
        )

        return {
            "thresholds": thresholds,
            "threshold_metrics": metrics,
        }

    