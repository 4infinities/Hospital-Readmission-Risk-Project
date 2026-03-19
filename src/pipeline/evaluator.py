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

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

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
        out_dir = self.artifacts_dir

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
        out_dir = self.artifacts_dir

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
                # Load final fitted model from registry
                target_col = "readmit_30d" if is_30d else "readmit_90d"
                pipe = self.registry.load_model(
                    name=name,
                    target=target_col,
                    suffix=suffix,
                )

                if pipe is None:
                    continue

                model_key = f"{name}_{'d30' if is_30d else 'd90'}"

                y_true = y[target_col]


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
                for t in [round(t, 2) for t in np.arange(0.5, 1, 0.05)]:
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

    def build_performance_report(
        self,
        pct_avoided: pd.DataFrame,
        avoided: pd.DataFrame,
        threshold_metrics: pd.DataFrame,
        dataset_end_date: pd.Timestamp,
        suffix: str = None
    ) -> pd.DataFrame:
        """
        Build a one-row-per-model performance report.

        Parameters
        ----------
        pct_avoided : DataFrame
            Single-row DF, columns like 'logreg_d30_0.5', 'rf_d90_0.75', ...
            Values are % of readmission cost avoided at that threshold.
        avoided : DataFrame
            Single-row DF, same columns as pct_avoided, values are absolute
            readmission cost avoided.
        threshold_metrics : DataFrame
            Rows: TP, FP, FN, TN, precision, recall, f1.
            Columns: same threshold names as pct_avoided / avoided (or subset).
        model_file_dir : Path
            Directory containing model files, named e.g. 'logreg_d30.pkl'.
        dataset_end_date : Timestamp
            End date of the dataset used for training.
        """

        if pct_avoided.empty:
            raise ValueError("pct_avoided is empty.")
        if avoided.empty:
            raise ValueError("avoided is empty.")

        # We assume there is one interesting row (e.g. 'total_pct_avoided_0.2_0.1').
        pct_row = pct_avoided.iloc[0]
        avoided_row = avoided.iloc[0]

        # Track best threshold per model_group
        best_by_group: dict[str, dict[str, Optional[float]]] = {}

        """GOOD"""
        for col in pct_row.index:
            # Skip columns that are not present in threshold_metrics at all
            # (you said that's acceptable).
            if col not in threshold_metrics:
                continue
            # Parse 'logreg_d30_0.5' -> model_group='logreg_d30', threshold='0.5'
            try:
                model_group, threshold_str = col.rsplit("_", 1)
            except ValueError:
                # Column name doesn't follow pattern; skip
                continue

            pct_value = pct_row[col]
            cost_value = avoided_row[col]

            group_info = best_by_group.get(model_group)
            if group_info is None:
                best_by_group[model_group] = {
                    "col": col,
                    "threshold": float(threshold_str),
                    "pct_saved": pct_value,
                    "cost_saved": cost_value,
                }
            else:
                # Replace if this threshold has higher pct_saved
                if pct_value > group_info["pct_saved"]:
                    group_info["col"] = col
                    group_info["threshold"] = threshold_str
                    group_info["pct_saved"] = pct_value
                    group_info["cost_saved"] = cost_value
        """GOOD"""
        rows = []

        for model_group, info in best_by_group.items():
            best_col = info["col"]
            best_threshold = info["threshold"]
            best_pct_saved = info["pct_saved"]
            best_cost_saved = info["cost_saved"]

            # Default metric values
            tp = fp = fn = tn = np.nan
            precision = recall = f1 = np.nan

            if best_col in threshold_metrics:
                    # threshold_metrics rows: TP, FP, FN, TN, precision, recall, f1
                    # case-sensitive: adjust if your actual row labels differ
                def safe_get(row_name: str, col_name: str) -> float:
                    if row_name in threshold_metrics.index:
                        return threshold_metrics.loc[row_name, col_name]
                    return np.nan

                tp = threshold_metrics.loc["TP", best_col]
                fp = safe_get("FP", best_col)
                fn = safe_get("FN", best_col)
                tn = safe_get("TN", best_col)
                precision = safe_get("precision", best_col)
                recall = safe_get("recall", best_col)
                f1 = safe_get("f1", best_col)

                # Training date from model file (e.g. 'logreg_d30.pkl')
            models_dir = Path(self.cfg_mgr.get_models_dir())
            if suffix is not None:
                model_path = models_dir / f"{model_group}_{suffix}.pkl"
            else:
                model_path = models_dir / f"{model_group}.pkl"

            print("DEBUG model_path:", model_path, "exists:", model_path.exists())
            if model_path.exists():
                print("gets in")
                train_date = pd.to_datetime(model_path.stat().st_mtime, unit="s")
            else:
                train_date = pd.NaT

            rows.append(
                    {
                        "model_name": model_group,
                        "best_threshold": float(best_threshold),
                        "pct_cost_saved": float(best_pct_saved),
                        "cost_saved": float(best_cost_saved),
                        "train_date": train_date,
                        "dataset_end_date": dataset_end_date,
                        "tp": tp,
                        "tn": tn,
                        "fp": fp,
                        "fn": fn,
                        "precision": precision,
                        "recall": recall,
                        "f1": f1,
                    }
                )

        report_df = pd.DataFrame(rows).set_index("model_name").sort_index()
        report_path = str(self.artifacts_dir) + r"\report.csv"
        report_old = pd.read_csv(report_path).set_index("model_name").sort_index()
        report = pd.concat([report_old, report_df])
        report.to_csv(report_path)
        return report

    