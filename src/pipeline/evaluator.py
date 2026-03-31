# evaluator.py

from __future__ import annotations

import json
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

from src.utils.logger import get_logger

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
    reports_dir: Optional[Path] = None

    def __post_init__(self):
        self.logger = get_logger(__name__)
        if self.reports_dir is None:
            self.reports_dir = Path(self.cfg_mgr.get_reports_dir())

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
        out_dir = self.reports_dir

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
        out_dir = self.reports_dir

        thr_path = out_dir / "thresholds.csv"
        thresholds.to_csv(thr_path, index=True)

        if threshold_metrics is not None:
            metrics_path = out_dir / "threshold_metrics.csv"
            threshold_metrics.to_csv(metrics_path, index=True)

    # ------------------------------------------------------------------
    # PSI helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _psi_score(baseline: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
        """Population Stability Index on predicted probability arrays."""
        bins = np.linspace(0, 1, n_bins + 1)
        bins[0] = -np.inf
        bins[-1] = np.inf

        base_counts, _ = np.histogram(baseline, bins=bins)
        curr_counts, _ = np.histogram(current, bins=bins)

        base_pct = (base_counts + 1e-8) / len(baseline)
        curr_pct = (curr_counts + 1e-8) / len(current)

        return float(np.sum((curr_pct - base_pct) * np.log(curr_pct / base_pct)))

    def save_psi_baseline(
        self,
        scores: dict[str, np.ndarray],
        path: str | Path,
    ) -> None:
        """
        Persist training-set score distributions to JSON.

        Parameters
        ----------
        scores : dict mapping model_key (e.g. 'logreg_d30') to 1-D probability array.
        path   : destination file, e.g. 'predictions/psi_baseline.json'
        """
        out = {k: v.tolist() for k, v in scores.items()}
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f)
        self.logger.info("[PSI] Baseline saved to %s", path)

    def load_psi_baseline(self, path: str | Path) -> dict[str, np.ndarray]:
        """Load PSI baseline distributions from JSON."""
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {k: np.array(v) for k, v in raw.items()}

    def compute_psi(
        self,
        baseline_path: str | Path,
        X: pd.DataFrame,
        model_names: Optional[List[str]] = None,
    ) -> dict[str, float]:
        """
        Compute PSI for each active model against saved baseline distributions.

        Returns dict mapping model_key -> psi_score.
        """
        baseline = self.load_psi_baseline(baseline_path)
        if model_names is None:
            model_names = self.cfg_mgr.list_active_models()

        result: dict[str, float] = {}
        for name in model_names:
            if not self.cfg_mgr.is_active(name):
                continue
            for target_col, flag in [("readmit_30d", "d30"), ("readmit_90d", "d90")]:
                key = f"{name}_{flag}"
                pipe = self.registry.load_model(name=name, target=target_col)
                if pipe is None or key not in baseline:
                    continue
                current_scores = pipe.predict_proba(X)[:, 1]
                result[key] = self._psi_score(baseline[key], current_scores)
        return result

    # ------------------------------------------------------------------
    # Monthly walk-forward evaluation
    # ------------------------------------------------------------------

    def evaluate_month(
        self,
        end_date: str,
        predictions_dir: str | Path,
        results_dir: str | Path,
        X: pd.DataFrame,
        index_stay_df: pd.DataFrame,
        psi_scores: dict,
        retrain_triggered: bool = False,
        model_names: Optional[List[str]] = None,
        cost_reducer=None,
    ) -> dict[str, pd.DataFrame]:
        """
        Evaluate prior-month predictions against actuals now in index_stay.

        Flow:
        1. Load prior month predictions from {predictions_dir}/{model}_predictions.csv
        2. Join to index_stay_df on stay_id to get actuals (+ cost cols if cost_reducer given)
        3. Compute roc_auc, avg_precision, precision, recall, f1
        4. Compute PSI on current X against baseline
        5. If cost_reducer: find best_threshold via max pct_saved; compute
           saved_cost, intervention_cost, net_cost at that threshold
        6. Append one row per model to {results_dir}/{model}_results.csv

        Parameters
        ----------
        end_date          : current month-end date string (used for labelling rows)
        predictions_dir   : folder containing {model}_predictions.csv files
        results_dir       : folder for {model}_results.csv output
        X                 : current month feature matrix (for PSI)
        index_stay_df     : full index_stay table loaded from BQ (has stay_id + actuals + cost cols)
        psi_baseline_path : path to psi_baseline.json
        retrain_triggered : whether retrain was triggered this month
        model_names       : subset of models; defaults to all active
        cost_reducer      : optional CostReducer instance for cost metrics
        """
        predictions_dir = Path(predictions_dir)
        results_dir = Path(results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)

        if model_names is None:
            model_names = self.cfg_mgr.list_active_models()

        _COST_COLS = ["stay_id", "total_readmission_cost", "cost_per_day_stay", "avg_cost_of_prev_stays"]
        actuals_cols = ["stay_id", "readmit_30d"]
        if cost_reducer is not None:
            # include cost cols in the actuals join so we only merge once
            actuals_cols += [c for c in _COST_COLS[1:] if c in index_stay_df.columns]
        actuals = index_stay_df[actuals_cols].copy()

        output: dict[str, pd.DataFrame] = {}

        for name in model_names:
            if not self.cfg_mgr.is_active(name):
                continue

            pred_path = predictions_dir / f"{name}_predictions.csv"
            if not pred_path.exists():
                self.logger.warning("[evaluate_month] No predictions file: %s", pred_path)
                continue

            preds = pd.read_csv(pred_path)
            # Keep only prior-month rows (all rows before current end_date)
            prior = preds[preds["end_date"] < end_date]
            if prior.empty:
                self.logger.info("[evaluate_month] No prior predictions for %s", name)
                continue

            merged = prior.merge(actuals, on="stay_id", how="inner")
            if merged.empty:
                self.logger.warning("[evaluate_month] No actuals matched for %s", name)
                continue

            y_true = merged["readmit_30d"].values
            y_proba = merged["prob"].values
            y_pred = (y_proba >= 0.5).astype(int)

            roc = roc_auc_score(y_true, y_proba)
            avg_p = average_precision_score(y_true, y_proba)
            prec, rec, f1, _ = precision_recall_fscore_support(
                y_true, y_pred, average="binary", zero_division=0
            )

            # --- cost reducer ---
            best_threshold = net_cost = saved_cost = intervention_cost = float("nan")
            if cost_reducer is not None:
                model_key = f"{name}_d30"

                # pred_values: readmit_30d actuals + prob column named as model_key
                pred_values = merged[["readmit_30d"]].copy().reset_index(drop=True)
                pred_values[model_key] = merged["prob"].values

                thresholds = self.build_thresholds(pred_values)

                # df_cost: cost cols + prob column named as model_key (for row[model] lookup)
                df_cost = merged[
                    ["total_readmission_cost", "cost_per_day_stay", "avg_cost_of_prev_stays"]
                ].copy().reset_index(drop=True)
                df_cost[model_key] = merged["prob"].values
                thresholds = thresholds.reset_index(drop=True)

                result = cost_reducer._estimate_cost_reduction_single(
                    df_cost=df_cost,
                    df_thresholds=thresholds,
                    prob_red=cost_reducer.def_prob_red,
                    desired_prob_red=cost_reducer.def_desired_prob_red,
                    tuning=True,  # avoids readmit_90d column requirement
                )

                pct_row = result.loc["total_pct_saved"]
                model_threshold_cols = [c for c in pct_row.index if c.startswith(model_key + "_")]
                if model_threshold_cols:
                    best_col = pct_row[model_threshold_cols].idxmax()
                    best_threshold = float(best_col.rsplit("_", 1)[1])
                    net_cost = float(result.loc["total_avoided", best_col])

                    # gross breakdown at best threshold
                    int_days, true_prob_red = cost_reducer._calc_intervention_days()
                    flagged = merged[merged["prob"] >= best_threshold]
                    saved_cost = float(
                        (true_prob_red * flagged["prob"] * flagged["total_readmission_cost"]).sum()
                    )
                    intervention_cost = float(
                        flagged.apply(
                            lambda r: cost_reducer._estimate_intervention_cost(r, int_days),
                            axis=1,
                        ).sum()
                    )

                    self.logger.info(
                        "[evaluate_month] %s cost: best_thr=%.2f saved=%.0f interv=%.0f net=%.0f",
                        name, best_threshold, saved_cost, intervention_cost, net_cost,
                    )

            psi_key = f"{name}_d30"
            row = {
                "end_date": end_date,
                "model_name": name,
                "roc_auc": round(roc, 4),
                "avg_precision": round(avg_p, 4),
                "precision": round(prec, 4),
                "recall": round(rec, 4),
                "f1": round(f1, 4),
                "best_threshold": best_threshold,
                "saved_cost": saved_cost,
                "intervention_cost": intervention_cost,
                "net_cost": net_cost,
                "n_predictions": len(merged),
                "n_readmitted": int(y_true.sum()),
                "psi_score": round(psi_scores.get(psi_key, float("nan")), 4),
                "retrain_triggered": retrain_triggered,
            }

            results_path = results_dir / f"{name}_results.csv"
            row_df = pd.DataFrame([row])
            if results_path.exists():
                row_df.to_csv(results_path, mode="a", header=False, index=False)
            else:
                row_df.to_csv(results_path, index=False)

            self.logger.info(
                "[evaluate_month] %s end_date=%s roc=%.3f psi=%.3f",
                name, end_date, roc, psi_scores.get(psi_key, float("nan")),
            )
            output[name] = row_df

        return output

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

                tp = safe_get("TP", best_col)
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

            self.logger.debug("model_path: %s, exists: %s", model_path, model_path.exists())
            if model_path.exists():
                self.logger.debug("Loading train_date from model file: %s", model_path)
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
        report_path = str(self.reports_dir) + r"\report.csv"
        report_old = pd.read_csv(report_path).set_index("model_name").sort_index()
        report = pd.concat([report_old, report_df])
        report.to_csv(report_path)
        return report

    