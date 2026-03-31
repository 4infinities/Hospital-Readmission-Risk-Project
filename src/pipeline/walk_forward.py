from __future__ import annotations

import calendar
import json
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline.bq_loader import BigQueryLoader
from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionary_builder import DictionaryBuilder
from src.utils.logger import get_logger

_BOOTSTRAP_DATE_COLS: dict[str, str] = {
    "encounters":  "start",
    "careplans":   "start",
    "conditions":  "start",
    "medications": "start",
    "procedures":  "start",
    "claims":      "currentillnessdate",  # matches SyntheaSegmenter.DATE_COLUMN
}


class WalkForwardOrchestrator:
    """
    Orchestrates one monthly walk-forward update step.

    Dependency order per month
    --------------------------
    D1  diagnoses_dictionary delta
    D2  procedures_dictionary delta
    D3  main_diagnoses delta
    D4  careplans_related_encounters delta
    H1–H5  helper tables DELETE+REBUILD (recipe index 3)
    D5  related_diagnoses delta
    I1  index_stay DELETE+REBUILD (recipe index 4)

    Usage
    -----
    orch = WalkForwardOrchestrator(transformer, dict_builder, recipe_path, project_root)
    orch.run_month("2015-01-31")          # single month, explicit date
    orch.run_next_month()                 # reads watermark, runs, advances
    orch.run_until("2024-12-31")          # loop until final end date
    """

    HELPER_RECIPE_ID = 3
    INDEX_RECIPE_ID = 4
    SLIM_RECIPE_ID = 5

    def __init__(
        self,
        transformer: BigQueryTransformer,
        dict_builder: DictionaryBuilder,
        loader: BigQueryLoader,
        recipe_path: str,
        project_root: str,
        watermark_path: str = "config/watermark.json",
        preprocessor=None,       # DataPreprocessor — required for ML
        registry=None,           # ModelRegistry — required for ML
        tuner=None,              # HyperparameterTuner — required for retune
        evaluator=None,          # Evaluator — required for ML
        cost_reducer=None,       # CostReducer — optional, passed to evaluate_month
        predictions_dir: str = "predictions",
        results_dir: str = "results",
        psi_baseline_path: str = "predictions/psi_baseline.json",
        index_stay_sql_path: Optional[str] = None,
    ):
        self.transformer = transformer
        self.dict_builder = dict_builder
        self.loader = loader
        self.recipe_path = recipe_path
        self.project_root = project_root
        self.watermark_path = Path(watermark_path).expanduser().resolve()
        self.preprocessor = preprocessor
        self.registry = registry
        self.tuner = tuner
        self.evaluator = evaluator
        self.cost_reducer = cost_reducer
        self.predictions_dir = Path(predictions_dir)
        self.results_dir = Path(results_dir)
        self.psi_baseline_path = Path(psi_baseline_path)
        self.index_stay_sql_path = Path(index_stay_sql_path).expanduser().resolve() if index_stay_sql_path else None
        self.logger = get_logger(__name__)

    # ---------- watermark ----------

    def _read_watermark(self) -> dict:
        with self.watermark_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _write_watermark(self, last_processed_date: str, next_end_date: str) -> None:
        # Preserve simulation_end_date — written once by SyntheaSegmenter, never overwritten here
        simulation_end_date = ""
        if self.watermark_path.exists():
            with self.watermark_path.open("r", encoding="utf-8") as f:
                existing = json.load(f)
            simulation_end_date = existing.get("simulation_end_date", "")

        data = {
            "last_processed_date": last_processed_date,
            "next_end_date": next_end_date,
            "simulation_end_date": simulation_end_date,
        }
        with self.watermark_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        self.logger.info(
            "Watermark updated: last_processed=%s, next=%s",
            last_processed_date,
            next_end_date,
        )

    @staticmethod
    def _advance_end_date(end_date: str) -> str:
        """Return the last day of the month following end_date."""
        d = date.fromisoformat(end_date)
        next_month = d.month % 12 + 1
        next_year = d.year + (1 if d.month == 12 else 0)
        last_day = calendar.monthrange(next_year, next_month)[1]
        return date(next_year, next_month, last_day).isoformat()

    # ---------- bootstrap ----------

    def bootstrap_prior_month_staging(self, first_end_date: str) -> None:
        """
        Create prior-month BQ staging tables needed by month-1 helper/delta SQL.

        On the first simulation month every helper/delta UPDATE SQL unions
        {{PREV_END_DATE_SAFE}} staging tables (e.g. encounters_2025_03_31).
        Those tables never exist because that month was loaded as part of the
        base bulk file, not a monthly segment.  This method creates them via
        CREATE OR REPLACE TABLE ... AS SELECT from the base tables, filtered to
        the prior calendar month.

        Call once, before run_month(first_end_date) or run_next_month().

        Parameters
        ----------
        first_end_date : str
            Month-end date of the first simulation month (e.g. '2025-04-30').
            Must match the current next_end_date in watermark.json.
        """
        prev_safe  = BigQueryTransformer._prev_end_date_safe(first_end_date)   # '2025_03_31'
        prev_iso   = prev_safe.replace("_", "-")                                # '2025-03-31'
        month_start = date.fromisoformat(prev_iso).replace(day=1).isoformat()  # '2025-03-01'

        slim = self.transformer.dataset_slim_fq
        raw  = self.transformer.dataset_raw_fq

        for table, col in _BOOTSTRAP_DATE_COLS.items():
            target = f"{table}_{prev_safe}"
            sql = (
                f"CREATE OR REPLACE TABLE `{raw}.{target}` AS\n"
                f"SELECT * FROM `{slim}.{table}_slim`\n"
                f"WHERE DATE({col}) >= '{month_start}'\n"
                f"  AND DATE({col}) <= '{prev_iso}'"
            )
            self.logger.info("[bootstrap] Creating prior-month staging: %s", target)
            self.transformer._run_query(sql)

        self.logger.info(
            "[bootstrap] Prior-month staging complete: window %s – %s",
            month_start,
            prev_iso,
        )

    # ---------- ML helpers ----------

    def _is_first_run(self) -> bool:
        """True if no predictions file exists yet (Phase 1 / first simulation month)."""
        model_names = self.registry.config_mgr.list_active_models()
        return not any(
            (self.predictions_dir / f"{name}_predictions.csv").exists()
            for name in model_names
        )

    def _fetch_index_stay(self) -> pd.DataFrame:
        """
        Load full index_stay table from BQ using the dedicated selection SQL.

        Uses sql/20_index_stay_selection.sql (path from index_stay_sql_path).
        Transformer substitutes {{DATASET_HELPERS}} and other standard tokens.
        Falls back to an explicit column list if the SQL path is not configured.
        """
        if self.index_stay_sql_path is None or not self.index_stay_sql_path.exists():
            raise RuntimeError(
                "index_stay_sql_path not set or file not found. "
                "Set 'index_stay_sql' in bigquery_config.json and pass it to WalkForwardOrchestrator."
            )
        with self.index_stay_sql_path.open("r", encoding="utf-8") as f:
            sql_raw = f.read()
        sql = self.transformer._transform_query(sql_raw)
        self.logger.info("[_fetch_index_stay] Loading index_stay from BQ")
        return self.transformer.fetch_to_dataframe(sql=sql, cache_path=None, query=True)

    def _save_predictions(
        self,
        end_date: str,
        X_test: pd.DataFrame,
        stay_ids: pd.Series,
    ) -> None:
        """
        Run predict_proba on X_test for all active models and append to
        predictions/{model}_predictions.csv.

        Columns: stay_id, prob, model_name, end_date
        """
        self.predictions_dir.mkdir(parents=True, exist_ok=True)
        model_names = self.registry.config_mgr.list_active_models()

        for name in model_names:
            pipe = self.registry.load_model(name=name, target="readmit_30d")
            if pipe is None:
                self.logger.warning("[_save_predictions] No model found for %s", name)
                continue

            proba = pipe.predict_proba(X_test)[:, 1]
            df = pd.DataFrame({
                "stay_id": stay_ids.values,
                "prob": proba,
                "model_name": name,
                "end_date": end_date,
            })

            pred_path = self.predictions_dir / f"{name}_predictions.csv"
            if pred_path.exists():
                df.to_csv(pred_path, mode="a", header=False, index=False)
            else:
                df.to_csv(pred_path, index=False)

            self.logger.info(
                "[_save_predictions] %s: %d predictions written for %s",
                name, len(df), end_date,
            )

    def _save_psi_baseline(self, X_train: pd.DataFrame) -> None:
        """
        Compute training-set score distributions and save to psi_baseline.json.
        Called once on the first run.
        """
        model_names = self.registry.config_mgr.list_active_models()
        scores: dict[str, "np.ndarray"] = {}

        for name in model_names:
            pipe = self.registry.load_model(name=name, target="readmit_30d")
            if pipe is None:
                continue
            import numpy as np
            scores[f"{name}_d30"] = pipe.predict_proba(X_train)[:, 1]

        self.evaluator.save_psi_baseline(scores, self.psi_baseline_path)

    def _should_retune(self, end_date: str, psi_scores: dict) -> bool:
        """
        Retune if: month number is divisible by 6, or any PSI score > 0.2.
        Month number is derived from the end_date relative to the simulation start
        by counting how many prediction files have rows (proxy for months elapsed).
        """
        model_names = self.registry.config_mgr.list_active_models()
        # count months elapsed by reading any existing predictions file
        months_elapsed = 0
        for name in model_names:
            p = self.predictions_dir / f"{name}_predictions.csv"
            if p.exists():
                months_elapsed = len(pd.read_csv(p)["end_date"].unique())
                break

        on_schedule = (months_elapsed > 0) and (months_elapsed % 6 == 0)
        psi_breach = any(v > 0.2 for v in psi_scores.values() if v == v)  # nan-safe
        return on_schedule or psi_breach

    def fit_and_evaluate(self, end_date: str) -> None:
        """
        Full ML step for one month: preprocess, evaluate prior, refit, predict.

        First run  : preprocess → retune → refit(force) → predict → save PSI baseline
        Subsequent : evaluate prior → PSI check → retrain? → refit(force) → predict
        """
        if self.preprocessor is None or self.registry is None or self.evaluator is None:
            self.logger.warning("[fit_and_evaluate] ML components not wired — skipping ML step")
            return

        self.logger.info("[ML] Starting fit_and_evaluate for end_date=%s", end_date)

        X_train, y_train, X_test, stay_ids = self.preprocessor.preprocess(
            end_date=end_date,
            transformer=self.transformer,
        )

        target_cols = ["readmit_30d"]
        first_run = self._is_first_run()

        if first_run:
            self.logger.info("[ML] First run — retuning before initial fit")
            if self.tuner is not None:
                self.tuner.tune_models(X_train, y_train)

            self.registry.fit_models(X_train, y_train, target_cols, force=True)
            self._save_predictions(end_date, X_test, stay_ids)
            self._save_psi_baseline(X_train)
            self.logger.info("[fit_and_evaluate] First run complete — predictions and PSI baseline saved")

        else:
            # Evaluate prior month before overwriting models
            index_stay_df = self._fetch_index_stay()
            psi_scores = self.evaluator.compute_psi(self.psi_baseline_path, X_test)
            retrain = self._should_retune(end_date, psi_scores)

            self.evaluator.evaluate_month(
                end_date=end_date,
                predictions_dir=self.predictions_dir,
                results_dir=self.results_dir,
                X=X_test,
                index_stay_df=index_stay_df,
                psi_scores=psi_scores,
                retrain_triggered=retrain,
                cost_reducer=self.cost_reducer,
            )

            if retrain and self.tuner is not None:
                self.logger.info("[ML] Retune triggered for end_date=%s", end_date)
                self.tuner.tune_models(X_train, y_train)

            self.registry.fit_models(X_train, y_train, target_cols, force=True)
            self._save_predictions(end_date, X_test, stay_ids)
            self.logger.info("[fit_and_evaluate] Month complete — predictions saved for %s", end_date)

    # ---------- core ----------

    def run_month(self, end_date: str) -> None:
        """
        Run the full update sequence for one monthly window ending at end_date.

        Parameters
        ----------
        end_date : str
            Month-end date string 'YYYY-MM-DD' (e.g. '2015-01-31').
            SQL derives window_start internally as DATE_TRUNC(end_date, MONTH) - INTERVAL 2 MONTH.
        """
        self.logger.info("=== Walk-forward update: end_date=%s ===", end_date)

        # S0: Load monthly CSV segment into BQ raw staging tables
        self.logger.info("[S0] Loading monthly segment for %s", end_date)
        self.loader.load_monthly_segment(end_date)

        # S0.5: Insert new month's records into master slim tables (recipe 5)
        self.logger.info("[S0.5] Inserting into slim tables for %s", end_date)
        self.transformer.run_query_sequence(
            self.recipe_path, self.SLIM_RECIPE_ID, self.project_root, end_date
        )

        # D1–D4: dictionary deltas (pre-helper)
        self.logger.info("[D1] Updating diagnoses_dictionary")
        self.dict_builder.update_diagnoses_dictionary(end_date)

        self.logger.info("[D2] Updating procedures_dictionary")
        self.dict_builder.update_procedures_dictionary(end_date)

        self.logger.info("[D3] Updating main_diagnoses")
        self.dict_builder.update_main_diagnoses(end_date)

        self.logger.info("[D4] Updating careplans_related_encounters")
        self.dict_builder.update_careplans_related_encounters(end_date)

        # H1–H5: helper table DELETE + REBUILD (recipe index 3)
        self.logger.info("[H1-H5] Running helper table updates")
        self.transformer.run_query_sequence(
            self.recipe_path, self.HELPER_RECIPE_ID, self.project_root, end_date
        )

        # D5: related_diagnoses (post-helper — needs helper_utilization)
        self.logger.info("[D5] Updating related_diagnoses")
        self.dict_builder.update_related_diagnoses(end_date)

        # I1: index_stay DELETE + REBUILD (recipe index 4)
        self.logger.info("[I1] Running index_stay update")
        self.transformer.run_query_sequence(
            self.recipe_path, self.INDEX_RECIPE_ID, self.project_root, end_date
        )

        # ML: preprocess → evaluate prior / first-run → refit → predict
        self.fit_and_evaluate(end_date)

        self.logger.info("=== Walk-forward update complete: end_date=%s ===", end_date)

    def run_next_month(self) -> str:
        """
        Read watermark, run update for next_end_date, advance watermark.

        Returns
        -------
        str
            The end_date that was processed.
        """
        wm = self._read_watermark()

        if wm.get("last_processed_date") is None:
            raise RuntimeError(
                "Watermark last_processed_date is null — Phase 1 base load has not been "
                "confirmed complete. Run initialize_watermark() after Phase 1 before "
                "starting the simulation loop."
            )

        end_date: str | None = wm.get("next_end_date")
        if not end_date:
            raise ValueError(
                "Watermark next_end_date is null — set it in config/watermark.json before running."
            )

        self.run_month(end_date)

        next_end = self._advance_end_date(end_date)
        self._write_watermark(last_processed_date=end_date, next_end_date=next_end)
        return end_date


    def run_until(self, final_end_date: str) -> None:
        """
        Loop run_next_month() until next_end_date would exceed final_end_date.

        Parameters
        ----------
        final_end_date : str
            Inclusive upper bound 'YYYY-MM-DD'. The month whose end_date equals
            final_end_date is included; the next month is not.
        """
        wm = self._read_watermark()
        while (wm.get("next_end_date") or "") <= final_end_date:
            processed = self.run_next_month()
            self.logger.info("Completed month: %s", processed)
            wm = self._read_watermark()
        self.logger.info(
            "run_until complete. last_processed=%s", wm.get("last_processed_date")
        )
