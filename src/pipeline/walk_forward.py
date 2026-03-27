from __future__ import annotations

import calendar
import json
from datetime import date
from pathlib import Path

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
    ):
        self.transformer = transformer
        self.dict_builder = dict_builder
        self.loader = loader
        self.recipe_path = recipe_path
        self.project_root = project_root
        self.watermark_path = Path(watermark_path).expanduser().resolve()
        self.logger = get_logger(__name__)

    # ---------- watermark ----------

    def _read_watermark(self) -> dict:
        with self.watermark_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def initialize_watermark(self, simulation_start: "date", base_cutoff_date: "date") -> None:
        """
        Write the initial watermark after Phase 1 base load completes.

        Derives next_end_date as the last day of simulation_start's month.
        Sets last_processed_date to base_cutoff_date (last day before simulation window).

        Parameters
        ----------
        simulation_start : date
            First day of the first simulation month (from SyntheaSegmenter).
        base_cutoff_date : date
            Last day of the pre-simulation period (from SyntheaSegmenter).
        """
        last_day = calendar.monthrange(simulation_start.year, simulation_start.month)[1]
        next_end_date = date(simulation_start.year, simulation_start.month, last_day).isoformat()
        self._write_watermark(
            last_processed_date=base_cutoff_date.isoformat(),
            next_end_date=next_end_date,
        )

    def _write_watermark(self, last_processed_date: str, next_end_date: str) -> None:
        data = {
            "last_processed_date": last_processed_date,
            "next_end_date": next_end_date,
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
