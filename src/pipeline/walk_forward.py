from __future__ import annotations

import calendar
import json
from datetime import date
from pathlib import Path

from pipeline.bq_loader import BigQueryLoader
from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionary_builder import DictionaryBuilder
from src.utils.logger import get_logger


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
