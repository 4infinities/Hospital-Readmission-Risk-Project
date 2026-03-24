from __future__ import annotations

import calendar
import json
import shutil
from datetime import date
from pathlib import Path
from typing import List

import pandas as pd

from src.utils.logger import get_logger


class SyntheaSegmenter:
    """
    Splits raw Synthea CSVs into a base bulk file and monthly segment files.

    Segmentation rule: a record belongs to the month in which its START date falls.
    max_date is anchored to encounters.START only — all other tables use the same window.

    Output layout (per profile):
        {segmented_path}/{table}_base.csv          — pre-simulation bulk file
        {segmented_path}/{table}_YYYY-MM-DD.csv    — one file per simulation month

    patients and organizations are static reference tables: copied to base only.

    After segment() runs, self.simulation_start and self.base_cutoff_date are set.
    """

    # Number of simulation months per profile
    SIMULATION_WINDOWS = {"mock": 12, "refactor": 60}

    # Tables segmented by START column
    SEGMENTED_TABLES = [
        "encounters",
        "careplans",
        "claims",
        "conditions",
        "medications",
        "procedures",
    ]

    # Tables copied as-is (no segmentation, base only)
    STATIC_TABLES = ["patients", "organizations"]

    def __init__(self, config_path: str, profile_name: str):
        self.logger = get_logger(__name__)
        self._config = self._load_json(config_path)
        self.profile_name = profile_name

        if profile_name not in self.SIMULATION_WINDOWS:
            raise ValueError(
                f"Unknown profile '{profile_name}'. "
                f"Must be one of: {list(self.SIMULATION_WINDOWS)}"
            )
        if profile_name not in self._config.get("profiles", {}):
            raise KeyError(f"Profile '{profile_name}' not found in config")

        profile_cfg = self._config["profiles"][profile_name]
        self.source_dir = Path(profile_cfg["local_input_dir"]).expanduser().resolve()
        self.segmented_path = Path(profile_cfg["segmented_path"]).expanduser().resolve()
        self.window_months: int = self.SIMULATION_WINDOWS[profile_name]

        # Set after segment() runs
        self.simulation_start: date | None = None
        self.base_cutoff_date: date | None = None

    @staticmethod
    def _load_json(path: str) -> dict:
        p = Path(path).expanduser().resolve()
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _last_day_of_month(year: int, month: int) -> date:
        return date(year, month, calendar.monthrange(year, month)[1])

    @staticmethod
    def _first_day_of_month(year: int, month: int) -> date:
        return date(year, month, 1)

    @staticmethod
    def _subtract_months(d: date, n: int) -> date:
        """Return the first day of the month n months before d's month."""
        total_months = d.year * 12 + (d.month - 1) - n
        year = total_months // 12
        month = total_months % 12 + 1
        return date(year, month, 1)

    def _derive_window(self, max_date: date) -> tuple[date, date]:
        """
        Derive simulation_start and base_cutoff_date from max_date.

        simulation_start = first day of the month (window_months - 1) before max_date's month.
        base_cutoff_date = last day of the month before simulation_start.
        """
        last_sim_month_start = self._first_day_of_month(max_date.year, max_date.month)
        simulation_start = self._subtract_months(last_sim_month_start, self.window_months - 1)

        cutoff_month = self._subtract_months(simulation_start, 1)
        base_cutoff_date = self._last_day_of_month(cutoff_month.year, cutoff_month.month)

        return simulation_start, base_cutoff_date

    def _generate_month_ends(self, simulation_start: date, max_date: date) -> List[date]:
        """Return a list of month-end dates from simulation_start's month through max_date's month."""
        ends = []
        year, month = simulation_start.year, simulation_start.month
        while (year, month) <= (max_date.year, max_date.month):
            ends.append(self._last_day_of_month(year, month))
            month += 1
            if month > 12:
                month = 1
                year += 1
        return ends

    def _safe_write(self, df: pd.DataFrame, path: Path, overwrite: bool) -> bool:
        """Write df to path. Returns True if written, False if skipped."""
        if path.exists() and not overwrite:
            self.logger.warning("File exists, skipping (overwrite=False): %s", path)
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        self.logger.info("Written: %s (%d rows)", path.name, len(df))
        return True

    def _copy_static(self, table: str, overwrite: bool) -> None:
        """Copy a static reference table CSV to segmented_path as {table}_base.csv."""
        src = self.source_dir / f"{table}.csv"
        dst = self.segmented_path / f"{table}_base.csv"
        if not src.is_file():
            self.logger.warning("Static table not found, skipping: %s", src)
            return
        if dst.exists() and not overwrite:
            self.logger.warning("File exists, skipping (overwrite=False): %s", dst)
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        self.logger.info("Copied static table: %s -> %s", src.name, dst.name)

    def segment(self, overwrite: bool = False) -> None:
        """
        Segment all Synthea CSVs into base and monthly files.

        Parameters
        ----------
        overwrite : bool
            If False (default), skip files that already exist.
            If True, overwrite existing files.
        """
        self.segmented_path.mkdir(parents=True, exist_ok=True)

        # Derive window from encounters.START (anchor table)
        enc_path = self.source_dir / "encounters.csv"
        if not enc_path.is_file():
            raise FileNotFoundError(f"encounters.csv not found in {self.source_dir}")

        self.logger.info("Reading encounters.csv to derive simulation window...")
        enc_dates = pd.read_csv(enc_path, usecols=["START"], parse_dates=["START"])
        max_date: date = enc_dates["START"].max().date()
        self.logger.info("Max encounters START date: %s", max_date)

        simulation_start, base_cutoff_date = self._derive_window(max_date)
        self.simulation_start = simulation_start
        self.base_cutoff_date = base_cutoff_date
        self.logger.info(
            "Simulation window: %s → %s (%d months). Base cutoff: %s",
            simulation_start,
            self._last_day_of_month(max_date.year, max_date.month),
            self.window_months,
            base_cutoff_date,
        )

        month_ends = self._generate_month_ends(simulation_start, max_date)

        # Segment each table
        for table in self.SEGMENTED_TABLES:
            csv_path = self.source_dir / f"{table}.csv"
            if not csv_path.is_file():
                self.logger.warning("Table not found, skipping: %s", csv_path)
                continue

            self.logger.info("Segmenting %s...", table)
            df = pd.read_csv(csv_path, parse_dates=["START"])
            df["_start_date"] = df["START"].dt.date

            # Base file: records with START < simulation_start
            base_df = df[df["_start_date"] < simulation_start].drop(columns=["_start_date"])
            self._safe_write(base_df, self.segmented_path / f"{table}_base.csv", overwrite)

            # Monthly files
            for end_date in month_ends:
                month_start = self._first_day_of_month(end_date.year, end_date.month)
                month_df = df[
                    (df["_start_date"] >= month_start) & (df["_start_date"] <= end_date)
                ].drop(columns=["_start_date"])
                end_str = end_date.isoformat()
                out_path = self.segmented_path / f"{table}_{end_str}.csv"
                self._safe_write(month_df, out_path, overwrite)

        # Copy static tables
        for table in self.STATIC_TABLES:
            self._copy_static(table, overwrite)

        self.logger.info("Segmentation complete. Output: %s", self.segmented_path)

    @classmethod
    def from_profile(cls, config_path: str, profile_name: str) -> "SyntheaSegmenter":
        return cls(config_path, profile_name)
