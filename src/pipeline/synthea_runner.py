from __future__ import annotations
import json
import logging
import shutil
import subprocess
from typing import Any, Dict, List, Optional, Iterable
from pathlib import Path
from datetime import date
import calendar


logger = logging.getLogger(__name__)


class SyntheaRunner:
    """
    Small helper class to run Synthea via Java and copy its CSV outputs
    into your project folder (e.g. data/raw/synthea/).
    """
    # ---------- STATIC / CLASS HELPERS FOR CONFIG ----------

    @staticmethod
    def _load_json_config(path: str) -> Dict[str, Any]:
        """
        Load a JSON config file and return it as a dict.
        This is now Synthea-specific, used only for SyntheaRunner.
        """
        cfg_path = Path(path).expanduser().resolve()
        with cfg_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def from_profile(
        cls,
        config_path: str,
        profile_name: str,
        java_path: str = "java",
    ) -> tuple["SyntheaRunner", Dict[str, Any]]:
        """
        Build a SyntheaRunner and its run-parameters from a named profile
        in a JSON config file.

        JSON structure (simplified):

        {
          "synthea": { ... },
          "profiles": {
            "mock": {
              "output_dir": "...",
              "num_patients": ...,
              ...
            },
            "train": { ... }
          }
        }

        Returns
        -------
        runner : SyntheaRunner
            Configured runner instance (with correct output_dir, files_to_copy, etc.).
        run_params : dict
            Dict with keys: num_patients, seed, clinician_seed,
            state, years_of_history to pass into runner.run(**run_params).
        """
        cfg = cls._load_json_config(config_path)

        synthea_cfg = cfg["synthea"]
        profiles_cfg = cfg["profiles"]

        if profile_name not in profiles_cfg:
            raise KeyError(f"Profile '{profile_name}' not found in config")

        profile = profiles_cfg[profile_name]

        files_to_copy = profile.get("files_to_copy")
        delete_source_files = profile.get("delete_source_files", False)
        output_dir = profile["output_dir"]

        # Create the runner using global synthea settings + profile output_dir
        runner = cls(
            synthea_home=synthea_cfg["synthea_home"],
            jar_name=synthea_cfg.get("jar_name", "synthea-with-dependencies.jar"),
            output_dir=output_dir,
            synthea_csv_dir=synthea_cfg.get("synthea_csv_dir", "output/csv"),
            java_path=java_path,
            files_to_copy=files_to_copy,
            delete_source_files=delete_source_files,
        )

        # Extract run parameters to feed into runner.run(...)
        run_params = {
            "num_patients": profile["num_patients"],
            "seed": profile["seed"],
            "clinician_seed": profile["clinician_seed"],
            "state": profile["state"],
            "years_of_history": profile["years_of_history"],
        }

        return runner, run_params

    def __init__(
        self,
        synthea_home: str,
        jar_name: str = "synthea-with-dependencies.jar",
        output_dir: str = "data/raw/synthea",
        synthea_csv_dir: str = "output/csv",
        java_path: str = "java",
        files_to_copy: Optional[Iterable[str]] = None,
        delete_source_files: bool = False,
    ):
        """
        Parameters
        ----------
        synthea_home : str
            Folder where Synthea is installed, e.g. F:\\Synthea.
        jar_name : str
            Name of the Synthea JAR file inside synthea_home.
        output_dir : str
            Directory where copied CSVs for this profile/run will live.
        synthea_csv_dir : str
            Path (relative to synthea_home) where Synthea writes CSVs,
            usually 'output/csv'.
        java_path : str
            Path to the Java executable.
        files_to_copy : iterable[str] or None
            If provided, only these filenames are copied.
        delete_source_files : bool
            If True, delete the source CSVs in Synthea's folder after copying.
        """
        self.synthea_home = Path(synthea_home).expanduser().resolve()
        self.jar_path = self.synthea_home / jar_name
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.synthea_csv_dir = self.synthea_home / synthea_csv_dir
        self.java_path = java_path
        self.files_to_copy = set(files_to_copy) if files_to_copy is not None else None
        self.delete_source_files = delete_source_files

        if not self.synthea_home.is_dir():
            raise ValueError(f"Synthea home does not exist: {self.synthea_home}")
        if not self.jar_path.is_file():
            raise ValueError(f"Synthea JAR not found at: {self.jar_path}")
        if not self.synthea_csv_dir.exists():
            logger.warning(
                "Synthea CSV directory does not exist yet: %s", self.synthea_csv_dir
            )

    def build_command(
        self,
        num_patients: int,
        seed: int,
        clinician_seed: int,
        state: str,
        years_of_history: int,
    ) -> List[str]:
        """
        Build the exact 'java -jar ...' command Synthea needs.

        This mirrors what you currently type in the terminal, but with
        parameters passed in from Python instead of hard-coded.
        """
        if num_patients <= 0:
            raise ValueError("num_patients must be positive")

        # Example:
        # java -jar synthea-with-dependencies.jar -s 100 -cs 100 -p 50000
        #   --exporter.csv.export=true --exporter.years_of_history=10 California
        cmd = [
            self.java_path,
            "-jar",
            str(self.jar_path),
            "-s",
            str(seed),
            "-cs",
            str(clinician_seed),
            "-p",
            str(num_patients),
            "--exporter.csv.export=true",
            f"--exporter.years_of_history={years_of_history}",
            state,
        ]
        return cmd

    def _move_synthea_csvs(self) -> Path:
        """
        Copy CSVs from Synthea's own output folder into your project.

        Source:  <synthea_home>/output/csv/*.csv
        Target:  <output_dir>/*.csv

        If self.files_to_copy is set, only those filenames are copied.
        Otherwise, all *.csv files are copied.

        Optionally delete the source files afterwards.

        Returns
        -------
        Path
            The target directory in your project that now contains the CSVs.
        """
        if not self.synthea_csv_dir.is_dir():
            raise FileNotFoundError(
                f"Synthea CSV directory not found: {self.synthea_csv_dir}"
            )

        target_dir = self.output_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Copying Synthea CSVs from %s to %s", self.synthea_csv_dir, target_dir
        )

        if self.files_to_copy is None:
            # Copy all CSVs
            files = list(self.synthea_csv_dir.glob("*.csv"))
        else:
            # Copy only the ones listed in files_to_copy
            files = []
            for name in self.files_to_copy:
                src = self.synthea_csv_dir / name
                if src.is_file():
                    files.append(src)
                else:
                    logger.warning("Requested file_to_copy not found: %s", src)

        # Copy every CSV file (patients.csv, encounters.csv, etc.)
        for csv_file in files:
            dest = target_dir / csv_file.name
            shutil.copy2(csv_file, dest)
            logger.info("Copied %s -> %s", csv_file, dest)

            if self.delete_source_files:
                try:
                    csv_file.unlink()
                    logger.info("Deleted source file: %s", csv_file)
                except OSError as e:
                    logger.warning(
                        "Failed to delete source file %s: %s", csv_file, e
                    )

        return target_dir

    def run(
        self,
        num_patients: int,
        seed: int,
        clinician_seed: int,
        state: str,
        years_of_history: int,
        dry_run: bool = False,
    ) -> Path:
        """
        High-level "one call" method: run Synthea and copy outputs.

        Steps:
        1) Build the java command.
        2) (Optionally) log what would happen (dry_run).
        3) Run Synthea in its home directory.
        4) Copy generated CSVs into your project data folder.

        Returns
        -------
        Path
            data/raw/synthea/<state>/ with the copied CSVs.
        """
        # Build the full java command from the given run parameters
        cmd = self.build_command(
            num_patients=num_patients,
            seed=seed,
            clinician_seed=clinician_seed,
            state=state,
            years_of_history=years_of_history,
        )

        logger.info("Running Synthea with command: %s", " ".join(cmd))
        logger.info("Synthea working directory: %s", self.synthea_home)
        logger.info("Synthea CSV directory: %s", self.synthea_csv_dir)
        logger.info("Profile output_dir: %s", self.output_dir)
        logger.info("Files to copy: %s", self.files_to_copy or "ALL *.csv")
        logger.info("Delete source files after copy: %s", self.delete_source_files)

        if dry_run:
            # Do not execute anything, just show where we would copy
            target_dir = self.output_dir / state
            logger.info("[DRY RUN] Would copy CSVs into: %s", target_dir)
            return target_dir

        # Actually run the Java process; check=True raises if Synthea fails
        subprocess.run(cmd, cwd=str(self.synthea_home), check=True)

        self._move_synthea_csvs()

        # Compute start_date: today shifted back by years_of_history years
        today = date.today()
        start_date = today.replace(year=today.year - years_of_history)

        # Compute end_date: start_date + 10 years, clamped to last day of that month
        end_year = start_date.year + 10
        end_month = start_date.month
        last_day = calendar.monthrange(end_year, end_month)[1]
        end_date = date(end_year, end_month, last_day)

        return start_date.isoformat(), end_date.isoformat()
