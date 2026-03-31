from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Any, Tuple

from google.cloud import bigquery
from google.oauth2 import service_account

from src.utils.logger import get_logger


class BigQueryLoader:
    """
    Helper class to load local Synthea CSVs into BigQuery datasets.

    - Reads config from bigquery_config.json.
    - For each profile (mock/train/test), uses:
        - project_id, location (global)
        - dataset (per profile)
        - local_input_dir (per profile)
    - Table names in BigQuery equal CSV base filenames (patients.csv -> patients).
    """

    # --------- static / class helpers for config ---------

    @staticmethod
    def _load_json_config(path: str) -> Dict[str, Any]:
        """
        Load a JSON config file and return it as a dict.
        """
        cfg_path = Path(path).expanduser().resolve()
        with cfg_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def from_profile(
        cls,
        config_path: str,
        profile_name: str,
        client: bigquery.Client | None = None,
    ) -> Tuple["BigQueryLoader", Dict[str, Any]]:
        """
        Build a BigQueryLoader and a profile dict from a named profile
        in the JSON config.
        """
        cfg = cls._load_json_config(config_path)

        profiles = cfg["profiles"]
        if profile_name not in profiles:
            raise KeyError(f"Profile '{profile_name}' not found in config")

        profile_cfg = profiles[profile_name]
        project_id = profile_cfg["project_id"]
        cred_path = profile_cfg.get("credentials_path")
        local_input_dir = profile_cfg["local_input_dir"]

        location = cfg["location"]
        dataset_raw = cfg["dataset"]
        dataset_slim = cfg.get("dataset_slim", "")
        dataset_helpers = cfg.get("dataset_helpers", "")

        credentials = None
        if cred_path is not None:
            cred_path = str(Path(cred_path).expanduser().resolve())
            credentials = service_account.Credentials.from_service_account_file(
                cred_path
            )

        client = bigquery.Client(
            project=project_id,
            location=location,
            credentials=credentials,
        )

        loader = cls(
            project_id=project_id,
            location=location,
            dataset_raw=dataset_raw,
            client=client,
            profile_name=profile_name,
            config=cfg,
            dataset_slim=dataset_slim,
            dataset_helpers=dataset_helpers,
        )

        profile_cfg_resolved = {
            "local_input_dir": str(Path(local_input_dir).expanduser().resolve()),
        }

        return loader, profile_cfg_resolved

    # --------- instance part ---------

    def __init__(
        self,
        project_id: str,
        location: str,
        dataset_raw: str,
        client: bigquery.Client | None = None,
        profile_name: str | None = None,
        config: Dict[str, Any] | None = None,
        dataset_slim: str | None = None,
        dataset_helpers: str | None = None,
    ):

        self.logger = get_logger(__name__)
        self.project_id = project_id
        self.location = location
        self.dataset_id = dataset_raw   # alias kept for full_dataset_id compat
        self.dataset_raw = dataset_raw
        self.dataset_slim = dataset_slim or ""
        self.dataset_helpers = dataset_helpers or ""
        self.client = client or bigquery.Client(project=project_id, location=location)
        self.profile_name = profile_name
        self._config = config or {}

    @property
    def full_dataset_id(self) -> str:
        """
        Return fully-qualified dataset ID: project.dataset
        """
        return f"{self.project_id}.{self.dataset_id}"

    def ensure_dataset_exists(self, target: str = "raw") -> None:
        """
        Create the dataset if it does not exist.

        Parameters
        ----------
        target : str
            Logical dataset target: "raw" (default), "slim", or "helpers".
        """
        _TARGET_DATASETS = {
            "raw":     self.dataset_raw,
            "slim":    self.dataset_slim,
            "helpers": self.dataset_helpers,
        }
        if target not in _TARGET_DATASETS:
            raise ValueError(f"Unknown target '{target}'. Must be one of: {list(_TARGET_DATASETS)}")
        full_id = f"{self.project_id}.{_TARGET_DATASETS[target]}"
        dataset_ref = bigquery.Dataset(full_id)
        dataset_ref.location = self.location

        try:
            self.client.get_dataset(dataset_ref)
            self.logger.info("Dataset already exists: %s", full_id)
        except Exception:
            self.logger.info("Creating dataset: %s", full_id)
            self.client.create_dataset(dataset_ref)

    def profile_prefix(self) -> str:
        """
        PROFILE prefix for table names in slim/helper datasets.
        train -> 'train_'
        mock  -> 'mock_'
        test/other -> ''
        """
        if self.profile_name == "train":
            return "train_"
        if self.profile_name == "mock":
            return "mock_"
        return ""

    def load_one_csv(
        self,
        local_csv_path: Path,
        table_name: str,
        target: str = "raw",
        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect_schema: bool = True,
        skip_leading_rows: int = 1,
    ) -> None:
        """
        Load a single local CSV file into a BigQuery table.

        Parameters
        ----------
        local_csv_path : Path
            Path to the CSV file on disk.
        table_name : str
            Name of the table inside this dataset (e.g. "patients").
        target : str
            Logical dataset target: "raw" (default), "slim", or "helpers".
        write_disposition : str
            WRITE_TRUNCATE / WRITE_APPEND / WRITE_EMPTY.
        autodetect_schema : bool
            Let BigQuery infer the schema from the CSV.
        skip_leading_rows : int
            Number of header rows to skip (1 if CSV has a header row).
        """
        local_csv_path = local_csv_path.expanduser().resolve()
        if not local_csv_path.is_file():
            raise FileNotFoundError(f"CSV not found: {local_csv_path}")

        _TARGET_DATASETS = {
            "raw":     self.dataset_raw,
            "slim":    self.dataset_slim,
            "helpers": self.dataset_helpers,
        }
        if target not in _TARGET_DATASETS:
            raise ValueError(f"Unknown target '{target}'. Must be one of: {list(_TARGET_DATASETS)}")
        dataset = _TARGET_DATASETS[target]
        table_id = f"{self.project_id}.{dataset}.{table_name}"
        self.logger.info("Loading CSV into BigQuery table: %s", table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=skip_leading_rows,
            autodetect=autodetect_schema,
            write_disposition=write_disposition,
        )

        with local_csv_path.open("rb") as f:
            load_job = self.client.load_table_from_file(
                f,
                table_id,
                job_config=job_config,
            )

        result = load_job.result()  # wait for job to complete
        self.logger.info(
            "Loaded %s rows into %s", result.output_rows, table_id
        )

    def load_profile_tables(
        self,
        profile_cfg: Dict[str, Any],
        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    ) -> None:
        """
        Load all CSVs in the profile's local_input_dir into BigQuery tables.

        - local_input_dir: directory with Synthea CSVs.
        - Table names are derived from CSV filenames without extension:
            patients.csv -> patients, encounters.csv -> encounters, etc.

        Parameters
        ----------
        profile_cfg : dict
            Must contain:
              - "dataset": dataset id for this profile (already matches self.dataset_id)
              - "local_input_dir": path where CSVs are stored.
        write_disposition : str
            WRITE_TRUNCATE / WRITE_APPEND / WRITE_EMPTY.
        """
        local_input_dir = Path(profile_cfg["local_input_dir"]).expanduser().resolve()
        if not local_input_dir.is_dir():
            raise NotADirectoryError(
                f"local_input_dir is not a directory: {local_input_dir}"
            )

        self.logger.info("Loading CSVs from %s into dataset %s", local_input_dir, self.full_dataset_id)

        # Ensure dataset exists before loading
        self.ensure_dataset_exists()

        # For each *.csv file, use its stem as the table name
        csv_files = sorted(local_input_dir.glob("*.csv"))
        if not csv_files:
            self.logger.warning("No CSV files found in %s", local_input_dir)

        for csv_path in csv_files:
            table_name = csv_path.stem  # "patients.csv" -> "patients"
            self.load_one_csv(
                local_csv_path=csv_path,
                table_name=table_name,
                write_disposition=write_disposition,
            )

    def load_base_segment(self) -> None:
        """
        Load base segment CSVs into BQ raw staging tables.

        Reads segmented_path from config for the active profile.
        For each *_base.csv file found, loads it into a table named after
        the table stem with the _base suffix stripped:
            encounters_base.csv -> encounters
            patients_base.csv   -> patients

        Parameters
        ----------
        None — profile and segmented_path are read from config.
        """
        if self.profile_name is None:
            raise ValueError("profile_name is not set on BigQueryLoader.")

        profile_cfg = self._config.get("profiles", {}).get(self.profile_name, {})
        segmented_path = profile_cfg.get("segmented_path")
        if not segmented_path:
            raise KeyError(
                f"'segmented_path' not found in config for profile '{self.profile_name}'"
            )

        seg_dir = Path(segmented_path).expanduser().resolve()
        if not seg_dir.is_dir():
            raise NotADirectoryError(f"segmented_path is not a directory: {seg_dir}")

        self.ensure_dataset_exists()

        base_files = sorted(seg_dir.glob("*_base.csv"))
        if not base_files:
            self.logger.warning("No *_base.csv files found in %s", seg_dir)

        for csv_path in base_files:
            table_name = csv_path.stem.removesuffix("_base")
            self.load_one_csv(
                local_csv_path=csv_path,
                table_name=table_name,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

    def load_monthly_segment(self, end_date: str) -> None:
        """
        Load one month's segmented CSVs into BQ monthly raw staging tables.

        Reads segmented_path from config for the active profile.
        For each of the 6 segmented tables, looks for:
            {segmented_path}/{table}_{end_date}.csv
        and loads it into:
            {dataset}.{table}_{end_date_safe}
        where end_date_safe = end_date with hyphens replaced by underscores.

        Parameters
        ----------
        end_date : str
            Month-end date string 'YYYY-MM-DD' (e.g. '2015-01-31').
        """
        if self.profile_name is None:
            raise ValueError("profile_name is not set on BigQueryLoader.")

        profile_cfg = self._config.get("profiles", {}).get(self.profile_name, {})
        segmented_path = profile_cfg.get("segmented_path")
        if not segmented_path:
            raise KeyError(
                f"'segmented_path' not found in config for profile '{self.profile_name}'"
            )

        seg_dir = Path(segmented_path).expanduser().resolve()
        end_date_safe = end_date.replace("-", "_")

        tables = [
            "encounters",
            "careplans",
            "claims",
            "conditions",
            "medications",
            "procedures",
        ]

        self.ensure_dataset_exists()

        for table in tables:
            csv_path = seg_dir / f"{table}_{end_date}.csv"
            if not csv_path.is_file():
                raise FileNotFoundError(
                    f"Monthly segment file not found: {table}_{end_date}.csv — "
                    f"expected at {csv_path}"
                )

            table_name = f"{table}_{end_date_safe}"
            self.load_one_csv(
                local_csv_path=csv_path,
                table_name=table_name,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

    def load_dictionaries(self) -> None:
        """
        Load diagnoses_dictionary, procedures_dictionary, and main_diagnoses CSVs
        into the helpers dataset with plain table names (no profile prefix).
        """
        dictionaries_dir = self._config.get("dictionaries_dir")
        if not dictionaries_dir:
            raise KeyError("Config must contain 'dictionaries_dir'")
        if not self.dataset_helpers:
            raise ValueError("dataset_helpers not set on BigQueryLoader")

        dict_dir = Path(dictionaries_dir).expanduser().resolve()
        self.ensure_dataset_exists("helpers")

        for filename, table_name in [
            ("diagnoses_dictionary.csv", "diagnoses_dictionary"),
            ("procedures_dictionary.csv", "procedures_dictionary"),
            ("main_diagnoses.csv", "main_diagnoses"),
        ]:
            self.load_one_csv(dict_dir / filename, table_name, target="helpers")

        self.logger.info("Dictionaries loaded to BQ.")

    def load_careplans(self) -> None:
        """Load careplans_related_encounters.csv into the helpers dataset."""
        careplans_dir = self._config.get("careplans_dir")
        if not careplans_dir:
            raise KeyError("Config must contain 'careplans_dir'")
        if not self.dataset_helpers:
            raise ValueError("dataset_helpers not set on BigQueryLoader")

        csv_path = Path(careplans_dir).expanduser().resolve() / "careplans_related_encounters.csv"
        self.ensure_dataset_exists("helpers")
        self.load_one_csv(csv_path, "careplans_related_encounters", target="helpers")
        self.logger.info("Careplans loaded to BQ.")

    def load_related_diagnoses(self) -> None:
        """Load related_diagnoses.csv into the helpers dataset."""
        related_dir = self._config.get("related_dir")
        if not related_dir:
            raise KeyError("Config must contain 'related_dir'")
        if not self.dataset_helpers:
            raise ValueError("dataset_helpers not set on BigQueryLoader")

        csv_path = Path(related_dir).expanduser().resolve() / "related_diagnoses.csv"
        self.ensure_dataset_exists("helpers")
        self.load_one_csv(csv_path, "related_diagnoses", target="helpers")
        self.logger.info("related_diagnoses loaded to BQ.")

