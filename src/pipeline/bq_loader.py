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

        project_id = cfg["project_id"]
        location = cfg["location"]
        cred_path = cfg.get("credentials_path")

        credentials = None
        if cred_path is not None:
            cred_path = str(Path(cred_path).expanduser().resolve())
            credentials = service_account.Credentials.from_service_account_file(
                cred_path
            )

        profiles = cfg["profiles"]
        if profile_name not in profiles:
            raise KeyError(f"Profile '{profile_name}' not found in config")

        profile_cfg = profiles[profile_name]
        dataset_id = profile_cfg["dataset"]
        local_input_dir = profile_cfg["local_input_dir"]

        client = bigquery.Client(
            project=project_id,
            location=location,
            credentials=credentials,
        )

        loader = cls(
            project_id=project_id,
            location=location,
            dataset_id=dataset_id,
            client=client,
            profile_name=profile_name,
            config=cfg,
        )

        profile_cfg_resolved = {
            "dataset": dataset_id,
            "local_input_dir": str(Path(local_input_dir).expanduser().resolve()),
        }

        return loader, profile_cfg_resolved

    # --------- instance part ---------

    def __init__(
        self,
        project_id: str,
        location: str,
        dataset_id: str,
        client: bigquery.Client | None = None,
        profile_name: str | None = None,
        config: Dict[str, Any] | None = None,
    ):

        self.logger = get_logger(__name__)
        self.project_id = project_id
        self.location = location
        self.dataset_id = dataset_id
        self.client = client or bigquery.Client(project=project_id, location=location)
        self.profile_name = profile_name
        self._config = config or {}

    @property
    def full_dataset_id(self) -> str:
        """
        Return fully-qualified dataset ID: project.dataset
        """
        return f"{self.project_id}.{self.dataset_id}"

    def ensure_dataset_exists(self) -> None:
        """
        Create the dataset if it does not exist.
        """
        dataset_ref = bigquery.Dataset(self.full_dataset_id)
        dataset_ref.location = self.location

        try:
            self.client.get_dataset(dataset_ref)
            self.logger.info("Dataset already exists: %s", self.full_dataset_id)
        except Exception:
            self.logger.info("Creating dataset: %s", self.full_dataset_id)
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

    def with_dataset(self, dataset_id: str) -> "BigQueryLoader":
        """
        Return a new BigQueryLoader attached to a different dataset
        but the same project, location, and client.
        """
        return BigQueryLoader(
            project_id=self.project_id,
            location=self.location,
            dataset_id=dataset_id,
            client=self.client,
            profile_name=self.profile_name,
            config=self._config,
        )


    def load_one_csv(
        self,
        local_csv_path: Path,
        table_name: str,
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

        table_id = f"{self.full_dataset_id}.{table_name}"
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
                self.logger.error(
                    "Monthly segment file not found: %s — expected at %s",
                    f"{table}_{end_date}.csv",
                    csv_path,
                )
                continue

            table_name = f"{table}_{end_date_safe}"
            self.load_one_csv(
                local_csv_path=csv_path,
                table_name=table_name,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

    def load_dictionaries(
    self,
    dir_key: str,
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    ) -> None:
        """
        Load dictionary CSVs for this loader's profile into the helpers dataset.
        - Uses self.profile_name for file filtering and table prefixing.
        - Uses self._config['dataset_helpers'] and self._config['dictionaries_dir'].
        """
        if self.profile_name is None:
            raise ValueError(
                "profile_name is not set on BigQueryLoader; required for load_dictionaries."
            )

        cfg = self._config or {}
        helpers_dataset = cfg.get("dataset_helpers")
        dictionaries_dir = cfg.get(dir_key)

        if not helpers_dataset or not dictionaries_dir:
            raise KeyError(
                f"Config must contain 'dataset_helpers' and {dir_key} "
                "to use load_dictionaries."
            )

        profile_name = self.profile_name
        prefix_for_tables = self.profile_prefix()

        dict_dir_path = Path(dictionaries_dir).expanduser().resolve()
        if not dict_dir_path.is_dir():
            raise NotADirectoryError(
                f"dictionaries_dir is not a directory: {dict_dir_path}"
            )

        dict_loader = self.with_dataset(helpers_dataset)
        dict_loader.ensure_dataset_exists()

        self.logger.info(
            "Loading dictionary CSVs for profile '%s' from %s into dataset %s "
            "with table prefix '%s'",
            profile_name,
            dict_dir_path,
            dict_loader.full_dataset_id,
            prefix_for_tables,
        )

        csv_files = sorted(dict_dir_path.glob("*.csv"))
        if not csv_files:
            self.logger.warning("No dictionary CSV files found in %s", dict_dir_path)

        for csv_path in csv_files:
            filename = csv_path.name

            if profile_name not in filename:
                self.logger.debug(
                    "Skipping dictionary file %s (profile '%s' not in name)",
                    filename,
                    profile_name,
                )
                continue

            stem = csv_path.stem

            base = stem.replace(f"{profile_name}_", "", 1)
            base = base.replace(f"{profile_name}-", "", 1)
            base = base.replace(profile_name, "", 1)
            base = base.lstrip("_").lstrip("-")

            if not base:
                self.logger.warning(
                    "Derived empty base table name from file %s for profile %s, skipping.",
                    filename,
                    profile_name,
                )
                continue

            table_name = f"{prefix_for_tables}{base}"

            self.logger.info(
                "Loading dictionary file %s into table %s.%s",
                csv_path,
                dict_loader.full_dataset_id,
                table_name,
            )

            dict_loader.load_one_csv(
                local_csv_path=csv_path,
                table_name=table_name,
                write_disposition=write_disposition,
            )

