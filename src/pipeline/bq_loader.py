from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, Tuple

from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


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

        JSON structure:

        {
          "project_id": "...",
          "location": "...",
          "profiles": {
            "mock": {
              "dataset": "mock_raw_data",
              "local_input_dir": "D:/.../mock"
            },
            ...
          }
        }

        Returns
        -------
        loader : BigQueryLoader
            Configured loader instance.
        profile_cfg : dict
            Dict containing:
              - "dataset": dataset_id for this profile
              - "local_input_dir": local path with CSVs
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
    ):
        """
        Parameters
        ----------
        project_id : str
            GCP project ID.
        location : str
            BigQuery location, e.g. "europe-west4".
        dataset_id : str
            Dataset name for this profile (e.g. "mock_raw_data").
        client : bigquery.Client or None
            If None, a new client is created.
        """
        self.project_id = project_id
        self.location = location
        self.dataset_id = dataset_id
        self.client = client or bigquery.Client(project=project_id, location=location)

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
            logger.info("Dataset already exists: %s", self.full_dataset_id)
        except Exception:
            logger.info("Creating dataset: %s", self.full_dataset_id)
            self.client.create_dataset(dataset_ref)

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
        logger.info("Loading CSV into BigQuery table: %s", table_id)

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
        logger.info(
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

        logger.info("Loading CSVs from %s into dataset %s", local_input_dir, self.full_dataset_id)

        # Ensure dataset exists before loading
        self.ensure_dataset_exists()

        # For each *.csv file, use its stem as the table name
        csv_files = sorted(local_input_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in %s", local_input_dir)

        for csv_path in csv_files:
            table_name = csv_path.stem  # "patients.csv" -> "patients"
            self.load_one_csv(
                local_csv_path=csv_path,
                table_name=table_name,
                write_disposition=write_disposition,
            )

    def load_dictionaries(
        self,
        profile_cfg: Dict[str, Any],
        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    ) -> None:

        A = 1

