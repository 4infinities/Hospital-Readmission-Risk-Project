from __future__ import annotations

import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple

from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


class BigQueryTransformer:
    """
    Runs an ordered sequence of SQL queries in BigQuery to build
    slim/helper/index tables.

    SQL placeholders:
      {{DATASET_RAW}}  -> project_id.<profile dataset>
      {{DATASET_SLIM}} -> project_id.<dataset_slim>   (same for all profiles)
      {{PROFILE}}      -> 'train_', 'mock_', or '' (for test)
    """

    # ---------- config helpers ----------

    @staticmethod
    def _load_json(path: str) -> Dict[str, Any]:
        p = Path(path).expanduser().resolve()
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def from_profile(
        cls,
        config_path: str,
        profile_name: str,
    ) -> Tuple["BigQueryTransformer", Dict[str, str]]:
        """
        Create a transformer bound to a given profile using bq_config.json.

        bq_config.json structure (simplified):

        {
          "project_id": "...",
          "location": "...",
          "credentials_path": "...",
          "dataset_slim": "data_slim",
          "profiles": {
            "train": {
              "dataset": "train_raw_data",
              "local_input_dir": "..."
            },
            ...
          }
        }
        """
        cfg = cls._load_json(config_path)

        project_id = cfg["project_id"]
        location = cfg["location"]
        dataset_slim = cfg["dataset_slim"]
        dataset_helpers = cfg["dataset_helpers"]
        profiles = cfg["profiles"]

        if profile_name not in profiles:
            raise KeyError(f"Profile '{profile_name}' not found in config")

        profile_cfg = profiles[profile_name]
        raw_dataset = profile_cfg["dataset"]      # per-profile raw dataset
        credentials_path = cfg.get("credentials_path")

        # Build credentials if a path is provided
        credentials = None
        if credentials_path:
            credentials_path = str(Path(credentials_path).expanduser().resolve())
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )

        client = bigquery.Client(
            project=project_id,
            location=location,
            credentials=credentials,
        )

        transformer = cls(
            project_id=project_id,
            location=location,
            raw_dataset_id=raw_dataset,
            slim_dataset_id=dataset_slim,
            helpers_dataset_id=dataset_helpers,
            profile_name=profile_name,
            client=client,
        )

        profile_info = {
            "profile_name": profile_name,
            "raw_dataset": raw_dataset,
            "dataset_slim": dataset_slim,
        }
        return transformer, profile_info

    # ---------- instance part ----------

    def __init__(
        self,
        project_id: str,
        location: str,
        raw_dataset_id: str,
        slim_dataset_id: str,
        helpers_dataset_id:str,
        profile_name: str,
        client: bigquery.Client,
    ):
        self.project_id = project_id
        self.location = location
        self.raw_dataset_id = raw_dataset_id       # e.g. train_raw_data
        self.slim_dataset_id = slim_dataset_id     # e.g. data_slim
        self.helpers_dataset_id = helpers_dataset_id
        self.profile_name = profile_name
        self.client = client

        self._ensure_extra_datasets_exist_once()

    @property
    def dataset_raw_fq(self) -> str:
        """
        Fully qualified raw dataset: project.dataset
        """
        return f"{self.project_id}.{self.raw_dataset_id}"

    @property
    def dataset_slim_fq(self) -> str:
        """
        Fully qualified slim dataset: project.dataset_slim
        """
        return f"{self.project_id}.{self.slim_dataset_id}"

    @property
    def dataset_helpers_fq(self) -> str:
        """
        Fully qualified slim dataset: project.dataset_slim
        """
        return f"{self.project_id}.{self.helpers_dataset_id}"

    @property
    def profile_prefix(self) -> str:
        """
        PROFILE prefix for table names in slim dataset.
        train -> 'train_'
        mock  -> 'mock_'
        test  -> ''
        """
        if self.profile_name == "train":
            return "train_"
        if self.profile_name == "mock":
            return "mock_"
        # default for test or any other: no prefix
        return ""

    # ---------- dataset creation (one-time) ----------

    def _ensure_extra_datasets_exist_once(self) -> None:
        """
        Ensure the fixed extra datasets (e.g. data_slim) exist.

        Called once in __init__. If datasets already exist, nothing bad happens.
        """
        extra_dataset_ids = [self.slim_dataset_id, self.helpers_dataset_id]
        for dataset_id in extra_dataset_ids:
            full_id = f"{self.project_id}.{dataset_id}"
            dataset_ref = bigquery.Dataset(full_id)
            dataset_ref.location = self.location

            try:
                self.client.get_dataset(dataset_ref)
                logger.info("Dataset already exists: %s", full_id)
            except Exception:
                logger.info("Creating dataset: %s", full_id)
                self.client.create_dataset(dataset_ref)

    # ---------- core helpers ----------

    @staticmethod
    def _load_query_file(path: str) -> str:
        sql_path = Path(path).expanduser().resolve()
        with sql_path.open("r", encoding="utf-8") as f:
            return f.read()

    def _transform_query(self, sql: str) -> str:
        """
        Replace placeholders in one SQL string.
        """
        sql = sql.replace("{{DATASET_RAW}}", self.dataset_raw_fq)
        sql = sql.replace("{{DATASET_SLIM}}", self.dataset_slim_fq)
        sql = sql.replace("{{DATASET_HELPERS}}", self.dataset_helpers_fq)
        sql = sql.replace("{{PROFILE}}", self.profile_prefix)
        return sql

    def _run_query(self, sql: str) -> None:
        """
        Execute a SQL statement and wait for completion.
        """
        logger.info("Running query:\n%s", sql)
        job = self.client.query(sql)
        job.result()
        logger.info("Query finished.")

    # ---------- public API: run a sequence ----------

    def run_query_sequence(
        self,
        recipe_path: str,
        recipes_id: int,
        project_root: str | None = None
    ) -> None:
        """
        Load a list of SQL file paths from a recipe JSON and execute them in order.

        Recipe JSON:

        {
          "queries": [
            "sql/base/01_patientsslim.sql",
            "sql/base/02_encountersslim.sql",
            ...
          ]
        }
        """
        recipe = self._load_json(recipe_path)
        query_paths: List[str] = recipe.get("queries", [])[recipes_id]

        if not query_paths:
            logger.warning("No queries found in recipe: %s", recipe_path)
            return

        base_dir = Path(project_root).expanduser().resolve() if project_root else Path.cwd()

        for rel_path in query_paths:
            full_path = base_dir / rel_path
            sql_raw = self._load_query_file(str(full_path))
            sql = self._transform_query(sql_raw)
            self._run_query(sql)

    def fetch_to_dataframe(
        self,
        sql: str,
        cache_path: str | None = None,
        query: bool = False
    ) -> pd.DataFrame:
        """
        Run a SQL query in BigQuery and return a pandas DataFrame.

        If cache_path is provided and the file exists, load from CSV instead
        of hitting BigQuery. If it does not exist, run the query, save CSV,
        then return the DataFrame.

        Assign query = True for a mandatory sql querying
        """
        if not query:
            if cache_path is not None:
                cache_path = str(Path(cache_path).expanduser().resolve())
                cache_file = Path(cache_path)
                if cache_file.is_file():
                    return pd.read_csv(cache_file)

        # Run query in BigQuery
        job = self.client.query(sql)
        df = job.to_dataframe()

        if cache_path is not None:
            df.to_csv(cache_path, index=False)

        return df
