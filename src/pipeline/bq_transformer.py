from __future__ import annotations

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple

from google.cloud import bigquery
from google.oauth2 import service_account

from src.utils.logger import get_logger


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
        raw_dataset = cfg["dataset"]
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
            client=client,
        )

        profile_info = {
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
        client: bigquery.Client,
    ):
        self.logger = get_logger(__name__)
        self.project_id = project_id
        self.location = location
        self.raw_dataset_id = raw_dataset_id       # e.g. train_raw_data
        self.slim_dataset_id = slim_dataset_id     # e.g. data_slim
        self.helpers_dataset_id = helpers_dataset_id
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
                self.logger.info("Dataset already exists: %s", full_id)
            except Exception:
                self.logger.info("Creating dataset: %s", full_id)
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
        return sql

    def _run_query(self, sql: str) -> None:
        """
        Execute a SQL statement and wait for completion.
        """
        self.logger.info("Running query:\n%s", sql)
        job = self.client.query(sql)
        job.result()
        self.logger.info("Query finished.")

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
            self.logger.warning("No queries found in recipe: %s", recipe_path)
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

    def run_helper_clinical_sanity_checks(
        self,
        checks_base_dir: str,
    ) -> None:
        """
        Run sanity checks on helper_clinical and helper_clinical_grouped.
        Raises RuntimeError with a descriptive message if any check fails.
        """
        base_dir = Path(checks_base_dir).expanduser().resolve()

        # --- A1: helper_clinical vs encounters_slim counts ---
        a1_path = base_dir / "helper_clinical_check_counts.sql"
        a1_sql_raw = self._load_query_file(str(a1_path))
        a1_sql = self._transform_query(a1_sql_raw)
        a1_row = list(self.client.query(a1_sql).result())[0]

        if a1_row["difference"] != 0 or a1_row["missing_in_helper"] > 0 or a1_row["extra_in_helper"] > 0:
            raise RuntimeError(
                "[A1] helper_clinical mismatch: "
                f"encounters={a1_row['encounters_count']}, "
                f"helper={a1_row['helper_clinical_count']}, "
                f"difference={a1_row['difference']}, "
                f"missing_in_helper={a1_row['missing_in_helper']}, "
                f"extra_in_helper={a1_row['extra_in_helper']}."
            )

        # --- A2: helper_clinical_grouped uniqueness ---
        a2_path = base_dir / "helper_clinical_grouped_check_unique.sql"
        a2_sql_raw = self._load_query_file(str(a2_path))
        a2_sql = self._transform_query(a2_sql_raw)
        a2_row = list(self.client.query(a2_sql).result())[0]

        if a2_row["duplicate_rows"] != 0:
            raise RuntimeError(
                f"[A2] helper_clinical_grouped has {a2_row['duplicate_rows']} duplicate stay_id rows."
            )

        # --- B: keys and dictionary joins ---
        b_path = base_dir / "helper_clinical_check_keys_and_dict.sql"
        b_sql_raw = self._load_query_file(str(b_path))
        b_sql = self._transform_query(b_sql_raw)
        b_row = list(self.client.query(b_sql).result())[0]

        if b_row["null_stay_id_count"] > 0:
            raise RuntimeError(
                f"[B1] helper_clinical has {b_row['null_stay_id_count']} rows with NULL stay_id."
            )
        if b_row["missing_dict_rows"] > 0:
            raise RuntimeError(
                f"[B2] helper_clinical has {b_row['missing_dict_rows']} rows "
                "with main_code but no dictionary match (main_name IS NULL)."
            )

        # --- C/D: flags and counts ---
        cd_path = base_dir / "helper_clinical_check_flags_and_counts.sql"
        cd_sql_raw = self._load_query_file(str(cd_path))
        cd_sql = self._transform_query(cd_sql_raw)
        cd_row = list(self.client.query(cd_sql).result())[0]

        bad_flags = {
            k: cd_row[k]
            for k in [
                "bad_has_diabetes",
                "bad_has_cancer",
                "bad_has_hiv",
                "bad_has_hf",
                "bad_has_alz",
                "bad_has_ckd",
                "bad_has_lf",
                "bad_is_planned",
                "bad_had_surgery",
            ]
            if cd_row[k] > 0
        }
        if bad_flags:
            raise RuntimeError(
                f"[C] helper_clinical has invalid/NULL flag values: {bad_flags}."
            )

        if cd_row["negative_num_chronic"] > 0 or cd_row["negative_num_procedures"] > 0:
            raise RuntimeError(
                f"[D1] helper_clinical has negative counts: "
                f"negative_num_chronic={cd_row['negative_num_chronic']}, "
                f"negative_num_procedures={cd_row['negative_num_procedures']}."
            )

        if cd_row["rows_with_chronic"] == 0:
            raise RuntimeError(
                "[D2] helper_clinical has no rows with num_chronic_conditions > 0."
            )
        if cd_row["rows_with_procedures"] == 0:
            raise RuntimeError(
                "[D3] helper_clinical has no rows with num_procedures > 0."
            )

        self.logger.info("helper_clinical sanity checks passed.")


    def run_helper_cost_sanity_checks(
        self,
        checks_base_dir: str,
    ) -> None:
        """
        Run sanity checks on helper_cost_aggregation and
        helper_cost_aggregation_grouped.
        Raises RuntimeError with a descriptive message if any check fails.
        """
        base_dir = Path(checks_base_dir).expanduser().resolve()

        # --- E1: helper_cost_aggregation vs encounters_slim ---
        e1_path = base_dir / "helper_cost_check_counts.sql"
        e1_sql_raw = self._load_query_file(str(e1_path))
        e1_sql = self._transform_query(e1_sql_raw)
        e1_row = list(self.client.query(e1_sql).result())[0]

        if (
            e1_row["difference"] != 0
            or e1_row["missing_in_helper"] > 0
            or e1_row["extra_in_helper"] > 0
        ):
            raise RuntimeError(
                "[E1] helper_cost_aggregation mismatch: "
                f"encounters={e1_row['encounters_count']}, "
                f"helper={e1_row['helper_cost_count']}, "
                f"difference={e1_row['difference']}, "
                f"missing_in_helper={e1_row['missing_in_helper']}, "
                f"extra_in_helper={e1_row['extra_in_helper']}."
            )

        # --- E2: grouped cost vs grouped clinical, uniqueness ---
        e2_path = base_dir / "helper_cost_grouped_check_unique.sql"
        e2_sql_raw = self._load_query_file(str(e2_path))
        e2_sql = self._transform_query(e2_sql_raw)
        e2_row = list(self.client.query(e2_sql).result())[0]

        if (
            e2_row["missing_in_cost"] > 0
            or e2_row["extra_in_cost"] > 0
            or e2_row["duplicate_stay_ids"] > 0
        ):
            raise RuntimeError(
                "[E2] helper_cost_aggregation_grouped mismatch: "
                f"clinical_grouped_stays={e2_row['clinical_grouped_stays']}, "
                f"cost_grouped_stays={e2_row['cost_grouped_stays']}, "
                f"missing_in_cost={e2_row['missing_in_cost']}, "
                f"extra_in_cost={e2_row['extra_in_cost']}, "
                f"duplicate_stay_ids={e2_row['duplicate_stay_ids']}."
            )

        # --- E3: value sanity on helper_cost_aggregation ---
        e3_path = base_dir / "helper_cost_check_values.sql"
        e3_sql_raw = self._load_query_file(str(e3_path))
        e3_sql = self._transform_query(e3_sql_raw)
        e3_row = list(self.client.query(e3_sql).result())[0]

        if e3_row["null_stay_id_count"] > 0:
            raise RuntimeError(
                f"[E3] helper_cost_aggregation has {e3_row['null_stay_id_count']} rows with NULL stay_id."
            )

        neg_fields = [
            "neg_admission_cost",
            "neg_proc_cost",
            "neg_med_cost",
            "neg_total_stay_cost",
            "neg_cost_per_day",
        ]
        neg_any = {f: e3_row[f] for f in neg_fields if e3_row[f] > 0}
        if neg_any:
            raise RuntimeError(
                f"[E3] helper_cost_aggregation has negative cost values: {neg_any}."
            )

        if e3_row["total_stay_less_than_sum"] > 0:
            raise RuntimeError(
                f"[E3] helper_cost_aggregation has {e3_row['total_stay_less_than_sum']} "
                "rows where total_stay_cost is materially below the sum of components."
            )

        # --- E4: value sanity on helper_cost_aggregation_grouped ---
        e4_path = base_dir / "helper_cost_grouped_check_values.sql"
        e4_sql_raw = self._load_query_file(str(e4_path))
        e4_sql = self._transform_query(e4_sql_raw)
        e4_row = list(self.client.query(e4_sql).result())[0]

        if e4_row["null_stay_id"] > 0:
            raise RuntimeError(
                f"[E4] helper_cost_aggregation_grouped has {e4_row['null_stay_id']} rows with NULL stay_id."
            )
        if e4_row["bad_length"] > 0:
            raise RuntimeError(
                f"[E4] helper_cost_aggregation_grouped has {e4_row['bad_length']} rows "
                "with non-positive length_of_encounter."
            )

        neg_group_fields = [
            "neg_admission_cost",
            "neg_proc_cost",
            "neg_med_cost",
            "neg_total_stay_cost",
            "neg_cost_per_day",
        ]
        neg_group_any = {f: e4_row[f] for f in neg_group_fields if e4_row[f] > 0}
        if neg_group_any:
            raise RuntimeError(
                f"[E4] helper_cost_aggregation_grouped has negative cost values: {neg_group_any}."
            )

        self.logger.info("helper_cost sanity checks passed.")

    def run_helper_utilization_sanity_checks(
        self,
        checks_base_dir: str,
        ) -> None:
        """
        Run sanity checks on helper_utilization.
        Raises RuntimeError with a descriptive message if any check fails.
        """
        base_dir = Path(checks_base_dir).expanduser().resolve()

        # --- U1: helper_utilization vs helper_clinical_grouped + duplicates ---
        u1_path = base_dir / "helper_utilization_check_counts.sql"
        u1_sql_raw = self._load_query_file(str(u1_path))
        u1_sql = self._transform_query(u1_sql_raw)
        u1_row = list(self.client.query(u1_sql).result())[0]

        if (
            u1_row["missing_in_util"] > 0
            or u1_row["extra_in_util"] > 0
            or u1_row["duplicate_stay_ids"] > 0
        ):
            raise RuntimeError(
                "[U1] helper_utilization mismatch: "
                f"clinical_grouped_stays={u1_row['clinical_grouped_stays']}, "
                f"util_stays={u1_row['util_stays']}, "
                f"missing_in_util={u1_row['missing_in_util']}, "
                f"extra_in_util={u1_row['extra_in_util']}, "
                f"duplicate_stay_ids={u1_row['duplicate_stay_ids']}."
            )

        # --- U2: key nulls ---
        u2_path = base_dir / "helper_utilization_check_keys.sql"
        u2_sql_raw = self._load_query_file(str(u2_path))
        u2_sql = self._transform_query(u2_sql_raw)
        u2_row = list(self.client.query(u2_sql).result())[0]

        if u2_row["null_stay_id"] > 0:
            raise RuntimeError(
                f"[U2] helper_utilization has {u2_row['null_stay_id']} rows with NULL stay_id."
            )
        if u2_row["null_encounterclass"] > 0:
            raise RuntimeError(
                f"[U2] helper_utilization has {u2_row['null_encounterclass']} rows with NULL encounterclass."
            )
        if u2_row["null_start"] > 0 or u2_row["null_stop"] > 0:
            raise RuntimeError(
                f"[U2] helper_utilization has NULL start/stop values: "
                f"null_start={u2_row['null_start']}, null_stop={u2_row['null_stop']}."
            )

        # --- U3: value ranges ---
        u3_path = base_dir / "helper_utilization_check_values.sql"
        u3_sql_raw = self._load_query_file(str(u3_path))
        u3_sql = self._transform_query(u3_sql_raw)
        u3_row = list(self.client.query(u3_sql).result())[0]

        if u3_row["neg_admissions_365d"] > 0 or u3_row["neg_tot_los_365d"] > 0:
            raise RuntimeError(
                f"[U3] helper_utilization has negative utilization counts: "
                f"neg_admissions_365d={u3_row['neg_admissions_365d']}, "
                f"neg_tot_los_365d={u3_row['neg_tot_los_365d']}."
            )

        if u3_row["bad_readmit_30d"] > 0 or u3_row["bad_readmit_90d"] > 0:
            raise RuntimeError(
                f"[U3] helper_utilization has invalid readmit flags: "
                f"bad_readmit_30d={u3_row['bad_readmit_30d']}, "
                f"bad_readmit_90d={u3_row['bad_readmit_90d']}."
            )

        if u3_row["bad_follow_flag"] > 0:
            raise RuntimeError(
                f"[U3] helper_utilization has invalid following_unplanned_admission_flag "
                f"values: {u3_row['bad_follow_flag']} rows."
            )

        if u3_row["neg_days_to_readmit"] > 0:
            raise RuntimeError(
                f"[U3] helper_utilization has {u3_row['neg_days_to_readmit']} rows "
                "with negative days_to_readmit."
            )

        if u3_row["bad_days_for_readmit90"] > 0:
            raise RuntimeError(
                f"[U3] helper_utilization has {u3_row['bad_days_for_readmit90']} rows "
                "where readmit_90d=1 but days_to_readmit is NULL or > 90."
            )

        # --- U4: logical consistency ---
        u4_path = base_dir / "helper_utilization_check_logic.sql"
        u4_sql_raw = self._load_query_file(str(u4_path))
        u4_sql = self._transform_query(u4_sql_raw)
        u4_row = list(self.client.query(u4_sql).result())[0]

        if u4_row["flag_inconsistent"] > 0:
            raise RuntimeError(
                f"[U4] helper_utilization has {u4_row['flag_inconsistent']} rows "
                "where readmit_90d=0 but following_unplanned_admission_flag=1."
            )

        if u4_row["missing_follow_stay_for_readmit90"] > 0:
            raise RuntimeError(
                f"[U4] helper_utilization has {u4_row['missing_follow_stay_for_readmit90']} "
                "rows with readmit_90d=1 but NULL following_stay_id."
            )

        if u4_row["missing_days_for_readmit90"] > 0:
            raise RuntimeError(
                f"[U4] helper_utilization has {u4_row['missing_days_for_readmit90']} "
                "rows with readmit_90d=1 but NULL days_to_readmit."
            )

        self.logger.info("helper_utilization sanity checks passed.")
