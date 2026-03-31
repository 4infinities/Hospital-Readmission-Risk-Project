from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

import pandas as pd

from pipeline.bq_transformer import BigQueryTransformer
from pipeline.dictionaries import (
    load_state,
    save_state,
    pack_dictionary,
    build_dictionary,
    build_flags,
    fill_descriptions,
    fix_flags,
    build_main_diagnoses,
    build_diagnoses_related,
    build_careplan_relations,
)
from pipeline.dictionary_config import (
    procedure_targets,
    diagnosis_targets,
    main_diags_output_cols,
)
from src.utils.logger import get_logger


class DictionaryBuilder:
    """
    Orchestrates SNOMED-based dictionary construction and incremental updates.

    All BQ I/O is delegated to the injected BigQueryTransformer.
    """

    def __init__(
        self,
        transformer: BigQueryTransformer,
        io_config_path: str = "config/dictionary_io_config.json",
    ):
        self.transformer = transformer
        io_path = Path(io_config_path).expanduser().resolve()
        self.io_config: Dict[str, Any] = self._load_json(io_path)
        self.logger = get_logger(__name__)

    # ---------- private helpers ----------

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _get_io(
        self,
        dict_type: str,
        need_dictionary_path: bool = False,
        end_date: Optional[str] = None,
    ) -> Tuple[Path, str, str, str, Optional[str]]:
        """
        Return IO paths and creation SQL for dict_type.

        Parameters
        ----------
        end_date : str or None
            If provided, substitutes ``{{END_DATE}}`` in the creation SQL with
            this value (format 'YYYY-MM-DD'). Required for creation queries that
            filter by date (09, 10, 11, 12). If None, no substitution is done.

        Returns
        -------
        state_path, data_path, sql, write_path, dictionary_path_or_None
        """
        if dict_type not in self.io_config:
            raise KeyError(f"dict_type '{dict_type}' not found in IO config")

        cfg = self.io_config[dict_type]
        state_path = Path(cfg["state"])
        data_path = cfg["data_path"]
        write_path = cfg["write_path"]
        sql = self.transformer.load_sql(cfg["sql"])
        _end = end_date if end_date is not None else "9999-12-31"
        sql = sql.replace("{{END_DATE}}", f"'{_end}'")

        dictionary_path = None
        if need_dictionary_path:
            if "dictionary_path" not in cfg:
                raise KeyError(
                    f"dictionary_path missing for dict_type '{dict_type}' in IO config"
                )
            dictionary_path = cfg["dictionary_path"]

        return state_path, data_path, sql, write_path, dictionary_path

    def _load_delta_sql(self, dict_type: str, end_date: str) -> str:
        cfg = self.io_config[dict_type]
        if "sql_delta" not in cfg:
            raise KeyError(f"'sql_delta' not found in IO config for dict_type '{dict_type}'")
        return self.transformer.load_sql_with_end_date(cfg["sql_delta"], end_date)

    def _append_to_csv(self, new_df: pd.DataFrame, write_path: str) -> None:
        """Read existing CSV (if any), concat new rows, write back."""
        p = Path(write_path)
        if p.exists():
            existing = pd.read_csv(p, index_col=0)
            combined = pd.concat([existing, new_df])
        else:
            combined = new_df
        combined.to_csv(write_path)

    def _helpers_table_fq(self, table_name: str) -> str:
        return (
            f"{self.transformer.project_id}"
            f".{self.transformer.helpers_dataset_id}"
            f".{table_name}"
        )

    # ---------- initial builds (full dataset, write to CSV only) ----------

    def build_procedures_dictionary(self, end_date: Optional[str] = None) -> Path:
        dict_type = "procedures"
        state_path, data_path, sql, write_path, _ = self._get_io(dict_type, end_date=end_date)

        self.logger.info("Building procedures dictionary")
        load_state(state_path)
        data = self.transformer.fetch_to_dataframe(sql=sql, cache_path=data_path, query=True)
        
        try:
            build_dictionary(data, procedure_targets, state_path)
        finally:
            save_state(state_path)

        build_flags(data, procedure_targets)
        fill_descriptions(data, procedure_targets)
        pack_dictionary(data, write_path)
        return Path(write_path).expanduser().resolve()

    def build_diagnoses_dictionary(self, end_date: Optional[str] = None) -> Path:
        dict_type = "diagnoses"
        state_path, data_path, sql, write_path, _ = self._get_io(dict_type, end_date=end_date)

        self.logger.info("Building diagnoses dictionary")
        load_state(state_path)
        data = self.transformer.fetch_to_dataframe(sql=sql, cache_path=data_path, query=True)
        print(sql)
        try:
            build_dictionary(data, diagnosis_targets, state_path)
        finally:
            save_state(state_path)

        build_flags(data, diagnosis_targets)
        fill_descriptions(data, diagnosis_targets)
        fix_flags(data)
        pack_dictionary(data, write_path)
        return Path(write_path).expanduser().resolve()

    def build_main_diagnoses(self, end_date: Optional[str] = None) -> Path:
        dict_type = "main_diagnoses"
        state_path, data_path, sql, write_path, dictionary_path = self._get_io(
            dict_type, need_dictionary_path=True, end_date=end_date
        )

        self.logger.info("Building main_diagnoses")
        data = self.transformer.fetch_to_dataframe(sql=sql, cache_path=data_path, query=True)
        load_state(state_path)
        main_df = build_main_diagnoses(
            data=data,
            output_cols=main_diags_output_cols,
            dictionary_path=dictionary_path,
            state_path=state_path,
        )
        pack_dictionary(main_df, write_path)
        return Path(write_path).expanduser().resolve()

    def build_careplans_related_diagnoses(self, end_date: Optional[str] = None) -> Path:
        dict_type = "careplans_related_diagnoses"
        state_path, data_path, sql, write_path, _ = self._get_io(dict_type, end_date=end_date)

        self.logger.info("Building careplans_related_diagnoses")
        data = self.transformer.fetch_to_dataframe(sql=sql, cache_path=data_path, query=True)
        relations = build_careplan_relations(data, state_path=state_path)
        pack_dictionary(relations, write_path)
        return Path(write_path).expanduser().resolve()

    def build_related_diagnoses(self, end_date: Optional[str] = None) -> Path:
        dict_type = "related_diagnoses"
        state_path, data_path, sql, write_path, _ = self._get_io(dict_type, end_date=end_date)

        self.logger.info("Building related_diagnoses")
        data = self.transformer.fetch_to_dataframe(sql=sql, cache_path=data_path, query=True)
        relations = build_diagnoses_related(data, state_path=state_path)
        pack_dictionary(relations, write_path)
        return Path(write_path).expanduser().resolve()

    # ---------- incremental delta updates (walk-forward) ----------

    def update_diagnoses_dictionary(self, end_date: str) -> None:
        """
        Classify new diagnosis codes in (start_date, end_date] and append to
        local CSV and BQ diagnoses_dictionary.
        start_date is derived as the last day of the month 2 months before end_date.
        """
        dict_type = "diagnoses"
        state_path, _, _, write_path, _ = self._get_io(dict_type)

        sql = self._load_delta_sql(dict_type, end_date)
        self.logger.info("Fetching diagnoses delta for window ending %s", end_date)
        data = self.transformer.fetch_to_dataframe(sql=sql, query=True)

        if data.empty:
            self.logger.info("No new diagnosis codes in window; skipping.")
            return

        self.logger.info("Found %d new diagnosis codes; classifying via SNOMED.", len(data))
        load_state(state_path)
        try:
            build_dictionary(data, diagnosis_targets, state_path)
        finally:
            save_state(state_path)

        build_flags(data, diagnosis_targets)
        fill_descriptions(data, diagnosis_targets)
        fix_flags(data)
        self._append_to_csv(data, write_path)
        self.transformer.append_dataframe(
            data.reset_index(), self._helpers_table_fq("diagnoses_dictionary")
        )
        self.logger.info("diagnoses_dictionary updated: %d new rows appended.", len(data))

    def update_procedures_dictionary(self, end_date: str) -> None:
        """
        Classify new procedure codes in (start_date, end_date] and append to
        local CSV and BQ procedures_dictionary.
        start_date is derived as the last day of the month 2 months before end_date.
        """
        dict_type = "procedures"
        state_path, _, _, write_path, _ = self._get_io(dict_type)

        sql = self._load_delta_sql(dict_type, end_date)
        self.logger.info("Fetching procedures delta for window ending %s", end_date)
        data = self.transformer.fetch_to_dataframe(sql=sql, query=True)

        if data.empty:
            self.logger.info("No new procedure codes in window; skipping.")
            return

        self.logger.info("Found %d new procedure codes; classifying via SNOMED.", len(data))
        load_state(state_path)
        try:
            build_dictionary(data, procedure_targets, state_path)
        finally:
            save_state(state_path)

        build_flags(data, procedure_targets)
        fill_descriptions(data, procedure_targets)
        self._append_to_csv(data, write_path)
        self.transformer.append_dataframe(
            data.reset_index(), self._helpers_table_fq("procedures_dictionary")
        )
        self.logger.info("procedures_dictionary updated: %d new rows appended.", len(data))

    def update_main_diagnoses(self, end_date: str) -> None:
        """
        Compute main_diagnosis for encounters in (start_date, end_date] and
        append to local CSV and BQ main_diagnoses.
        start_date is derived as the last day of the month 2 months before end_date.
        Must run after update_diagnoses_dictionary for the same window.
        """
        dict_type = "main_diagnoses"
        state_path, _, _, write_path, dictionary_path = self._get_io(
            dict_type, need_dictionary_path=True
        )

        sql = self._load_delta_sql(dict_type, end_date)
        self.logger.info("Fetching main_diagnoses delta for window ending %s", end_date)
        data = self.transformer.fetch_to_dataframe(sql=sql, query=True)

        if data.empty:
            self.logger.info("No new encounters in window for main_diagnoses; skipping.")
            return

        self.logger.info("Computing main diagnoses for %d new encounters.", len(data))
        load_state(state_path)
        main_df = build_main_diagnoses(
            data=data,
            output_cols=main_diags_output_cols,
            dictionary_path=dictionary_path,
            state_path=state_path,
        )
        self._append_to_csv(main_df, write_path)
        upload_df = main_df.reset_index()
        upload_df["main_diagnosis_code"] = pd.to_numeric(
            upload_df["main_diagnosis_code"], errors="coerce"
        ).astype("Int64")
        self.transformer.append_dataframe(
            upload_df, self._helpers_table_fq("main_diagnoses")
        )
        self.logger.info("main_diagnoses updated: %d new rows appended.", len(main_df))

    def update_careplans_related_encounters(self, end_date: str) -> None:
        """
        Compute careplan–encounter relations for encounters in (start_date, end_date]
        and append to local CSV and BQ careplans_related_encounters.
        start_date is derived as the last day of the month 2 months before end_date.
        Must run after update_main_diagnoses for the same window.
        """
        dict_type = "careplans_related_diagnoses"
        state_path, _, _, write_path, _ = self._get_io(dict_type)

        sql = self._load_delta_sql(dict_type, end_date)
        self.logger.info(
            "Fetching careplans_related_encounters delta for window ending %s", end_date
        )
        data = self.transformer.fetch_to_dataframe(sql=sql, query=True)

        if data.empty:
            self.logger.info("No new careplan–encounter pairs in window; skipping.")
            return

        self.logger.info(
            "Computing careplan relations for %d new encounter-careplan pairs.", len(data)
        )
        relations = build_careplan_relations(data, state_path=state_path)
        self._append_to_csv(relations, write_path)
        self.transformer.append_dataframe(
            relations.reset_index(), self._helpers_table_fq("careplans_related_encounters")
        )
        self.logger.info(
            "careplans_related_encounters updated: %d new rows appended.", len(relations)
        )

    def update_related_diagnoses(self, end_date: str) -> None:
        """
        Compute related_diagnoses for index stays in (start_date, end_date] and
        upsert to local CSV and BQ related_diagnoses.
        start_date is derived as the last day of the month 2 months before end_date.
        Must run after helper_utilization and main_diagnoses are updated for
        the same window (D5 in the dependency order).

        Uses DELETE-before-insert to avoid duplicates: the 2-month window overlaps
        with the prior month's window, so without deletion the overlapping month's
        stay_ids would be appended twice.
        """
        dict_type = "related_diagnoses"
        state_path, _, _, write_path, _ = self._get_io(dict_type)

        sql = self._load_delta_sql(dict_type, end_date)
        self.logger.info(
            "Fetching related_diagnoses delta for window ending %s", end_date
        )
        data = self.transformer.fetch_to_dataframe(sql=sql, query=True)

        if data.empty:
            self.logger.info("No new index stays in window for related_diagnoses; skipping.")
            return

        self.logger.info("Computing related_diagnoses for %d new stays.", len(data))
        relations = build_diagnoses_related(data, state_path=state_path)

        table_fq = self._helpers_table_fq("related_diagnoses")
        unique_stay_ids = list(set(relations.index.tolist()))

        # --- BQ: DDL-only window dedup (CREATE OR REPLACE drops window rows, free-tier safe) ---
        # The 2-month window overlaps the prior month, so window stay_ids may already exist.
        # DELETE is DML (billing required); CREATE OR REPLACE TABLE AS SELECT is DDL — always allowed.
        ids_sql = ", ".join(f"'{sid}'" for sid in unique_stay_ids)
        recreate_sql = (
            f"CREATE OR REPLACE TABLE `{table_fq}` AS\n"
            f"SELECT * FROM `{table_fq}`\n"
            f"WHERE stay_id NOT IN ({ids_sql})"
        )
        self.logger.info(
            "[related_diagnoses] Recreating table without %d window stay_ids",
            len(unique_stay_ids),
        )
        self.transformer._run_query(recreate_sql)
        self.transformer.append_dataframe(relations.reset_index(), table_fq)

        # --- CSV: remove stale window rows, then write fresh ---
        p = Path(write_path)
        if p.exists():
            existing = pd.read_csv(p, index_col=0)
            existing = existing[~existing.index.isin(unique_stay_ids)]
            combined = pd.concat([existing, relations])
        else:
            combined = relations
        combined.to_csv(write_path)

        self.logger.info(
            "related_diagnoses updated: %d rows inserted for window ending %s (deduped).",
            len(relations),
            end_date,
        )
