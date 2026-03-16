from __future__ import annotations

import json
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

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
    build_careplan_relations
)

from pipeline.dictionary_config import (
    procedure_targets,
    diagnosis_targets,
    main_diags_output_cols,
)

logger = logging.getLogger(__name__)


class DictionaryBuilder:
    """
    High-level builder for all SNOMED-based dictionaries and related tables.
    """

    def __init__(
        self,
        transformer: BigQueryTransformer,
        io_config_path: str = "config/dictionary_io_config.json",
        config_path_bq: str = "config/bq_config.json",
        profile_name: str = "train",  # "train", "mock", or "test"
    ):
        self.transformer = transformer
        self.io_config_path = Path(io_config_path).expanduser().resolve()
        self.io_config = self._load_json(self.io_config_path)

        self.config_path_bq = Path(config_path_bq).expanduser().resolve()
        self.bq_config = self._load_json(self.config_path_bq)

        if profile_name not in self.bq_config["profiles"]:
            raise KeyError(f"profile '{profile_name}' not found in bq_config.json")
        self.profile_name = profile_name

        self.project_id = self.bq_config["project_id"]
        self.dataset_slim = self.bq_config["dataset_slim"]
        self.dataset_helpers = self.bq_config.get("dataset_helpers")

    # ---------- generic helpers ----------

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _profile_prefix(source_type: str) -> str:
        if source_type == "train":
            return "train_"
        if source_type == "test":
            return ""
        if source_type == "mock":
            return "mock_"
        raise ValueError(f"Unknown source_type: {source_type}")

    @staticmethod
    def _apply_profile(template: str, profile_prefix: str) -> str:
        return template.replace("{{PROFILE}}", profile_prefix)

    def _load_sql_from_file_with_bq_placeholders(
        self,
        path_str: str,
        source_type: str,
    ) -> str:
        sql_path = Path(path_str).expanduser().resolve()
        raw_sql = sql_path.read_text(encoding="utf-8")

        profile_prefix = self._profile_prefix(source_type)
        profile_cfg = self.bq_config["profiles"][self.profile_name]
        raw_dataset_id = profile_cfg["dataset"]

        dataset_raw_fq = f"{self.project_id}.{raw_dataset_id}"
        dataset_slim_fq = f"{self.project_id}.{self.dataset_slim}"
        dataset_helpers_fq = (
            f"{self.project_id}.{self.dataset_helpers}"
            if self.dataset_helpers
            else ""
        )

        sql = raw_sql
        sql = sql.replace("{{DATASET_RAW}}", dataset_raw_fq)
        sql = sql.replace("{{DATASET_SLIM}}", dataset_slim_fq)
        sql = sql.replace("{{DATASET_HELPERS}}", dataset_helpers_fq)
        sql = sql.replace("{{PROFILE}}", profile_prefix)
        return sql

    # ---------- single IO helper for all dict types ----------

    def _get_basic_io(
        self,
        dict_type: str,
        source_type: str,
        need_dictionary_path: bool = False,
    ) -> Tuple[Path, str, str, str, Optional[str]]:
        """
        Generic IO loader for any dict_type.

        For each dict_type in dictionary_io_config.json we expect:
          - "state"
          - "data_path"
          - "sql"        (path to .sql file)
          - "write_path"
          - optionally "dictionary_path" (for dicts that need it)

        Returns:
          state_path, data_path, sql, write_path, dictionary_path_or_None
        """
        if dict_type not in self.io_config:
            raise KeyError(f"dict_type '{dict_type}' not found in IO config")

        cfg = self.io_config[dict_type]
        profile_prefix = self._profile_prefix(source_type)

        state_path = Path(cfg["state"])
        data_path = self._apply_profile(cfg["data_path"], profile_prefix)
        write_path = self._apply_profile(cfg["write_path"], profile_prefix)

        sql_file = self._apply_profile(cfg["sql"], profile_prefix)
        sql = self._load_sql_from_file_with_bq_placeholders(sql_file, source_type)

        dictionary_path = None
        if need_dictionary_path:
            if "dictionary_path" not in cfg:
                raise KeyError(
                    f"dictionary_path missing for dict_type '{dict_type}' "
                    "in IO config"
                )
            dictionary_path = self._apply_profile(
                cfg["dictionary_path"], profile_prefix
            )

        return state_path, data_path, sql, write_path, dictionary_path

    # ---------- procedures / diagnoses ----------

    def build_procedures_dictionary(self, source_type: str = "train") -> Path:
        dict_type = "procedures"
        targets = procedure_targets

        state_path, data_path, sql, write_path, _ = self._get_basic_io(
            dict_type=dict_type,
            source_type=source_type,
            need_dictionary_path=False,
        )

        logger.info("Building %s dictionary for %s split", dict_type, source_type)
        load_state(state_path)
        data = self.transformer.fetch_to_dataframe(
                sql=sql,
                cache_path=data_path,
                query=True
                )
        try:
            build_dictionary(data, targets, state_path)
        finally:
            save_state(state_path)

        build_flags(data, targets)
        fill_descriptions(data, targets)
        pack_dictionary(data, write_path)
        return Path(write_path).expanduser().resolve()

    def build_diagnoses_dictionary(self, source_type: str = "train") -> Path:
        dict_type = "diagnoses"
        targets = diagnosis_targets

        state_path, data_path, sql, write_path, _ = self._get_basic_io(
            dict_type=dict_type,
            source_type=source_type,
            need_dictionary_path=False,
        )

        logger.info("Building %s dictionary for %s split", dict_type, source_type)
        load_state(state_path)
        data = self.transformer.fetch_to_dataframe(
                sql=sql,
                cache_path=data_path,
                query=True
                )
        try:
            build_dictionary(data, targets, state_path)
        finally:
            save_state(state_path)

        build_flags(data, targets)
        fill_descriptions(data, targets)
        fix_flags(data)
        pack_dictionary(data, write_path)
        return Path(write_path).expanduser().resolve()

    # ---------- main diagnoses ----------

    def build_main_diagnoses(self, source_type: str = "train") -> Path:
        dict_type = "main_diagnoses"

        state_path, data_path, sql, write_path, dictionary_path = self._get_basic_io(
            dict_type=dict_type,
            source_type=source_type,
            need_dictionary_path=True,
        )

        logger.info("Building main_diagnoses for %s split", source_type)
        logger.info("Dictionary path: %s", dictionary_path)

        data = self.transformer.fetch_to_dataframe(
                sql=sql,
                cache_path=data_path,
                query=True
                )
        load_state(state_path)

        main_df = build_main_diagnoses(
            data=data,
            output_cols=main_diags_output_cols,
            dictionary_path=dictionary_path,
            state_path=state_path,
        )
        pack_dictionary(main_df, write_path)
        return Path(write_path).expanduser().resolve()

    # ---------- related diagnoses (placeholder for now) ----------

    def build_related_diagnoses(self, source_type: str = "train") -> Path:
        dict_type = "related_diagnoses"

        state_path, data_path, sql, write_path, dictionary_path = self._get_basic_io(
            dict_type=dict_type,
            source_type=source_type,
            need_dictionary_path=True,
        )

        logger.info("Building related_diagnoses for %s split", source_type)
        logger.info("Dictionary path: %s", dictionary_path)

        data = self.transformer.fetch_to_dataframe(
                sql=sql,
                cache_path=data_path,
                query=True
                )
        relations = build_diagnoses_related(data, state_path = state_path)
        pack_dictionary(relations, write_path)
        return Path(write_path).expanduser().resolve()

    # ---------- careplans-related diagnoses (placeholder for now) ----------

    def build_careplans_related_diagnoses(self, source_type: str = "train") -> Path:
        dict_type = "careplans_related_diagnoses"

        state_path, data_path, sql, write_path, dictionary_path = self._get_basic_io(
            dict_type=dict_type,
            source_type=source_type,
            need_dictionary_path=True,
        )

        logger.info(
            "Building careplans_related_diagnoses for %s split", source_type
        )
        logger.info("Dictionary path: %s", dictionary_path)

        data = self.transformer.fetch_to_dataframe(
                sql=sql,
                cache_path=data_path,
                query=True
                )
        relations = build_careplan_relations(data, state_path = state_path)
        pack_dictionary(relations, write_path)
        return Path(write_path).expanduser().resolve()
