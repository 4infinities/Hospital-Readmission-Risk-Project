import json
from datetime import date
from pathlib import Path

import pandas as pd
import numpy as np

from src.utils.logger import get_logger


class DataPreprocessor:
    """
    Simple preprocessing pipeline for Hospital Readmission Risk models.

    Steps:
    - select model features (numeric_cols)
    - fill missing numeric values with 0
    - one-hot encode categoricals and drop reference dummies
    - log-transform selected cost features
    - split into X (features) and y (readmission flags)
    """

    # ---------- config helpers ----------

    @staticmethod
    def _load_json(path: str) -> dict:
        cfg_path = Path(path).expanduser().resolve()
        with cfg_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def from_config(
        cls,
        config_path: str,
        drop_dummy_cols: list[str] | None = None,
    ) -> "DataPreprocessor":
        """
        Build a DataPreprocessor from a JSON config.

        Expected JSON keys:
          - data_path: path template to CSV cache, may contain {{PROFILE}}
          - sql: path to SQL file for index selection (absolute or relative)
          - numeric_cols: list of feature/label columns to keep
          - log_cols: list of numeric columns to log-transform
        """
        cfg = cls._load_json(config_path)
        data_cfg = cfg["data"]

        return cls(
            data_path_template=data_cfg["data_path"],
            sql_path=data_cfg["sql"],
            numeric_cols=data_cfg["numeric_cols"],
            log_cols=data_cfg["log_cols"],
            drop_dummy_cols=drop_dummy_cols,
        )

    # ---------- instance part ----------

    def __init__(
        self,
        data_path_template: str,
        sql_path: str,
        numeric_cols: list[str],
        log_cols: list[str],
        drop_dummy_cols: list[str] | None = None,
    ):
        self.logger = get_logger(__name__)
        self.data_path_template = data_path_template
        self.sql_path = sql_path
        self.numeric_cols = numeric_cols
        self.log_cols = log_cols
        self.drop_dummy_cols = drop_dummy_cols or ["gender_F", "stay_type_emergency"]

    # --- internal steps ---

    def _select_numeric_values(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.numeric_cols].copy()

    def _fillna_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.fillna(0).copy()

    def _dummies_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = pd.get_dummies(df)
        df = df.drop(columns=[c for c in self.drop_dummy_cols if c in df.columns])
        return df.copy()

    def _log_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.log_cols:
            if col in df.columns:
                name = "log_" + col
                df[name] = np.log1p(df[col])
                df = df.drop(columns=col)
        return df.copy()

    def _data_flags_split(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        flag_cols = ["readmit_30d", "readmit_90d", "rel_readmit_30d", "rel_readmit_90d"]
        flags = df[flag_cols].copy()
        data = df.drop(columns=flag_cols).copy()
        return data, flags

    # --- public API ---

    def preprocess_df(self, df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        df = self._select_numeric_values(df_raw)
        df = self._fillna_numeric(df)
        df = self._dummies_transform(df)
        df = self._log_transform(df)
        X, y = self._data_flags_split(df)
        return X, y

    def preprocess(
        self,
        end_date: str,
        transformer,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series]:
        """
        Query index_stay from BQ scoped by end_date and return train/test split.

        Train  = all rows with discharge_date < first day of end_date's month.
        Test   = rows with discharge_date in end_date's month (current window, no labels used).

        Returns
        -------
        X_train, y_train, X_test, stay_ids_test
        """
        # Build SQL: replace {{END_DATE}} and standard transformer tokens
        sql_path = Path(self.sql_path).expanduser().resolve()
        with sql_path.open("r", encoding="utf-8") as f:
            sql_raw = f.read()

        # Remove window filter from creation SQL — we want the full history
        # The selection SQL (20_index_stay_selection.sql) has no {{END_DATE}} token,
        # so just apply the standard transformer placeholders
        sql = transformer._transform_query(sql_raw)

        self.logger.info("[preprocess] Fetching index_stay from BQ for end_date=%s", end_date)
        df_raw = transformer.fetch_to_dataframe(sql=sql, cache_path=None, query=True)

        # Split boundary: first day of end_date's month
        end = date.fromisoformat(end_date)
        month_start = end.replace(day=1)

        df_raw["discharge_date"] = pd.to_datetime(df_raw["discharge_date"]).dt.date

        train_mask = df_raw["discharge_date"] < month_start
        test_mask = (df_raw["discharge_date"] >= month_start) & (df_raw["discharge_date"] <= end)

        df_train = df_raw[train_mask].copy()
        df_test = df_raw[test_mask].copy()

        self.logger.info(
            "[preprocess] Train rows=%d  Test rows=%d", len(df_train), len(df_test)
        )

        stay_ids_test = df_test["stay_id"].reset_index(drop=True)

        X_train, y_train = self.preprocess_df(df_train)
        X_test, _ = self.preprocess_df(df_test)   # labels not used for test

        # Align columns — train may have dummies test doesn't and vice versa
        X_test = X_test.reindex(columns=X_train.columns, fill_value=0)

        return X_train, y_train, X_test, stay_ids_test

    def load_and_preprocess(
        self,
        transformer,
        force_query: bool = False,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Use BigQueryTransformer to load index table and run preprocessing.

        transformer: BigQueryTransformer instance
        profile_name: 'mock' | 'train' | 'test' (used to render paths/templates)
        force_query: if True, always hit BigQuery (ignore cache)
        """
        # 1) Resolve cache path from template
        data_path_str = self.data_path_template.replace("{{PROFILE}}", transformer.profile_prefix)
        cache_path = str(Path(data_path_str).expanduser().resolve())

        # 2) Load SQL text from file, then apply transformer placeholders
        sql_path = Path(self.sql_path).expanduser().resolve()
        with sql_path.open("r", encoding="utf-8") as f:
            sql_raw = f.read()

        sql = transformer._transform_query(sql_raw)

        # 3) Fetch raw data
        df_raw = transformer.fetch_to_dataframe(
            sql=sql,
            cache_path=cache_path,
            query=force_query,
        )

        # 4) Preprocess
        return self.preprocess_df(df_raw)
