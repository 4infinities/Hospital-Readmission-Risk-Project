import json
from pathlib import Path

import pandas as pd
import numpy as np


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

    def load_and_preprocess(
        self,
        transformer,
        profile_name: str,
        force_query: bool = False,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Use BigQueryTransformer to load index table and run preprocessing.

        transformer: BigQueryTransformer instance
        profile_name: 'mock' | 'train' | 'test' (used to render paths/templates)
        force_query: if True, always hit BigQuery (ignore cache)
        """
        # 1) Resolve cache path from template
        data_path_str = self.data_path_template.replace("{{PROFILE}}", profile_name)
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
