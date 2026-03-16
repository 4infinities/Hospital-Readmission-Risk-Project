"""
Preprocessing for the Hospital Readmission Risk models.

Pipeline:
- select model features
- fill missing numeric values
- one-hot encode categoricals
- log-transform skewed cost features
- split features and readmission targets
"""

import pandas as pd
import numpy as np
from config import numeric_cols, log_cols


def select_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only columns defined in `numeric_cols`."""
    return df[numeric_cols].copy()


def dummies_transform(
    df: pd.DataFrame,
    drop_cols: list[str] = ["gender_F", "stay_type_emergency"],
) -> pd.DataFrame:
    """One-hot encode all columns and drop reference dummies."""
    df = pd.get_dummies(df)
    df = df.drop(columns=drop_cols)
    return df.copy()


def fillna_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Fill NaNs with 0 in all columns."""
    df = df.fillna(0)
    return df.copy()


# Optional consistency check between readmission flags (kept disabled).
"""
def readmission_sanity_check(df: pd.DataFrame) -> pd.DataFrame:
    # Enforce consistency between readmission labels and flags.
    mask = df["following_unplanned_admission_flag"] == 0
    df.loc[mask, ["readmit_30d", "readmit_90d"]] = 0
    mask = df["readmit_90d"] == 0
    df.loc[mask, "following_unplanned_admission_flag"] = 0
    return df.copy()
"""


def log_transform(df: pd.DataFrame, cols: list[str] = log_cols) -> pd.DataFrame:
    """Add log1p-transformed versions of `cols` and drop originals."""
    for col in cols:
        name = "log_" + col
        df[name] = np.log1p(df[col])
        df = df.drop(columns=col)
    return df.copy()


def data_flags_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split dataframe into features and readmission flags."""
    flags = df[["readmit_30d", "readmit_90d", "rel_readmit_30d", "rel_readmit_90d"]]
    data = df.drop(
        columns=["readmit_30d", "readmit_90d", "rel_readmit_30d", "rel_readmit_90d"]
    )
    return data, flags


def build_preprocessor(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full preprocessing pipeline on `df_raw`."""
    df = select_numeric_values(df_raw)
    df = fillna_numeric(df)
    df = dummies_transform(df)
    # df = readmission_sanity_check(df)
    df = log_transform(df)
    df_numeric, df_results = data_flags_split(df)
    return df_numeric, df_results


def preprocess_data(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Thin wrapper around `build_preprocessor`."""
    return build_preprocessor(df_raw)
