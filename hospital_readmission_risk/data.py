"""
Data loading utilities for the Hospital Readmission Risk project.

This module provides a single helper, `load_data`, which abstracts away
reading result sets either from a cached local CSV file or directly
from BigQuery.

Typical usage:

    from data import load_data

    sql = \"\"\"SELECT * FROM `project.dataset.table`\"\"\"
    df = load_data("data/encounters.csv", sql)

Environment / config requirements:
- `config.credentials` must point to a Google Cloud service account JSON file.
- `config.project_name` must be the GCP project id that owns the BigQuery dataset.
"""

from google.cloud import bigquery
from pathlib import Path
import os
import pandas as pd
import db_dtypes
from config import credentials, project_name

def load_data(data_path: str, sql: str, query: bool = False) -> pd.DataFrame:
    """
    Load a dataset either from a local CSV cache or from BigQuery.

    This function implements a simple caching pattern:

    - If `query` is False (default) and a file exists at `data_path`,
      the CSV is read into a DataFrame and returned.
    - Otherwise, the provided SQL is executed against BigQuery using
      the configured project and service account credentials. The
      result is converted to a pandas DataFrame, written to `data_path`
      as a CSV, and returned.

    Parameters
    ----------
    data_path : str
        Filesystem path where the dataset CSV is stored or should be
        created (e.g. "data/encounters_slim.csv").
    sql : str
        Standard SQL query to run in BigQuery when refreshing the data.
    query : bool, optional
        If True, always run the BigQuery query even if `data_path`
        already exists. If False, reuse the local CSV when present.
        Default is False.

    Returns
    -------
    pandas.DataFrame
        Tabular data representing the query result or cached CSV.

    Raises
    ------
    google.api_core.exceptions.GoogleAPIError
        If the BigQuery query fails.
    FileNotFoundError
        If `query` is False and `data_path` does not exist (very unlikely,
        only when the file disappears between the existence check and read).
    """

    path = Path(data_path)

    if not query and path.exists():
        return pd.read_csv(path, sep = ',')

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials

    client = bigquery.Client(project = project_name)

    job = client.query(sql)
    rows = list(job.result())

    data_raw = [dict(r) for r in rows]

    pd.DataFrame(data_raw).to_csv(data_path, index = False)

    return pd.DataFrame(data_raw)
