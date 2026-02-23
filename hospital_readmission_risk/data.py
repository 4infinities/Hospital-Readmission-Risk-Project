from google.cloud import bigquery
from pathlib import Path
import os
import pandas as pd
import db_dtypes
from config import credentials, project_name


def load_data(data_path, sql, query = False):

    """
    Loads data from file if it exists, otherwise loads data froim BigQuery and creates a file
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


def filter_dates(data, date_col = 'admission_datetime', start_date = '2018-02-08'):

    """
    Filters encounter dates no more recent than 8 years
    """

    data[date_col] = pd.to_datetime(data[date_col], utc=True)

    cutoff = pd.Timestamp(start_date, tz="UTC")

    return data[pd.to_datetime(data[date_col]) >= cutoff].copy()


def get_data(data_path, query = False):

    df = load_data(data_path, query)

    return filter_dates(df)