import pandas as pd
import numpy as np
from config import numeric_cols, log_cols

def select_numeric_values(df):

    return df[numeric_cols].copy()

def dummies_transform(df, drop_cols = ['gender_F', 'stay_type_emergency']):

    df = pd.get_dummies(df)
    df = df.drop(columns = drop_cols)

    return df.copy()

def fillna_numeric(df):

    df = df.fillna(0)

    return df.copy()  
"""
def readmission_sanity_check(df):

    mask = df['following_unplanned_admission_flag'] == 0
    df.loc[mask, ['readmit_30d', 'readmit_90d']] = 0
    mask = df['readmit_90d'] == 0
    df.loc[mask, 'following_unplanned_admission_flag'] = 0

    return df.copy()
"""
def log_transform(df, cols = log_cols):

    for col in cols:

        name = 'log_' + col

        df[name] = np.log1p(df[col])

        df = df.drop(columns = col)

    return df.copy()

def data_flags_split(df):

    flags = df[['readmit_30d', 'readmit_90d', 'rel_readmit_30d', 'rel_readmit_90d']]

    data = df.drop(columns = ['readmit_30d', 'readmit_90d', 'rel_readmit_30d', 'rel_readmit_90d'])

    return data, flags

def build_preprocessor(df_raw):

    df = select_numeric_values(df_raw)

    df = fillna_numeric(df)
    
    df = dummies_transform(df)

    #df = readmission_sanity_check(df)

    df = log_transform(df)

    df_numeric, df_results = data_flags_split(df)

    return df_numeric, df_results

def preprocess_data(df_raw):

    return build_preprocessor(df_raw)