import pandas as pd
import numpy as np

def build_cost_base(df_raw, df_flag, rows, d30):

    flag = ('30d' if d30 else '90d')

    cols = ['stay_id','cost_per_day_stay','total_readmission_cost','avg_cost_of_prev_stays'] 
    
    df = df_raw.loc[rows][cols].copy()

    df['readmit_' + flag] = df_flag.loc[rows, 'readmit_' + flag]

    return df

def attach_predictions(df, df_pred, d30 = True):

    flag = ('d30' if d30 else 'd90')

    cols_flag = [col for col in df_pred if flag in col]

    df[cols_flag] = df_pred[cols_flag]

    return df

def cost_reduction_preprocessor(df_raw, df_pred, df_flag, d30 = True):

    rows = df_pred.index

    df = build_cost_base(df_raw, df_flag, rows, d30)

    df = attach_predictions(df, df_pred, d30)

    return df

def separate_model_threshold(name):

    underscore_pos = name.rfind("_")
    model = name[:underscore_pos]
    threshold = float(name[(underscore_pos + 1):])

    return model, threshold

def estimate_intervention_cost(row, df_pred, df_cost, model, threshold):

    relative_prob = df_pred.loc[row, model] / threshold

    extra_day_stay_cost = np.nanmin(df_cost.loc[row, 'cost_per_day_stay'], df_cost.loc[row, 'avg_cost_of_prev_stays'])

    intervention_cost = np.min(np.floor(relative_prob), 3) * extra_day_stay_cost

    return intervention_cost

def estimate_gain(row, col, df_thresholds, df_pred, df_cost, gains, model, threshold, r):

    if(df_thresholds.loc[row, col] == 1):

        exp_avoided_cost = r * df_pred.loc[row, model] * df_cost.loc[row, 'total_readmission_cost']

        intervention_cost = estimate_intervention_cost(row, df_pred, df_cost, model, threshold)

        gains.loc[row, col] = exp_avoided_cost - intervention_cost

    else: 

        gains.loc[row, col] = 0

    return gains

def estimate_cost_reduction(df_cost, df_pred, df_thresholds, d30 = True, r = 0.2):

    flag = ('d30' if d30 else 'd90')

    gains = pd.DataFrame(index = df_cost.index)

    for col in df_thresholds.columns:

        if (flag in col):

            model, threshold = separate_model_threshold(col)

            for row in df_thresholds.index:

                gains = estimate_gain(row, col, df_thresholds, df_pred, df_cost, gains, model, threshold, r)
                
    gains.loc['total_avoided'] = gains.sum(axis = 0)

    return gains

def map_estimate_cost_reduction(df_cost, df_pred, df_thresholds, rmin, rmax, rstep, d30 = True):

    r_range = np.arange(rmin, rmax, rstep)

    map = []

    for r in r_range:

        map.append(estimate_cost_reduction(df_cost, df_pred, df_thresholds, d30, r))

    return map
    
    
