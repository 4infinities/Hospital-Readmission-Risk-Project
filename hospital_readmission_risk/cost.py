import pandas as pd
import numpy as np
from config import cost_cols, def_desired_prob_red, def_prob_red
"""
def build_cost_base(df_test, df_flag, rows, d30):

    flag = ('30d' if d30 else '90d')

    cols = ['stay_id','cost_per_day_stay','total_readmission_cost','avg_cost_of_prev_stays'] 
    
    df = df_test.loc[rows, cols].copy()

    df['readmit_' + flag] = df_flag.loc[rows, 'readmit_' + flag]

    return df

def attach_predictions(df, df_pred, d30 = True):

    flag = ('d30' if d30 else 'd90')

    cols_flag = [col for col in df_pred if flag in col]

    df[cols_flag] = df_pred[cols_flag]

    return df
"""
def cost_reduction_preprocessor(df_test, df_pred):

    df = df_test[cost_cols].join(df_pred)

    df['total_readmission_cost'] = df['total_readmission_cost'].fillna(0)

    return df

def separate_model_threshold(name):

    underscore_pos = name.rfind("_")
    model = name[:underscore_pos]
    threshold = float(name[(underscore_pos + 1):])

    return model, threshold

def calc_intervention_days(prob_red = def_prob_red, desired_prob_red = def_desired_prob_red):
    
    days = np.ceil(np.log(1 - desired_prob_red) / np.log(1 - prob_red))

    true_prob_red = (1 - prob_red) ** days

    return days, true_prob_red

def estimate_intervention_cost(stay_data, prob_red = def_prob_red, desired_prob_red = def_desired_prob_red):

    intervention_days, true_prob_red = calc_intervention_days(prob_red, desired_prob_red)

    extra_day_stay_cost = np.nanmax([stay_data['cost_per_day_stay'], stay_data['avg_cost_of_prev_stays']])

    intervention_cost = intervention_days * extra_day_stay_cost

    return intervention_cost, true_prob_red

def estimate_gain(threshold_flag, row, model, prob_red = def_prob_red, desired_prob_red = def_desired_prob_red):

    if threshold_flag == 1:

        intervention_cost, true_prob_red = estimate_intervention_cost(row, prob_red, desired_prob_red)

        exp_avoided_cost = true_prob_red * row[model] * row['total_readmission_cost']

        return exp_avoided_cost - intervention_cost

    return 0

def estimate_cost_reduction(df_cost, df_thresholds, d30 = True, prob_red = def_prob_red, desired_prob_red = def_desired_prob_red):

    gains = pd.DataFrame(index = df_cost.index)

    for col_name, col in df_thresholds.items():

        model_gain : dict[int, float] = {}

        if 'd30' in col_name:

            model, threshold = separate_model_threshold(col_name)

            for row_name, row in df_cost.iterrows():

                model_gain[row_name] = estimate_gain(col[row_name], row, model, prob_red, desired_prob_red)

            gains = gains.join(pd.Series(data = model_gain, name = col_name))

    totals = gains.sum(axis = 0)
    totals.name = 'total_avoided'

    return pd.concat([gains, totals.to_frame().T])

def map_estimate_cost_reduction(df_cost, df_thresholds, prob_red_min, prob_red_max, desired_prob_red_min, desired_prob_red_max):

    prob = np.round(np.arange(prob_red_min, prob_red_max, 0.05), 2)
    desired_prob = np.round(np.arange(desired_prob_red_min, desired_prob_red_max, 0.05), 2)

    map : dict[float, dict[float, pd.DataFrame()]] = {}

    avoided = pd.DataFrame(columns = df_thresholds.columns)

    for r in prob:

        for desired_r in desired_prob:

            if r <= desired_r: 

                map.setdefault(desired_r, {})
                
                map[desired_r][r] = estimate_cost_reduction(df_cost, df_thresholds, prob_red = r, desired_prob_red = desired_r)

                saved_costs = pd.Series(map[desired_r][r].loc['total_avoided'])
                saved_costs.name = f"total_avoided_{desired_r}_{r}"

                avoided = pd.concat([avoided, saved_costs.to_frame().T])

    return map, avoided
    
    
