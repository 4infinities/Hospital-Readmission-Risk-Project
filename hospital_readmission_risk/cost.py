"""
Cost reduction utilities for readmission intervention scenarios.

Responsibilities:
- Join model predictions with cost columns.
- Compute days of intervention needed for a desired risk reduction.
- Estimate per-stay intervention cost vs avoided readmission cost.
- Aggregate total cost savings for different model thresholds.
- Map savings across grids of daily and desired probability reductions.
"""

import pandas as pd
import numpy as np
from config import cost_cols, def_desired_prob_red, def_prob_red


def cost_reduction_preprocessor(df_test: pd.DataFrame, df_pred: pd.DataFrame) -> pd.DataFrame:
    """Prepare cost dataframe by joining labels/costs with model predictions."""
    df = df_test[cost_cols].join(df_pred)
    df["total_readmission_cost"] = df["total_readmission_cost"].fillna(0)
    return df


def separate_model_threshold(name: str) -> tuple[str, float]:
    """Split a '<model>_<threshold>' column name into model and threshold."""
    underscore_pos = name.rfind("_")
    model = name[:underscore_pos]
    threshold = float(name[(underscore_pos + 1):])
    return model, threshold


def calc_intervention_days(
    prob_red: float = def_prob_red,
    desired_prob_red: float = def_desired_prob_red,
) -> tuple[int, float]:
    """Compute intervention length (days) and effective risk reduction."""
    days = np.ceil(np.log(1 - desired_prob_red) / np.log(1 - prob_red))
    true_prob_red = 1 - (1 - prob_red) ** days
    return int(days), true_prob_red


def estimate_intervention_cost(
    stay_data: pd.Series,
    intervention_days: int
) -> float:
    """Estimate intervention cost for one stay given extra days."""
    extra_day_stay_cost = np.nanmax(
        [stay_data["cost_per_day_stay"], stay_data["avg_cost_of_prev_stays"]]
    )
    intervention_cost = intervention_days * extra_day_stay_cost
    return float(intervention_cost)


def estimate_gain(
    threshold_flag: int,
    row: pd.Series,
    model: str,
    int_days: int,
    true_prob_red: float,
) -> float:
    """Estimate net gain (avoided cost minus intervention cost) for one stay."""
    if threshold_flag == 1:
        intervention_cost = estimate_intervention_cost(row, int_days)
        exp_avoided_cost = true_prob_red * row[model] * row["total_readmission_cost"]
        return float(exp_avoided_cost - intervention_cost)
    return 0.0


def calc_pct_saved(
    total_saved: pd.Series,
    total_readmit_30d: float,
) -> pd.Series:
    """Compute percentage saved for all model thresholds."""
    pct_saved = pd.Series(index=total_saved.index, name="total_pct_saved")
    for key, value in total_saved.items():
        pct_saved[key] = value / total_readmit_30d
    return pct_saved


def estimate_cost_reduction(
    df_cost: pd.DataFrame,
    df_thresholds: pd.DataFrame,
    prob_red: float = def_prob_red,
    desired_prob_red: float = def_desired_prob_red,
) -> pd.DataFrame:
    """Compute per-stay and total cost reduction for all model thresholds."""
    gains = pd.DataFrame(index=df_cost.index)

    intervention_days, true_prob_reduction = calc_intervention_days(
        prob_red,
        desired_prob_red,
    )

    for col_name, col in df_thresholds.items():
        model_gain: dict[int, float] = {}

        if "_d" in col_name:
            model, threshold = separate_model_threshold(col_name)

            for row_name, row in df_cost.iterrows():
                model_gain[row_name] = estimate_gain(
                    col[row_name],
                    row,
                    model,
                    intervention_days,
                    true_prob_reduction,
                )

            gains = gains.join(pd.Series(data=model_gain, name=col_name))

    totals = gains.sum(axis=0)
    totals.name = "total_avoided"

    total_readmit_30d = df_cost[df_cost["readmit_30d"] == 1]["total_readmission_cost"].sum()

    pct_saved = calc_pct_saved(totals, total_readmit_30d)

    return pd.concat([gains, totals.to_frame().T, pct_saved.to_frame().T])


def map_estimate_cost_reduction(
    df_cost: pd.DataFrame,
    df_thresholds: pd.DataFrame,
    prob_red_min: float,
    prob_red_max: float,
    desired_prob_red_min: float,
    desired_prob_red_max: float,
):
    """Evaluate cost reduction over grids of daily and desired probability reductions."""
    prob = np.round(np.arange(prob_red_min, prob_red_max, 0.05), 2)
    desired_prob = np.round(np.arange(desired_prob_red_min, desired_prob_red_max, 0.05), 2)

    mapping: dict[float, dict[float, pd.DataFrame]] = {}
    avoided = pd.DataFrame(columns=df_thresholds.columns)
    pct_avoided = pd.DataFrame(columns=df_thresholds.columns)

    for r in prob:
        for desired_r in desired_prob:
            if r <= desired_r:
                mapping.setdefault(desired_r, {})

                mapping[desired_r][r] = estimate_cost_reduction(
                    df_cost,
                    df_thresholds,
                    prob_red=r,
                    desired_prob_red=desired_r,
                )

                map_name = f"D:\\Python Projects\\Hospital readmission risk\\scripts\\data\\artifacts\\map_{desired_r}_{r}_old.csv"

                mapping[desired_r][r].to_csv(map_name)

                saved_costs = pd.Series(mapping[desired_r][r].loc["total_avoided"])
                saved_costs.name = f"total_avoided_{desired_r}_{r}"
                avoided = pd.concat([avoided, saved_costs.to_frame().T])

                pct_saved = pd.Series(mapping[desired_r][r].loc["total_pct_saved"])
                pct_saved.name = f"total_pct_avoided_{desired_r}_{r}"
                pct_avoided = pd.concat([pct_avoided, pct_saved.to_frame().T])

    avoided.to_csv("D:\\Python Projects\\Hospital readmission risk\\scripts\\data\\artifacts\\avoided_old.csv")
    pct_avoided.to_csv("D:\\Python Projects\\Hospital readmission risk\\scripts\\data\\artifacts\\pct_avoided_old.csv")
    return mapping, avoided, pct_avoided

