from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

import numpy as np
import pandas as pd

from src.utils.logger import get_logger


@dataclass
class CostReducer:
    """
    End‑to‑end cost reduction helper.
    Holds config (columns, default reductions) and implements all computations.
    """
    index_path: str
    cost_cols: list[str]
    def_prob_red: float
    def_desired_prob_red: float
    prob_red_min: float
    prob_red_max: float
    desired_prob_red_min: float
    desired_prob_red_max: float
    artifacts_dir: Path

    # ------------------------
    # Construction helpers
    # ------------------------

    def __post_init__(self):
        self.logger = get_logger(__name__)

    @classmethod
    def from_config(
        cls, 
        json_path: str | Path,
        tuning: bool = False,
        ) -> "CostReducer":
        """Optional: load config from JSON if you want later."""
        path = Path(json_path)

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        index_path = data["data_path"]

        if tuning:
            index_path = data["tuning_path"]

        artifacts_dir = Path("data") / "artifacts"
        return cls(
            index_path=index_path,
            cost_cols=data["cost_cols"],
            def_prob_red=float(data["def_prob_red"]),
            def_desired_prob_red=float(data["def_desired_prob_red"]),
            prob_red_min=float(data["prob_red_min"]),
            prob_red_max=float(data["prob_red_max"]),
            desired_prob_red_min=float(data["desired_prob_red_min"]),
            desired_prob_red_max=float(data["desired_prob_red_max"]),
            artifacts_dir=artifacts_dir
        )

    # ------------------------
    # Internal helpers
    # ------------------------

    def _cost_reduction_preprocessor(
        self,
        df_pred: pd.DataFrame,
    ) -> pd.DataFrame:
        df_test = pd.read_csv(self.index_path)
        df = df_test[self.cost_cols].join(df_pred)
        df["total_readmission_cost"] = df["total_readmission_cost"].fillna(0)
        return df

    @staticmethod
    def _separate_model_threshold(name: str) -> tuple[str, float]:
        underscore_pos = name.rfind("_")
        model = name[:underscore_pos]
        threshold = float(name[(underscore_pos + 1):])
        return model, threshold

    def _calc_intervention_days(
        self,
        prob_red: float | None = None,
        desired_prob_red: float | None = None,
    ) -> tuple[int, float]:
        if prob_red is None:
            prob_red = self.def_prob_red
        if desired_prob_red is None:
            desired_prob_red = self.def_desired_prob_red

        days = np.ceil(np.log(1 - desired_prob_red) / np.log(1 - prob_red))
        true_prob_red = 1 - (1 - prob_red) ** days
        return int(days), true_prob_red

    def _estimate_intervention_cost(
        self,
        stay_data: pd.Series,
        intervention_days: int,
    ) -> float:
        extra_day_stay_cost = np.nanmax(
            [stay_data["cost_per_day_stay"], stay_data["avg_cost_of_prev_stays"]]
        )
        intervention_cost = intervention_days * extra_day_stay_cost
        return float(intervention_cost)

    def _estimate_gain(
        self,
        threshold_flag: int,
        row: pd.Series,
        model: str,
        int_days: int,
        true_prob_red: float,
    ) -> float:
        if threshold_flag == 1:
            intervention_cost = self._estimate_intervention_cost(row, int_days)
            exp_avoided_cost = true_prob_red * row[model] * row["total_readmission_cost"]
            return float(exp_avoided_cost - intervention_cost)
        return 0.0

    @staticmethod
    def _calc_pct_saved(
        total_saved: pd.Series,
        total_readmit_30d: float,
        total_readmit_90d: float,
    ) -> pd.Series:
        pct_saved = pd.Series(index=total_saved.index, name="total_pct_saved")
        for key, value in total_saved.items():
            denom = total_readmit_30d if "_d30" in key else total_readmit_90d
            if denom <= 0 or not np.isfinite(denom):
                pct_saved[key] = 0.0
            else:
                pct_saved[key] = value / denom
        return pct_saved

    def _estimate_cost_reduction_single(
        self,
        df_cost: pd.DataFrame,
        df_thresholds: pd.DataFrame,
        prob_red: float,
        desired_prob_red: float,
        tuning: bool = False
    ) -> pd.DataFrame:
        gains = pd.DataFrame(index=df_cost.index)

        intervention_days, true_prob_reduction = self._calc_intervention_days(
            prob_red=prob_red,
            desired_prob_red=desired_prob_red,
        )

        for col_name, col in df_thresholds.items():
            model_gain: dict[int, float] = {}

            if "_d" in col_name:
                model, threshold = self._separate_model_threshold(col_name)

                for row_name, row in df_cost.iterrows():
                    model_gain[row_name] = self._estimate_gain(
                        col[row_name],
                        row,
                        model,
                        intervention_days,
                        true_prob_reduction,
                    )

                gains = gains.join(pd.Series(data=model_gain, name=col_name))

        totals = gains.sum(axis=0)
        totals.name = "total_avoided"

        cols30 = df_thresholds['readmit_30d'] == 1
        if not tuning:
            cols90 = df_thresholds['readmit_90d'] == 1

        total_readmit_30d = df_cost[cols30]["total_readmission_cost"].sum()
        total_readmit_90d = 0 if tuning else df_cost[cols90]["total_readmission_cost"].sum()
        if total_readmit_30d == 0:
            total_readmit_30d = np.nan  # or a small epsilon
        if total_readmit_90d == 0:
            total_readmit_90d = np.nan

        pct_saved = self._calc_pct_saved(totals, total_readmit_30d, total_readmit_90d)
        return pd.concat([gains, totals.to_frame().T, pct_saved.to_frame().T])

    # ------------------------
    # Public API
    # ------------------------
    def evaluate_single_scenario(
    self,
    df_pred: pd.DataFrame,
    df_thresholds: pd.DataFrame,
    fold_index: pd.Index,
    ) -> float:
        """
        For a given validation fold (rows = fold_index), compute net
        cost savings percentage using default prob_red/des_prob_red.
        Returns a single scalar: max(total_pct_saved) across thresholds.
        """
        # 1. Load full index CSV
        df_cost_full = self._cost_reduction_preprocessor(df_pred)

        # 2. Subset to validation fold indices
        """Useless??"""
        df_cost = df_cost_full.loc[fold_index]
        #df_thresholds_fold = df_thresholds.loc[fold_index]

        # 3. Single scenario with defaults
        result = self._estimate_cost_reduction_single(
            df_cost=df_cost,
            df_thresholds=df_thresholds,
            prob_red=self.def_prob_red,
            desired_prob_red=self.def_desired_prob_red,
            tuning=True
        )

        # 4. Extract total_pct_saved row, take max across thresholds
        result = result.replace([np.inf, -np.inf], np.nan).fillna(0)
        pct_row = result.loc["total_pct_saved"]
        score = float(pct_row.max())
        if not np.isfinite(score):
            score = 0.0
        return score

    def map_estimate_cost_reduction(
        self,
        pred_values: pd.DataFrame,
        df_thresholds: pd.DataFrame,
        step: float = 0.05,
    ):
        """
        Main entrypoint: compute cost reduction over a grid of assumptions.
        Returns:
          mapping    dict[desired_prob][prob] -> detailed DataFrame
          avoided    DataFrame with total_avoided per grid point
          pct_avoided DataFrame with total_pct_saved per grid point
        """
        # Use defaults if caller does not override
        base_p = self.def_prob_red
        base_desired = self.def_desired_prob_red

        if self.prob_red_min is None:
            self.prob_red_min = base_p
        if self.prob_red_max is None:
            self.prob_red_max = base_p
        if self.desired_prob_red_min is None:
            self.desired_prob_red_min = base_desired
        if self.desired_prob_red_max is None:
            self.desired_prob_red_max = base_desired

        df_cost = self._cost_reduction_preprocessor(pred_values)

        prob = np.round(np.arange(self.prob_red_min, self.prob_red_max + step, step), 2)
        desired_prob = np.round(
            np.arange(self.desired_prob_red_min, self.desired_prob_red_max + step, step), 2
        )

        mapping: dict[float, dict[float, pd.DataFrame]] = {}
        avoided = pd.DataFrame(columns=df_thresholds.columns)
        pct_avoided = pd.DataFrame(columns=df_thresholds.columns)

        for r in prob:
            for desired_r in desired_prob:
                if r <= desired_r:
                    mapping.setdefault(desired_r, {})

                    result = self._estimate_cost_reduction_single(
                        df_cost=df_cost,
                        df_thresholds=df_thresholds,
                        prob_red=r,
                        desired_prob_red=desired_r,
                    )
                    mapping[desired_r][r] = result

                    saved_costs = pd.Series(result.loc["total_avoided"])
                    saved_costs.name = f"total_avoided_{desired_r}_{r}"
                    avoided = pd.concat([avoided, saved_costs.to_frame().T])

                    pct_saved = pd.Series(result.loc["total_pct_saved"])
                    pct_saved.name = f"total_pct_avoided_{desired_r}_{r}"
                    pct_avoided = pd.concat([pct_avoided, pct_saved.to_frame().T])

        out_dir = self.artifacts_dir

        avoided_path = out_dir / "avoided.csv"
        avoided.to_csv(avoided_path, index=True)
        pct_path = out_dir / "pct_avoided.csv"
        pct_avoided.to_csv(pct_path, index=True)

        return mapping, avoided, pct_avoided
