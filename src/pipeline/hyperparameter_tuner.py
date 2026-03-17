from __future__ import annotations

from typing import List, Optional, Sequence

import numpy as np
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold
from sklearn.metrics import make_scorer

try:
    from lightgbm import LGBMClassifier
except ImportError:
    LGBMClassifier = None

from pipeline.model_config_manager import ModelConfigManager
from pipeline.cost_reducer import CostReducer


class HyperparameterTuner:
    """
    Runs hyperparameter optimization for classification models defined in
    ModelConfigManager, using RandomizedSearchCV on a Pipeline(StandardScaler + model),
    with a custom cost-based scoring function derived from CostReducer.
    """

    def __init__(
        self,
        config_mgr: ModelConfigManager,
        target_col: str,
        top_fracs: Sequence[float] | None = None,
    ):
        """
        Parameters
        ----------
        config_mgr : ModelConfigManager
            Configuration manager with model and tuning settings.
        cost_reducer : CostReducer
            Instance used to compute cost-based metrics.
        top_fracs : sequence of float, optional
            Fractions of highest-risk patients to flag (0–0.5). The scorer will
            evaluate each top_frac and take the maximum % cost saved as the score.
            If None, defaults to np.arange(0.05, 0.55, 0.05).
        """
        self.config_mgr = config_mgr
        self.target_col = target_col
        if top_fracs is None:
            self.top_fracs = np.round(np.arange(0.05, 0.55, 0.05), 2)
        else:
            self.top_fracs = np.asarray(top_fracs, dtype=float)

    # ------------------------------------------------------------------
    # Estimator construction (unchanged)
    # ------------------------------------------------------------------

    def _build_estimator(self, estimator_type: str) -> Pipeline:
        if estimator_type == "sklearn_logistic_regression":
            base_model = LogisticRegression()
            return Pipeline(
                steps=[
                    ("scaler", StandardScaler()),
                    ("logreg", base_model),
                ]
            )

        if estimator_type == "sklearn_random_forest":
            base_model = RandomForestClassifier()
            return Pipeline(
                steps=[
                    ("scaler", StandardScaler(with_mean=False)),
                    ("rf", base_model),
                ]
            )

        if estimator_type == "lightgbm_classifier":
            if LGBMClassifier is None:
                raise ImportError(
                    "lightgbm is not installed but estimator_type='lightgbm_classifier' was requested."
                )
            base_model = LGBMClassifier(objective="binary")
            return Pipeline(
                steps=[
                    ("scaler", StandardScaler()),
                    ("lightgbm", base_model),
                ]
            )

        raise ValueError(f"Unsupported estimator_type: {estimator_type}")

    # ------------------------------------------------------------------
    # Cost-based scorer factory
    # ------------------------------------------------------------------

    def _make_cost_savings_scorer(
        self,
        model_name: str,
        cost_config_path: str
    ):
        """
        Build a scorer that, for each CV validation fold:
          - Predicts probabilities.
          - For each top_frac in self.top_fracs:
              * Flags top_frac of patients as high risk.
              * Builds df_pred and df_thresholds for that fold.
              * Calls CostReducer.evaluate_single_scenario to get % cost saved.
          - Returns the maximum % cost saved across all top_fracs.
        """

        cost_reducer = CostReducer.from_config(cost_config_path, tuning=True)
        top_fracs = self.top_fracs

        def _score(estimator, X_val, y_val):
            # 1. Predict probabilities for the positive class
            print(f"[SCORER] fold size={len(X_val)}, model={model_name}")
            proba = estimator.predict_proba(X_val)[:, 1]

            # 2. Base prediction DataFrame
            pred_col = f"{model_name}_d30"
            df_pred_fold = pd.DataFrame(
                data={pred_col: proba},
                index=X_val.index,
            )

            best_score = -1000

            # 3. Loop over top_fracs and pick the best cost-based score
            for frac in top_fracs:
                if frac <= 0 or frac > 0.5:
                    continue

                # 3a. Compute flag for top frac of risk
                cutoff = 1 - frac
                flags = (proba >= cutoff).astype(int)

                print(
                    f"[SCORER] frac={frac}, cutoff={cutoff:.4f}, "
                    f"pos_flags={flags.sum()}, neg_flags={(flags == 0).sum()}"
                )
                # Column name must contain "_d" so _separate_model_threshold works.
                # Example name: "logreg_d30_0.10"
                thr_col = f"{model_name}_d30_{cutoff:.2f}"

                df_thresholds_fold = pd.DataFrame(
                    data={thr_col: flags},
                    index=X_val.index,
                )
                df_thresholds_fold[self.target_col] = y_val

                # 3b. Evaluate cost reduction for this threshold
                score_frac = cost_reducer.evaluate_single_scenario(
                    df_pred=df_pred_fold,
                    df_thresholds=df_thresholds_fold,
                    fold_index=X_val.index,
                )

                if score_frac > best_score:
                    best_score = score_frac

            return float(best_score)

        # Wrap into a sklearn scorer
        return _score

    # ------------------------------------------------------------------
    # Public API: tune models
    # ------------------------------------------------------------------

    def tune_models(
        self,
        X,
        y,
        cost_config_path: str,
        model_names: Optional[List[str]] = None,
    ) -> None:
        """
        Run hyperparameter tuning for the given models (or all active models)
        using a cost-based scoring function.

        Side effects:
        Updates best_params and best_score in the ModelConfigManager in-place.
        Call config_mgr.save() afterwards to persist.
        """
        if not self.target_col:
            raise ValueError("target_cols must contain at least one column name.")

        if model_names is None:
            model_names = self.config_mgr.list_active_models()

        tuning_params = self.config_mgr.tuning_params
        n_iter = tuning_params.get("n_iter", 50)
        n_splits = tuning_params.get("n_splits", 5)
        shuffle = tuning_params.get("shuffle", True)
        random_state = tuning_params.get("random_state", 42)
        n_jobs = tuning_params.get("n_jobs", -1)
        verbose = tuning_params.get("verbose", 3)

        cv = StratifiedKFold(
            n_splits=n_splits,
            shuffle=shuffle,
            random_state=random_state,
        )

        # Single-target tuning: if y is a DataFrame, take that column.
        if hasattr(y, "__getitem__") and isinstance(self.target_col, str):
            # Here you probably want just one column, e.g. 'readmit_30d'
            y_target = y[self.target_col]
        else:
            y_target = y

        for model_name in model_names:
            if not self.config_mgr.is_active(model_name):
                continue

            search_space = self.config_mgr.get_search_space(model_name)
            if not search_space:
                continue

            estimator_type = self.config_mgr.get_estimator_type(model_name)
            estimator = self._build_estimator(estimator_type)

            # Build cost-based scorer for this model
            score = self._make_cost_savings_scorer(model_name=model_name, cost_config_path=cost_config_path)

            print(f"y_target: {y_target}")
            print(f"estimator: {estimator}")
            print(f"scorer: {score}")

            search = RandomizedSearchCV(
                estimator=estimator,
                param_distributions=search_space,
                n_iter=n_iter,
                scoring=score,
                error_score="raise",
                cv=cv,
                n_jobs=n_jobs,
                verbose=verbose,
                random_state=random_state,
            )
            search.fit(X, y_target)

            best_params = search.best_params_
            best_score = float(search.best_score_)

            self.config_mgr.set_best_params(model_name, best_params)
            self.config_mgr.set_best_score(model_name, best_score)
            print(f"{model_name}: best cost-based score (pct_saved) is {best_score}")
