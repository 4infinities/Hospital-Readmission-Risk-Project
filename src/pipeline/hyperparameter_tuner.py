# hyperparameter_tuner.py

from __future__ import annotations

from typing import List, Optional

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold

try:
    from lightgbm import LGBMClassifier
except ImportError:
    LGBMClassifier = None  # handle gracefully if lightgbm not installed

from pipeline.model_config_manager import ModelConfigManager


class HyperparameterTuner:
    """
    Runs hyperparameter optimization for classification models defined in
    ModelConfigManager, using RandomizedSearchCV on a Pipeline(StandardScaler + model).
    """

    def __init__(self, config_mgr: ModelConfigManager):
        self.config_mgr = config_mgr

    def _build_estimator(self, estimator_type: str) -> Pipeline:
        """
        Map estimator_type string to an actual sklearn / LightGBM estimator
        wrapped in a Pipeline with StandardScaler.

        All tunable hyperparameters are left at library defaults and must be
        controlled via the search_space in models_config.json.
        """
        if estimator_type == "sklearn_logistic_regression":
            base_model = LogisticRegression()
            pipe = Pipeline(
                steps=[
                    ("scaler", StandardScaler()),
                    ("logreg", base_model),
                ]
            )
            return pipe

        if estimator_type == "sklearn_random_forest":
            base_model = RandomForestClassifier()
            pipe = Pipeline(
                steps=[
                    ("scaler", StandardScaler(with_mean=False)),
                    ("rf", base_model),
                ]
            )
            return pipe

        if estimator_type == "lightgbm_classifier":
            if LGBMClassifier is None:
                raise ImportError(
                    "lightgbm is not installed but estimator_type='lightgbm_classifier' was requested."
                )
            base_model = LGBMClassifier(objective="binary")
            pipe = Pipeline(
                steps=[
                    ("scaler", StandardScaler()),
                    ("lightgbm", base_model),
                ]
            )
            return pipe

        raise ValueError(f"Unsupported estimator_type: {estimator_type}")

    def tune_models(
        self,
        X,
        y,
        target_cols: List[str],
        model_names: Optional[List[str]] = None,
    ) -> None:
        """
        Run hyperparameter tuning for the given models (or all active models).

        Parameters
        ----------
        X : array-like / DataFrame
            Feature matrix.
        y : DataFrame or Series
            Contains target column(s).
        target_cols : list[str]
            Column(s) in y to use as target. For single-target classification
            pass e.g. ['readmit_30d'].
        model_names : list[str] or None
            If None, all active models from config_mgr are tuned.

        Side effects
        ------------
        Updates best_params and best_score in the ModelConfigManager in-place.
        Call config_mgr.save() afterwards to persist.
        """
        if not target_cols:
            raise ValueError("target_cols must contain at least one column name.")

        if model_names is None:
            model_names = self.config_mgr.list_active_models()

        tuning_params = self.config_mgr.tuning_params
        n_iter = tuning_params.get("n_iter", 50)
        scoring = tuning_params.get("scoring", "average_precision")
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
        if hasattr(y, "__getitem__") and isinstance(target_cols[0], str):
            y_target = y[target_cols]
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

            search = RandomizedSearchCV(
                estimator=estimator,
                param_distributions=search_space,
                n_iter=n_iter,
                scoring=scoring,
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
            print(f"{model_name}: best score is {best_score}")
