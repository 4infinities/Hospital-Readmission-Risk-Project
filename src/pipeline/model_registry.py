from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import re
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from lightgbm import LGBMClassifier

from pipeline.model_config_manager import ModelConfigManager

from src.utils.logger import get_logger


@dataclass
class ModelRegistry:
    """
    Registry for final readmission models.

    Responsibilities:
    - Rebuild sklearn / LightGBM Pipelines based on ModelConfigManager.
    - Apply best_params from tuning to the pipelines.
    - Fit final models on full training data for given target column(s).
    - Save and load fitted pipelines using joblib.
    """

    config_mgr: ModelConfigManager
    models_dir: Path

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __post_init__(self):
        self.logger = get_logger(__name__)

    @classmethod
    def from_config(cls, config_path: str, models_dir: Optional[str] = None) -> "ModelRegistry":
        config_mgr = ModelConfigManager.from_config(config_path)

        if models_dir is None:
            cfg_models_dir = config_mgr.get_models_dir()
            if cfg_models_dir is None:
                # fallback to default relative path
                models_dir_path = Path("models")
            else:
                models_dir_path = Path(cfg_models_dir)
        else:
            models_dir_path = Path(models_dir)

        return cls(config_mgr=config_mgr, models_dir=models_dir_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_base_estimator(self, estimator_type: str):
        """
        Build the bare estimator (no scaling, no pipeline) based on estimator_type.
        """
        if estimator_type == "sklearn_logistic_regression":
            return LogisticRegression()

        if estimator_type == "sklearn_random_forest":
            return RandomForestClassifier()

        if estimator_type == "lightgbm_classifier":
            return LGBMClassifier(objective="binary")

        raise ValueError(f"Unknown estimator_type: {estimator_type}")

    def _build_estimator(self, model_name: str) -> Pipeline:
        """
        Build a Pipeline for a given model_name, using estimator_type from
        ModelConfigManager.

        The pipeline step name is the model_name itself, which must match
        the prefix used in search_space (e.g. 'logreg' for params like 'logreg__C').
        """
        est_type = self.config_mgr.get_estimator_type(model_name)
        base_estimator = self._build_base_estimator(est_type)

        # RandomForest with sparse inputs: avoid centering
        if est_type == "sklearn_random_forest":
            scaler = StandardScaler(with_mean=False)
        else:
            scaler = StandardScaler()

        pipe = Pipeline(
            steps=[
                ("scaler", scaler),
                (model_name, base_estimator),
            ]
        )
        return pipe

    def _apply_best_params(self, model_name: str, pipe: Pipeline) -> Pipeline:
        """
        Apply best_params from ModelConfigManager to a Pipeline.
        """
        best_params = self.config_mgr.get_best_params(model_name)
        if best_params:
            pipe.set_params(**best_params)
        return pipe

    def _build_model_path(
        self,
        name: str,
        target: str,
        suffix: Optional[str] = None,
    ) -> Path:
        """
        Build filesystem path for a saved model.

        Examples:
        - models/logreg__readmit_30d.pkl
        - models/logreg__readmit_30d__v1.pkl  (if suffix='v1')
        """
        self.models_dir.mkdir(parents=True, exist_ok=True)

        flag = 'd30' if target == 'readmit_30d' else 'd90'

        base = f"{name}_{flag}"
        if suffix:
            base = f"{base}_{suffix}"

        filename = f"{base}.pkl"
        return self.models_dir / filename

    # ------------------------------------------------------------------
    # Public API: fit / save / load
    # ------------------------------------------------------------------

    def fit_models(
        self,
        X,
        y,
        target_cols: List[str],
        model_names: Optional[List[str]] = None,
        suffix: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Pipeline]:
        """
        Fit final models on full training data for each target in target_cols.

        Parameters
        ----------
        X : pd.DataFrame or array-like
            Feature matrix.
        y : pd.DataFrame
            DataFrame with target columns (e.g. readmit_30d, readmit_90d).
        target_cols : list[str]
            List of target columns to fit models for.
        model_names : list[str], optional
            Restrict to this subset of models; if None, use all active models.
        suffix : str, optional
            Optional suffix to distinguish different training runs in filenames.
        force : bool
            If True, refit and overwrite even if a saved model already exists.
            Required for monthly refit (same hyperparams, new training data).

        Returns
        -------
        dict[str, Pipeline]
            Mapping of "<model>__<target>" -> fitted Pipeline.
        """
        if not target_cols:
            raise ValueError("target_cols must be non-empty")

        missing = [c for c in target_cols if c not in y.columns]
        if missing:
            raise ValueError(f"Missing target columns in y: {missing}")

        if model_names is None:
            model_names = self.config_mgr.list_active_models()

        fitted: Dict[str, Pipeline] = {}

        for target_col in target_cols:
            y_target = y[target_col]

            for name in model_names:
                if not self.config_mgr.is_active(name):
                    continue

                path = self._build_model_path(name=name, target=target_col, suffix=suffix)

                if path.exists() and not force:
                    pipe = joblib.load(path)
                else:
                    pipe = self._build_estimator(name)
                    pipe = self._apply_best_params(name, pipe)
                    pipe.fit(X, y_target)
                    self.save_model(name=name, estimator=pipe, target=target_col, suffix=suffix)

                model_key = f"{name}__{target_col}"
                fitted[model_key] = pipe

        return fitted

    def save_model(
        self,
        name: str,
        estimator: Pipeline,
        target: str,
        suffix: Optional[str] = None,
    ) -> Path:
        """
        Save a fitted Pipeline to disk.
        """
        path = self._build_model_path(name=name, target=target, suffix=suffix)
        joblib.dump(estimator, path)
        return path

    def load_model(
        self,
        name: str,
        target: str,
        suffix: Optional[str] = None,
    ) -> Optional[Pipeline]:
        """
        Load a fitted Pipeline from disk.
        """
        path = self._build_model_path(name=name, target=target, suffix=suffix)
        if path.exists():
            return joblib.load(path)

        return None

