from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


class ModelConfigManager:
    """
    Helper to manage model and CV configuration for training and hyperparameter tuning.

    Expected JSON structure (top-level):

    {
      "data": { ... },         # used by DataPreprocessor
      "cv": { ... },           # cross-validation settings
      "models": {
        "logreg": {
          "active": true,
          "estimator_type": "sklearn_logistic_regression",
          "search_space": { ... },
          "best_params": null
        },
        ...
      }
    }
    """

    # ---------- config helpers ----------

    @staticmethod
    def _load_json(path: str) -> Dict[str, Any]:
        cfg_path = Path(path).expanduser().resolve()
        with cfg_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _save_json(path: str, cfg: Dict[str, Any]) -> None:
        cfg_path = Path(path).expanduser().resolve()
        with cfg_path.open("w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)

    @classmethod
    def from_config(cls, config_path: str) -> "ModelConfigManager":
        cfg = cls._load_json(config_path)
        return cls(cfg=cfg, config_path=config_path)

    # ---------- instance part ----------

    def __init__(self, cfg: Dict[str, Any], config_path: str):
        self._cfg = cfg
        self._config_path = str(Path(config_path).expanduser().resolve())

    # ----- access to full config (if needed) -----

    @property
    def raw_config(self) -> Dict[str, Any]:
        """Return the full underlying config dict."""
        return self._cfg

    # ----- CV configuration -----

    @property
    def cv_params(self) -> Dict[str, Any]:
        """
        Return CV configuration dict, e.g.:
        {
          "n_splits": 5,
          "scoring": "roc_auc",
          "shuffle": true,
          "random_state": 42
        }
        """
        return self._cfg.get("cv", {})

    @property
    def tuning_params(self) -> Dict[str, Any]:
        """
        Return hyperparameter tuning configuration, e.g.:
        {
          "n_iter": 50,
          "scoring": "average_precision",
          "n_splits": 5,
          "shuffle": true,
          "random_state": 42,
          "n_jobs": -1,
          "verbose": 3
        }
        """
        return self._cfg.get("tuning", {})

    # ----- model-level accessors -----

    def get_models_dir(self) -> Optional[str]:
        return self._cfg.get("models_dir")

    def get_reports_dir(self) -> Optional[str]:
        return self._cfg.get("data", {})["reports_dir"]

    def list_models(self) -> List[str]:
        """List all model names defined under 'models'."""
        return list(self._cfg.get("models", {}).keys())

    def list_active_models(self) -> List[str]:
        """List model names where models[model]['active'] is true or missing."""
        models = self._cfg.get("models", {})
        return [name for name, m in models.items() if m.get("active", True)]

    def get_model_cfg(self, model_name: str) -> Dict[str, Any]:
        """Return the config dict for a given model name."""
        models = self._cfg.get("models", {})
        if model_name not in models:
            raise KeyError(f"Model '{model_name}' not found in models_config.")
        return models[model_name]

    def is_active(self, model_name: str) -> bool:
        """Return whether a model is marked as active."""
        m = self.get_model_cfg(model_name)
        return bool(m.get("active", True))

    def get_estimator_type(self, model_name: str) -> str:
        """
        Return estimator_type string for a model (used to route to the correct
        sklearn / LightGBM / XGBoost constructor).
        """
        m = self.get_model_cfg(model_name)
        return m.get("estimator_type", model_name)

    def get_search_space(self, model_name: str) -> Dict[str, Any]:
        """Return hyperparameter search space for tuning."""
        m = self.get_model_cfg(model_name)
        return m.get("search_space", {})

    def get_best_params(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Return best_params dict if present, else None."""
        m = self.get_model_cfg(model_name)
        return m.get("best_params")

    def set_best_params(self, model_name: str, params: Dict[str, Any]) -> None:
        """Update best_params dict for a model in memory."""
        m = self.get_model_cfg(model_name)
        m["best_params"] = params

    def get_best_score(self, model_name: str) -> Optional[float]:
        m = self.get_model_cfg(model_name)
        return m.get("best_score")

    def set_best_score(self, model_name: str, score: float) -> None:
        m = self.get_model_cfg(model_name)
        m["best_score"] = score

    # ----- persistence -----

    def save(self) -> None:
        """
        Persist the current config (including updated best_params) back to disk.
        """
        self._save_json(self._config_path, self._cfg)
