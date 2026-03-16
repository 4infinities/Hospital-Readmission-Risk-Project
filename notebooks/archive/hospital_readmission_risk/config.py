"""
Configuration for the Hospital Readmission Risk project.

Defines:
- model specs and hyperparameters
- BigQuery credentials and SQL
- feature lists and metrics
- default cost / intervention assumptions
"""

# Models to train and evaluate.
models = [
    {
        "name": "logreg",
        "params": {
            "class_weight": "balanced",
            "solver": "saga",
            "max_iter": 1000,
        },
    },
    {
        "name": "rf",
        "params": {
            "n_estimators": 250,
            "min_samples_split": 10,
            "min_samples_leaf": 5,
            "max_features": 0.6,
            "max_depth": None,
            "class_weight": "balanced_subsample",
            "random_state": 42,
        },
    },
    {
        "name": "lightgbm",
        "params": {
            "subsample": 0.7,
            "reg_lambda": 0.25,
            "reg_alpha": 0.5,
            "num_leaves": 127,
            "n_estimators": 200,
            "min_child_samples": 10,
            "max_depth": -1,
            "learning_rate": 0.05,
            "colsample_bytree": 0.9,
            "objective": "binary",
            "force_col_wise": True,
            "is_unbalance": True,
            "random_state": 42,
        },
    },
]

# Evaluation metrics.
cv_scoring = ["roc_auc", "average_precision"]
proba_metrics = ["roc", "pr", "brier_loss_total"]
pred_metrics = ["precision", "recall", "f1"]

# Cost analysis columns.
cost_cols = [
    "stay_id",
    "cost_per_day_stay",
    "total_readmission_cost",
    "avg_cost_of_prev_stays",
]

# Default intervention assumptions.
def_prob_red = 0.1
def_desired_prob_red = 0.2
