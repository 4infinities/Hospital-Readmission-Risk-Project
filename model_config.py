logreg_params = {
    'class_weight': 'balanced', 
    'solver': 'saga', 
    'max_iter': 1000}

rf_params= {
    'n_estimators': 500,
    'min_samples_split': 2,
    'min_samples_leaf': 2,
    'max_features': 0.5,
    'max_depth': None,
    'class_weight': 'balanced_subsample',
    "random_state": 42,
}

lgbm_params = {
    'subsample': 0.6,
    'reg_lambda': 0.5,
    'reg_alpha': 0.0,
    'num_leaves': 64,
    'n_estimators': 400,
    'min_child_samples': 5,
    'max_depth': -1,
    'learning_rate': 0.03,
    'colsample_bytree': 0.5,
    "class_weight": "balanced",
    "random_state": 42,
}

MODEL_CONFIGS = [
    {
        "name": "logreg",
        "params": logreg_params,
    },
    {
        "name": "rf",
        "params": rf_params,
    },
    {
        "name": "lightgbm",
        "params": lgbm_params,
    },
]
