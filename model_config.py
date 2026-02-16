logreg_params = {
    'class_weight': 'balanced', 
    'solver': 'saga', 
    'max_iter': 1000}

rf_params= {
    'n_estimators': 250,
    'min_samples_split': 10,
    'min_samples_leaf': 5,
    'max_features': 0.6,
    'max_depth': None,
    'class_weight': 'balanced_subsample',
    "random_state": 42,
}

lgbm_params = {
    'subsample': 0.7,
    'reg_lambda': 0.25,
    'reg_alpha': 0.5,
    'num_leaves': 127,
    'n_estimators': 200,
    'min_child_samples': 10,
    'max_depth': -1,
    'learning_rate': 0.05,
    'colsample_bytree': 0.9,
    "objective" : "binary", 
    "force_col_wise" : True,
    "random_state": 42
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

