models = [
    {
        "name": "logreg",
        "params": {
    'class_weight': 'balanced', 
    'solver': 'saga', 
    'max_iter': 1000
        },
    },
    {
        "name": "rf",
        "params": {
    'n_estimators': 250,
    'min_samples_split': 10,
    'min_samples_leaf': 5,
    'max_features': 0.6,
    'max_depth': None,
    'class_weight': 'balanced_subsample',
    "random_state": 42,
        },
    },
    {
        "name": "lightgbm",
        "params": {
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
    "is_unbalance" : True,
    "random_state": 42
        },
    },
]

data_path = "D:\Python Projects\Hospital readmission risk\data\cleaned\index_stay.csv"

credentials = r"D:\Python Projects\Hospital readmission risk\.secrets\hospital-readmission-4-code.json"

project_name = "hospital-readmission-4"

numeric_cols = [
    'patient_age', 'gender', 'length_of_stay', 'num_diagnoses', 'stay_type',
    'num_chronic_conditions', 'num_procedures', 'has_diabetes', 'has_cancer',
    'has_hiv', 'has_hf', 'has_alz', 'has_ckd', 'had_surgery', 'admission_cost',
    'total_procedure_costs', 'total_medication_costs', 'total_stay_cost', 
    'admissions_365d', 'tot_length_of_stay_365d', 'avg_cost_of_prev_stays',
    'is_planned', 'following_unplanned_admission_flag', 'readmit_30d', 'readmit_90d'
    ]

log_cols = ['total_stay_cost', 'avg_cost_of_prev_stays', 'total_procedure_costs',
    'total_medication_costs']

cv_scoring = ["roc_auc", "average_precision"]

proba_metrics = ['roc', 'pr', 'brier_loss_total']

pred_metrics = ['precision', 'recall', 'f1']