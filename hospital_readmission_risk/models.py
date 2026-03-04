import pandas as pd
import numpy as np
from pandas.core.internals.managers import new_block
from sklearn.model_selection import StratifiedKFold, train_test_split, cross_validate, KFold
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_fscore_support, brier_score_loss
from config import models, cv_scoring, proba_metrics, pred_metrics
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from lightgbm import LGBMClassifier
from sklearn.ensemble import RandomForestClassifier

def make_train_test_split(X, y):

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

    return X_train, X_test, y_train, y_test

def model_config_builder(models):

    model_dict = {
    "logreg": LogisticRegression,
    "rf": RandomForestClassifier,
    "lightgbm": LGBMClassifier,
    }

    models_with_params = {}

    for model in models:

        cls = model_dict[model['name']]
        models_with_params[model['name']] = cls(**model['params'])

    return models_with_params

def set_name (model_name, d30 = True):

    name = model_name + ('_d30' if d30 else '_d90')
    
    return name 

def get_cv_columns():

    cols = []

    for col in cv_scoring:

        cols.append(col)
        cols.append(f"{col}_std")

    return cols    

def build_pipeline (model_name, model):

    return Pipeline([('scaler', StandardScaler()), (model_name, model)])

def evaluate_with_cv(pipe, X, y, model_key, log_df):
    
    cv_results = cross_validate(
        estimator=pipe,
        X=X,
        y=y,
        cv=StratifiedKFold(n_splits = 5, shuffle = True, random_state = 42),
        scoring=cv_scoring,
        return_train_score=False,
    )

    cv_metrics : dict[str, float] = {}
    """
    for metric in cv_scoring:
        
        key = f"test_{metric}"
        cv_metrics[metric] = cv_results[key].mean()
        cv_metrics[f"{metric}_std"] = cv_results[key].std()
        
        key = f"test_{metric}"
        cv_metrics[metric] = cv_results[key]
    """
    scores = cv_results["test_roc_auc"]          # shape (n_folds,)
    aps = cv_results["test_average_precision"]   # shape (n_folds,)

    fold_df = pd.DataFrame({
            "model": model_key,
            "fold": range(len(scores)),
            "roc_auc": scores,
            "average_precision": aps,
                })

    cv_metrics = pd.DataFrame(cv_metrics, index = [model_key])

    return pd.concat([log_df, fold_df], ignore_index = True)

def train_model(X, y, model_name, model, cv_log, d30 = True, skip_cross_val = False):

    model_name = set_name(model_name, d30)

    pipe = build_pipeline(model_name, model)

    if not skip_cross_val:
        cv_log = evaluate_with_cv(pipe, X, y, model_name, cv_log)
    
    pipe.fit(X,y)
    
    return pipe, model_name, cv_log

def get_predictions(X, pipe, model_name, pred_values):

    y_proba = pipe.predict_proba(X)[:,1]
    y_pred =  pipe.predict(X)

    pred_values[model_name] = y_proba

    return y_proba, y_pred, pred_values
"""
def metrics_config_builder(metrics):

    metrics_dict = {
    "roc": roc_auc_score,
    "pr": average_precision_score,
    "brier_loss_total": brier_score_loss
    }

    metrics_with_params = {}

    for metric in metrics:

        metrics_with_params[metric] = metrics_dict[metric]

    return metrics_with_params
"""
def get_continuous_metrics(y, y_proba) -> dict:

    return {'roc': roc_auc_score(y, y_proba),
                'pr': average_precision_score(y, y_proba),
                'brier_loss_total': brier_score_loss(y, y_proba)}

def get_discrete_metrics(y, y_pred) -> dict:

    precision, recall, f1, _= precision_recall_fscore_support(y, y_pred, average="binary")

    return {
            'precision': precision,
            'recall': recall,
            'f1': f1
            }

def get_normalized_coefs(coefs):

    total = sum(abs(coefs))

    return coefs / total

def get_coefs(pipe, model_name, coefs):

    est = pipe.named_steps[model_name]

    if isinstance(est, LogisticRegression):
    # est.coef_.shape == (1, n_features) for binary
        coefs[model_name] = est.coef_[0]

    elif hasattr(est, "feature_importances_"):
    # trees, random forest, gradient boosting
        coefs[model_name] = est.feature_importances_

    norm_name = 'norm_' + model_name

    coefs[norm_name] = get_normalized_coefs(coefs[model_name])

    return coefs

def evaluate_model(X, y_true, model_name, pipe, coefs, metrics, pred_values):

    y_proba, y_pred, pred_values = get_predictions(X, pipe, model_name, pred_values)

    new_metrics = get_continuous_metrics(y_true, y_proba)

    new_metrics.update(get_discrete_metrics(y_true, y_pred))

    new = pd.DataFrame(new_metrics, index = [model_name])

    coefs = get_coefs(pipe, model_name, coefs)

    return coefs, pd.concat([metrics, new]), pred_values

def build_model(X_train, y_train, X_test, y_test, name, models, coefs, metrics_log, pred_values, cv_log, d30, skip_cross_val):

    trained_pipe, name, cv_log = train_model(X_train, y_train, name, models[name], cv_log, d30, skip_cross_val)
    """
    if pred_values.shape[0] < 2:

        pred_values['X'] = X_test.index

        pred_values.set_index('X', inplace = True)

    if pred_values.shape[1] < 1:

        pred_values['rel_readmit_30d'] = y_test
    elif pred_values.shape[1] < 3:

        pred_values['readmit_90d'] = y_test
    """
    coefs, metrics_log, pred_values = evaluate_model(X_test, y_test, name, trained_pipe, coefs, metrics_log, pred_values)

    return coefs, metrics_log, pred_values, cv_log

def build_both_models(X_train, y_train, X_test, y_test, name, models, coefs, metrics_log, pred_values, cv_log, skip_cross_val):

    coefs, metrics_log, pred_values, cv_log = build_model(X_train, y_train['rel_readmit_30d'], X_test, y_test['rel_readmit_30d'], name, models, coefs, metrics_log, pred_values, cv_log, d30 = True, skip_cross_val =skip_cross_val)
    
    #coefs, metrics_log, pred_values, cv_log = build_model(X, flags['readmit_90d'], name, models, coefs, metrics_log, pred_values, cv_log, d30 = False)

    return coefs, metrics_log, pred_values, cv_log
"""
def build_models(models, df_numeric, df_results):

    models_built = model_config_builder(models)

    #################

    metrics_log = pd.DataFrame(columns = ['roc', 'pr', 'brier_loss_total', 'brier_loss_half', 'precision', 'recall', 'f1'])

    coefs = pd.DataFrame(index = df_numeric.columns)

    pred_values = pd.DataFrame()

    for model_name in models_built:

        coefs, metrics_log, pred_values = build_both_models(df_numeric, df_results, model_name, models_built, coefs, metrics_log, pred_values)

    return {
        'coefs': coefs,
        'metrics_log': metrics_log,
        'pred_values': pred_values,
            }
"""
def merge_predictions(source):

    values = pd.DataFrame(columns = source[0].columns)

    for table in source:

        values = pd.concat([values, table])

    values = values.sort_index()

    return values 

def build_models_cv(models, df_numeric, df_results, n_splits=5, random_state=42):

    models_built = model_config_builder(models)

    all_metrics_logs = []
    all_coefs = []
    all_pred_values = []

    kf = KFold(n_splits=n_splits, shuffle=True, random_state=random_state)

    X = df_numeric

    y_30 = df_results['readmit_30d']
    y_90 = df_results['readmit_90d']

    for fold_idx, (train_idx, test_idx) in enumerate(kf.split(X), start=1):

        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y30_train, y30_test = y_30.iloc[train_idx], y_30.iloc[test_idx]
        y90_train, y90_test = y_90.iloc[train_idx], y_90.iloc[test_idx]

        metrics_log = pd.DataFrame(columns = ['roc', 'pr', 'brier_loss_total', 'precision', 'recall', 'f1'])

        coefs = pd.DataFrame(index = df_numeric.columns)

        pred_values = pd.DataFrame()

        for model_name in models_built:

            coefs, metrics_log, pred_values = build_model(
                X_train, X_test, y30_train, y30_test, model_name, models_built, coefs, metrics_log, pred_values, d30 = True)

            coefs, metrics_log, pred_values = build_model(
                X_train, X_test, y90_train, y90_test, model_name, models_built, coefs, metrics_log, pred_values, d30 = False)

        # store per‑fold results (or aggregate here)
        all_metrics_logs.append(metrics_log.assign(fold=fold_idx))
        all_coefs.append(coefs.add_suffix(f"_fold{fold_idx}"))
        all_pred_values.append(pred_values.assign(fold=fold_idx))

    # combine folds (e.g. mean over folds for final table)
    ...
    all_pred_values = merge_predictions(all_pred_values)

    return {
        "coefs": all_coefs,
        "metrics_log": all_metrics_logs,
        "pred_values": all_pred_values,
    }
"""
This needs to be built shortly
"""
def build_and_evaluate_models(models, X_train, y_train, X_test, y_test, skip_cross_val = False):

    models_built = model_config_builder(models)

    cv_log = pd.DataFrame(columns = get_cv_columns())
    coefs = pd.DataFrame(index = X_train.columns)
    pred_values = y_test.copy()
    metrics_log = pd.DataFrame(columns = pred_metrics + proba_metrics)

    for name in models_built:

        coefs, metrics_log, pred_values, cv_log = build_both_models(X_train, y_train, X_test, y_test, name, models_built, coefs, metrics_log, pred_values, cv_log, skip_cross_val)

    pred_values.drop(columns = ['rel_readmit_90d', 'readmit_30d', 'readmit_90d'], inplace = True)

    return {
        "coefs": coefs,
        "metrics_log": metrics_log,
        "pred_values": pred_values,
        "cv_log": cv_log
    }

def build_thresholds(values):

    thresholds = pd.DataFrame(index = values.index)

    for col in values.columns:

        if('_d' in col):

            for t in [round(t, 2) for t in np.arange(0.05, 1, 0.05)]:

                thresholds[col + '_' + str(t)] = (values[col] >= t).astype(int)

        else: 

            thresholds[col] = values[col]

    return thresholds

def calc_threshold_metrics(thresholds, metrics):

    for model_threshold in thresholds.columns:

        data: dict[str, float] = {}

        if(model_threshold != 'rel_readmit_30d'):

            true_col = 'rel_readmit_30d'

            data.update({
                'TP' : ((thresholds[model_threshold] == 1) & (thresholds[true_col] == 1)).sum(),
                'FP' : ((thresholds[model_threshold] == 1) & (thresholds[true_col] == 0)).sum(),
                'FN' : ((thresholds[model_threshold] == 0) & (thresholds[true_col] == 1)).sum(),
                'TN' : ((thresholds[model_threshold] == 0) & (thresholds[true_col] == 0)).sum()
            })

            y_true = thresholds[true_col].astype(int)

            data.update(get_discrete_metrics(y_true, thresholds[model_threshold]))

            metrics[model_threshold] = pd.Series(data)

    return metrics

def build_threshold_metrics(values):

    thresholds = build_thresholds(values)

    metrics_index = ['TP', 'FP', 'FN', 'TN', 'precision', 'recall', 'f1']

    metrics = pd.DataFrame(index = metrics_index)

    metrics = calc_threshold_metrics(thresholds, metrics)

    return thresholds, metrics
