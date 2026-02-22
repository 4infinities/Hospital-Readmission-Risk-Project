import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_validate, KFold
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

def build_pipeline (model_name, model):

    return Pipeline([('scaler', StandardScaler()), (model_name, model)])

"""
def evaluate_with_cv(pipe, X, y, model_key, log_df):
    
    cv_results = cross_validate(
        estimator=pipe,
        X=X,
        y=y,
        cv=5,
        scoring=cv_scoring,
        return_train_score=False,
    )

    log_df.loc[model_key, "roc_auc"] = cv_results["test_roc_auc"].mean()
    log_df.loc[model_key, "roc_auc_std"] = cv_results["test_roc_auc"].std()
    log_df.loc[model_key, "pr_auc"] = cv_results["test_average_precision"].mean()
    log_df.loc[model_key, "pr_auc_std"] = cv_results["test_average_precision"].std()

    return log_df
"""

def train_model(X, y, model_name, model, d30 = True):

    model_name = set_name(model_name, d30)

    pipe = build_pipeline(model_name, model)
    
    pipe.fit(X,y)
    
    return pipe, model_name

def get_predictions(X, pipe, model_name, pred):

    y_proba = pipe.predict_proba(X)[:,1]
    y_pred =  pipe.predict(X)

    pred[model_name] = y_proba

    return y_proba, y_pred, pred
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
def get_continuous_metrics(y, y_proba, model_name, metrics_data):

    metrics_data.loc[model_name, 'roc'] = roc_auc_score(y, y_proba)
    metrics_data.loc[model_name, 'pr'] = average_precision_score(y, y_proba)
    metrics_data.loc[model_name, 'brier_loss_total'] = brier_score_loss(y, y_proba)

    return metrics_data

def get_discrete_metrics(y, y_pred, model_name, metrics_data, transposed = False):

    precision, recall, f1, _= precision_recall_fscore_support(y, y_pred, average="binary")

    if not transposed:

        metrics_data.loc[model_name, 'precision'] = precision
        metrics_data.loc[model_name, 'recall'] = recall
        metrics_data.loc[model_name, 'f1'] = f1

    else: 

        metrics_data.loc['precision', model_name] = precision
        metrics_data.loc['recall', model_name] = recall
        metrics_data.loc['f1', model_name] = f1

    return metrics_data

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

def evaluate_model(X, y_true, model_name, pipe, coefs, metrics, pred):

    y_proba, y_pred, pred = get_predictions(X, pipe, model_name, pred)

    metrics = get_continuous_metrics(y_true, y_proba, model_name, metrics)

    metrics = get_discrete_metrics(y_true, y_pred, model_name, metrics)

    coefs = get_coefs(pipe, model_name, coefs)

    return coefs, metrics, pred

def build_model(X_train, X_test, y_train, y_test, name, models, coefs, metrics_log, pred_values, d30):

    trained_pipe, name = train_model(X_train, y_train, name, models[name], d30)

    if pred_values.shape[0] < 2:

        pred_values['X'] = X_test.index

        pred_values.set_index('X', inplace = True)

    if pred_values.shape[1] < 1:

        pred_values['readmit_30d'] = y_test

    elif pred_values.shape[1] < 3:

        pred_values['readmit_90d'] = y_test

    coefs, metrics_log, pred_values = evaluate_model(X_test, y_test, name, trained_pipe, coefs, metrics_log, pred_values)

    return coefs, metrics_log, pred_values

"""
def build_both_models(X, flags, name, models, coefs, metrics_log, pred_values):

    coefs, metrics_log, pred_values = build_model(X, flags['readmit_30d'], name, models, coefs, metrics_log, pred_values, d30 = True)
    
    coefs, metrics_log, pred_values = build_model(X, flags['readmit_90d'], name, models, coefs, metrics_log, pred_values, d30 = False)

    return coefs, metrics_log, pred_values

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

    values = values.drop(columns = 'fold')

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
            # call your refactored build_both_models_from_split here
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

        if(model_threshold not in ['readmit_30d', 'readmit_90d']):

            true_col = ('readmit_30d' if 'd30' in model_threshold else 'readmit_90d')

            metrics.loc['TP', model_threshold] = ((thresholds[model_threshold] == 1) & (thresholds[true_col] == 1)).sum()
            metrics.loc['FP', model_threshold] = ((thresholds[model_threshold] == 1) & (thresholds[true_col] == 0)).sum()
            metrics.loc['FN', model_threshold] = ((thresholds[model_threshold] == 0) & (thresholds[true_col] == 1)).sum()
            metrics.loc['TN', model_threshold] = ((thresholds[model_threshold] == 0) & (thresholds[true_col] == 0)).sum()

            y_true = thresholds[true_col].astype(int)

            metrics = get_discrete_metrics(y_true, thresholds[model_threshold], model_threshold, metrics, transposed = True)

    return metrics

def build_threshold_metrics(values):

    thresholds = build_thresholds(values)

    metrics_index = ['TP', 'FP', 'FN', 'TN', 'precision', 'recall', 'f1']

    metrics = pd.DataFrame(index = metrics_index)

    metrics = calc_threshold_metrics(thresholds, metrics)

    return thresholds, metrics
