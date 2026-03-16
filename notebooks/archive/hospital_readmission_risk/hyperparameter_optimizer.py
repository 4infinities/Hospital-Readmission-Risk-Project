import pandas as pd
import numpy as np
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold


def randomize_search(estimator, param_distribution, X_train, y_train, random_state = 42):

    search = RandomizedSearchCV(
    estimator=estimator,
    param_distributions=param_distribution,
    n_iter=50,
    scoring="average_precision",   # or "roc_auc"
    cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=random_state),
    n_jobs=-1,
    verbose=3,
    random_state=random_state
    )
    search.fit(X_train, y_train)
    """
    terminals = 0
    new_distribution = param_distribution
    for key in search.best_params_:

        if search.best_params_[key].isinstance(float) or search.best_params_[key].isinstance(int):
            if search.best_params_[key] in [param_distribution[key][0], param_distribution[key][-1]]:
                terminals += 1
                low_boundary = (np.roundup(search.best_params_[key] / 2) if search.best_params_[key].isinstance(int) else search.best_params_[key] / 2)
                high_boundary = search.best_params_[key] * 2
                new_distribution[key] = [low_boundary, search.best_params_[key], high_boundary]

    if terminals > 0:
        return randomize_search(estimator, new_distribution, X_train, y_train, random_state)
    """
    return search.best_params_, search.best_score_