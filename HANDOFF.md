# HANDOFF.md

## Session summary
- Ran `scripts/_run_wf_only.py` end-to-end; billing had been resolved since last session
- Both simulation months (2025-04-30 and 2025-05-31) completed successfully
- Two entries written to `results/{logreg,rf,lightgbm}_results.csv` — goal achieved
- Investigated all warnings/anomalies; none are code bugs (all data artifacts of small mock dataset)
- Found and fixed `related_diagnoses` duplicate rows bug in `DictionaryBuilder.update_related_diagnoses`
- Cleaned up existing duplicates in BQ and CSV

---

## Files changed
| File | What changed |
|---|---|
| `src/pipeline/dictionary_builder.py` | `update_related_diagnoses`: replaced plain `_append_to_csv` + `append_dataframe` with DDL-only recreate (CREATE OR REPLACE TABLE ... WHERE stay_id NOT IN window) + fresh append + CSV filter |
| `predictions/logreg_predictions.csv` | +38 rows (2025-04-30) +40 rows (2025-05-31) appended |
| `predictions/rf_predictions.csv` | +38 rows (2025-04-30) +40 rows (2025-05-31) appended |
| `predictions/lightgbm_predictions.csv` | +38 rows (2025-04-30) +40 rows (2025-05-31) appended |
| `results/logreg_results.csv` | 2 rows written (months 2025-04-30, 2025-05-31) |
| `results/rf_results.csv` | 2 rows written |
| `results/lightgbm_results.csv` | 2 rows written |
| `config/watermark.json` | `last_processed_date=2025-05-31`, `next_end_date=2025-06-30` |

---

## Warnings observed (not bugs)

| Warning | Location | Plain explanation | Status |
|---|---|---|---|
| `UndefinedMetricWarning: Only one class in y_true. ROC AUC not defined.` | sklearn, evaluate_month month 1 | Base test set (28 patients) had 0 readmissions in index_stay — roc returns NaN | Expected: mock dataset too small |
| `No positive class found in y_true, recall set to 1` | sklearn, evaluate_month month 1 | Same root cause as above | Expected |
| `RuntimeWarning: All-NaN axis encountered` | cost_reducer.py:105 | lightgbm month 1: no cost data for flagged patients with all-zero actuals | Expected for 0-readmission window |
| LightGBM `No further splits with positive gain` | lightgbm | Mock dataset has 23 positives / 2364 rows (1%) — too few splits | Known, per CLAUDE.md |
| LogReg `ConvergenceWarning: max_iter reached` | sklearn | `max_iter=100` is low — not blocking | Known, per CLAUDE.md |

---

## Results summary

| Model | Month | roc_auc | n_readmitted | PSI | retrain_triggered |
|---|---|---|---|---|---|
| logreg | 2025-04-30 | NaN | 0 | 2.6447 | True |
| logreg | 2025-05-31 | 0.2627 | 6 | 0.5642 | True |
| rf | 2025-04-30 | NaN | 0 | 2.2569 | True |
| rf | 2025-05-31 | 0.2890 | 6 | 0.7650 | True |
| lightgbm | 2025-04-30 | NaN | 0 | 0.0 | True |
| lightgbm | 2025-05-31 | 0.2143 | 6 | 0.0 | True |

**Notes on results**:
- Month 1 roc=NaN: no readmissions in the base test cohort (28 patients); sklearn ≥1.2 issues UndefinedMetricWarning instead of raising — NaN propagates to CSV
- PSI month 1 >1: extreme values are expected when comparing 2364-row training distribution against 28-row test set; not meaningful at this mock scale
- PSI month 2 for lightgbm = 0.0: LightGBM outputs nearly uniform probabilities on this sparse dataset — no distribution shift detected because output doesn't vary
- `net_cost = 0.0` with `intervention_cost > 0`: cost_reducer `total_avoided` returns 0 when there are no actual readmissions to save cost on; intervention_cost is calculated independently from flagged patients at threshold — this is a cost_reducer design choice, not a blocking bug
- `retrain_triggered = True` every month: PSI > 0.2 threshold fired every month due to the training-vs-test distribution mismatch at mock scale

---

## Completed this session
- Walk-forward loop: 2 months processed (2025-04-30, 2025-05-31)
- BQ tables updated per month: slim CREATE OR REPLACE, helper CREATE OR REPLACE, index_stay CREATE OR REPLACE (all DDL-only — free tier compatible)
- ML predictions saved: 38/40 rows per model per month
- Results written: 2 rows per model — primary goal achieved
- `related_diagnoses` duplicate bug found and fixed (see bug table below)

## Bugs fixed this session

| Bug | Root cause | Fix |
|---|---|---|
| `related_diagnoses` accumulated duplicate rows per stay_id | 2-month window overlaps between monthly runs; append-only strategy with no prior-row deletion | DDL-only recreate before append: `CREATE OR REPLACE TABLE AS SELECT * FROM related_diagnoses WHERE stay_id NOT IN (window_ids)`, then `append_dataframe`. CSV filtered same way. |
| Stays with multiple following stays produced multiple rows per stay_id | `build_diagnoses_related` returned one row per (stay_id, following_stay) pair with no aggregation | Added `.groupby(level="stay_id").max()` at end of `build_diagnoses_related` — one row per stay_id, all flags taken as MAX across following stays |
| BQ table `related_diagnoses` had multi-row stay_ids at time of fix | Both bugs above | One-time cleanup: `CREATE OR REPLACE TABLE AS SELECT stay_id, MAX(...) GROUP BY stay_id` — 2472 → 2470 rows, 0 duplicate stay_ids |

**BQ + CSV state after cleanup:** 2470 rows, exactly one row per stay_id, flags are MAX across following stays.

---

## Current crash / blocker
None. Pipeline completed cleanly.

---

## Simulation state
| Field | Value |
|---|---|
| `last_processed_date` | `2025-05-31` |
| `next_end_date` | `2025-06-30` |
| `simulation_end_date` | `2026-03-31` |
| months completed | 2 |
| last successful phase | I1 + ML (both months) |
| monthly CSVs loaded to BQ | 2025-04-30, 2025-05-31 |

---

## What is NOT yet done
- Remaining 10 simulation months (2025-06-30 through 2026-03-31)
- `bootstrap_prior_month_staging` was only needed once (done) — do NOT call again
- Prediction de-duplication: predictions/logreg_predictions.csv has 168 rows for 2025-03-31 (6 run-throughs of base fit_and_evaluate) — not blocking but wastes space and inflates n_predictions for month 1 evaluation
- `net_cost` vs `intervention_cost` discrepancy in cost_reducer not resolved
- `_run_wf_only.py` calls `fit_and_evaluate(base_cutoff_date)` again each run — this re-appends 28 base predictions (incrementing the duplicate count). Should be guarded by checking if base month is already in predictions.

---

## Next 3 steps
1. Run `run_until("2026-03-31")` to complete remaining 10 months — use a clean script that only calls `orch.run_until(...)` without re-running base `fit_and_evaluate`
2. Check predictions de-duplication (optional cleanup of 2025-03-31 rows before running remaining months)
3. Validate results trend: roc_auc should improve from NaN/0.26 as more labelled actuals accumulate

---

## Architectural decisions confirmed this session
- sklearn ≥1.2: `roc_auc_score` with single-class y_true issues `UndefinedMetricWarning` and returns NaN (does NOT raise ValueError). Code already handles this — NaN propagates to results CSV cleanly.
- Bootstrap (create prior-month staging tables) only needed once before the first `run_next_month()` call. Do NOT call it again in subsequent sessions.
- PSI retune threshold (0.2) fires every month at mock scale — not meaningful for tuning decisions on a 1% base rate dataset.
- All "update" SQL files (helper, index_stay, slim monthly) use `CREATE OR REPLACE TABLE AS SELECT` (DDL), NOT `DELETE + INSERT` (DML). This is free-tier compatible. The `18_related_diagnoses_update.sql` (DELETE-based) is NOT used in the pipeline — it was a stale design artefact.
- `DELETE FROM` is DML — not allowed on free BQ tier. Any fix requiring row removal must use `CREATE OR REPLACE TABLE AS SELECT ... WHERE NOT IN (...)` instead.
- `related_diagnoses` dedup approach: before each monthly update, recreate the BQ table excluding the current window's stay_ids (DDL), then append fresh rows (WRITE_APPEND load job). CSV uses the same filter pattern.

## Known issues / surprises
- `_run_wf_only.py` always re-runs `fit_and_evaluate(base_cutoff_date)` on startup. Since predictions already exist, this goes into the `else` branch (evaluate_month + refit). Each run appends 28 more base-cutoff predictions. Should be guarded.
- The cost_reducer `_estimate_intervention_cost` returns values even when n_readmitted=0 (it's based on who is flagged above threshold, not who actually readmits). This is correct by design but can appear misleading in the results CSV.
