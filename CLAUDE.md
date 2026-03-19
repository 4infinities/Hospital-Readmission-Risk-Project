# CLAUDE.md — Hospital Readmission Risk Pipeline

## Project Overview
ML pipeline predicting 30-day unplanned readmission risk across California hospitals.
Fully OOP, Python, runs from Jupyter. Synthetic EHR data via Synthea. BigQuery as DWH.

## Architecture Summary
- Raw data: Synthea CSVs → BigQuery (no partitioning at load)
- Slim tables: filtered/clean, partitioned by discharge month, clustered by patient_id + encounter_type
- Helper tables: aggregations built on slim, updated via MERGE/upsert (incremental only)
*As of now there are only first creation queries in existence, not the update queries, they have to be built
- Walk-forward validation: 10 years train, 60 months simulation, retune every 6 months or on PSI > 0.2
*Not yet implemented. Earlier it was one pure 10 year train dataset and pure 8 year another test-dataset.

## Current vs Target Data Architecture

### CURRENT STATE (do not break this while refactoring)
Two separate Synthea datasets in BigQuery:
- Train dataset: raw tables, slim tables, helper tables, index stay table → produces X_train, y_train
- Test dataset: same structure, separate BQ tables → produces X_test, y_test
Both processed through identical preprocessing pipeline.
Models trained on X_train/y_train, evaluated on X_test/y_test as a single batch.

### TARGET STATE (walk-forward, not yet implemented)

Each iteration simulates a real monthly data arrival:

1. Watermark advances by one month
2. New month's data is MERGED into slim tables (partitioned append, not full rebuild)
3. Helper tables updated via MERGE (incremental aggregations only)
4. Dictionary delta checked — new SNOMED codes mapped to existing flags
5. DataPreprocessor queries BQ directly, scoped by watermark dates → produces X_test
6. Model predicts on X_test → stored to predictions/ with month suffix
7. Next iteration: previous month outcomes now exist in BQ index stay table
8. Evaluator queries BQ for those outcomes → evaluates previous predictions
9. Full training set re-queried from BQ (watermark expanded) → model retrained
10. Repeat for 60 months

⚠ Data must flow through BQ at every step — no in-memory date filtering as substitute for 
  real incremental BQ operations. The pipeline must be deployable as-is.
⚠ Walk-forward loop is NOT YET BUILT.
⚠ Do not delete existing dual-dataset BQ tables until walk-forward is validated end-to-end.

## Key Classes (src/)
- SyntheaRunner — generates synthetic data (run once)
- BigQueryLoader — loads CSVs to BQ raw/helpers datasets. Only class that writes to BQ from local files.
- BigQueryTransformer — SQL recipe sequences → slim/helper tables
- DictionaryBuilder — orchestrates SNOMED concept mapping functions from 
  `src/pipeline/dictionaries.py` into CSVs only. No BQ interaction.
- DataPreprocessor — STATELESS, column operations only, no fit/transform state
- ModelConfigManager — model config JSON, active model list, hyperparams
- HyperparameterTuner — GridSearchCV, custom business metric scorer that optimizes 
net cost savings (not AUC/F1). Scorer runs full CostReducer logic per candidate. Do not replace with standard sklearn scorers.
- ModelRegistry — fits, saves models as joblib PKL
- Evaluator — predictions, classic metrics, threshold metrics, performance report
- CostReducer — intervention logic, avoided cost estimation

## Critical Design Decisions — Do Not Change Without Asking
- DataPreprocessor is intentionally STATELESS. No scalers, no encoders fitted here.
- Diagnoses are never used as raw codes. Only binary flags: is_chronic, is_renal, is_cardiac, is_respiratory, is_liver that are feature engineered via dictionaries and helper_clinical_table.
- Feature space is FIXED at first build. New codes map to existing flags only.
- y_test is NOT stored — target design: outcomes retrieved from BQ index stay table 
at next iteration. ⚠ NOT YET IMPLEMENTED — current pipeline stores y_test explicitly.
- Watermark pattern: last_processed_date and next_end_date stored in config (⚠ NOT YET IMPLEMENTED — dates should be month-end dates e.g. 2015-01-31)
- Imbalance handling: LR=class_weight='balanced', RF=balanced_subsample, LGBM=is_unbalance=True.
- Readmission definition: UNPLANNED only. Careplan-related readmissions are excluded.
- Related unplanned readmission flags and 90-day flags are calculated purely for visualization later

## Environment
- OS: Windows, PowerShell
- Python: .venv (active), .venv-old (baseline comparison, do not touch)
- Run Python with: `.venv\Scripts\python.exe` or activate with `.venv\Scripts\Activate.ps1`
- BigQuery auth: Application Default Credentials (ADC). Run `gcloud auth application-default login` if BQ calls fail.
- BQ quota: 1TB/month. Always use MERGE/upsert for helper tables. Never run full rebuilds in simulation loop.

## Logging
`src/utils/logger.py` exists with implementation but has NOT been tested or plugged 
into any class yet. First task when touching any class: add logging using this pattern:

    from src.utils.logger import get_logger
    self.logger = get_logger(__name__)

Verify logger.py works before wiring it into the full pipeline — test it in isolation first:

    python -c "from src.utils.logger import get_logger; l = get_logger('test'); l.info('ok')"

Log files should write to `logs/pipeline_YYYYMMDD.log`.

## Testing
- No pytest unit tests exist yet. Test coverage is a pending task.
- For pipeline smoke testing, use the mock profile in bigquery_config.json 
(2-year train, 1-year monthly simulation). Do not use the full dataset for smoke tests.

## Known Issues
- SQL queries in BigQueryTransformer are refactored and clean UP TO the dictionary 
creation step. Dictionary creation queries are NOT yet refactored — do not assume they follow the same patterns.
- pipreqs requires: `pipreqs --encoding iso-8859-1 --ignore .venv`
- HRRP penalty deferred to next version — do not implement in current scope.

## What NOT to Do
- Do not run full table rebuilds in the walk-forward loop — use MERGE.
- Do not add new feature dimensions mid-simulation — feature space must be fixed.
- Do not modify .venv-old.
- Do not commit .pkl files, .env, or GCP service account JSONs.
- Do not add scalers or encoders to DataPreprocessor.