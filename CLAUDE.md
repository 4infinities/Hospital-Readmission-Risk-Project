# CLAUDE.md — Hospital Readmission Risk Pipeline

## Project Overview
ML pipeline predicting 30-day unplanned readmission risk across California hospitals.
Fully OOP, Python, runs from Jupyter. Synthetic EHR data via Synthea. BigQuery as DWH.

---

## Architecture Summary

- Raw data: Synthea CSVs → segmented by `SyntheaSegmenter` → BigQuery
- Base load: pre-cutoff records loaded once as bulk batch (`encounters_base.csv`, etc.)
- Monthly segments: one CSV per month per table (e.g. `encounters_2015-01-31.csv`), loaded and processed individually in simulation loop
- Slim tables: one isolated BQ table per month (`slim_encounters_2015_01_31`); consolidated at end of simulation via one-time INSERT into master slim table
- Helper tables: core stays fixed; new rows appended per month using prior helper data as baseline — no full rebuild
- Index stay table: grows per month via INSERT
- Walk-forward validation: full historical base load → 60-month monthly simulation; retune every 6 months or on PSI > 0.2

---

## Pipeline Phases

### Phase 1 — Base load (run once)
```
SyntheaRunner
  → SyntheaSegmenter
  → BigQueryLoader (base CSVs: encounters_base.csv etc.)
  → BigQueryTransformer (slim / helper / index creation queries)
  → DictionaryBuilder (full build)
  → DataPreprocessor → X_train, y_train
  → ModelRegistry (initial model fit)
```

### Phase 2 — Monthly simulation loop (per month, 60 iterations)
```
1. BigQueryLoader loads monthly CSV segment (e.g. encounters_2015-01-31.csv)
   → BQ raw staging table for that month
2. BigQueryTransformer creates isolated slim table for that month only
3. DictionaryBuilder runs delta over 2-month window
   (current month + prior month — catches encounter groups spanning month boundary)
4. Helper tables: new rows appended using existing helper data as per-patient baseline
   — delta patients only, JOIN to existing aggregates. Never full rebuild.
5. Index stay table: new month's rows INSERTed (grows cumulatively)
6. DataPreprocessor queries BQ scoped by watermark → X_test
7. Model predicts → stored to predictions/ with month-end date suffix
8. Next iteration: prior month outcomes now in index stay table
   → Evaluator retrieves and scores prior month's predictions
9. Retrain if on schedule (every 6 months) or PSI > 0.2
10. Watermark advances; repeat
```

### Phase 3 — End-of-simulation consolidation (run once)
One-time INSERT of all monthly slim tables into a single master slim table per entity.
Master table is a physical copy, partitioned by discharge month, queryable like any BQ table.

---

## Current State — What Is and Is Not Built

### ✅ Completed and working
- All 10 pipeline classes wired with `get_logger(__name__)` via `src/utils/logger.py`
- SQL comments added to all 20 files in `sql/` (creation queries)
- Slim table creation queries partitioned by discharge month, clustered correctly
- All 7 incremental update SQL files written to `sql/update/` (files 13–19)
- All 4 dictionary delta SQL files written to `sql/update/` (files 09–12)
- `related_diagnoses` delta SQL at `sql/update/18_related_diagnoses_delta.sql`
- `BigQueryTransformer`: `load_sql`, `load_sql_with_end_date`, `run_query_sequence(end_date)`, `append_dataframe`
- `DictionaryBuilder` refactored: takes only `(transformer, io_config_path)`; no BQ config, no profile logic; all 5 `update_*` methods take `end_date` only
- `WalkForwardOrchestrator` (`src/pipeline/walk_forward.py`): `run_month`, `run_next_month`, `run_until`
- Watermark config at `config/watermark.json`: `last_processed_date` / `next_end_date`
- `scripts/mock_test_runner.ipynb`: 13-cell smoke test notebook (written, not yet run end-to-end)
- `bigquery_recipes.json`: recipe `[3]` = helper updates (13,15,14,16,17); recipe `[4]` = index_stay update (19)
- Placeholder token standardised: `{{END_DATE}}` only across all update + delta SQL

### ⚠ Not yet built
- `SyntheaSegmenter` — does not exist yet; is the next major class to implement
- Walk-forward simulation loop — `mock_test_runner.ipynb` written but not yet executed end-to-end
- Slim table MERGE/incremental append — removed from scope; slim tables created once from full CSV load
- Dual-dataset BQ tables (train/test) — still active; do not delete until walk-forward validated end-to-end
- `y_test` not yet removed from pipeline — target design retrieves outcomes from BQ index stay table at next iteration; current pipeline still stores `y_test` explicitly
- Watermark not yet wired into Phase 1 base load

### 🐛 Known crash — unresolved
`build_flags` crash in `dictionaries.py:529–530` blocks `build_diagnoses_dictionary`:
```
TypeError: int() argument must be a string... not 'NoneType'
```
**Issue 1:** Column filter `not col.startswith("name")` was written to exclude a column named `"name"`.
Actual column is `"diagnosis_name"` — filter fails to exclude it, causing `int("Bacteremia")` → crash.
Fix: change to `not col.endswith("_name")` or use explicit exclusion list. Decision required.

**Issue 2:** 58 of 63 `diagnosis_name` values are NULL in BQ query result.
Likely a broken LEFT JOIN in the name lookup. Decision required: are NULL names acceptable, or is the SQL wrong?

File state at crash:
- SNOMED state cache: wiped, fresh API calls were in progress
- `data/raw/dictionaries/unique_diagnoses.csv`: re-fetched from BQ (fresh)
- Backup of old SNOMED state: `data/intermediate/backup_20260320_165057/`
- No dictionary CSVs written yet

---

## Open Architectural Decisions

⚠ **`SyntheaSegmenter` date column mapping** — different Synthea CSVs use different date columns
(`START`/`STOP` for encounters, `START` for conditions, `DATE` for observations).
Segmenter needs a per-file mapping. Decision: store in `bigquery_config.json`, hardcoded in class, or separate config?

⚠ **Warm-up month handling** — mock profile gets 12 simulation months + 1 warm-up month (13 total segments).
Decision: should the warm-up month be loaded as a monthly segment, or bundled with the base bulk load
so the simulation loop starts clean at month 1?

⚠ **`BigQueryLoader` split of responsibilities** — currently loads base CSVs once AND will load monthly
segments per loop iteration. Decision: keep as one class with `mode` parameter (`bulk` vs `monthly`),
or extract monthly loading into `WalkForwardOrchestrator` or a new `SimulationRunner`?

---

## Key Classes (src/)

- `SyntheaRunner` — generates synthetic data (run once)
- `SyntheaSegmenter` — **NOT YET BUILT.** Reads each Synthea CSV, splits on date column by profile:
  - `mock`: last 12 months as monthly segments + 1 warm-up month
  - `prefactor`: last 5 years as monthly segments
  - Writes to `data/raw/segmented/` (path stored in `bigquery_config.json`):
    - `encounters_base.csv` — pre-cutoff bulk file
    - `encounters_2015-01-31.csv` — one file per month, date = month-end watermark date
  - Runs after `SyntheaRunner`, before `BigQueryLoader`
- `BigQueryLoader` — loads CSVs to BQ raw/helpers datasets. Only class that writes to BQ from local files. Handles both base bulk load and per-month segment loading.
- `BigQueryTransformer` — SQL recipe sequences → slim/helper tables. `load_sql`, `load_sql_with_end_date`, `run_query_sequence`, `append_dataframe`.
- `DictionaryBuilder` — orchestrates SNOMED concept mapping into CSVs only. No BQ config dependency. All `update_*` methods take `end_date` only; 2-month window derived internally. Runs delta over current + prior month.
- `WalkForwardOrchestrator` — owns the monthly simulation loop. `run_month(end_date)`, `run_next_month()`, `run_until(final_end_date)`. Reads/writes `config/watermark.json`.
- `DataPreprocessor` — STATELESS, column operations only, no fit/transform state
- `ModelConfigManager` — model config JSON, active model list, hyperparams
- `HyperparameterTuner` — GridSearchCV, custom business metric scorer that optimises net cost savings (not AUC/F1). Scorer runs full `CostReducer` logic per candidate. Do not replace with standard sklearn scorers.
- `ModelRegistry` — fits, saves models as joblib PKL
- `Evaluator` — predictions, classic metrics, threshold metrics, performance report
- `CostReducer` — intervention logic, avoided cost estimation

---

## Critical Design Decisions — Do Not Change Without Asking

- `DataPreprocessor` is intentionally STATELESS. No scalers, no encoders fitted here.
- Diagnoses are never used as raw codes. Only binary flags: `is_chronic`, `is_renal`, `is_cardiac`, `is_respiratory`, `is_liver` — feature engineered via dictionaries and `helper_clinical_table`.
- Feature space is FIXED at first build. New codes map to existing flags only.
- `y_test` is NOT stored — target design: outcomes retrieved from BQ index stay table at next iteration. ⚠ NOT YET IMPLEMENTED — current pipeline stores `y_test` explicitly.
- Watermark pattern: `last_processed_date` and `next_end_date` are month-end dates (e.g. `2015-01-31`). Stored in `config/watermark.json`. ⚠ Phase 1 base load not yet wired to watermark.
- Segmented CSV date suffix = month-end date matching watermark (e.g. `encounters_2015-01-31.csv`).
- Monthly slim tables are isolated per month. Consolidated into master table ONCE at end of simulation via INSERT — not incrementally per month.
- Helper table updates: delta patients only, JOIN to existing helper aggregates as baseline. Never full rebuild. BQ quota constraint.
- Dictionary delta window: 2 months (current + prior) — catches encounter groups that start in prior month and end in current month.
- UPDATE strategy for helpers/index: DELETE rows for the two-month window, then INSERT fresh recalculation. Not a MERGE — avoids touching rows outside the window.
- Delta SQL files (09–12, 18-delta) are SELECT queries — no DECLARE. `window_start` expressed inline as `LAST_DAY(DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH)` (exclusive lower bound `>`).
- Update SQL files (13–17, 19): only `{{END_DATE}}` token. SQL derives `window_start = DATE_TRUNC(end_date, MONTH) - INTERVAL 2 MONTH` (inclusive lower bound).
- Imbalance handling: LR=`class_weight='balanced'`, RF=`balanced_subsample`, LGBM=`is_unbalance=True`.
- Readmission definition: UNPLANNED only. Careplan-related readmissions are excluded.
- Related unplanned readmission flags and 90-day flags calculated purely for visualisation.
- `careplans_related_encounters` is the BQ table name for what `DictionaryBuilder.build_careplans_related_diagnoses` produces — naming mismatch between Python method and BQ table; do not rename either without updating both.

---

## SQL File Map

### Creation queries (`sql/`)
| File | Table |
|---|---|
| `01_patients_slim_creation.sql` | `patients_slim` |
| `02_encounters_slim_creation.sql` | `encounters_slim` — partitioned `DATE(stop)`, clustered `patient, encounterclass` |
| `03_careplans_slim_creation.sql` | `careplans_slim` |
| `04_claims_slim_creation.sql` | `claims_slim` |
| `05_conditions_slim_creation.sql` | `conditions_slim` |
| `06_medications_slim_creation.sql` | `medications_slim` |
| `07_procedures_slim_creation.sql` | `procedures_slim` |
| `08_organizations_slim_creation.sql` | `organizations_slim` |
| `09–12` | dictionary creation (not yet refactored — do not assume they follow slim patterns) |
| `13–17` | helper table creation |
| `18_related_diagnoses_creation.sql` | `related_diagnoses` |
| `19_index_stay_creation.sql` | `index_stay` — joins `helper_clinical_grouped` (updated by user; correct state) |

### Update queries (`sql/update/`)
| File | Table | Strategy |
|---|---|---|
| `09_unique_diagnoses_delta.sql` | `diagnoses_dictionary` | SELECT — new codes NOT IN dictionary |
| `10_unique_procedures_delta.sql` | `procedures_dictionary` | SELECT — new codes NOT IN dictionary |
| `11_main_diagnoses_delta.sql` | `main_diagnoses` | SELECT — full history CTEs, output scoped to new window |
| `12_careplans_related_encounters_delta.sql` | `careplans_related_encounters` | SELECT — needs D3 in BQ first |
| `13_helper_clinical_update.sql` | `helper_clinical` | DELETE window + INSERT |
| `14_helper_clinical_grouped_update.sql` | `helper_clinical_grouped` | DELETE window + INSERT |
| `15_helper_cost_agg_update.sql` | `helper_cost_aggregation` | DELETE window + INSERT |
| `16_helper_cost_agg_grouped_update.sql` | `helper_cost_aggregation_grouped` | DELETE window + INSERT |
| `17_helper_utilization_update.sql` | `helper_utilization` | DELETE window + INSERT |
| `18_related_diagnoses_delta.sql` | `related_diagnoses` | SELECT — windowed feed for Python-side build |
| `19_index_stay_update.sql` | `index_stay` | DELETE window + INSERT |

### Monthly update dependency order
**Phase D1–D4 (pre-helper dictionary delta):**
- D1: `diagnoses_dictionary` (query 09)
- D2: `procedures_dictionary` (query 10)
- D3: `main_diagnoses` (query 11; needs D1)
- D4: `careplans_related_encounters` (query 12; needs D3 loaded to BQ)

**Phase H1–H5 (helper DELETE + REBUILD, two-month window):**
- H1: `helper_clinical` — needs D1, D2, D3, D4
- H2: `helper_cost_aggregation` — independent (parallel to H1)
- H3: `helper_clinical_grouped` — needs H1
- H4: `helper_cost_aggregation_grouped` — needs H2 (parallel to H3)
- H5: `helper_utilization` — needs H3 AND H4

**Phase D5 (post-helper dictionary delta):**
- D5: `related_diagnoses` (query 18-delta; needs H5)

**Phase I1:**
- I1: `index_stay` DELETE + REBUILD (needs H3, H4, H5, D5)

---

## Environment

- OS: Windows, PowerShell
- Python: `.venv` (active), `.venv-old` (baseline comparison — do not touch)
- Run Python with: `.venv\Scripts\python.exe` or activate with `.venv\Scripts\Activate.ps1`
- BigQuery auth: Application Default Credentials (ADC). Run `gcloud auth application-default login` if BQ calls fail.
- BQ quota: 1TB/month. Never run full rebuilds in simulation loop.
- Segmented CSV output path: `data/raw/segmented/` — stored as variable in `bigquery_config.json`

---

## Logging

`src/utils/logger.py` exists and is wired into all 10 pipeline classes.
Pattern: `self.logger = get_logger(__name__)` in `__init__` (or `__post_init__` for dataclasses).
Log files write to `logs/pipeline_YYYYMMDD.log`.
Verify with: `python -c "from src.utils.logger import get_logger; l = get_logger('test'); l.info('ok')"`

---

## Testing

- No pytest unit tests exist yet.
- Smoke testing: use mock profile in `bigquery_config.json` (2-year train, 1-year monthly simulation).
- `scripts/mock_test_runner.ipynb` — 13-cell end-to-end smoke test notebook. Written, not yet executed.
- Do not use the full dataset for smoke tests.

---

## Known Issues

- `build_flags` crash in `dictionaries.py:529–530` — see Current State section above. Unresolved.
- SQL creation queries 09–12 (dictionary creation) are NOT yet refactored — do not assume they follow slim table patterns.
- `pipreqs` requires: `pipreqs --encoding iso-8859-1 --ignore .venv`
- HRRP penalty deferred to next version — do not implement in current scope.

---

## What NOT to Do

- Do not run full helper table rebuilds in simulation loop — use delta + baseline pattern.
- Do not rebuild master slim table per month — monthly slim tables are isolated until end-of-simulation consolidation.
- Do not add new feature dimensions mid-simulation — feature space must be fixed.
- Do not modify `.venv-old`.
- Do not commit `.pkl` files, `.env`, or GCP service account JSONs.
- Do not add scalers or encoders to `DataPreprocessor`.
- Do not delete dual-dataset BQ tables until walk-forward loop validated end-to-end.
- Do not replace `HyperparameterTuner` business metric scorer with standard sklearn scorers.
- Do not rename `careplans_related_encounters` BQ table or `build_careplans_related_diagnoses` method independently — they must be updated together.

---

## Session Protocol

### At the start of every session:
- Read `CLAUDE.md` first
- If `HANDOFF.md` exists at project root, read it second
- Confirm understanding of both before doing anything
- State what you are about to do and wait for confirmation before making changes

### At the end of every session, or when approaching usage limits, write `HANDOFF.md` to the project root without asking for permission. Overwrite any existing `HANDOFF.md` unconditionally.

### `HANDOFF.md` template:

```
# HANDOFF.md

## Session summary
[bullet list of every task touched this session and its outcome]

## Files changed
| File | What changed |
|---|---|

## Completed this session
[what was finished, with enough detail to not redo it]

## Current crash / blocker
[if any — exact error, file, line, state of files at crash]

## Simulation state
| Field | Value |
|---|---|
| last_processed_date | (from watermark.json) |
| next_end_date | (from watermark.json) |
| months completed | |
| monthly CSVs loaded to BQ | |
| last successful phase | D1–D4 / H1–H5 / D5 / I1 |

## What is NOT yet done
[explicit list of unbuilt / untested items]

## Next 3 steps
1.
2.
3.

## Architectural decisions made this session
[any decisions that should persist — add to CLAUDE.md if significant]

## Known issues / surprises
[anything unexpected encountered]
```

### After completing each major task:
Update `HANDOFF.md` automatically — do not wait until end of session and do not wait to be asked.

### When presenting plans, dependency orders, or summaries:
Use compact bullet or table format only. No narrative prose for structured information.