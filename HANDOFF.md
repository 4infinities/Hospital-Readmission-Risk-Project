# HANDOFF.md

## Session summary

### Task 1 — Logger wiring (completed previous session)
All 10 pipeline classes in `src/pipeline/` now use `self.logger = get_logger(__name__)`.
- 4 classes migrated from module-level `logging.getLogger` to instance-level `get_logger`
- 4 classes had logging added fresh
- 3 dataclasses (`ModelRegistry`, `Evaluator`, `CostReducer`) use `__post_init__`
- 7 `print()` calls in `HyperparameterTuner` and `Evaluator` converted to `logger.debug/info`

### Task 2 — SQL comments on all existing creation queries (completed previous session)
One-line comments added per logical block to all 20 files in `sql/`. No logic changed.
`19_index_stay_creation.sql` was updated by the user to join `helper_clinical_grouped`
instead of `helper_clinical` — this is now the correct state.

### Task 3 — Incremental update queries (completed this session)
All 7 update SQL files written and saved to `sql/update/`.

---

## Completed: Incremental update files

| File | Table | Strategy |
|---|---|---|
| `sql/update/13_helper_clinical_update.sql` | `helper_clinical` | DELETE window + INSERT |
| `sql/update/14_helper_clinical_grouped_update.sql` | `helper_clinical_grouped` | DELETE window + INSERT |
| `sql/update/15_helper_cost_agg_update.sql` | `helper_cost_aggregation` | DELETE window + INSERT |
| `sql/update/16_helper_cost_agg_grouped_update.sql` | `helper_cost_aggregation_grouped` | DELETE window + INSERT |
| `sql/update/17_helper_utilization_update.sql` | `helper_utilization` | DELETE window + INSERT |
| `sql/update/18_related_diagnoses_update.sql` | `related_diagnoses` | DELETE window + INSERT |
| `sql/update/19_index_stay_update.sql` | `index_stay` | DELETE window + INSERT |

### Placeholder tokens used in all update files
- `{{WINDOW_START_DATE}}` — passed in as a DATE value; window starts at `DATE_TRUNC({{WINDOW_START_DATE}}, MONTH) - INTERVAL 2 MONTH`
- `{{WINDOW_END_DATE}}` — passed in as a DATE value; the current end date (= `window_end` variable)
- `{{DATASET_SLIM}}`, `{{DATASET_HELPERS}}` — same as creation queries

### Filter conventions
- **DELETE subquery**: `WHERE start >= window_start AND stop <= window_end` (bare column names, no alias)
- **Two-bound window filter** (per-encounter output CTEs and final SELECT): `e.start >= window_start AND e.stop <= window_end`
- **Single-bound filter** (full patient history CTEs): `... <= window_end` only (no lower bound)
- **Grouped table final filter**: `WHERE flag.stay_id IN (SELECT id FROM encounters_slim WHERE start >= window_start AND stop <= window_end)`

### Completed: Dictionary delta files (D1–D4)

| File | Table | Type | Strategy |
|---|---|---|---|
| `sql/update/09_unique_diagnoses_delta.sql` | `diagnoses_dictionary` | SELECT feed | New window codes NOT IN dictionary |
| `sql/update/10_unique_procedures_delta.sql` | `procedures_dictionary` | SELECT feed | New window codes NOT IN dictionary |
| `sql/update/11_main_diagnoses_delta.sql` | `main_diagnoses` | SELECT feed | Full history CTEs; output scoped to new window encounters |
| `sql/update/12_careplans_related_encounters_delta.sql` | `careplans_related_encounters` | SELECT feed | New window encounters only; requires D3 in BQ first |

### Placeholder token standardisation (completed this session)
All `{{WINDOW_START_DATE}}` / `{{WINDOW_END_DATE}}` tokens in `sql/update/` (files 13–19) renamed to
`{{START_DATE}}` / `{{END_DATE}}` to match creation queries. Single Python replace function now works
across all query types with string `'YYYY-MM-DD'` values.

---

## Confirmed dependency order

**Phase 1 — Dictionary delta (pre-helper):**
- D1: `diagnoses_dictionary` delta (feeds from query 09)
- D2: `procedures_dictionary` delta (feeds from query 10)
- D3: `main_diagnoses` delta (feeds from query 11; needs D1 done)
- D4: `careplans_related_encounters` delta (feeds from query 12; needs D3 loaded to BQ)

**Phase 2 — Helper table DELETE + REBUILD (two-month window):**
- H1: `helper_clinical` — needs D1, D2, D3, D4
- H2: `helper_cost_aggregation` — independent of dictionaries (can run parallel to H1)
- H3: `helper_clinical_grouped` — needs H1
- H4: `helper_cost_aggregation_grouped` — needs H2 (can run parallel to H3)
- H5: `helper_utilization` — needs H3 AND H4

**Phase 3 — Dictionary delta (post-helper):**
- D5: `related_diagnoses` delta (feeds from query 18; needs H5 done)

**Phase 4 — Index stay:**
- I1: `index_stay` DELETE + REBUILD (needs H3, H4, H5, D5)

---

### Completed: Slim table partitioning (this session)

`PARTITION BY DATE_TRUNC(..., MONTH)` and updated `CLUSTER BY` added to 6 slim creation queries.

| File | Partition column | Cluster BY |
|---|---|---|
| `02_encounters_slim_creation.sql` | `DATE_TRUNC(DATE(stop), MONTH)` | `patient, encounterclass` |
| `03_careplans_slim_creation.sql` | `DATE_TRUNC(stop, MONTH)` | `patient, encounter` |
| `04_claims_slim_creation.sql` | `DATE_TRUNC(currentillnessdate, MONTH)` | `patientid, encounter` |
| `05_conditions_slim_creation.sql` | `DATE_TRUNC(stop, MONTH)` | `patient, code` |
| `06_medications_slim_creation.sql` | `DATE_TRUNC(stop, MONTH)` | `encounter, code` |
| `07_procedures_slim_creation.sql` | `DATE_TRUNC(stop, MONTH)` | `patient, encounter` |

Skipped: `01_patients_slim` and `08_organizations_slim` — no date columns.
Note: `encounters_slim.stop` is TIMESTAMP → partition uses `DATE(stop)` wrapper.

---

## What is NOT yet done

- **Dictionary delta queries (D1–D4):** SQL files written (see below). Python-side `DictionaryBuilder`
  logic to consume these queries and append results to BQ tables is NOT yet implemented.
- **Walk-forward loop wiring:** `BigQueryTransformer` does not yet call the update files.
  The execution order and parameter passing (`WINDOW_START_DATE`, `WINDOW_END_DATE`) need to be wired.
- **Slim table MERGE queries:** Incremental append of new month's data into slim tables
  (not yet built — currently slim tables are created once from full CSV load).

---

## Architectural decisions made

- UPDATE strategy: DELETE rows for the two-month window, then INSERT fresh recalculation.
  Not a MERGE — avoids touching rows outside the window.
- Two-month window formula: `DATE_TRUNC(@WINDOW_START_DATE, MONTH) - INTERVAL 2 MONTH`
  to `@WINDOW_END_DATE`. Captures groups that span month boundaries.
- `index_stay` is included in the DELETE + REBUILD scope (two-month window only).
- Dictionary delta: append-only for new concept_ids; no DELETE or upsert needed.
- New update SQL files go in `sql/update/` (created this session).
- `related_diagnoses` is a post-helper dictionary and must be rebuilt AFTER `helper_utilization`.
- Grouped table updates (H3, H4) use full patient history for group boundary detection
  but restrict INSERT output to groups whose representative encounter is in the window.
- `helper_utilization` update uses full patient history for pairwise lookback/followup
  CTEs; final WHERE restricts to window encounter group representatives.

---

## Known issues / surprises

- `careplans_related_encounters` is the BQ table name for what
  `DictionaryBuilder.build_careplans_related_diagnoses` produces — naming mismatch
  between Python method and BQ table name to be aware of.
- Dictionary creation queries (09–12, 18) are noted in CLAUDE.md as not yet refactored —
  the incremental delta logic will need to work with them as-is.
