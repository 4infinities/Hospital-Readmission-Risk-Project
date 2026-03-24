# HANDOFF.md

## Session summary
- RV-1 (update query data source architecture) implemented: all 5 helper update SQL files rewritten
- RV-2 (patient_known_chronic_codes) implemented: creation SQL + delta SQL written
- `{{PREV_END_DATE_SAFE}}` token added to `BigQueryTransformer`
- `bigquery_recipes.json` updated: pkcc added to recipe[1] (creation) and recipe[3] (update, before H1)
- `sql/update/20_patient_known_chronic_codes_delta.sql` renamed/deleted (replaced by 22)

---

## Files changed
| File | What changed |
|---|---|
| `sql/21_patient_known_chronic_codes_creation.sql` | New — full-history creation using claims_slim |
| `sql/update/22_patient_known_chronic_codes_delta.sql` | New — monthly delta using claims_{{END_DATE_SAFE}} staging |
| `sql/update/13_helper_clinical_update.sql` | Full rewrite: self-referential pattern, staging tables |
| `sql/update/14_helper_clinical_grouped_update.sql` | Full rewrite: staging tables, prior_group_anchor, continuation exclusion |
| `sql/update/15_helper_cost_agg_update.sql` | Added `patient_id` column |
| `sql/update/16_helper_cost_agg_grouped_update.sql` | Full rewrite: staging tables, prior_group_anchor, continuation exclusion |
| `sql/update/17_helper_utilization_update.sql` | Full rewrite: staging tables, self-join to hu for 365d lookback |
| `src/pipeline/bq_transformer.py` | Added `{{PREV_END_DATE_SAFE}}` token substitution + `_prev_end_date_safe()` helper |
| `config/bigquery_recipes.json` | Added pkcc to recipe[1]; added delta (22) to recipe[3] before H1 |

---

## Completed this session
### Task #8 — Helper update SQL rewrite (self-referential pattern)

**update/13** (`helper_clinical`):
- `window_encounters`: UNION of `encounters_{{END_DATE_SAFE}}` + `encounters_{{PREV_END_DATE_SAFE}}`
- `patient_baseline`: joins `helper_clinical_grouped` on `patient_id` directly — no `encounters_slim` scan (DELETE already removed window rows, so all hcg rows are pre-window)
- `num_procedures`: from `procedures_{{END_DATE_SAFE}}` staging only
- `num_chronic_conditions`: correlated subquery on `patient_known_chronic_codes`
- Comorbidity flags: `GREATEST(baseline, new_onset_from_staging)`
- Surgery: `GREATEST(hcg.last_surgery_date_baseline, new_surgery_from_staging)`
- `is_planned`: `careplans_related_encounters` + `procedures_{{END_DATE_SAFE}}`

**update/14** (`helper_clinical_grouped`):
- `window_encounters`: UNION of two staging tables (with `encounterclass` + `type_flag`)
- `prior_group_anchor`: from `hcg.patient_id` JOIN `helper_utilization.stop` (no encounters_slim scan)
- Grouping CTE runs on window encounters only, using `COALESCE(LAG(stop), last_prior_stop)` for boundary
- `first_group_change_per_patient`: detects continuation groups
- Final WHERE: excludes groups where `group_number=0` and `first_group_change=0` (prior row retained)
- Added `patient_id`, `last_surgery_date`

**update/15** (`helper_cost_aggregation`): Added `patient_id` column only.

**update/16** (`helper_cost_aggregation_grouped`): Same rewrite pattern as update/14. Added `patient_id`.

**update/17** (`helper_utilization`):
- Same staging-table window grouping + `prior_group_anchor` + continuation exclusion
- `prior_inpatient_base`: from `helper_utilization` where `stop < window_start` (no encounters_slim)
- `all_inpatient`: `prior_inpatient_base` UNION `window_inpatient` for pairwise 365d lookback
- `pairwise_follow`: uses `window_inpatient` only (post-window readmissions captured in future iterations)
- Added `patient_id`

**patient_known_chronic_codes**:
- `sql/21_patient_known_chronic_codes_creation.sql`: full-history creation from `claims_slim` + `encounters_slim` + `diagnoses_dictionary`
- `sql/update/22_patient_known_chronic_codes_delta.sql`: monthly delta from `claims_{{END_DATE_SAFE}}` staging; INSERT NOT IN existing table

**`{{PREV_END_DATE_SAFE}}` token**: last day of prior calendar month in `YYYY_MM_DD` format. Added to both `load_sql_with_end_date()` and `run_query_sequence()` in `BigQueryTransformer`.

---

## Current crash / blocker
`build_flags` crash in `dictionaries.py:529–530` — deferred, not blocking.
```
TypeError: int() argument must be a string... not 'NoneType'
```
Fix: `not col.endswith("_name")` instead of `not col.startswith("name")`.
File state: SNOMED state cache wiped. Backup at `data/intermediate/backup_20260320_165057/`.

---

## Simulation state
| Field | Value |
|---|---|
| `last_processed_date` | `null` (watermark not yet written — Phase 1 not yet run) |
| `next_end_date` | `2025-02-28` (stale placeholder — will be overwritten by Phase 1 segmenter) |
| months completed | 0 |
| monthly CSVs loaded to BQ | 0 |
| last successful phase | None — base load not yet run end-to-end |

---

## What is NOT yet done
- `mock_test_runner.ipynb` — never run end-to-end
- `SyntheaSegmenter` — never run (no segmented files exist yet)
- Walk-forward loop — never executed
- Watermark Phase 1 wiring — code exists but never executed
- `build_flags` crash — deferred

---

## Next 3 steps
1. Fix `build_flags` crash (`dictionaries.py:529`) — change `startswith("name")` to `endswith("_name")`
2. Run Phase 1 end-to-end via `mock_test_runner.ipynb` (Synthea → BQ → slim → helpers → index)
3. Validate `patient_known_chronic_codes` populated correctly after Phase 1

---

## Architectural decisions made this session
| Decision | Choice |
|---|---|
| `window_encounters` source in update queries | UNION of `encounters_{{END_DATE_SAFE}}` + `encounters_{{PREV_END_DATE_SAFE}}` staging tables |
| `patient_baseline` in update/13 | Join `hcg.patient_id` directly — no encounters_slim scan needed post-DELETE |
| `prior_group_anchor` in update/14,16,17 | `helper_clinical_grouped.patient_id` JOIN `helper_utilization.stop` |
| Continuation group handling | Skip group_number=0 with first_group_change=0 — prior hcg row retained as-is |
| 365d lookback in update/17 | Self-join to `helper_utilization` pre-window rows (inpatient only) |
| `{{PREV_END_DATE_SAFE}}` token | Last day of prior calendar month, `YYYY_MM_DD` format |
| `patient_known_chronic_codes` file numbering | Creation = sql/21, delta = sql/update/22 |
| `patient_known_chronic_codes` in recipes | Recipe[1] (creation, after helpers); recipe[3] (delta, first entry, before H1) |

---

## Known issues / surprises
- `bigquery_recipes.json` recipe indices: 0=slim creation, 1=helper creation (+pkcc), 2=index creation, 3=helper update (+pkcc delta first), 4=index update, 5=monthly slim insert
- update/17 `pairwise_follow`: only detects within-window readmissions. Post-window readmissions captured in next iteration (by design — no future data available)
- update/14,16: cross-boundary groups (window encounter joins prior group within 12h) are not updated in hcg — prior row retained with pre-window flags. Accepted trade-off.
- Continuation group detection relies on `helper_utilization` for prior stop dates — only covers clinical encounter types. Wellness/ambulatory prior stops not captured. Edge case accepted.
- `encounters_slim` still used in DELETE condition for update/13,14,16,17 (bounded scan: 2 months). This is acceptable — the issue was avoiding unbounded full scans in the INSERT.
