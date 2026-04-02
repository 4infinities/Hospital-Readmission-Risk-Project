# HANDOFF.md

## Session summary
- Created `scripts/_run_refactor_pipeline.py` — full pipeline from data generation to per-month reports
- Ran `_run_refactor_pipeline.py`; Phases 1-2 completed; Phase 3 crashed on BQ 403 (permissions)
- Fixed script to be idempotent: Phases 1-2 skipped on re-run if segmented CSVs already exist
- Segmented data for refactor profile is on disk and ready

---

## Files changed
| File | What changed |
|---|---|
| `scripts/_run_refactor_pipeline.py` | New script — full refactor pipeline; phases 1-2 idempotent skip |
| `scripts/_run_predict_only.py` | Created prior session (predict-only, mock profile) |
| `config/watermark.json` | Overwritten by refactor run: `last_processed_date=2021-04-30`, `next_end_date=2021-05-31`, `simulation_end_date=2026-04-30` |
| `data/raw/Synthea/` | Overwritten with refactor CSVs (50k patients, 15 years history) |
| `data/raw/segmented/` | Refactor segmented CSVs written (base + 60 monthly files per table) |

---

## Current crash / blocker
**BQ 403 Forbidden on `hospital-readmission-4`**

```
403 Access Denied: Dataset hospital-readmission-4:raw_data
  Permission bigquery.tables.create denied (or it may not exist)
```

- Credentials: `.secrets/hospital-readmission-4-code.json` (service account)
- Required: datasets `raw_data`, `data_slim`, `helper_tables` must exist in `hospital-readmission-4`
- Required: service account needs `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` on that project

**To fix (pick one):**
1. GCP Console → `hospital-readmission-4` → BigQuery → create the 3 datasets, then IAM → grant the SA the roles above
2. `! gcloud auth application-default login` if using ADC instead of SA key
3. `! gcloud projects add-iam-policy-binding hospital-readmission-4 --member=serviceAccount:<SA_EMAIL> --role=roles/bigquery.admin`

**After fixing:** re-run `scripts/_run_refactor_pipeline.py` — Phases 1-2 will be skipped automatically.

---

## Simulation state (refactor profile)
| Field | Value |
|---|---|
| `last_processed_date` | `2021-04-30` (base cutoff — Phase 1/2 done) |
| `next_end_date` | `2021-05-31` (first simulation month) |
| `simulation_end_date` | `2026-04-30` |
| profile | `refactor` |
| months total | 60 (2021-05-31 through 2026-04-30) |
| patients generated | 56,350 (50k alive, 6,350 deceased) |
| base_cutoff_date | 2021-04-30 |
| BQ load status | NOT started — blocked on 403 |

---

## What is NOT yet done
- Phase 3+: base BQ load, slim/helper tables, dictionaries, index_stay, ML, walk-forward
- **Note:** mock profile data is gone — `data/raw/Synthea/` and `data/raw/segmented/` now contain refactor data
- `predictions/` and `results/` still contain mock run data — will be appended to on re-run (not cleared)
- Consider clearing `predictions/` and `results/` before re-running if a clean state is desired

---

## Next 3 steps
1. Fix `hospital-readmission-4` BQ permissions (datasets + SA roles)
2. Re-run `scripts/_run_refactor_pipeline.py` — picks up from Phase 3 (base load)
3. Monitor Phases 3-5 (BQ load + table creation); then walk-forward loop runs automatically

---

## Key design decisions in `_run_refactor_pipeline.py`
- Tune-once enforced by: `tuner` passed to `WalkForwardOrchestrator` for base fit only; `orch.tuner = None` set before `run_until` — PSI/schedule checks still compute but cannot trigger retuning
- Phases 1-2 idempotent: skip if `encounters_base.csv` exists in segmented_path
- Reports accumulate per-month in `results/{model}_results.csv` via `evaluate_month` inside the walk-forward loop — no separate final report step

## Known issues / warnings
- `predictions/` and `results/` still contain mock profile data; they will be appended to unless cleared
- `config/watermark.json` now reflects refactor profile dates — do NOT run mock pipeline scripts against it
