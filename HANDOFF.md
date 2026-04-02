# HANDOFF.md

## Session summary
- Ran `_cleanup_monthly_tables.py` — deleted 12 stale `2023-12-31` partial tables from prior crashed run
- Re-ran `_run_refactor_pipeline.py` — Phase 7 resumed from `2023-12-31`
- Pipeline ran successfully through `2024-09-30` (months 2023-12 through 2024-09, 10 months)
- Crashed at `2024-10-31` S0.5 (INSERT into master slim tables) on BQ free storage quota (10 GB exhausted)

---

## Files changed
| File | What changed |
|---|---|
| `scripts/_cleanup_monthly_tables.py` | New script — one-shot cleanup of old monthly BQ tables |
| `config/watermark.json` | Advanced to `last_processed_date=2024-09-30`, `next_end_date=2024-10-31` |
| `predictions/` | Appended per-month predictions for 2023-12 through 2024-09 |
| `results/` | Appended per-month evaluation results for 2023-12 through 2024-09 |

---

## Current crash / blocker
**BQ 403 Free Storage Quota Exceeded on `hospital-492008`**

```
google.api_core.exceptions.Forbidden: 403
Quota exceeded: Your project exceeded quota for free storage for projects.
```

- Crash phase: `run_month("2024-10-31")` → S0.5 (INSERT into master slim tables, recipe 5)
- Root cause: permanent tables (patients_slim, encounters_slim, helper tables, index_stay) have grown to fill the 10 GB BQ free tier after 41 months of cumulative data
- Monthly staging table cleanup is working correctly — not the source of the issue

**Fix:**
GCP Console → `hospital-492008` → Billing → link a billing account.
Once billing is active, re-run `scripts/_run_refactor_pipeline.py` — resumes from `2024-10-31`.

**State of BQ raw_data at crash:**
- `2024-08-31` tables: kept by cleanup (2 prior months before 2024-10-31)
- `2024-09-30` tables: kept by cleanup
- `2024-10-31` tables: S0 loaded these before crash — stale partial tables sitting in BQ

---

## Simulation state (refactor profile)
| Field | Value |
|---|---|
| `last_processed_date` | `2024-09-30` |
| `next_end_date` | `2024-10-31` |
| `simulation_end_date` | `2026-04-30` |
| profile | `refactor` |
| months completed (total) | 41 of 60 (from 2021-05-31 through 2024-09-30) |
| months completed (this session) | 10 (2023-12-31 through 2024-09-30) |
| last successful phase | ML complete for `2024-09-30` |
| next phase to run | `2024-10-31` S0 (monthly CSV load) |

---

## What is NOT yet done
- Months 2024-10 through 2026-04 (19 months remaining)
- Stale `2024-10-31` partial BQ tables need cleanup before or on next run (cleanup runs automatically at start of `run_month`)

---

## Next 3 steps
1. Enable billing on `hospital-492008` (GCP Console → Billing)
2. Re-run `scripts/_run_refactor_pipeline.py` — auto-resumes from `2024-10-31`, cleanup purges the stale tables first
3. Monitor to completion (remaining ~19 months × ~15 min/month ≈ 4-5 hours)

---

## Architectural decisions made this session
- `_cleanup_old_monthly_tables` confirmed working — deleted 12 tables per month correctly
- Free storage quota hit is a BQ project billing issue, not a code issue

## Known issues / warnings
- BQ 10 GB free tier exhausted — billing must be enabled before proceeding
- LGBMClassifier fitted with feature names warning is cosmetic (sklearn version mismatch) — does not affect results
- LogReg convergence warning (max_iter reached) — model still fits; increase `max_iter` if needed in a future session
