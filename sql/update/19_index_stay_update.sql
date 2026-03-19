-- index_stay incremental update: DELETE rows for the two-month window, then reinsert fresh calculations
-- Final feature table — must run AFTER all helpers and related_diagnoses are updated
-- Depends on: encounters_slim, patients_slim, helper_clinical_grouped, helper_cost_aggregation_grouped,
--             helper_utilization, related_diagnoses
DECLARE window_start DATE DEFAULT DATE_TRUNC({{START_DATE}}, MONTH) - INTERVAL 2 MONTH;
DECLARE window_end   DATE DEFAULT {{END_DATE}};

-- Remove window rows before recalculation
DELETE FROM {{DATASET_HELPERS}}.index_stay
WHERE stay_id IN (
  SELECT id FROM {{DATASET_SLIM}}.encounters_slim
  WHERE start >= window_start AND stop <= window_end
);

-- Reinsert recalculated rows for the two-month window; one row per clinical encounter group
INSERT INTO {{DATASET_HELPERS}}.index_stay
SELECT
  e.patient AS patient_id,
  -- Age computed at admission time from patient birthdate
  date_diff(date(util.start), p.birthdate, year) AS patient_age,
  p.gender AS gender,
  clin.stay_id,
  e.organization AS hospital_id,
  util.start AS admission_datetime,
  util.stop AS discharge_datetime,
  date(util.stop) AS discharge_date,
  EXTRACT(year FROM util.stop) AS discharge_year,
  EXTRACT(month FROM util.stop) AS discharge_month,
  cost.length_of_encounter AS length_of_stay,
  util.encounterclass AS stay_type,
  -- Clinical flags and diagnosis category flags from helper_clinical_grouped
  clin.main_code,
  clin.main_name,
  clin.is_disorder,
  clin.is_symptom,
  clin.inflammation,
  clin.musculoskeletal,
  clin.nervous,
  clin.respiratory,
  clin.cardiac,
  clin.renal,
  clin.trauma,
  clin.intoxication,
  clin.num_disorders,
  clin.num_findings,
  clin.num_chronic_conditions,
  clin.num_procedures,
  clin.has_diabetes,
  clin.has_cancer,
  clin.has_hiv,
  clin.has_hf,
  clin.has_alz,
  clin.has_ckd,
  clin.has_lf,
  clin.had_surgery,
  clin.is_planned,
  -- Cost features from helper_cost_aggregation_grouped
  cost.admission_cost,
  cost.total_procedure_costs,
  cost.total_medication_costs,
  cost.total_stay_cost,
  cost.cost_per_day_stay,
  -- Utilization and readmission outcome features from helper_utilization
  util.admissions_365d,
  util.tot_length_of_stay_365d,
  util.avg_cost_of_prev_stays,
  util.readmit_30d,
  util.readmit_90d,
  -- Careplan-related readmission flags from related_diagnoses (visualization only)
  rel.is_related,
  rel_readmit_30d,
  rel_readmit_90d,
  util.days_to_readmit,
  util.following_stay_id,
  -- total_readmission_cost = cost of the FOLLOWING stay (used by CostReducer)
  util.total_stay_cost AS total_readmission_cost,
  util.following_unplanned_admission_flag
FROM {{DATASET_SLIM}}.encounters_slim e
LEFT JOIN {{DATASET_SLIM}}.patients_slim p
  ON e.patient = p.id
-- All features come from grouped-level helpers keyed on the group representative encounter id
LEFT JOIN {{DATASET_HELPERS}}.helper_clinical_grouped clin
  ON clin.stay_id = e.id
LEFT JOIN {{DATASET_HELPERS}}.helper_cost_aggregation_grouped cost
  ON cost.stay_id = e.id
LEFT JOIN {{DATASET_HELPERS}}.helper_utilization util
  ON util.stay_id = e.id
LEFT JOIN {{DATASET_HELPERS}}.related_diagnoses rel
  ON e.id = rel.stay_id
WHERE
  -- Restrict to clinical encounter types only
  e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
  -- Restrict to the two-month window
  AND e.start >= window_start AND e.stop <= window_end;
