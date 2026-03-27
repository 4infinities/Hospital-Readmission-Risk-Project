-- index_stay DDL-only update: CREATE OR REPLACE preserving pre-window rows + inserting fresh window rows
-- Final feature table — must run AFTER all helpers and related_diagnoses are updated
-- Window encounter ids sourced from monthly staging tables — no encounters_slim scan
-- patient and organization columns come from staging tables (same schema as encounters_slim)
-- patients_slim retained for birthdate and gender (small reference table, not a scan concern)
-- Depends on: encounters_{{END_DATE_SAFE}}, encounters_{{PREV_END_DATE_SAFE}},
--             patients_slim, helper_clinical_grouped, helper_cost_aggregation_grouped,
--             helper_utilization, related_diagnoses
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.index_stay AS
WITH
  bounds AS (
    SELECT
      DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH AS window_start,
      DATE({{END_DATE}}) AS window_end
  ),
  window_ids AS (
    SELECT id
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
    UNION DISTINCT
    SELECT id
    FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
  ),
  existing AS (
    SELECT
      patient_id,
      patient_age,
      gender,
      stay_id,
      hospital_id,
      admission_datetime,
      discharge_datetime,
      discharge_date,
      discharge_year,
      discharge_month,
      length_of_stay,
      stay_type,
      main_code,
      main_name,
      is_disorder,
      is_symptom,
      inflammation,
      musculoskeletal,
      nervous,
      respiratory,
      cardiac,
      renal,
      trauma,
      intoxication,
      num_disorders,
      num_findings,
      num_chronic_conditions,
      num_procedures,
      has_diabetes,
      has_cancer,
      has_hiv,
      has_hf,
      has_alz,
      has_ckd,
      has_lf,
      had_surgery,
      is_planned,
      admission_cost,
      total_procedure_costs,
      total_medication_costs,
      total_stay_cost,
      cost_per_day_stay,
      admissions_365d,
      tot_length_of_stay_365d,
      avg_cost_of_prev_stays,
      readmit_30d,
      readmit_90d,
      is_related,
      rel_readmit_30d,
      rel_readmit_90d,
      days_to_readmit,
      following_stay_id,
      total_readmission_cost,
      following_unplanned_admission_flag
    FROM {{DATASET_HELPERS}}.index_stay
    WHERE stay_id NOT IN (SELECT id FROM window_ids)
  ),
  -- Window encounters from monthly staging tables providing patient_id and hospital_id
  -- Scoped to clinical encounter types only
  window_enc AS (
    SELECT id, patient, organization
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
      AND encounterclass IN ('urgentcare', 'emergency', 'inpatient')
    UNION DISTINCT
    SELECT id, patient, organization
    FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
      AND encounterclass IN ('urgentcare', 'emergency', 'inpatient')
  ),
  new_rows AS (
    SELECT
      we.patient                                              AS patient_id,
      DATE_DIFF(DATE(util.start), p.birthdate, YEAR)         AS patient_age,
      p.gender                                               AS gender,
      clin.stay_id                                           AS stay_id,
      we.organization                                        AS hospital_id,
      util.start                                             AS admission_datetime,
      util.stop                                              AS discharge_datetime,
      DATE(util.stop)                                        AS discharge_date,
      EXTRACT(YEAR  FROM util.stop)                          AS discharge_year,
      EXTRACT(MONTH FROM util.stop)                          AS discharge_month,
      cost.length_of_encounter                               AS length_of_stay,
      util.encounterclass                                    AS stay_type,
      clin.main_code                                         AS main_code,
      clin.main_name                                         AS main_name,
      clin.is_disorder                                       AS is_disorder,
      clin.is_symptom                                        AS is_symptom,
      clin.inflammation                                      AS inflammation,
      clin.musculoskeletal                                   AS musculoskeletal,
      clin.nervous                                           AS nervous,
      clin.respiratory                                       AS respiratory,
      clin.cardiac                                           AS cardiac,
      clin.renal                                             AS renal,
      clin.trauma                                            AS trauma,
      clin.intoxication                                      AS intoxication,
      clin.num_disorders                                     AS num_disorders,
      clin.num_findings                                      AS num_findings,
      clin.num_chronic_conditions                            AS num_chronic_conditions,
      clin.num_procedures                                    AS num_procedures,
      clin.has_diabetes                                      AS has_diabetes,
      clin.has_cancer                                        AS has_cancer,
      clin.has_hiv                                           AS has_hiv,
      clin.has_hf                                            AS has_hf,
      clin.has_alz                                           AS has_alz,
      clin.has_ckd                                           AS has_ckd,
      clin.has_lf                                            AS has_lf,
      clin.had_surgery                                       AS had_surgery,
      clin.is_planned                                        AS is_planned,
      cost.admission_cost                                    AS admission_cost,
      cost.total_procedure_costs                             AS total_procedure_costs,
      cost.total_medication_costs                            AS total_medication_costs,
      cost.total_stay_cost                                   AS total_stay_cost,
      cost.cost_per_day_stay                                 AS cost_per_day_stay,
      util.admissions_365d                                   AS admissions_365d,
      util.tot_length_of_stay_365d                          AS tot_length_of_stay_365d,
      util.avg_cost_of_prev_stays                            AS avg_cost_of_prev_stays,
      util.readmit_30d                                       AS readmit_30d,
      util.readmit_90d                                       AS readmit_90d,
      rel.is_related                                         AS is_related,
      rel.rel_readmit_30d                                    AS rel_readmit_30d,
      rel.rel_readmit_90d                                    AS rel_readmit_90d,
      util.days_to_readmit                                   AS days_to_readmit,
      util.following_stay_id                                 AS following_stay_id,
      util.total_stay_cost                                   AS total_readmission_cost,
      util.following_unplanned_admission_flag                AS following_unplanned_admission_flag
    FROM window_enc we
    LEFT JOIN {{DATASET_SLIM}}.patients_slim p
      ON we.patient = p.id
    LEFT JOIN {{DATASET_HELPERS}}.helper_clinical_grouped clin
      ON clin.stay_id = we.id
    LEFT JOIN {{DATASET_HELPERS}}.helper_cost_aggregation_grouped cost
      ON cost.stay_id = we.id
    LEFT JOIN {{DATASET_HELPERS}}.helper_utilization util
      ON util.stay_id = we.id
    LEFT JOIN {{DATASET_HELPERS}}.related_diagnoses rel
      ON we.id = rel.stay_id
  )
SELECT
  patient_id,
  patient_age,
  gender,
  stay_id,
  hospital_id,
  admission_datetime,
  discharge_datetime,
  discharge_date,
  discharge_year,
  discharge_month,
  length_of_stay,
  stay_type,
  main_code,
  main_name,
  is_disorder,
  is_symptom,
  inflammation,
  musculoskeletal,
  nervous,
  respiratory,
  cardiac,
  renal,
  trauma,
  intoxication,
  num_disorders,
  num_findings,
  num_chronic_conditions,
  num_procedures,
  has_diabetes,
  has_cancer,
  has_hiv,
  has_hf,
  has_alz,
  has_ckd,
  has_lf,
  had_surgery,
  is_planned,
  admission_cost,
  total_procedure_costs,
  total_medication_costs,
  total_stay_cost,
  cost_per_day_stay,
  admissions_365d,
  tot_length_of_stay_365d,
  avg_cost_of_prev_stays,
  readmit_30d,
  readmit_90d,
  is_related,
  rel_readmit_30d,
  rel_readmit_90d,
  days_to_readmit,
  following_stay_id,
  total_readmission_cost,
  following_unplanned_admission_flag
FROM existing
UNION ALL
SELECT
  patient_id,
  patient_age,
  gender,
  stay_id,
  hospital_id,
  admission_datetime,
  discharge_datetime,
  discharge_date,
  discharge_year,
  discharge_month,
  length_of_stay,
  stay_type,
  main_code,
  main_name,
  is_disorder,
  is_symptom,
  inflammation,
  musculoskeletal,
  nervous,
  respiratory,
  cardiac,
  renal,
  trauma,
  intoxication,
  num_disorders,
  num_findings,
  num_chronic_conditions,
  num_procedures,
  has_diabetes,
  has_cancer,
  has_hiv,
  has_hf,
  has_alz,
  has_ckd,
  has_lf,
  had_surgery,
  is_planned,
  admission_cost,
  total_procedure_costs,
  total_medication_costs,
  total_stay_cost,
  cost_per_day_stay,
  admissions_365d,
  tot_length_of_stay_365d,
  avg_cost_of_prev_stays,
  readmit_30d,
  readmit_90d,
  is_related,
  rel_readmit_30d,
  rel_readmit_90d,
  days_to_readmit,
  following_stay_id,
  total_readmission_cost,
  following_unplanned_admission_flag
FROM new_rows
