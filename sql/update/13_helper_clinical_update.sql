-- helper_clinical DDL-only update: CREATE OR REPLACE preserving pre-window rows + fresh window recalculation
-- Self-referential architecture: comorbidity baselines from prior helper_clinical_grouped;
-- incremental flags from monthly staging tables only (no full slim scans).
-- window_encounters sourced from two monthly staging tables, not encounters_slim.
-- patient_baseline joins directly on hcg.patient_id — hcg not yet modified at H1 time,
-- so all hcg rows are from prior iterations and serve as valid pre-window baselines.
-- Depends on: encounters_{{END_DATE_SAFE}}, encounters_{{PREV_END_DATE_SAFE}},
--             claims_{{END_DATE_SAFE}}, procedures_{{END_DATE_SAFE}},
--             procedures_dictionary, diagnoses_dictionary, main_diagnoses,
--             careplans_related_encounters, helper_clinical_grouped, patient_known_chronic_codes
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_clinical AS
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
      stay_id,
      patient_id,
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
      num_procedures,
      num_chronic_conditions,
      has_diabetes,
      has_cancer,
      has_hiv,
      has_hf,
      has_alz,
      has_ckd,
      has_lf,
      is_planned,
      had_surgery,
      last_surgery_date
    FROM {{DATASET_HELPERS}}.helper_clinical
    WHERE stay_id NOT IN (SELECT id FROM window_ids)
  ),
  -- Window encounters: union of current and prior month staging tables
  -- Covers the full 2-month window without scanning encounters_slim
  window_encounters AS (
    SELECT id, patient, start, stop
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
    UNION ALL
    SELECT id, patient, start, stop
    FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
  ),
  -- Per-patient historical baseline from helper_clinical_grouped.
  -- hcg not yet modified at H1 time, so all rows are pre-window baselines.
  -- Scoped to patients present in the window to avoid scanning the full hcg table.
  patient_baseline AS (
    SELECT
      hcg.patient_id                      AS patient,
      MAX(hcg.has_diabetes)               AS has_diabetes_baseline,
      MAX(hcg.has_cancer)                 AS has_cancer_baseline,
      MAX(hcg.has_hiv)                    AS has_hiv_baseline,
      MAX(hcg.has_hf)                     AS has_hf_baseline,
      MAX(hcg.has_alz)                    AS has_alz_baseline,
      MAX(hcg.has_ckd)                    AS has_ckd_baseline,
      MAX(hcg.has_lf)                     AS has_lf_baseline,
      MAX(hcg.last_surgery_date)          AS last_surgery_date_baseline
    FROM (SELECT DISTINCT patient FROM window_encounters) wp
    JOIN {{DATASET_HELPERS}}.helper_clinical_grouped hcg ON hcg.patient_id = wp.patient
    GROUP BY hcg.patient_id
  ),
  -- Unpivot all 8 claim diagnosis columns from current month's staging table
  new_claims_long AS (
    SELECT DISTINCT stay_id, code
    FROM (
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis1 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis1 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis2 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis2 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis3 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis3 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis4 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis4 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis5 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis5 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis6 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis6 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis7 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis7 IS NOT NULL
      UNION ALL
      SELECT appointmentid AS stay_id, CAST(SAFE_CAST(diagnosis8 AS FLOAT64) AS INT64) AS code
      FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} WHERE diagnosis8 IS NOT NULL
    ) t
  ),
  -- New disease onset dates from current month's claims (per patient)
  -- Only detects NEW onsets this month; historical presence covered by patient_baseline
  new_condition_starts AS (
    SELECT
      claims.patientid                                           AS patient,
      MIN(IF(dict.is_diabetes = 1, claims.currentillnessdate, NULL)) AS new_diabetes_start,
      MIN(IF(dict.is_cancer   = 1, claims.currentillnessdate, NULL)) AS new_cancer_start,
      MIN(IF(dict.is_hiv      = 1, claims.currentillnessdate, NULL)) AS new_hiv_start,
      MIN(IF(dict.is_hf       = 1, claims.currentillnessdate, NULL)) AS new_hf_start,
      MIN(IF(dict.is_dementia = 1, claims.currentillnessdate, NULL)) AS new_alz_start,
      MIN(IF(dict.is_ckd      = 1, claims.currentillnessdate, NULL)) AS new_ckd_start,
      MIN(IF(dict.is_lf       = 1, claims.currentillnessdate, NULL)) AS new_lf_start
    FROM new_claims_long ncl
    JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict ON ncl.code = dict.code
    JOIN {{DATASET_RAW}}.claims_{{END_DATE_SAFE}} claims ON ncl.stay_id = claims.appointmentid
    GROUP BY claims.patientid
  ),
  -- Count qualifying procedures per window encounter from current month's staging only
  new_procedures AS (
    SELECT
      proc.encounter AS stay_id,
      COUNT(proc.code) AS num_procedures
    FROM {{DATASET_RAW}}.procedures_{{END_DATE_SAFE}} proc
    JOIN {{DATASET_HELPERS}}.procedures_dictionary pd ON proc.code = pd.code
    WHERE pd.is_procedure = 1
    GROUP BY proc.encounter
  ),
  -- Most recent surgery date from current month's staging per window encounter
  new_surgeries AS (
    SELECT
      we.id AS stay_id,
      MAX(
        IF(proc.start < we.start AND DATE_DIFF(we.start, proc.start, DAY) < 730,
          proc.start, NULL)
      ) AS new_surgery_date
    FROM window_encounters we
    JOIN {{DATASET_RAW}}.procedures_{{END_DATE_SAFE}} proc ON we.patient = proc.patient
    JOIN {{DATASET_HELPERS}}.procedures_dictionary pd ON proc.code = pd.code
    WHERE pd.is_surgery = 1
    GROUP BY we.id
  ),
  -- Combined surgery date: most recent of baseline and new surgeries, NULL-safe
  surgery_combined AS (
    SELECT
      we.id AS stay_id,
      CASE
        WHEN pb.last_surgery_date_baseline IS NULL AND ns.new_surgery_date IS NULL THEN NULL
        WHEN pb.last_surgery_date_baseline IS NULL THEN ns.new_surgery_date
        WHEN ns.new_surgery_date IS NULL THEN pb.last_surgery_date_baseline
        ELSE GREATEST(pb.last_surgery_date_baseline, ns.new_surgery_date)
      END AS last_surgery_date
    FROM window_encounters we
    LEFT JOIN patient_baseline pb ON we.patient = pb.patient
    LEFT JOIN new_surgeries ns ON we.id = ns.stay_id
  ),
  -- Encounters that follow a planning procedure from current month's staging
  new_planning_encounters AS (
    SELECT DISTINCT we.id AS stay_id
    FROM window_encounters we
    JOIN {{DATASET_RAW}}.procedures_{{END_DATE_SAFE}} proc ON we.patient = proc.patient
    JOIN {{DATASET_HELPERS}}.procedures_dictionary pd ON proc.code = pd.code
    WHERE pd.is_planning = 1 AND proc.start < we.start
  ),
  -- Final assembly: one row per window encounter
  new_rows AS (
    SELECT
      we.id                                    AS stay_id,
      we.patient                               AS patient_id,
      dict.code                                AS main_code,
      dict.name                                AS main_name,
      COALESCE(dict.is_disorder,     0)        AS is_disorder,
      COALESCE(dict.is_symptom,      0)        AS is_symptom,
      COALESCE(dict.inflammation,    0)        AS inflammation,
      COALESCE(dict.musculoskeletal, 0)        AS musculoskeletal,
      COALESCE(dict.nervous,         0)        AS nervous,
      COALESCE(dict.respiratory,     0)        AS respiratory,
      COALESCE(dict.cardiac,         0)        AS cardiac,
      COALESCE(dict.renal,           0)        AS renal,
      COALESCE(dict.trauma,          0)        AS trauma,
      COALESCE(dict.intoxication,    0)        AS intoxication,
      COALESCE(main.num_of_disorders, 0)       AS num_disorders,
      COALESCE(main.num_of_findings,  0)       AS num_findings,
      COALESCE(np.num_procedures, 0)           AS num_procedures,
      (
        SELECT COUNT(*)
        FROM {{DATASET_HELPERS}}.patient_known_chronic_codes pkcc
        WHERE pkcc.patient_id = we.patient AND pkcc.first_seen_date <= we.start
      )                                        AS num_chronic_conditions,
      GREATEST(
        COALESCE(pb.has_diabetes_baseline, 0),
        CASE WHEN ncs.new_diabetes_start IS NOT NULL AND DATE(we.start) >= DATE(ncs.new_diabetes_start) THEN 1 ELSE 0 END
      )                                        AS has_diabetes,
      GREATEST(
        COALESCE(pb.has_cancer_baseline, 0),
        CASE WHEN ncs.new_cancer_start IS NOT NULL AND DATE(we.start) >= DATE(ncs.new_cancer_start) THEN 1 ELSE 0 END
      )                                        AS has_cancer,
      GREATEST(
        COALESCE(pb.has_hiv_baseline, 0),
        CASE WHEN ncs.new_hiv_start IS NOT NULL AND DATE(we.start) >= DATE(ncs.new_hiv_start) THEN 1 ELSE 0 END
      )                                        AS has_hiv,
      GREATEST(
        COALESCE(pb.has_hf_baseline, 0),
        CASE WHEN ncs.new_hf_start IS NOT NULL AND DATE(we.start) >= DATE(ncs.new_hf_start) THEN 1 ELSE 0 END
      )                                        AS has_hf,
      GREATEST(
        COALESCE(pb.has_alz_baseline, 0),
        CASE WHEN ncs.new_alz_start IS NOT NULL AND DATE(we.start) >= DATE(ncs.new_alz_start) THEN 1 ELSE 0 END
      )                                        AS has_alz,
      GREATEST(
        COALESCE(pb.has_ckd_baseline, 0),
        CASE WHEN ncs.new_ckd_start IS NOT NULL AND DATE(we.start) >= DATE(ncs.new_ckd_start) THEN 1 ELSE 0 END
      )                                        AS has_ckd,
      GREATEST(
        COALESCE(pb.has_lf_baseline, 0),
        CASE WHEN ncs.new_lf_start IS NOT NULL AND DATE(we.start) >= DATE(ncs.new_lf_start) THEN 1 ELSE 0 END
      )                                        AS has_lf,
      GREATEST(
        COALESCE(care.is_related, 0),
        CASE WHEN npe.stay_id IS NOT NULL THEN 1 ELSE 0 END
      )                                        AS is_planned,
      CASE
        WHEN sc.last_surgery_date IS NOT NULL
          AND we.start > sc.last_surgery_date
          AND DATE_DIFF(we.start, sc.last_surgery_date, DAY) < 730
        THEN 1 ELSE 0
      END                                      AS had_surgery,
      sc.last_surgery_date                     AS last_surgery_date
    FROM window_encounters we
    LEFT JOIN {{DATASET_HELPERS}}.main_diagnoses main ON we.id = main.id
    LEFT JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict ON main.main_diagnosis_code = dict.code
    LEFT JOIN new_procedures np ON we.id = np.stay_id
    LEFT JOIN patient_baseline pb ON we.patient = pb.patient
    LEFT JOIN new_condition_starts ncs ON we.patient = ncs.patient
    LEFT JOIN {{DATASET_HELPERS}}.careplans_related_encounters care ON we.id = care.stay_id
    LEFT JOIN new_planning_encounters npe ON we.id = npe.stay_id
    LEFT JOIN surgery_combined sc ON we.id = sc.stay_id
  )
SELECT
  stay_id,
  patient_id,
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
  num_procedures,
  num_chronic_conditions,
  has_diabetes,
  has_cancer,
  has_hiv,
  has_hf,
  has_alz,
  has_ckd,
  has_lf,
  is_planned,
  had_surgery,
  last_surgery_date
FROM existing
UNION ALL
SELECT
  stay_id,
  patient_id,
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
  num_procedures,
  num_chronic_conditions,
  has_diabetes,
  has_cancer,
  has_hiv,
  has_hf,
  has_alz,
  has_ckd,
  has_lf,
  is_planned,
  had_surgery,
  last_surgery_date
FROM new_rows
