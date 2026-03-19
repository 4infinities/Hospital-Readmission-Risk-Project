-- helper_clinical: one row per encounter, computes clinical flags, diagnosis category flags,
-- chronic condition counts, specific disease presence flags, planned-stay flag, and surgery recency flag
-- Depends on: encounters_slim, claims_slim, procedures_slim, procedures_dictionary,
--             diagnoses_dictionary, main_diagnoses, careplans_related_encounters
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_clinical
AS (
  WITH
    -- Unpivot all 8 claim diagnosis columns into one row per (stay_id, code) for join-based lookups
    claims_long AS (
      SELECT DISTINCT
        stay_id, code
      FROM
        (
          SELECT encounter AS stay_id, CAST(diagnosis1 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis1 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION ALL
  SELECT encounter AS stay_id, CAST(diagnosis2 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis2 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION ALL
  SELECT encounter AS stay_id, CAST(diagnosis3 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis3 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION ALL
  SELECT encounter AS stay_id, CAST(diagnosis4 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis4 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION ALL
  SELECT encounter AS stay_id, CAST(diagnosis5 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis5 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION ALL
  SELECT encounter AS stay_id, CAST(diagnosis6 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis6 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION ALL
  SELECT encounter AS stay_id, CAST(diagnosis7 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis7 IS NOT NULL and currentillnessdate <= {{END_DATE}}
  UNION ALL
  SELECT encounter AS stay_id, CAST(diagnosis8 AS INT64) AS code
  FROM {{DATASET_SLIM}}.claims_slim WHERE diagnosis8 IS NOT NULL and currentillnessdate <= {{END_DATE}}
        ) t
    ),
    -- All (patient, diagnosis code, encounter_start) tuples — used to compute chronic condition history
    patient_diagnoses AS (
      SELECT
        e.patient,
        c.code,
        e.start AS encounter_start
      FROM {{DATASET_SLIM}}.encounters_slim e
      JOIN claims_long c
        ON e.id = c.stay_id
        where e.stop <= {{END_DATE}}
    ),
    -- Count of distinct qualifying procedures per encounter (is_procedure = 1 in procedures_dictionary)
    procedures AS (
      SELECT
        proc.encounter AS stay_id,
        COUNT(proc.code) AS num_procedures
      FROM {{DATASET_SLIM}}.procedures_slim proc
      LEFT JOIN
        {{DATASET_HELPERS}}.procedures_dictionary proc_dict
        ON proc.code = proc_dict.code
      WHERE proc_dict.is_procedure = 1
      GROUP BY proc.encounter
      HAVING MAX(proc.stop) <= {{END_DATE}}
    ),
    -- Subset of patient_diagnoses limited to codes flagged is_chronic in diagnoses_dictionary
    chronic_patient_codes AS (
      SELECT
        pd.patient,
        pd.code,
        pd.encounter_start
      FROM patient_diagnoses pd
      JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict
        ON pd.code = dict.code
      WHERE dict.is_chronic = 1
    ),
    -- Earliest encounter date per patient per chronic code (lifetime onset, not per-stay)
    first_chronic_per_code AS (
      SELECT
        patient,
        code,
        MIN(encounter_start) AS first_chronic_date
      FROM chronic_patient_codes
      GROUP BY patient, code
    ),
    -- For each encounter, count how many distinct chronic conditions the patient had before or at that encounter
    chronic_conditions AS (
      SELECT
        e.id AS stay_id,
        e.patient,
        e.start AS encounter_start,
        (
          SELECT COUNT(*)
          FROM first_chronic_per_code f
          WHERE
            f.patient = e.patient
            AND f.first_chronic_date <= e.start
        ) AS num_chronic_conditions
      FROM {{DATASET_SLIM}}.encounters_slim e
      where e.stop <= {{END_DATE}}
    ),
    -- For each patient, find the first claim date per specific disease flag (diabetes, cancer, etc.)
    patient_condition_starts AS (
      SELECT
        claims.patientid AS patient,
        MIN(IF(dict.is_diabetes = 1, claims.currentillnessdate, NULL))
          AS diabetes_start,
        MIN(IF(dict.is_cancer = 1, claims.currentillnessdate, NULL))
          AS cancer_start,
        MIN(IF(dict.is_hiv = 1, claims.currentillnessdate, NULL)) AS hiv_start,
        MIN(IF(dict.is_hf = 1, claims.currentillnessdate, NULL)) AS hf_start,
        MIN(IF(dict.is_dementia = 1, claims.currentillnessdate, NULL))
          AS alz_start,
        MIN(IF(dict.is_ckd = 1, claims.currentillnessdate, NULL)) AS ckd_start,
        MIN(IF(dict.is_lf = 1, claims.currentillnessdate, NULL))
          AS lf_start
      FROM claims_long cl
      JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict
        ON cl.code = dict.code
      JOIN {{DATASET_SLIM}}.claims_slim claims
        ON cl.stay_id = claims.encounter
      where claims.currentillnessdate <= {{END_DATE}}
      GROUP BY claims.patientid
    ),
    -- Flag each encounter: 1 if the patient already had the specific disease before this encounter started
    patient_conditions AS (
      SELECT
        e.id AS stay_id,
        CASE
          WHEN
            pcs.diabetes_start IS NOT NULL
            AND e.start >= pcs.diabetes_start
            THEN 1
          ELSE 0
          END AS has_diabetes,
        CASE
          WHEN
            pcs.cancer_start IS NOT NULL
            AND e.start >= pcs.cancer_start
            THEN 1
          ELSE 0
          END AS has_cancer,
        CASE
          WHEN
            pcs.hiv_start IS NOT NULL
            AND e.start >= pcs.hiv_start
            THEN 1
          ELSE 0
          END AS has_hiv,
        CASE
          WHEN
            pcs.hf_start IS NOT NULL
            AND e.start >= pcs.hf_start
            THEN 1
          ELSE 0
          END AS has_hf,
        CASE
          WHEN
            pcs.alz_start IS NOT NULL
            AND e.start >= pcs.alz_start
            THEN 1
          ELSE 0
          END AS has_alz,
        CASE
          WHEN
            pcs.ckd_start IS NOT NULL
            AND e.start >= pcs.ckd_start
            THEN 1
          ELSE 0
          END AS has_ckd,
        CASE
          WHEN
            pcs.lf_start IS NOT NULL
            AND e.start >= pcs.lf_start
            THEN 1
          ELSE 0
          END AS has_lf
      FROM {{DATASET_SLIM}}.encounters_slim e
      LEFT JOIN patient_condition_starts pcs
        ON pcs.patient = e.patient
      where e.stop <= {{END_DATE}}
    ),
    -- Find encounters that follow a planning procedure (is_planning = 1); row_number = 1 means first post-plan encounter
    plans_by_procedure AS (
      SELECT
        e.id AS stay_id,
        row_number()
          OVER (PARTITION BY e.patient ORDER BY e.start ASC) AS num_after_plan
      FROM {{DATASET_SLIM}}.encounters_slim e
      LEFT JOIN {{DATASET_SLIM}}.procedures_slim proc
        ON e.patient = proc.patient
      LEFT JOIN
        {{DATASET_HELPERS}}.procedures_dictionary proc_dict
        ON proc_dict.code = proc.code
      WHERE
        proc_dict.is_planning = 1
        AND e.start > proc.start
        AND e.stop <= {{END_DATE}}
    ),
    -- Retain only the first post-planning-procedure encounter per patient as "planned"
    plans_by_procedure_flag AS (
      SELECT
        stay_id,
        1 AS is_planned
      FROM plans_by_procedure
      WHERE num_after_plan = 1
    ),
    -- Combine procedure-based and careplan-based planned-stay signals into a single is_planned flag
    planned_stays as (
      SELECT
        e.id as stay_id,
        greatest(coalesce(proc.is_planned, 0), coalesce(care.is_related, 0))
          AS is_planned
      FROM {{DATASET_SLIM}}.encounters_slim e
      left join {{DATASET_HELPERS}}.careplans_related_encounters care
      on e.id = care.stay_id
      LEFT JOIN plans_by_procedure_flag proc
        ON e.id = proc.stay_id
      where e.stop <= {{END_DATE}}
    ),
    -- For each encounter, compute days since each prior surgery (is_surgery = 1 in procedures_dictionary)
    surgeries_and_dates AS (
      SELECT
        date_diff(e.start, proc.start, day) AS days_from_surgery,
        e.id AS stay_id,
      FROM {{DATASET_SLIM}}.encounters_slim e
      LEFT JOIN {{DATASET_SLIM}}.procedures_slim proc
        ON e.patient = proc.patient
      LEFT JOIN
        {{DATASET_HELPERS}}.procedures_dictionary proc_dict
        ON proc_dict.code = proc.code
      WHERE
        proc_dict.is_surgery = 1
        AND e.start > proc.start
        and e.stop <= {{END_DATE}}
    ),
    -- Flag encounter as had_surgery = 1 if any prior surgery occurred within 730 days
    surgeries AS (
      SELECT
        e.id AS stay_id,
        max(
          CASE
            WHEN coalesce(surg.days_from_surgery, 1000) < 730
              THEN 1
            ELSE 0
            END) AS had_surgery
      FROM {{DATASET_SLIM}}.encounters_slim e
      LEFT JOIN surgeries_and_dates surg
        ON e.id = surg.stay_id
      where e.stop <= {{END_DATE}}
      GROUP BY e.id
    )
  -- Final assembly: join all CTEs onto encounters_slim; one output row per encounter
  SELECT
    e.id AS stay_id,
    dict.code AS main_code,
    dict.name AS main_name,
    COALESCE(dict.is_disorder, 0) AS is_disorder,
    COALESCE(dict.is_symptom, 0) AS is_symptom,
    COALESCE(dict.inflammation, 0) AS inflammation,
    COALESCE(dict.musculoskeletal, 0) AS musculoskeletal,
    COALESCE(dict.nervous, 0) AS nervous,
    COALESCE(dict.respiratory, 0) AS respiratory,
    COALESCE(dict.cardiac, 0) AS cardiac,
    COALESCE(dict.renal, 0) AS renal,
    COALESCE(dict.trauma, 0) AS trauma,
    COALESCE(dict.intoxication, 0) AS intoxication,
    COALESCE(main.num_of_disorders, 0) AS num_disorders,
    COALESCE(main.num_of_findings, 0) AS num_findings,
    coalesce(proc.num_procedures, 0) AS num_procedures,
    cc.num_chronic_conditions,
    pc.has_diabetes,
    pc.has_cancer,
    pc.has_hiv,
    pc.has_hf,
    pc.has_alz,
    pc.has_ckd,
    pc.has_lf,
    ps.is_planned,
    coalesce(surg.had_surgery, 0) AS had_surgery,
  FROM {{DATASET_SLIM}}.encounters_slim e
  LEFT JOIN {{DATASET_HELPERS}}.main_diagnoses main
    ON e.id = main.id
  LEFT JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict
    ON main.main_diagnosis_code = dict.code
  LEFT JOIN procedures proc
    ON e.id = proc.stay_id
  LEFT JOIN chronic_conditions cc
    ON e.id = cc.stay_id
  LEFT JOIN patient_conditions pc
    ON e.id = pc.stay_id
  LEFT JOIN planned_stays ps
    ON e.id = ps.stay_id
  LEFT JOIN surgeries surg
    ON e.id = surg.stay_id
  where e.stop <= {{END_DATE}}
);