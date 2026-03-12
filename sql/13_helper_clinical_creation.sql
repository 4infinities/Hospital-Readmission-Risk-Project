CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical
AS (
  WITH
    end_date AS (
      SELECT MAX(stop) AS end_ts
      FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim
    ),

    -- explode claims into one row per (stay, code) so we can see which of them are disorders
    claims_long AS (
      SELECT DISTINCT
        stay_id, code
      FROM
        (
          SELECT encounter AS stay_id, CAST(diagnosis1 AS INT64) AS code
          FROM {{DATASET_SLIM}}.{{PROFILE}}claims_slim
          WHERE diagnosis1 IS NOT NULL
          UNION ALL
          SELECT encounter AS stay_id, CAST(diagnosis2 AS INT64) AS code
          FROM {{DATASET_SLIM}}.{{PROFILE}}claims_slim
          WHERE diagnosis2 IS NOT NULL
          UNION ALL
          SELECT encounter AS stay_id, CAST(diagnosis3 AS INT64) AS code
          FROM {{DATASET_SLIM}}.{{PROFILE}}claims_slim
          WHERE diagnosis3 IS NOT NULL
          UNION ALL
          SELECT encounter AS stay_id, CAST(diagnosis4 AS INT64) AS code
          FROM {{DATASET_SLIM}}.{{PROFILE}}claims_slim
          WHERE diagnosis4 IS NOT NULL
          UNION ALL
          SELECT encounter AS stay_id, CAST(diagnosis5 AS INT64) AS code
          FROM {{DATASET_SLIM}}.{{PROFILE}}claims_slim
          WHERE diagnosis5 IS NOT NULL
          UNION ALL
          SELECT encounter AS stay_id, CAST(diagnosis6 AS INT64) AS code
          FROM {{DATASET_SLIM}}.{{PROFILE}}claims_slim
          WHERE diagnosis6 IS NOT NULL
          UNION ALL
          SELECT encounter AS stay_id, CAST(diagnosis7 AS INT64) AS code
          FROM {{DATASET_SLIM}}.{{PROFILE}}claims_slim
          WHERE diagnosis7 IS NOT NULL
          UNION ALL
          SELECT encounter AS stay_id, CAST(diagnosis8 AS INT64) AS code
          FROM {{DATASET_SLIM}}.{{PROFILE}}claims_slim
          WHERE diagnosis8 IS NOT NULL
        ) t
    ),
    -- every row is a patient, their diagnosis and stay_id where they got this diagnosis, and what is the start of it
    patient_diagnoses AS (
      SELECT
        e.patient,
        c.code,
        e.start AS encounter_start
      FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
      JOIN claims_long c
        ON e.id = c.stay_id
    ),
    procedures AS (
      SELECT
        proc.encounter AS stay_id,
        COUNT(proc.code) AS num_procedures
      FROM {{DATASET_SLIM}}.{{PROFILE}}procedures_slim proc
      LEFT JOIN
        {{DATASET_HELPERS}}.{{PROFILE}}procedures_dictionary proc_dict
        ON proc.code = proc_dict.code
      WHERE proc_dict.is_procedure = 1
      GROUP BY proc.encounter
    ),
    chronic_patient_codes AS (
      SELECT
        pd.patient,
        pd.code,
        pd.encounter_start
      FROM patient_diagnoses pd
      JOIN {{DATASET_HELPERS}}.{{PROFILE}}diagnoses_dictionary dict
        ON pd.code = dict.code
      WHERE dict.is_chronic = 1
    ),
    first_chronic_per_code AS (
      SELECT
        patient,
        code,
        MIN(encounter_start) AS first_chronic_date
      FROM chronic_patient_codes
      GROUP BY patient, code
    ),
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
      FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
    ),
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
      JOIN {{DATASET_HELPERS}}.{{PROFILE}}diagnoses_dictionary dict
        ON cl.code = dict.code
      JOIN {{DATASET_SLIM}}.{{PROFILE}}claims_slim claims
        ON cl.stay_id = claims.encounter
      GROUP BY claims.patientid
    ),
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
      FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
      LEFT JOIN patient_condition_starts pcs
        ON pcs.patient = e.patient
    ),
    plans_by_procedure AS (
      SELECT
        e.id AS stay_id,
        row_number()
          OVER (PARTITION BY e.patient ORDER BY e.start ASC) AS num_after_plan
      FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
      LEFT JOIN {{DATASET_SLIM}}.{{PROFILE}}procedures_slim proc
        ON e.patient = proc.patient
      LEFT JOIN
        {{DATASET_HELPERS}}.{{PROFILE}}procedures_dictionary proc_dict
        ON proc_dict.code = proc.code
      WHERE
        proc_dict.is_planning = 1
        AND e.start > proc.start
    ),
    plans_by_procedure_flag AS (
      SELECT
        stay_id,
        1 AS is_planned
      FROM plans_by_procedure
      WHERE num_after_plan = 1
    ),
    planned_stays as (
      SELECT
        e.id as stay_id,
        greatest(coalesce(proc.is_planned, 0), coalesce(care.is_related, 0))
          AS is_planned
      FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
      left join {{DATASET_HELPERS}}.{{PROFILE}}careplans_related_encounters care
      on e.id = care.stay_id
      LEFT JOIN plans_by_procedure_flag proc
        ON e.id = proc.stay_id
    ),
    surgeries_and_dates AS (
      SELECT
        date_diff(e.start, proc.start, day) AS days_from_surgery,
        e.id AS stay_id,
      FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
      LEFT JOIN {{DATASET_SLIM}}.{{PROFILE}}procedures_slim proc
        ON e.patient = proc.patient
      LEFT JOIN
        {{DATASET_HELPERS}}.{{PROFILE}}procedures_dictionary proc_dict
        ON proc_dict.code = proc.code
      WHERE
        proc_dict.is_surgery = 1
        AND e.start > proc.start
    ),
    surgeries AS (
      SELECT
        e.id AS stay_id,
        max(
          CASE
            WHEN coalesce(surg.days_from_surgery, 1000) < 730
              THEN 1
            ELSE 0
            END) AS had_surgery
      FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
      LEFT JOIN surgeries_and_dates surg
        ON e.id = surg.stay_id
      GROUP BY e.id
    )
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
  FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim e
  LEFT JOIN {{DATASET_HELPERS}}.{{PROFILE}}main_diagnoses main
    ON e.id = main.id
  LEFT JOIN {{DATASET_HELPERS}}.{{PROFILE}}diagnoses_dictionary dict
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
);
