CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical_grouped
AS
WITH
  group_flags AS (
    SELECT
      id,
      patient,
      start,
      CASE encounterclass
        WHEN 'ambulatory' THEN 1
        WHEN 'outpatient' THEN 2
        WHEN 'virtual' THEN 3
        WHEN 'urgentcare' THEN 4
        WHEN 'emergency' THEN 5
        WHEN 'inpatient' THEN 6
        ELSE 99
        END type_flag,
      CASE
        WHEN
          date_diff(
            start,
            lag(stop, 1) OVER (PARTITION BY patient ORDER BY start ASC),
            hour)
          < 12
          THEN 0
        ELSE 1
        END AS group_change
    FROM {{DATASET_SLIM}}.{{PROFILE}}encounters_slim
  ),
  clusters AS (
    SELECT
      id,
      patient,
      start,
      type_flag,
      sum(group_change)
        OVER (
          PARTITION BY patient
          ORDER BY start ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) group_number
    FROM group_flags
  ),
  best_stay_per_group AS (
    SELECT
      patient,
      group_number,
      id AS group_id,
      type_flag,
      ROW_NUMBER()
        OVER (
          PARTITION BY patient, group_number
          ORDER BY
            type_flag DESC,  -- highest type_flag wins
            start ASC,  -- tie-breaker: earliest start
            id ASC  -- final tie-breaker
        ) AS rn
    FROM clusters
  ),
  final_groups AS (
    SELECT
      clust.id,
      best.group_id,
      CASE best.type_flag
        WHEN 1 THEN 'ambulatory'
        WHEN 2 THEN 'outpatient'
        WHEN 3 THEN 'virtual'
        WHEN 4 THEN 'urgentcare'
        WHEN 5 THEN 'emergency'
        WHEN 6 THEN 'inpatient'
        ELSE 'unknown'
        END AS encounterclass
    FROM clusters clust
    LEFT JOIN best_stay_per_group best
      ON
        best.patient = clust.patient
        AND best.group_number = clust.group_number
        AND best.rn = 1
  ),
  flags AS (
    SELECT
      final.group_id AS stay_id,
      any_value(final.encounterclass) AS encounterclass,
      sum(hc.num_procedures) num_procedures,
      max(hc.num_chronic_conditions) num_chronic_conditions,
      MAX(hc.has_diabetes) AS has_diabetes,
      MAX(hc.has_cancer) AS has_cancer,
      MAX(hc.has_hiv) AS has_hiv,
      MAX(hc.has_hf) AS has_hf,
      MAX(hc.has_alz) AS has_alz,
      MAX(hc.has_ckd) AS has_ckd,
      MAX(hc.has_lf) AS has_lf,
      MAX(hc.is_planned) AS is_planned,
      MAX(hc.had_surgery) AS had_surgery
    FROM final_groups final
    LEFT JOIN {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical hc
      ON final.id = hc.stay_id
    WHERE final.encounterclass IN ('urgentcare', 'inpatient', 'emergency')
    GROUP BY final.group_id
  )
SELECT
  flag.stay_id,
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
  flag.num_procedures,
  flag.num_chronic_conditions,
  flag.has_diabetes,
  flag.has_cancer,
  flag.has_hiv,
  flag.has_hf,
  flag.has_alz,
  flag.has_ckd,
  flag.has_lf,
  flag.is_planned,
  flag.had_surgery
FROM flags flag
LEFT JOIN {{DATASET_HELPERS}}.{{PROFILE}}main_diagnoses main
  ON flag.stay_id = main.id
LEFT JOIN {{DATASET_HELPERS}}.{{PROFILE}}diagnoses_dictionary dict
  ON main.main_diagnosis_code = dict.code
