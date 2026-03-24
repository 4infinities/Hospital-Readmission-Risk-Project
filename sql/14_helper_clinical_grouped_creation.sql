-- helper_clinical_grouped: one row per encounter GROUP (consecutive encounters < 12h apart)
-- aggregates helper_clinical flags across all member encounters; restricted to urgentcare/emergency/inpatient groups
-- Depends on: encounters_slim, helper_clinical, main_diagnoses, diagnoses_dictionary
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_clinical_grouped
AS
WITH
  -- Assign type_flag rank per encounter class; detect group boundary when gap to prior stop >= 12h
  group_flags AS (
    SELECT
      id,
      patient,
      start,
      CASE encounterclass
        when 'wellness' then 0
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
    FROM {{DATASET_SLIM}}.encounters_slim
  where stop <= {{END_DATE}}
  ),
  -- Cumulative sum of group_change yields a monotonically increasing group_number per patient
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
  -- Elect the representative (highest type_flag, then earliest start) encounter per group
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
  -- Map every individual encounter to its group's representative encounter id and class label
  final_groups AS (
    SELECT
      clust.id,
      best.group_id,
      CASE best.type_flag
      WHEN 0 THEN 'wellness'
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
  -- Aggregate helper_clinical flags across all members of each group; only keep clinical groups
  flags AS (
    SELECT
      final.group_id AS stay_id,
      any_value(final.encounterclass) AS encounterclass,
      -- Sum procedures across all group members
      sum(hc.num_procedures) num_procedures,
      -- Take the maximum chronic count (worst-case across member encounters)
      max(hc.num_chronic_conditions) num_chronic_conditions,
      MAX(hc.has_diabetes) AS has_diabetes,
      MAX(hc.has_cancer) AS has_cancer,
      MAX(hc.has_hiv) AS has_hiv,
      MAX(hc.has_hf) AS has_hf,
      MAX(hc.has_alz) AS has_alz,
      MAX(hc.has_ckd) AS has_ckd,
      MAX(hc.has_lf) AS has_lf,
      MAX(hc.is_planned) AS is_planned,
      MAX(hc.had_surgery) AS had_surgery,
      ANY_VALUE(hc.patient_id) AS patient_id,
      MAX(hc.last_surgery_date) AS last_surgery_date
    FROM final_groups final
    LEFT JOIN {{DATASET_HELPERS}}.helper_clinical hc
      ON final.id = hc.stay_id
    -- Restrict to clinically significant encounter groups only
    WHERE final.encounterclass IN ('urgentcare', 'inpatient', 'emergency')
    GROUP BY final.group_id
  )
-- Final output: attach main diagnosis and category flags from the group representative encounter
SELECT
  flag.stay_id,
  flag.patient_id,
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
  flag.had_surgery,
  flag.last_surgery_date
FROM flags flag
LEFT JOIN {{DATASET_HELPERS}}.main_diagnoses main
  ON flag.stay_id = main.id
LEFT JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict
  ON main.main_diagnosis_code = dict.code
