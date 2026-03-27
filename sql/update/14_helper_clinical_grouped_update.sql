-- helper_clinical_grouped DDL-only update: CREATE OR REPLACE preserving pre-window rows + fresh window recalculation
-- Self-referential architecture: grouping runs on staging-table window encounters only.
-- prior_group_anchor derived from helper_clinical_grouped + helper_utilization (no encounters_slim scan).
-- prior_group_anchor fix: hcg.stay_id NOT IN window_ids excludes current window rows from the anchor,
-- replicating the semantic of the original DELETE (which removed window rows before the INSERT read hcg).
-- Groups that continue a prior group (gap < 12h to prior stop) are skipped — prior hcg row is retained.
-- Depends on: encounters_{{END_DATE_SAFE}}, encounters_{{PREV_END_DATE_SAFE}},
--             helper_clinical (freshly updated by H1), helper_clinical_grouped, helper_utilization,
--             main_diagnoses, diagnoses_dictionary
CREATE OR REPLACE TABLE {{DATASET_HELPERS}}.helper_clinical_grouped AS
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
    FROM {{DATASET_HELPERS}}.helper_clinical_grouped
    WHERE stay_id NOT IN (SELECT id FROM window_ids)
  ),
  -- Window encounters with type_flags: union of current and prior month staging tables
  window_encounters AS (
    SELECT
      id, patient, start, stop, encounterclass,
      CASE encounterclass
        WHEN 'wellness'   THEN 0
        WHEN 'ambulatory' THEN 1
        WHEN 'outpatient' THEN 2
        WHEN 'virtual'    THEN 3
        WHEN 'urgentcare' THEN 4
        WHEN 'emergency'  THEN 5
        WHEN 'inpatient'  THEN 6
        ELSE 99
      END AS type_flag
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
    UNION ALL
    SELECT
      id, patient, start, stop, encounterclass,
      CASE encounterclass
        WHEN 'wellness'   THEN 0
        WHEN 'ambulatory' THEN 1
        WHEN 'outpatient' THEN 2
        WHEN 'virtual'    THEN 3
        WHEN 'urgentcare' THEN 4
        WHEN 'emergency'  THEN 5
        WHEN 'inpatient'  THEN 6
        ELSE 99
      END AS type_flag
    FROM {{DATASET_RAW}}.encounters_{{PREV_END_DATE_SAFE}}, bounds
    WHERE DATE(start) >= bounds.window_start AND DATE(stop) <= bounds.window_end
  ),
  -- Most recent prior group stop per window patient, from helper_utilization
  -- hcg.stay_id NOT IN window_ids: excludes current window hcg rows (from prior iteration overlap)
  -- replicating the effect of the original DELETE that removed window rows before reading hcg
  prior_group_anchor AS (
    SELECT
      wp.patient,
      MAX(hu.stop) AS last_prior_stop
    FROM (SELECT DISTINCT patient FROM window_encounters) wp
    JOIN {{DATASET_HELPERS}}.helper_clinical_grouped hcg
      ON hcg.patient_id = wp.patient
    JOIN {{DATASET_HELPERS}}.helper_utilization hu ON hcg.stay_id = hu.stay_id
    WHERE hcg.stay_id NOT IN (SELECT id FROM window_ids)
    GROUP BY wp.patient
  ),
  -- Group boundary detection for window encounters only
  group_flags AS (
    SELECT
      we.id, we.patient, we.start, we.stop, we.type_flag,
      CASE
        WHEN DATE_DIFF(we.start,
          COALESCE(
            LAG(we.stop) OVER (PARTITION BY we.patient ORDER BY we.start ASC),
            pga.last_prior_stop
          ), hour) < 12
        THEN 0
        ELSE 1
      END AS group_change
    FROM window_encounters we
    LEFT JOIN prior_group_anchor pga ON we.patient = pga.patient
  ),
  -- Cumulative sum yields group_number per patient within the window
  clusters AS (
    SELECT
      id, patient, start, type_flag,
      SUM(group_change) OVER (
        PARTITION BY patient ORDER BY start ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS group_number
    FROM group_flags
  ),
  -- Detect patients whose first window encounter continues a prior group (group_number=0, group_change=0)
  first_group_change_per_patient AS (
    SELECT patient, group_change AS first_group_change
    FROM group_flags
    QUALIFY ROW_NUMBER() OVER (PARTITION BY patient ORDER BY start ASC) = 1
  ),
  -- Elect the representative encounter per group (highest type_flag, then earliest start)
  best_stay_per_group AS (
    SELECT
      patient,
      group_number,
      id AS group_id,
      type_flag AS best_type_flag,
      ROW_NUMBER() OVER (
        PARTITION BY patient, group_number
        ORDER BY type_flag DESC, start ASC, id ASC
      ) AS rn
    FROM clusters
  ),
  -- Map every window encounter to its group representative and class label
  final_groups AS (
    SELECT
      clust.id,
      best.group_id,
      CASE best.best_type_flag
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
      ON best.patient = clust.patient
      AND best.group_number = clust.group_number
      AND best.rn = 1
  ),
  -- Aggregate helper_clinical flags across all window group members
  flags AS (
    SELECT
      final.group_id                    AS stay_id,
      ANY_VALUE(final.encounterclass)   AS encounterclass,
      ANY_VALUE(hc.patient_id)          AS patient_id,
      SUM(hc.num_procedures)            AS num_procedures,
      MAX(hc.num_chronic_conditions)    AS num_chronic_conditions,
      MAX(hc.has_diabetes)              AS has_diabetes,
      MAX(hc.has_cancer)                AS has_cancer,
      MAX(hc.has_hiv)                   AS has_hiv,
      MAX(hc.has_hf)                    AS has_hf,
      MAX(hc.has_alz)                   AS has_alz,
      MAX(hc.has_ckd)                   AS has_ckd,
      MAX(hc.has_lf)                    AS has_lf,
      MAX(hc.is_planned)                AS is_planned,
      MAX(hc.had_surgery)               AS had_surgery,
      MAX(hc.last_surgery_date)         AS last_surgery_date
    FROM final_groups final
    LEFT JOIN {{DATASET_HELPERS}}.helper_clinical hc ON final.id = hc.stay_id
    WHERE final.encounterclass IN ('urgentcare', 'inpatient', 'emergency')
    GROUP BY final.group_id
  ),
  -- Final output: attach main diagnosis flags; exclude groups that continue a prior group
  new_rows AS (
    SELECT
      flag.stay_id                              AS stay_id,
      flag.patient_id                           AS patient_id,
      dict.code                                 AS main_code,
      dict.name                                 AS main_name,
      COALESCE(dict.is_disorder,     0)         AS is_disorder,
      COALESCE(dict.is_symptom,      0)         AS is_symptom,
      COALESCE(dict.inflammation,    0)         AS inflammation,
      COALESCE(dict.musculoskeletal, 0)         AS musculoskeletal,
      COALESCE(dict.nervous,         0)         AS nervous,
      COALESCE(dict.respiratory,     0)         AS respiratory,
      COALESCE(dict.cardiac,         0)         AS cardiac,
      COALESCE(dict.renal,           0)         AS renal,
      COALESCE(dict.trauma,          0)         AS trauma,
      COALESCE(dict.intoxication,    0)         AS intoxication,
      COALESCE(main.num_of_disorders, 0)        AS num_disorders,
      COALESCE(main.num_of_findings,  0)        AS num_findings,
      flag.num_procedures                       AS num_procedures,
      flag.num_chronic_conditions               AS num_chronic_conditions,
      flag.has_diabetes                         AS has_diabetes,
      flag.has_cancer                           AS has_cancer,
      flag.has_hiv                              AS has_hiv,
      flag.has_hf                               AS has_hf,
      flag.has_alz                              AS has_alz,
      flag.has_ckd                              AS has_ckd,
      flag.has_lf                               AS has_lf,
      flag.is_planned                           AS is_planned,
      flag.had_surgery                          AS had_surgery,
      flag.last_surgery_date                    AS last_surgery_date
    FROM flags flag
    LEFT JOIN {{DATASET_HELPERS}}.main_diagnoses main ON flag.stay_id = main.id
    LEFT JOIN {{DATASET_HELPERS}}.diagnoses_dictionary dict ON main.main_diagnosis_code = dict.code
    -- Exclude group_number=0 groups for patients whose first window encounter continues a prior group
    WHERE flag.stay_id NOT IN (
      SELECT best.group_id
      FROM best_stay_per_group best
      JOIN first_group_change_per_patient fgc ON best.patient = fgc.patient
      WHERE best.group_number = 0
        AND fgc.first_group_change = 0
        AND best.rn = 1
    )
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
