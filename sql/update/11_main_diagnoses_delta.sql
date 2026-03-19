-- main_diagnoses delta: return encounter-group-diagnosis data for encounters in the new window
-- Feed for DictionaryBuilder: compute main_diagnosis_code and append-only to main_diagnoses
-- Full patient history used for correct group boundary detection; output scoped to new window encounters
-- Depends on: encounters_slim, claims_slim
WITH
-- Full history up to END_DATE required for correct cumulative group_number assignment
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
      WHEN date_diff(
        start,
        lag(stop, 1) OVER (PARTITION BY patient ORDER BY start ASC),
        hour
      ) < 12
      THEN 0
      ELSE 1
    END AS group_change
  FROM {{DATASET_SLIM}}.encounters_slim
  WHERE stop <= {{END_DATE}}
),
clusters AS (
  SELECT
    id,
    patient,
    start,
    type_flag,
    sum(group_change) OVER (
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
    ROW_NUMBER() OVER (
      PARTITION BY patient, group_number
      ORDER BY
        type_flag DESC,
        start ASC,
        id ASC
    ) AS rn
  FROM clusters
),
final_groups AS (
  SELECT
    clust.id,
    best.group_id,
  FROM clusters clust
  LEFT JOIN best_stay_per_group best
    ON best.patient = clust.patient
   AND best.group_number = clust.group_number
   AND best.rn = 1
),
claims AS (
  SELECT DISTINCT
    encounter AS id,
    diagnosis1,
    diagnosis2,
    diagnosis3,
    diagnosis4,
    diagnosis5,
    diagnosis6,
    diagnosis7,
    diagnosis8
  FROM {{DATASET_SLIM}}.claims_slim
  WHERE currentillnessdate > {{START_DATE}} AND currentillnessdate <= {{END_DATE}}
)
-- Output only encounters in the new window; full history above is only for group boundary accuracy
SELECT
  final.id,
  final.group_id,
  cl.diagnosis1,
  cl.diagnosis2,
  cl.diagnosis3,
  cl.diagnosis4,
  cl.diagnosis5,
  cl.diagnosis6,
  cl.diagnosis7,
  cl.diagnosis8
FROM final_groups final
LEFT JOIN claims cl
  ON final.id = cl.id
WHERE final.id IN (
  SELECT id FROM {{DATASET_SLIM}}.encounters_slim
  WHERE start > {{START_DATE}} AND stop <= {{END_DATE}}
);
