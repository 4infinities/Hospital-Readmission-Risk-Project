-- Feed query for DictionaryBuilder (main_diagnoses): for every encounter, determine its group_id and attach all diagnosis codes from claims
-- Used by DictionaryBuilder.build_main_diagnoses to identify the representative (main) diagnosis for each encounter group
WITH
-- Assign a type_flag rank to each encounter class and detect group boundaries (< 12h gap = same group)
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
      WHEN date_diff(
        start,
        lag(stop, 1) OVER (PARTITION BY patient ORDER BY start ASC),
        hour
      ) < 12
      THEN 0
      ELSE 1
    END AS group_change
  FROM {{DATASET_SLIM}}.encounters_slim
  where stop <= {{END_DATE}}
),
-- Cumulative sum of group_change gives each encounter a monotonically increasing group number per patient
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
-- Within each group, elect the representative encounter (highest type_flag, then earliest start)
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
-- Map every individual encounter to the group_id of its representative encounter
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
-- Flatten claims to one row per encounter with all 8 diagnosis columns
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
  where currentillnessdate <= {{END_DATE}}
)
-- Final output: each encounter with its group_id and all raw diagnosis codes
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