-- main_diagnoses delta: return encounter-group-diagnosis data for current month encounters
-- Feed for DictionaryBuilder: compute main_diagnosis_code and append-only to main_diagnoses
-- Uses current month staging only — previous months already in main_diagnoses; avoids duplication in append-only table
-- Prior group context for boundary detection comes from helper_clinical_grouped + helper_utilization (no full scan)
-- Continuation encounters (gap < 12h to prior group) are mapped to the existing prior group representative
-- Depends on: encounters_{{END_DATE_SAFE}}, claims_{{END_DATE_SAFE}},
--             helper_clinical_grouped, helper_utilization
WITH
  bounds AS (
    SELECT
      LAST_DAY(DATE_TRUNC({{END_DATE}}, MONTH) - INTERVAL 2 MONTH) AS window_start,
      DATE({{END_DATE}}) AS window_end
  ),
  -- Current month encounters only, with type_flag for group representative election
  window_encounters AS (
    SELECT
      id, patient, start, stop,
      CASE encounterclass
        WHEN 'ambulatory' THEN 1
        WHEN 'outpatient' THEN 2
        WHEN 'virtual'    THEN 3
        WHEN 'urgentcare' THEN 4
        WHEN 'emergency'  THEN 5
        WHEN 'inpatient'  THEN 6
        ELSE 99
      END AS type_flag
    FROM {{DATASET_RAW}}.encounters_{{END_DATE_SAFE}}, bounds
    WHERE DATE(start) > bounds.window_start AND DATE(stop) <= bounds.window_end
  ),
  -- Most recent prior group per window patient: representative id + group stop time
  -- hcg at D3 time contains the full prior history (helpers not yet updated for this window)
  -- This gives the last group stop before the current window encounters, used for boundary detection
  prior_group_anchor AS (
    SELECT
      wp.patient,
      hcg.stay_id            AS prior_rep_id,
      MAX(hu.stop)           AS last_prior_stop
    FROM (SELECT DISTINCT patient FROM window_encounters) wp
    JOIN {{DATASET_HELPERS}}.helper_clinical_grouped hcg ON hcg.patient_id = wp.patient
    JOIN {{DATASET_HELPERS}}.helper_utilization      hu  ON hcg.stay_id    = hu.stay_id
    GROUP BY wp.patient, hcg.stay_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY wp.patient ORDER BY MAX(hu.stop) DESC) = 1
  ),
  -- Group boundary detection for window encounters using prior group stop as the left anchor
  group_flags AS (
    SELECT
      we.id, we.patient, we.start, we.stop, we.type_flag,
      CASE
        WHEN DATE_DIFF(
          we.start,
          COALESCE(
            LAG(we.stop) OVER (PARTITION BY we.patient ORDER BY we.start ASC),
            pga.last_prior_stop
          ),
          hour
        ) < 12
        THEN 0
        ELSE 1
      END AS group_change
    FROM window_encounters we
    LEFT JOIN prior_group_anchor pga ON we.patient = pga.patient
  ),
  -- Cumulative group number per patient within the current month
  clusters AS (
    SELECT
      id, patient, start, stop, type_flag,
      SUM(group_change) OVER (
        PARTITION BY patient ORDER BY start ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS group_number
    FROM group_flags
  ),
  -- Detect patients whose very first window encounter continues a prior group
  first_group_change_per_patient AS (
    SELECT patient, group_change AS first_group_change
    FROM group_flags
    QUALIFY ROW_NUMBER() OVER (PARTITION BY patient ORDER BY start ASC) = 1
  ),
  -- Elect the representative encounter for each NEW group (highest type_flag, earliest start)
  best_stay_per_group AS (
    SELECT
      patient,
      group_number,
      id AS group_id,
      ROW_NUMBER() OVER (
        PARTITION BY patient, group_number
        ORDER BY type_flag DESC, start ASC, id ASC
      ) AS rn
    FROM clusters
  ),
  -- Map each window encounter to its group_id:
  --   continuation encounters (group_number=0, first_group_change=0) → prior group representative
  --   new group encounters → window-elected representative
  final_groups AS (
    SELECT
      clust.id,
      CASE
        WHEN clust.group_number = 0 AND fgc.first_group_change = 0
        THEN pga.prior_rep_id
        ELSE best.group_id
      END AS group_id
    FROM clusters clust
    LEFT JOIN best_stay_per_group best
      ON best.patient = clust.patient
     AND best.group_number = clust.group_number
     AND best.rn = 1
    LEFT JOIN first_group_change_per_patient fgc ON fgc.patient = clust.patient
    LEFT JOIN prior_group_anchor              pga ON pga.patient = clust.patient
  ),
  -- Claims for current month encounters; diagnosis columns feed Python's build_main_diagnoses
  claims AS (
    SELECT DISTINCT
      appointmentid AS id,
      diagnosis1, diagnosis2, diagnosis3, diagnosis4,
      diagnosis5, diagnosis6, diagnosis7, diagnosis8
    FROM {{DATASET_RAW}}.claims_{{END_DATE_SAFE}}
  )
SELECT
  final.id,
  final.group_id,
  cl.diagnosis1, cl.diagnosis2, cl.diagnosis3, cl.diagnosis4,
  cl.diagnosis5, cl.diagnosis6, cl.diagnosis7, cl.diagnosis8
FROM final_groups final
LEFT JOIN claims cl ON final.id = cl.id;
