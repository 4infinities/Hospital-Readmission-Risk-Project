-- Recompute 365-day history for a small sample and compare
WITH encounters_pure AS (
  SELECT
    e.id,
    e.patient,
    e.start,
    e.stop
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  WHERE e.encounterclass IN ('urgentcare','emergency','inpatient')
),
encounters_inpatient AS (
  SELECT
    e.id,
    e.patient,
    e.start,
    e.stop,
    DATE_DIFF(e.stop, e.start, DAY) AS length_of_stay
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  WHERE e.encounterclass = 'inpatient'
),
pairwise AS (
  SELECT
    pure.id AS stay_id,
    pure.patient,
    pure.start AS index_start,
    inp.id   AS prev_inp_id,
    inp.stop AS prev_inp_stop,
    DATE_DIFF(pure.start, inp.stop, DAY) AS days_since_prev_inp,
    inp.length_of_stay
  FROM encounters_pure pure
  LEFT JOIN encounters_inpatient inp
    ON pure.patient = inp.patient
   AND inp.stop < pure.start
)
SELECT
  u.stay_id,
  u.admissions_365d,
  u.tot_length_of_stay_365d,
  p.recalc_adm_365d,
  p.recalc_los_365d
FROM `healthcare-test-486920.Raw_csvs_test.helper_utilization` u
JOIN (
  SELECT
    stay_id,
    COUNTIF(days_since_prev_inp BETWEEN 0 AND 365) AS recalc_adm_365d,
    SUM(IF(days_since_prev_inp BETWEEN 0 AND 365, length_of_stay, 0)) AS recalc_los_365d
  FROM pairwise
  GROUP BY stay_id
) p
ON u.stay_id = p.stay_id
WHERE u.admissions_365d != p.recalc_adm_365d
   OR u.tot_length_of_stay_365d != p.recalc_los_365d
LIMIT 50;

-- recompute avg_cost_of_prev_stays from pairwise + helper_cost_aggregation

WITH encounters_pure AS (
  SELECT
    e.id,
    e.patient,
    e.start
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  WHERE e.encounterclass IN ('urgentcare','emergency','inpatient')
),
encounters_inpatient AS (
  SELECT
    e.id,
    e.patient,
    e.start,
    e.stop
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  WHERE e.encounterclass = 'inpatient'
),
pairwise AS (
  SELECT
    pure.id AS stay_id,
    DATE_DIFF(pure.start, inp.stop, DAY) AS days_since_prev_inp,
    hc.total_stay_cost AS prev_stay_cost
  FROM encounters_pure pure
  LEFT JOIN encounters_inpatient inp
    ON pure.patient = inp.patient
   AND inp.stop < pure.start
  LEFT JOIN `healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation` hc
    ON inp.id = hc.stay_id
),
recalc AS (
  SELECT
    stay_id,
    ROUND(AVG(IF(days_since_prev_inp BETWEEN 0 AND 365, prev_stay_cost, NULL)), 2) AS recalc_avg_prev_cost
  FROM pairwise
  GROUP BY stay_id
)
SELECT
  u.stay_id,
  u.avg_cost_of_prev_stays,
  r.recalc_avg_prev_cost
FROM `healthcare-test-486920.Raw_csvs_test.helper_utilization` u
JOIN recalc r USING (stay_id)
WHERE u.avg_cost_of_prev_stays != r.recalc_avg_prev_cost;



-- days_to_readmit should match difference between following_stay_date and index stop
WITH idx AS (
  SELECT
    e.id AS stay_id,
    e.stop AS index_stop
  FROM `healthcare-test-486920.Raw_csvs_test.encounters_slim` e
  WHERE e.encounterclass IN ('urgentcare','emergency','inpatient')
)
SELECT
  u.stay_id,
  u.following_stay_id,
  u.following_stay_date,
  i.index_stop,
  u.days_to_readmit,
  DATE_DIFF(u.following_stay_date, i.index_stop, DAY) AS recomputed_days
FROM `healthcare-test-486920.Raw_csvs_test.helper_utilization` u
JOIN idx i USING (stay_id)
WHERE u.following_stay_id IS NOT NULL
  AND u.days_to_readmit != DATE_DIFF(u.following_stay_date, i.index_stop, DAY)
LIMIT 50;

-- following_unplanned_admission_flag coherent with helper_clinical.is_planned
SELECT
  u.stay_id,
  u.following_stay_id,
  u.following_unplanned_admission_flag,
  hc.is_planned
FROM `healthcare-test-486920.Raw_csvs_test.helper_utilization` u
LEFT JOIN `healthcare-test-486920.Raw_csvs_test.helper_clinical` hc
  ON hc.stay_id = u.following_stay_id
WHERE u.following_stay_id IS NOT NULL
  AND u.following_unplanned_admission_flag != (1 - hc.is_planned)
LIMIT 50;


