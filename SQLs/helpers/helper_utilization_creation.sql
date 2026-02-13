create or replace table healthcare-test-486920.Raw_csvs_test.helper_utilization as
with encounters_pure as
(
  select
  e.id,
  e.patient,
  e.start,
  e.stop
from healthcare-test-486920.Raw_csvs_test.encounters_slim e
WHERE e.encounterclass IN ('urgentcare', 'emergency', 'inpatient')
),

encounters_inpatient as
(
select
  e.id,
  e.patient,
  e.start,
  e.stop,
  date_diff(e.stop, e.start, day) as length_of_stay
from healthcare-test-486920.Raw_csvs_test.encounters_slim e
where e.encounterclass = 'inpatient'
),

pairwise AS (
  SELECT
    pure.id AS stay_id,
    pure.patient,
    pure.start AS index_start,
    inp.id   AS prev_inp_id,
    inp.stop AS prev_inp_stop,
    DATE_DIFF(pure.start, inp.stop, DAY) AS days_since_prev_inp,
    inp.length_of_stay,
    help_cost.total_stay_cost as prev_stay_cost,
    row_number() over(partition by pure.id order by inp.stop desc) as rn_prev
  FROM encounters_pure pure
  LEFT JOIN encounters_inpatient inp
    ON pure.patient = inp.patient
   AND inp.stop < pure.start
   left join healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation help_cost
   on inp.id = help_cost.stay_id
),

prev_data as
(
SELECT
  pair.stay_id,
  COUNTIF(pair.days_since_prev_inp BETWEEN 0 AND 365) AS admissions_365d,
  SUM(if (pair.days_since_prev_inp BETWEEN 0 AND 365, pair.length_of_stay, 0)) AS tot_length_of_stay_365d,
  round(avg(if (pair.days_since_prev_inp BETWEEN 0 AND 365, pair.prev_stay_cost, null)),2) as avg_cost_of_prev_stays,
  MAX(IF(pair.rn_prev = 1, pair.prev_inp_id, NULL))  AS prev_stay_id,
  MAX(IF(pair.rn_prev = 1, pair.prev_inp_stop, NULL)) AS prev_stay_date
FROM pairwise pair
GROUP BY stay_id
),

pairwise_follow as
(
  select
  pure.id AS stay_id,
  pure.patient,
  pure.stop AS index_stop,
  inp.id   AS fol_inp_id,
  inp.start AS fol_inp_start,
  DATE_DIFF(inp.start, pure.stop, DAY) AS days_to_readmit,
  row_number() over(partition by pure.id order by inp.start asc) as rn_fol
  FROM encounters_pure pure
  LEFT JOIN encounters_inpatient inp
    ON pure.patient = inp.patient
    AND inp.start > pure.stop
),

follow_data as
(
select
  pair.stay_id,
  max(if(pair.rn_fol = 1, pair.fol_inp_start, null)) as following_stay_date,
  max(if(pair.rn_fol = 1, pair.fol_inp_id, null)) as following_stay_id,
  max(if(pair.rn_fol = 1, days_to_readmit, null)) as days_to_readmit,
  case when max(if(pair.rn_fol = 1, days_to_readmit, null)) <= 30
  then 1 else 0 end readmit_30d,
  case when max(if(pair.rn_fol = 1, days_to_readmit, null)) <= 90
  then 1 else 0 end readmit_90d,
from pairwise_follow pair
group by stay_id
)

select
  pre.stay_id,
  pre.admissions_365d,
  pre.tot_length_of_stay_365d,
  pre.avg_cost_of_prev_stays,
  pre.prev_stay_id,
  pre.prev_stay_date,  
  fol.following_stay_id,
  fol.following_stay_date,
  fol.days_to_readmit,
  fol.readmit_30d,
  fol.readmit_90d,
  help_cost.total_stay_cost,
  1 - help_clin.is_planned as following_unplanned_admission_flag
  from prev_data pre
  left join follow_data fol
  on pre.stay_id = fol.stay_id
  left join healthcare-test-486920.Raw_csvs_test.helper_clinical help_clin
  on help_clin.stay_id = fol.following_stay_id
  left join `healthcare-test-486920.Raw_csvs_test.helper_cost_aggregation` help_cost
  on help_cost.stay_id = fol.following_stay_id



