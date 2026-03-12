WITH
  flags AS (
    SELECT
      COUNT(*) AS total_rows,
      COUNTIF(has_diabetes NOT IN (0,1) OR has_diabetes IS NULL) AS bad_has_diabetes,
      COUNTIF(has_cancer NOT IN (0,1) OR has_cancer IS NULL) AS bad_has_cancer,
      COUNTIF(has_hiv NOT IN (0,1) OR has_hiv IS NULL) AS bad_has_hiv,
      COUNTIF(has_hf NOT IN (0,1) OR has_hf IS NULL) AS bad_has_hf,
      COUNTIF(has_alz NOT IN (0,1) OR has_alz IS NULL) AS bad_has_alz,
      COUNTIF(has_ckd NOT IN (0,1) OR has_ckd IS NULL) AS bad_has_ckd,
      COUNTIF(has_lf NOT IN (0,1) OR has_lf IS NULL) AS bad_has_lf,
      COUNTIF(is_planned NOT IN (0,1) OR is_planned IS NULL) AS bad_is_planned,
      COUNTIF(had_surgery NOT IN (0,1) OR had_surgery IS NULL) AS bad_had_surgery
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical
  ),
  counts AS (
    SELECT
      COUNTIF(num_chronic_conditions < 0) AS negative_num_chronic,
      COUNTIF(num_procedures < 0) AS negative_num_procedures,
      COUNTIF(num_chronic_conditions > 0) AS rows_with_chronic,
      COUNTIF(num_procedures > 0) AS rows_with_procedures
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical
  )
SELECT
  flags.total_rows,
  flags.bad_has_diabetes,
  flags.bad_has_cancer,
  flags.bad_has_hiv,
  flags.bad_has_hf,
  flags.bad_has_alz,
  flags.bad_has_ckd,
  flags.bad_has_lf,
  flags.bad_is_planned,
  flags.bad_had_surgery,
  counts.negative_num_chronic,
  counts.negative_num_procedures,
  counts.rows_with_chronic,
  counts.rows_with_procedures
FROM flags
CROSS JOIN counts;

