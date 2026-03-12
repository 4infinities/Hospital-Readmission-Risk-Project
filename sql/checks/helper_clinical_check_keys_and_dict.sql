WITH
  nullz AS (
    SELECT
      COUNTIF(stay_id IS NULL) AS null_stay_id_count,
      COUNTIF(main_code IS NULL) AS null_main_code_count
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical
  ),
  missing_dict AS (
    SELECT
      COUNT(*) AS missing_dict_rows
    FROM {{DATASET_HELPERS}}.{{PROFILE}}helper_clinical hc
    WHERE main_code IS NOT NULL
      AND main_name IS NULL
  )
SELECT
  nullz.null_stay_id_count,
  nullz.null_main_code_count,
  missing_dict.missing_dict_rows
FROM nullz
CROSS JOIN missing_dict;
