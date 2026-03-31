-- Monthly slim: create isolated careplans slim table for this month
-- DDL-only (no DML): CREATE OR REPLACE TABLE creates a standalone per-month table
-- Source: monthly raw staging table careplans_{{END_DATE_SAFE}}
-- JOIN to encounters_slim_{{END_DATE_SAFE}} scopes to valid encounters for this month only
CREATE OR REPLACE TABLE {{DATASET_RAW}}.careplans_slim_{{END_DATE_SAFE}}
AS
SELECT
  care.Start,
  care.stop,
  care.patient,
  care.encounter,
  care.description,
  care.reasoncode AS code
FROM {{DATASET_RAW}}.careplans_{{END_DATE_SAFE}} care
JOIN {{DATASET_RAW}}.encounters_slim_{{END_DATE_SAFE}} e
  ON care.encounter = e.id
