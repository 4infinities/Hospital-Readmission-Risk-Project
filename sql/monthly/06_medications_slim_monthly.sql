-- Monthly slim: create isolated medications slim table for this month
-- DDL-only (no DML): CREATE OR REPLACE TABLE creates a standalone per-month table
-- Source: monthly raw staging table medications_{{END_DATE_SAFE}}
-- JOIN to encounters_slim_{{END_DATE_SAFE}} scopes to valid encounters for this month only
CREATE OR REPLACE TABLE {{DATASET_RAW}}.medications_slim_{{END_DATE_SAFE}}
AS
SELECT
  m.start,
  m.stop,
  m.encounter,
  m.code,
  m.description,
  m.base_cost,
  m.dispenses,
  m.totalcost
FROM {{DATASET_RAW}}.medications_{{END_DATE_SAFE}} m
JOIN {{DATASET_RAW}}.encounters_slim_{{END_DATE_SAFE}} e
  ON m.encounter = e.id
