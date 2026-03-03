CREATE OR REPLACE TABLE hospital-readmission-4.data_slim.organizations_slim
AS
SELECT
  org.id,
  org.name,
  org.utilization
FROM hospital-readmission-4.raw_data.organizations org
JOIN hospital-readmission-4.data_slim.encounters_slim e
  ON org.id = e.organization
