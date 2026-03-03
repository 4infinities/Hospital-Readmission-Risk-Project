CREATE OR REPLACE TABLE hospital-readmission-4.raw_data_for_dictionaries.unique_procedures
AS
SELECT DISTINCT 
code, 
description as name
FROM hospital-readmission-4.data_slim.procedures_slim
