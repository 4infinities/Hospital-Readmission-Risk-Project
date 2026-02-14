CREATE OR REPLACE TABLE healthcare-test-486920.Raw_csvs_test.unique_procedures
AS
SELECT DISTINCT code, description
FROM healthcare-test-486920.Raw_csvs_test.procedures_slim
