create or replace table healthcare-test-486920.Raw_csvs_test.organizations_slim
as
Select 
  org.id, 
  org.name, 
  org.utilization
from healthcare-test-486920.Raw_csvs_test.organizations org
join healthcare-test-486920.Raw_csvs_test.encounters_slim e
on org.id = e.organization