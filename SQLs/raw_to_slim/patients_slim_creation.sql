drop table healthcare-test-486920.Raw_csvs_test.patients_slim;

create table healthcare-test-486920.Raw_csvs_test.patients_slim
cluster by id
as
select 
  id, 
  birthdate, 
  deathdate, 
  race, 
  gender 
from healthcare-test-486920.Raw_csvs_test.patients