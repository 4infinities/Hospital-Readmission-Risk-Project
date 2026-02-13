create or replace table healthcare-test-486920.Raw_csvs_test.claims_slim
cluster by encounter
as
Select
  cl.appointmentid as encounter,
  cl.patientid, 
  cl.diagnosis1, 
   cl.diagnosis2, 
    cl.diagnosis3, 
     cl.diagnosis4, 
      cl.diagnosis5, 
       cl.diagnosis6, 
        cl.diagnosis7,
         cl.diagnosis8,  
  cl.currentillnessdate, 
from healthcare-test-486920.Raw_csvs_test.claims cl
join healthcare-test-486920.Raw_csvs_test.encounters_slim e
on cl.appointmentid = e.id
