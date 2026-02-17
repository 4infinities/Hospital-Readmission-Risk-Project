
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

sql_query = "select patientid, patientsex, implementationversionname from `chc-nih-chest-xray.nih_chest_xray.nih_chest_xray` limit 10"

query_result = client.query(sql_query)

df = pd.DataFrame(query_result)

print(df.head())
print(df.info())