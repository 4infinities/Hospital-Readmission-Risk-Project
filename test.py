from google.cloud import bigquery

client = bigquery.Client()

print(client.project)