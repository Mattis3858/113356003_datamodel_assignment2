from google.cloud import bigquery
from google.oauth2 import service_account

# Define project and dataset information
project_id = "assignment2-113356003-441002"
dataset_id = "bq_assignment2"

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    'assignment2-113356003-441002-3c88df9a7aaa.json')
bq_client = bigquery.Client(project=project_id, credentials=credentials)

# Write the SQL query to join Bigtable and Cloud Storage external tables
# Query to retrieve the schema and column names for the bigtable_external table
query = """
    SELECT
        bt.user_info.user_name.`cell`[SAFE_OFFSET(0)].value AS user_name,
        bt.activity_log.activity_type.`cell`[SAFE_OFFSET(0)].value AS activity_type,
        bt.activity_log.activity_time.`cell`[SAFE_OFFSET(0)].value AS activity_timestamp,
    FROM
        `assignment2-113356003-441002.bq_assignment2.bigtable_external` AS bt
    INNER JOIN
        `assignment2-113356003-441002.bq_assignment2.cloud_storage_external` AS cs
    ON
        bt.user_info.user_name.`cell`[SAFE_OFFSET(0)].value= cs.user_info
    WHERE
        bt.activity_log.activity_type.`cell`[SAFE_OFFSET(0)].value = 'login'
    ORDER BY
        bt.activity_log.activity_time.`cell`[SAFE_OFFSET(0)].value DESC

"""

# Execute the query and convert results to a DataFrame
query_job = bq_client.query(query)
task4_results = query_job.result().to_dataframe()

# Display the results
print("Task 4: Query Results")
print(task4_results)
