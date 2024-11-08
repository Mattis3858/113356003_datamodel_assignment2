from google.cloud import bigquery
from google.oauth2 import service_account

# Define project and dataset information
project_id = "assignment2-113356003-441002"
dataset_id = "bq_assignment2"

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    'assignment2-113356003-441002-3c88df9a7aaa.json')
bq_client = bigquery.Client(project=project_id, credentials=credentials)
# Define the SQL query
query = """
    WITH login_counts AS (
        SELECT
            user_name,
            COUNT(*) AS login_count
        FROM (
            SELECT
                bt.user_info.user_name.`cell`[SAFE_OFFSET(0)].value AS user_name
            FROM
                `assignment2-113356003-441002.bq_assignment2.bigtable_external` AS bt
            INNER JOIN
                `assignment2-113356003-441002.bq_assignment2.cloud_storage_external` AS cs
            ON
                bt.user_info.user_name.`cell`[SAFE_OFFSET(0)].value = cs.user_info
            WHERE
                bt.activity_log.activity_type.`cell`[SAFE_OFFSET(0)].value = 'login'
        )
        GROUP BY
            user_name
    )
    SELECT
        user_name,
        login_count,
        RANK() OVER (ORDER BY login_count DESC) AS rank
    FROM
        login_counts
    ORDER BY
        rank
    LIMIT 2
"""

# Execute the query and convert the result to a DataFrame
df = bq_client.query(query).to_dataframe()

# Display the results
print(df)