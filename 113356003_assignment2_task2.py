from google.cloud import bigquery
from google.oauth2 import service_account
import base64

# Set up BigQuery client
project_id = "assignment2-113356003-441002"
instance_id = 'studentinstance'
dataset_id = "bq_assignment2"
table_id = "bigtable_external"
data_id = 'student_data'

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    'assignment2-113356003-441002-3c88df9a7aaa.json')

client = bigquery.Client(project=project_id, credentials=credentials)

source_uri = f"https://googleapis.com/bigtable/projects/{project_id}/instances/{instance_id}/tables/{data_id}"


# Define the external table configuration
external_config = bigquery.ExternalConfig("BIGTABLE")
external_config.source_uris = [source_uri]

# Define the 'user_info' column family and columns
## Ｃolumn 1
user_info_family_column_1 = bigquery.BigtableColumn()
user_info_family_column_1.qualifier_encoded = base64.b64encode(b"user_info").decode("utf-8")
user_info_family_column_1.field_name = "user_name"
user_info_family_column_1.type_ = "STRING"

## Define the column families
user_info_family = bigquery.BigtableColumnFamily()
user_info_family.family_id = "user_info"
user_info_family.columns = [user_info_family_column_1]

# Define the 'activity_log' column family and columns
## Ｃolumn 1
activity_log_family_column1 = bigquery.BigtableColumn()
activity_log_family_column1.qualifier_encoded = base64.b64encode(b"activity").decode(
    "utf-8"
)
activity_log_family_column1.field_name = "activity_type"
activity_log_family_column1.type_ = "STRING"

## Ｃolumn 2
activity_log_family_column2 = bigquery.BigtableColumn()
activity_log_family_column2.qualifier_encoded = base64.b64encode(b"activity_log").decode(
    "utf-8"
)
activity_log_family_column2.field_name = "activity_time"
activity_log_family_column2.type_ = "STRING"

## Define the column families
activity_log_family = bigquery.BigtableColumnFamily()
activity_log_family.family_id = "activity_log"
activity_log_family.columns = [activity_log_family_column1, activity_log_family_column2]

# Add the column families to the external configuration
external_config.bigtable_options.column_families = [
    user_info_family,
    activity_log_family,
]

external_table_name = "bigtable_external"

# Create a table reference
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(external_table_name)
table = bigquery.Table(table_ref)
table.external_data_configuration = external_config

# Create the table in BigQuery
client.create_table(table, exists_ok=True)

print(f"External table '{external_table_name}' created successfully!")
# Run a sample query on the external table
# query = f"SELECT * FROM `{project_id}.{dataset_id}.bigtable_external`"
# query_job = client.query(query)
# task2_results = query_job.result()

# print("Query on external table executed successfully.")