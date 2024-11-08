from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

project_id = 'assignment2-113356003-441002'
dataset_id = "bq_assignment2"
table_id = "cloud_storage_external"

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    'assignment2-113356003-441002-3c88df9a7aaa.json')

storage_client = storage.Client(project=project_id, credentials=credentials)

def create_bucket(bucket_name):
    """Creates a new bucket if it does not exist."""
    bucket = storage_client.bucket(bucket_name)

    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

def upload_to_bucket(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to a Cloud Storage bucket."""

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

bucket_name = "assignment2_bucket1"  # Replace with your desired bucket name
source_file_name = "activity_log.csv"  # Replace with your local CSV file path
destination_blob_name = "activity_log.csv"  # File name in the bucket
file_path = "gs://assignment2_bucket1/activity_log.csv" 

# Create the bucket if it does not exist
create_bucket(bucket_name)

# Upload the file to the bucket
upload_to_bucket(bucket_name, source_file_name, destination_blob_name)

bigquery_client = bigquery.Client(project=project_id, credentials=credentials)

#Define the dataset
dataset_ref = bigquery_client.dataset(dataset_id)
# Define the external table configuration with the updated schema
table_ref = dataset_ref.table(table_id)
external_config = bigquery.ExternalConfig("CSV")
external_config.source_uris = [file_path]
external_config.options.skip_leading_rows = 1
external_config.schema = [
    bigquery.SchemaField("user_info", "STRING"),
    bigquery.SchemaField("activity", "STRING"),
    bigquery.SchemaField("activity_log", "TIMESTAMP"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("device_type", "STRING"),
]

# Create the external table with the updated schema
table = bigquery.Table(table_ref)
table.external_data_configuration = external_config

table = bigquery_client.create_table(table)  # Recreate the table with the new schema
print(f"Created external table {table_id} linked to {file_path}.")
