from google.cloud import bigquery
from google.oauth2 import service_account

# Define project and dataset information
project_id = "assignment2-113356003-441002"
dataset_id = "bq_assignment2"

credentials = service_account.Credentials.from_service_account_file(
    'assignment2-113356003-441002-3c88df9a7aaa.json')
bq_client = bigquery.Client(project=project_id, credentials=credentials)
# Get the dataset
dataset = bq_client.get_dataset(dataset_id)
# Define the new access entry
access_entry = bigquery.AccessEntry(
    role="READER",  # Equivalent to bigquery.dataViewer
    entity_type="userByEmail",
    entity_id="113356042@g.nccu.edu.tw"
)

# Add the new access entry to the dataset's access entries
entries = list(dataset.access_entries)
entries.append(access_entry)
dataset.access_entries = entries

# Update the dataset with the new access permissions
bq_client.update_dataset(dataset, ["access_entries"])

print(f"Access granted to {access_entry.entity_id} with role {access_entry.role}.")
