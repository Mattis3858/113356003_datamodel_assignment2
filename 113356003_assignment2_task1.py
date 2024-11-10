from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.oauth2 import service_account

project_id = 'assignment2-113356003-441002'
instance_id = 'studentinstance'
instance_table_id = 'student_data'

credentials = service_account.Credentials.from_service_account_file(
    'assignment2-113356003-441002-3c88df9a7aaa.json')

client = bigtable.Client(project=project_id, admin=True, credentials=credentials)
instance = client.instance(instance_id)

# Create a Bigtable instance if it does not exist
if not instance.exists():
    print(f"Creating instance {instance_id}")
    instance.create()

table = instance.table(instance_table_id)
column_families = {"user_info": None, "activity_log": None}

if table.exists():
    print(f"Table: {instance_table_id} exists. Deleting the table.")
    table.delete()

if not table.exists():
    print(f"""Table: {instance_table_id} is not exist. Creates the table.""")
    table.create(column_families=column_families)

data = [
    {"row_key": "User1", "user_info": "Alice", "activity": "login", "activity_log": "2024-01-01T12:00:00Z"},
    {"row_key": "User2", "user_info": "Bob", "activity": "purchase", "activity_log": "2024-01-02T08:30:00Z"},
    {"row_key": "User3", "user_info": "Cindy", "activity": "login", "activity_log": "2024-01-03T15:45:00Z"},
    {"row_key": "User4", "user_info": "David", "activity": "purchase", "activity_log": "2024-01-05T17:36:00Z"},
]

# Insert data into the table
for entry in data:
    print(entry)
    row = table.direct_row(entry["row_key"])
    row.set_cell("user_info", "user_info", entry["user_info"])
    row.set_cell("activity_log", "activity", entry["activity"])
    row.set_cell("activity_log", "activity_log", entry["activity_log"])
    row.commit()

from google.cloud.bigtable import row_filters

table = instance.table(instance_table_id)

# 定義過濾器來讀取所有行
filter_ = row_filters.PassAllFilter(True)

# 掃描表格並讀取資料
rows = table.read_rows(filter_=filter_)
rows.consume_all()  # 確保抓取所有行

import pandas as pd

# Create a list to store the data
data = []

# Iterate over rows and append data to the list
for row_key, row in rows.rows.items():
    row_data = {"Row Key": row_key.decode("utf-8")}
    for column_family_id, columns in row.cells.items():
        for column_name, cells in columns.items():
            for cell in cells:
                column = f"{column_family_id}:{column_name.decode('utf-8')}"
                row_data[column] = cell.value.decode("utf-8")
    data.append(row_data)

# Convert data to a pandas DataFrame
df = pd.DataFrame(data)
print(f"DataFrame: {df}")

# print("Data inserted successfully.")