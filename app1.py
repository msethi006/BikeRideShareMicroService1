from aws_helper import list_all_s3_files, download_and_read_csv_from_s3, sql_columns
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine
import json
with open("config.json", "r") as file:
    config = json.load(file)

bucket_name = config["bucket_name"]
aws_access_key_id = config["aws_access_key_id"]
aws_secret_access_key = config["aws_secret_access_key"]
aws_region = config["aws_region"]

files = list_all_s3_files(bucket_name, aws_access_key_id, aws_secret_access_key, aws_region)
files = sorted(files, reverse=True)
print("Files in bucket:")
for file in files:
    print(file)

file = '2020_03_01.csv'
df = download_and_read_csv_from_s3(bucket_name, file, aws_access_key_id, aws_secret_access_key, aws_region)
df_columns = df.columns.tolist()
missing_in_df = set(sql_columns) - set(df_columns)
extra_in_df = set(df_columns) - set(sql_columns)

if not missing_in_df and not extra_in_df:
    print(f"{file} All columns match the SQL structure!")
    df['started_at'] = pd.to_datetime(df['started_at'])
    df['ended_at'] = pd.to_datetime(df['ended_at'])
    username = config["username"]
    password = config["password"]
    host = config["host"]
    port = config["port"]  
    database = config["database"]

    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"

    engine = create_engine(connection_string)
    #df.to_sql('citibike_trips', con=engine, if_exists='append', index=False)
    selected_indices = [2, 5, 8]  
    filtered_df = df.loc[selected_indices]

    table = config["table"]
    filtered_df.to_sql(table, con=engine, if_exists='append', index=False)

else:
    if missing_in_df:
        print(f"{file} Missing columns in DataFrame:", missing_in_df)
    if extra_in_df:
        print(f"{file} Extra columns in DataFrame:", extra_in_df)