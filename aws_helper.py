import boto3
import pandas as pd
import os
from tqdm import tqdm

def list_all_s3_files(bucket_name, aws_access_key_id, aws_secret_access_key, aws_region):
    # Create an S3 client with provided credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    
    # List all objects in the bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            files.append(obj['Key'])
    
    return files

def download_and_read_csv_from_s3(bucket_name, file_key, aws_access_key_id, aws_secret_access_key, aws_region):
    # Create an S3 client with provided credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    
    # Ensure the local directory exists
    local_dir = './data'
    os.makedirs(local_dir, exist_ok=True)
    local_file_path = f'{local_dir}/{file_key}'

    # Get the file size for the progress bar
    head_response = s3_client.head_object(Bucket=bucket_name, Key=file_key)
    file_size = head_response['ContentLength']

    # Download with a progress bar
    with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Downloading {file_key}") as pbar:
        def progress_callback(bytes_amount):
            pbar.update(bytes_amount)

        s3_client.download_file(bucket_name, file_key, local_file_path, Callback=progress_callback)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(local_file_path, low_memory=False)
    return df

# SQL Table structure column names
sql_columns = [
    'ride_id',
    'rideable_type',
    'started_at',
    'ended_at',
    'start_station_name',
    'start_station_id',
    'end_station_name',
    'end_station_id',
    'start_lat',
    'start_lng',
    'end_lat',
    'end_lng',
    'member_casual'
]