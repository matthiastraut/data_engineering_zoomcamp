from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import requests
from google.cloud import storage

# To be set in the Airflow Environment Variables or via .env
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET", "bucket-name")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-id")
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)] # Jan to June 2024

def download_file(month: str):
    """Downloads parquet file from NYC TLC website to /tmp."""
    file_name = f"green_tripdata_2024-{month}.parquet"
    url = f"{URL_PREFIX}{month}.parquet"
    local_path = f"/tmp/{file_name}"
    
    print(f"Downloading {url}...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"Successfully downloaded to {local_path}")
    else:
        raise Exception(f"Failed to download {url}. Status code: {response.status_code}")

def upload_to_gcs(month: str, bucket_name: str):
    """Uploads file to GCS using the storage client directly."""
    file_name = f"green_tripdata_2024-{month}.parquet"
    local_path = f"/tmp/{file_name}"
    gcs_path = f"green_taxi_2024/{file_name}"
    
    # Use service account if available, else default credentials
    key_path = "/opt/airflow/gcp.json"
    if os.path.exists(key_path):
        client = storage.Client.from_service_account_json(key_path)
    else:
        client = storage.Client()
        
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    
    print(f"Uploading {local_path} to gs://{bucket_name}/{gcs_path}...")
    blob.upload_from_filename(local_path, timeout=600) # 10 minute timeout
    print("Upload complete.")

def cleanup_file(month: str):
    """Removes the local file after upload."""
    local_path = f"/tmp/green_tripdata_2024-{month}.parquet"
    if os.path.exists(local_path):
        os.remove(local_path)
        print(f"Cleaned up {local_path}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id="hw3_green_taxi_to_gcs",
    default_args=default_args,
    schedule="@once",
    catchup=False,
    description="Loads Jan-Jun 2024 Green Taxi Parquet files to GCS for HW3",
    tags=['zoomcamp', 'hw3'],
) as dag:

    for month in MONTHS:
        file_name = f"green_tripdata_2024-{month}.parquet"
        
        # 1. Download from source
        download_task = PythonOperator(
            task_id=f"download_taxi_data_{month}",
            python_callable=download_file,
            op_kwargs={'month': month},
        )

        # 2. Upload to GCS
        upload_task = PythonOperator(
            task_id=f"upload_to_gcs_{month}",
            python_callable=upload_to_gcs,
            op_kwargs={
                'month': month,
                'bucket_name': BUCKET_NAME
            },
        )

        # 3. Cleanup local storage
        cleanup_task = PythonOperator(
            task_id=f"cleanup_local_{month}",
            python_callable=cleanup_file,
            op_kwargs={'month': month},
        )

        download_task >> upload_task >> cleanup_task
