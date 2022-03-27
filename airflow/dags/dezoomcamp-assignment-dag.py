import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from google.cloud import storage

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dtc-de")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_airflow_test")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'dezoomcamp')

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
dataset_zip_file_path = f"{AIRFLOW_HOME}/IncidentLevelSTATA.zip"
dataset_unzip_file_path = f"{AIRFLOW_HOME}/IncidentLevelSTATA/IncidentLevelSTATA/STATA/"
dataset_url = 'https://dasil.grinnell.edu/DataRepository/NIBRS/IncidentLevelSTATA.zip'
dataset_file = 'None'
default_args = {
    "owner": "airflow",
    # "depends_on_past": False,
    "retries": 1,
    "start_date": days_ago(0),
}


def convert_file_to_parquet():
    for file_path in os.listdir(dataset_unzip_file_path):
        if file_path.endswith('.dta'):
            ## Excluding 2010 and 2013 file becuase of inconsistency in the data columns.
            if ("2010" in file_path or "2013" in file_path):
                print("Skipping file ", file_path)
                continue
            print("About loading dta file to memory")
            chunksize = 8 * (10 ** 5)
            count = 1
            with (pd.io.stata.read_stata(f'{dataset_unzip_file_path}{file_path}', chunksize=chunksize)) as data:
                for chunk in data:
                    pq_file_path = file_path.replace('.dta', f'-{count}.parquet')
                    print("About writing dta file to parquet ", count)
                    chunk.to_parquet(f'{dataset_unzip_file_path}{pq_file_path}')
                    count = count + 1

def create_external_table_from_parquets():
    files_list = list()
    client = storage.Client()
    for blob in client.list_blobs(BUCKET, prefix='incidents'):
        print(str(blob))
        files_list.append(blob.name)
    print(f'Writing to external table with parquets => {files_list}')
    bgopt = BigQueryCreateExternalTableOperator(
        task_id=f"create_incident_external_table",
        bucket=BUCKET,
        source_objects=files_list, #pass a list
        destination_project_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.incidents",
        source_format='PARQUET', #use source_format instead of file_format
    )

    bgopt.execute({})

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket_name, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def upload_parquet_file_to_gcs():
    for file_path in os.listdir(dataset_unzip_file_path):
        if file_path.endswith('.parquet'):
            file_name = f'{dataset_unzip_file_path}{file_path}'
            bucket_path = f'incidents/{file_path}'
            upload_to_gcs(BUCKET, bucket_path, file_name)


with DAG(
    dag_id="dezoomcamp-assignment",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS -k {dataset_url} > {dataset_zip_file_path}"
    )

    unzip_file = BashOperator(
        task_id = 'unzip_file',
        bash_command=f"unzip -q {dataset_zip_file_path} -d {AIRFLOW_HOME}"
    )

    convert_unzipped_files_to_parquets = PythonOperator(
        task_id = 'convert_dat_to_parquet',
        python_callable=convert_file_to_parquet,
        op_kwargs={
            
        }
    )

    upload_parquets_to_gcs = PythonOperator(
        task_id="upload_parquet_file_to_gcs",
            python_callable=upload_parquet_file_to_gcs,
            op_kwargs={},
    )

    create_incident_external_table = PythonOperator(
        task_id=f"create_incident_external_table",
        python_callable=create_external_table_from_parquets,
        op_kwargs={}
    )

download_dataset_task >> unzip_file >> convert_unzipped_files_to_parquets >> upload_parquets_to_gcs >> create_incident_external_table