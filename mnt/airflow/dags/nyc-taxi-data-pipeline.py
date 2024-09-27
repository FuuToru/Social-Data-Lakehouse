import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from minio import Minio

# Default args for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@host.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the file name and dataset URL for each month
dataset_file = "green_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
dataset_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
path_to_local_home = "/opt/airflow/dags/files"

# MinIO connection details
minio_client = Minio(
    "minio:9000", access_key="minio", secret_key="minio123", secure=False
)


# Function to upload files to MinIO
def upload_to_minio(filename, bucket_name, object_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    minio_client.fput_object(bucket_name, object_name, filename)
    print(
        f"Successfully uploaded {filename} to MinIO bucket {bucket_name} as {object_name}"
    )


# Define the DAG
with DAG(
    dag_id="nyc_taxi",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
) as dag:

    # Task to download the dataset
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -o {path_to_local_home}/{dataset_file} {dataset_url}",
    )

    # Task to upload the dataset to MinIO
    load_to_minio_task = PythonOperator(
        task_id="load_to_minio_task",
        python_callable=upload_to_minio,
        op_kwargs={
            "filename": f"{path_to_local_home}/{dataset_file}",
            "bucket_name": "lakehouse",
            "object_name": "raw/{{ logical_date.strftime('%Y') }}/{{ logical_date.strftime('%m') }}/"
            + dataset_file,
        },
    )

    # Task to upload taxi_zone_lookup.csv to MinIO
    load_csv_to_minio_task = PythonOperator(
        task_id="load_csv_to_minio_task",
        python_callable=upload_to_minio,
        op_kwargs={
            "filename": "/opt/airflow/dags/files/taxi_zone_lookup.csv",
            "bucket_name": "lakehouse",
            "object_name": "raw/taxi_zone_lookup.csv",
        },
    )

    # Spark task to process data from bronze to silver
    bronze_to_silver_task = SparkSubmitOperator(
        task_id="bronze_to_silver_task",
        application="/opt/airflow/dags/scripts/process_bronze_to_silver.py",
        name="bronze_to_silver_processing",
        conn_id="spark_conn",
        verbose=True,
        conf={
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.warehouse.dir": "s3a://lakehouse/",
        },
        application_args=[
            "--year",
            "{{ logical_date.strftime('%Y') }}",
            "--month",
            "{{ logical_date.strftime('%m') }}",
        ],
    )

    # Spark task to validate the silver data
    validate_silver_task = SparkSubmitOperator(
        task_id="validate_silver_task",
        application="/opt/airflow/dags/scripts/validate_silver.py",
        name="validate_silver_processing",
        conn_id="spark_conn",
        verbose=True,
        application_args=[
            "--year",
            "{{ logical_date.year }}",
            "--month",
            "{{ logical_date.month }}",
        ],
    )

    # Spark task to append data to fact_trip in the gold layer
    fact_trip_task = SparkSubmitOperator(
        task_id="fact_trip_task",
        application="/opt/airflow/dags/scripts/fact_trip.py",
        name="fact_trip",
        conn_id="spark_conn",
        verbose=True,
        application_args=[
            "--year",
            "{{ logical_date.year }}",
            "--month",
            "{{ logical_date.month }}",
        ],
    )

    # Spark tasks to create dimension tables
    dim_weekday_task = SparkSubmitOperator(
        task_id="dim_weekday_task",
        application="/opt/airflow/dags/scripts/dim_weekday.py",
        conn_id="spark_conn",
        verbose=True,
        name="dim_weekday_task",
    )

    dim_paymenttype_task = SparkSubmitOperator(
        task_id="dim_paymenttype_task",
        application="/opt/airflow/dags/scripts/dim_paymenttype.py",
        conn_id="spark_conn",
        verbose=True,
        name="dim_paymenttype_task",
    )

    dim_location_task = SparkSubmitOperator(
        task_id="dim_location_task",
        application="/opt/airflow/dags/scripts/dim_location.py",
        conn_id="spark_conn",
        verbose=True,
        name="dim_location_task",
    )

    # Slack notification task (now uses `logical_date`)
    sending_slack_notification = SlackAPIPostOperator(
        task_id="sending_slack_notification",
        slack_conn_id="slack_conn",
        username="airflow",
        text="DAG nyc_taxi for {{ logical_date.strftime('%Y-%m') }} has been successfully completed!",
        channel="#airflow-exploit",
    )

    # Set task dependencies
    (
        [download_dataset_task, load_csv_to_minio_task]
        >> load_to_minio_task
        >> bronze_to_silver_task
        >> validate_silver_task
        >> fact_trip_task
        >> [dim_weekday_task, dim_paymenttype_task, dim_location_task]
    )

    # Ensure Slack notification is triggered after all tasks are completed
    (
        [fact_trip_task, dim_weekday_task, dim_paymenttype_task, dim_location_task]
        >> sending_slack_notification
    )
