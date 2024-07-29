from __future__ import print_function

from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from airflow import DAG

# Default arguments for the DAG
args = {
    "owner": "airflow",
    "provide_context": True,
    "catchup": False,
}

# Define the DAG
dag = DAG(
    dag_id="daily_etl",
    default_args=args,
    start_date=datetime(year=2024, month=7, day=22),
    schedule_interval="0 7 * * *",  # Daily at 07:00 AM
    max_active_runs=1,
    concurrency=1,
)

# Start task to print the current date
start_task = BashOperator(
    task_id="start_task",
    bash_command="echo daily ETL for today_date: {{ ds }}",
    dag=dag,
)


# Function to create a BashOperator for spark-submit tasks
def create_spark_submit_operator(task_id, script_name):
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
        spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 2g \
        --conf spark.network.timeout=10000s \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        --py-files /opt/airflow/dags/utils/common.py \
        --jars /opt/airflow/jars/aws-java-sdk-1.12.367.jar,\
/opt/airflow/jars/delta-core_2.12-2.2.0.jar,\
/opt/airflow/jars/delta-storage-2.2.0.jar,\
/opt/airflow/jars/hadoop-aws-3.3.4.jar,\
/opt/airflow/jars/mysql-connector-java-8.0.19.jar,\
/opt/airflow/jars/s3-2.18.41.jar,\
/opt/airflow/jars/aws-java-sdk-bundle-1.12.367.jar \
        /opt/airflow/dags/etl/{script_name}
        """,
        dag=dag,
    )


# Create tasks for each ETL layer
bronze_layer = create_spark_submit_operator("bronze_layer", "bronze.py")
silver_layer = create_spark_submit_operator("silver_layer", "silver.py")
gold_layer = create_spark_submit_operator("gold_layer", "gold.py")
create_table_task = create_spark_submit_operator("create_table", "create_table.py")

# Dummy task to signify successful completion
success_task = DummyOperator(
    task_id="success_task",
    dag=dag,
)

# Define task dependencies
(
    start_task
    >> bronze_layer
    >> silver_layer
    >> gold_layer
    >> create_table_task
    >> success_task
)
