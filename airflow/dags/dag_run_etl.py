from __future__ import print_function

from datetime import datetime

from airflow.operators.bash import BashOperator

from airflow import DAG

args = {
    "owner": "airflow",
    "provide_context": True,
    "catchup": False,
}

dag = DAG(
    dag_id="daily_etl",
    default_args=args,
    start_date=datetime(year=2024, month=7, day=22),
    schedule_interval="0 7 * * *",
    max_active_runs=1,
    concurrency=1,
)

start_task = BashOperator(
    task_id="start_task",
    bash_command="echo daily ETL for today_date: {{ ds }}",
    dag=dag,
)

task_spark_initial = BashOperator(
    task_id="spark_initial",
    bash_command="""
    spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g --conf spark.network.timeout=10000s \
    --packages io.delta:delta-core_2.12:1.0.0 \
    --py-files {{ var.value.airflow_home }}/dags/utils/common.py \
    --jars {{ var.value.airflow_home }}/dags/jars/aws-java-sdk-1.11.534.jar,\
{{ var.value.airflow_home }}/dags/jars/aws-java-sdk-bundle-1.11.874.jar,\
{{ var.value.airflow_home }}/dags/jars/delta-core_2.12-1.0.0.jar,\
{{ var.value.airflow_home }}/dags/jars/hadoop-aws-3.2.0.jar,\
{{ var.value.airflow_home }}/dags/jars/mariadb-java-client-2.7.4.jar \
    {{ var.value.airflow_home }}/dags/etl/spark_initial.py s3a://datalake/bitcoin_initial.csv s3a://datalake/deltatables/bitcoin/
    """,
    dag=dag,
)

task_spark_create_table = BashOperator(
    task_id="spark_create_table",
    bash_command="""
    spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g --conf spark.network.timeout=10000s \
    --packages io.delta:delta-core_2.12:1.0.0 \
    --py-files {{ var.value.airflow_home }}/dags/utils/common.py \
    --jars {{ var.value.airflow_home }}/dags/jars/aws-java-sdk-1.11.534.jar,\
{{ var.value.airflow_home }}/dags/jars/aws-java-sdk-bundle-1.11.874.jar,\
{{ var.value.airflow_home }}/dags/jars/delta-core_2.12-1.0.0.jar,\
{{ var.value.airflow_home }}/dags/jars/hadoop-aws-3.2.0.jar,\
{{ var.value.airflow_home }}/dags/jars/mariadb-java-client-2.7.4.jar \
    {{ var.value.airflow_home }}/dags/etl/spark_create_table.py
    """,
    dag=dag,
)

task_spark_update = BashOperator(
    task_id="spark_update",
    bash_command="""
    spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g --conf spark.network.timeout=10000s \
    --packages io.delta:delta-core_2.12:1.0.0 \
    --py-files {{ var.value.airflow_home }}/dags/utils/common.py \
    --jars {{ var.value.airflow_home }}/dags/jars/aws-java-sdk-1.11.534.jar,\
{{ var.value.airflow_home }}/dags/jars/aws-java-sdk-bundle-1.11.874.jar,\
{{ var.value.airflow_home }}/dags/jars/delta-core_2.12-1.0.0.jar,\
{{ var.value.airflow_home }}/dags/jars/hadoop-aws-3.2.0.jar,\
{{ var.value.airflow_home }}/dags/jars/mariadb-java-client-2.7.4.jar \
    {{ var.value.airflow_home }}/dags/etl/spark_update.py s3a://datalake/bitcoin_newdata.csv s3a://datalake/deltatables/bitcoin/
    """,
    dag=dag,
)

task_spark_run_query = BashOperator(
    task_id="spark_run_query",
    bash_command="""
    spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g --conf spark.network.timeout=10000s \
    --packages io.delta:delta-core_2.12:1.0.0 \
    --py-files {{ var.value.airflow_home }}/dags/utils/common.py \
    --jars {{ var.value.airflow_home }}/dags/jars/aws-java-sdk-1.11.534.jar,\
{{ var.value.airflow_home }}/dags/jars/aws-java-sdk-bundle-1.11.874.jar,\
{{ var.value.airflow_home }}/dags/jars/delta-core_2.12-1.0.0.jar,\
{{ var.value.airflow_home }}/dags/jars/hadoop-aws-3.2.0.jar,\
{{ var.value.airflow_home }}/dags/jars/mariadb-java-client-2.7.4.jar \
    {{ var.value.airflow_home }}/dags/etl/spark_run_query.py
    """,
    dag=dag,
)

(
    start_task
    >> task_spark_initial
    >> task_spark_create_table
    >> task_spark_update
    >> task_spark_run_query
)
