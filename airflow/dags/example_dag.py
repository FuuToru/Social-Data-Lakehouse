# airflow/dags/example_dag.py

from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator

from airflow import DAG

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "retries": 1,
}

dag = DAG("example_dag", default_args=default_args, schedule_interval="@daily")

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

start >> end
