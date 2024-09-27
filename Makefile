.PHONY: spark trino-cluster minio mysql postgres-airflow hive-metastore airflow superset clean

all: minio mysql postgres-airflow hive-metastore spark trino-cluster airflow superset

minio:
	docker-compose up -d minio
	sleep 2

mysql:
	docker-compose up -d mysql
	sleep 2

postgres-airflow:
	docker-compose up -d postgres-airflow
	sleep 2

hive-metastore:
	docker-compose up -d hive-metastore
	sleep 2

spark:
	docker-compose up -d spark-master
	sleep 2
	docker-compose up -d spark-worker
	sleep 2

trino-cluster:
	docker-compose up -d trino-coordinator trino-worker
	sleep 2

airflow:
	docker-compose up -d airflow
	sleep 2

superset:
	docker-compose up -d superset
	sleep 2

clean:
	docker-compose down
