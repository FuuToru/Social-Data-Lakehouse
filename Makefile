.PHONY: airflow spark hive scale-spark minio superset down presto-cluster presto-cli run-spark

down:
	docker-compose down -v

minio:
	docker-compose up -d minio
	sleep 10

airflow:
	docker-compose up -d airflow
	sleep 10

spark:
	docker-compose up -d spark-master
	sleep 5
	docker-compose up -d spark-worker

hive:
	docker-compose up -d mariadb
	sleep 10
	docker-compose up -d hive

presto-cluster:
	docker-compose up -d presto presto-worker

superset:
	docker-compose up -d superset
	sleep 10
	docker-compose exec superset superset-init

scale-spark:
	docker-compose scale spark-worker=3

presto-cli:
	docker-compose exec presto \
	presto --server localhost:8888 --catalog hive --schema default

run-spark:
	docker-compose exec airflow \
	spark-submit --master spark://spark-master:7077 \
	--deploy-mode client --driver-memory 2g \
	--num-executors 2 \
	--packages io.delta:delta-core_2.12:1.0.0 \
	--jars dags/jars/aws-java-sdk-1.11.534.jar,\
	dags/jars/aws-java-sdk-bundle-1.11.874.jar,\
	dags/jars/delta-core_2.12-1.0.0.jar,\
	dags/jars/hadoop-aws-3.2.0.jar \
dags/etl/spark_app.py
