#!/bin/bash

# Load Spark configurations
. "$SPARK_HOME/sbin/spark-config.sh"
. "$SPARK_HOME/bin/load-spark-env.sh"

# Create log directory if not exists
mkdir -p $SPARK_WORKER_LOG_DIR

# Redirect stdout to a log file
ln -sf /dev/stdout $SPARK_WORKER_LOG_DIR/spark-worker.out

# Start Spark Worker and connect to the Spark Master
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port $SPARK_WORKER_UI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG_DIR/spark-worker.out
