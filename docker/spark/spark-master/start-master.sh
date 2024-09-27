#!/bin/bash

# Export the hostname for the master
export SPARK_MASTER_HOST=`hostname`

# Load Spark configurations
. "$SPARK_HOME/sbin/spark-config.sh"
. "$SPARK_HOME/bin/load-spark-env.sh"

# Create log directory if not exists
mkdir -p $SPARK_MASTER_LOG_DIR

# Redirect stdout to a log file
ln -sf /dev/stdout $SPARK_MASTER_LOG_DIR/spark-master.out

# Start Spark Master with specified IP, port, and UI port
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_UI_PORT >> $SPARK_MASTER_LOG_DIR/spark-master.out
