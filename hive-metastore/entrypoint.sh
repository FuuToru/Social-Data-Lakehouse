#!/bin/sh

export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar
export JAVA_HOME=/usr/local/openjdk-8

# Make sure mariadb is ready
MAX_TRIES=8
CURRENT_TRY=1
SLEEP_BETWEEN_TRY=4
until [ "$(telnet mysql 3306 | sed -n 2p)" = "Connected to mysql." ] || [ "$CURRENT_TRY" -gt "$MAX_TRIES" ]; do
    echo "Waiting for mysql server..."
    sleep "$SLEEP_BETWEEN_TRY"
    CURRENT_TRY=$((CURRENT_TRY + 1))
done

if [ "$CURRENT_TRY" -gt "$MAX_TRIES" ]; then
  echo "WARNING: Timeout when waiting for mysql."
fi

# Initialize Hive Metastore schema if needed
if [ "$IS_RESUME" != "true" ]; then
  /opt/hive/bin/schematool -initSchema -dbType mysql --verbose
else
  /opt/hive/bin/schematool -info -dbType mysql --verbose
fi

# Start Metastore
/opt/hive/bin/hive --service metastore
