#!/bin/bash

# Set the directory for the JAR files
JAR_DIR="airflow/jars"

# Create the directory if it doesn't exist
mkdir -p ${JAR_DIR}

# Change to the JAR directory
cd ${JAR_DIR}

# Print the current directory
echo "Current directory: ${PWD}"

# Download the necessary JAR files
wget --no-verbose https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar
wget --no-verbose https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar
wget --no-verbose https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar
wget --no-verbose https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar
wget --no-verbose https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar
wget --no-verbose https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
wget --no-verbose https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar
wget --no-verbose https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar

# Print success message
echo "Downloaded JAR files to ${JAR_DIR}"

# Navigate back to the original directory
cd - > /dev/null 2>&1

# Print success message
echo "Script execution completed successfully"
