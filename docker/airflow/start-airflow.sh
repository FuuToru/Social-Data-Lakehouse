#!/bin/bash

# Initialize the Airflow database
airflow db init

# Create Airflow user (if not already created)
airflow users create \
    --username airflow \
    --password airflow \
    --firstname FirstName \
    --lastname LastName \
    --role Admin \
    --email admin@example.com || echo "User already exists."

# Start the Airflow scheduler in the background
airflow scheduler &

# Start the Airflow webserver
exec airflow webserver --port 8080
