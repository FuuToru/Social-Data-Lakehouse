#!/bin/sh
set -o errexit
set -o nounset

# Initialize the Airflow database
airflow db init

# Create an Airflow user with admin role
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --password admin \
    --role Admin \
    --email email@email.com

# Start the Airflow webserver in the background and then the scheduler
airflow webserver -p 8000 & 
sleep 10  # Increase sleep time to ensure the webserver starts before the scheduler
airflow scheduler
