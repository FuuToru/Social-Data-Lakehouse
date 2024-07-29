#!/bin/bash

# Wait for the database to be ready
sleep 10

# Initialize the Superset database and create an admin user
superset db upgrade
superset fab create-admin --username ${SUPERSET_ADMIN:-admin} --firstname Admin --lastname User --email admin@admin.com --password ${SUPERSET_PASSWORD:-admin}
superset init

# Start Superset
superset run -p 8088 -h 0.0.0.0 --with-threads --reload --debugger
