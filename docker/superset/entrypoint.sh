#!/bin/bash
set -e

# Upgrade the database
superset db upgrade

# Create an admin user if it doesn't exist
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@superset.com --password admin || echo "Admin user already exists"

# Initialize Superset
superset init

# Start the Superset web server
gunicorn --bind 0.0.0.0:8088 "superset.app:create_app()"
