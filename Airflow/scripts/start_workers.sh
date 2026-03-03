#!/bin/bash

# Number of workers to launch (defaults to 1)
n=${1:-1}

# Paths
PROJECT_DIR="/Users/ndaicnsarizona/Desktop/deidentification/deIdentification"
VENV_PATH="air_env"

echo "Cleaning up existing worker sessions..."

# 1. Kill any existing screen sessions that start with 'worker_'
# We use -ls to list, grep to find matches, and awk to grab the PID/name
screen -ls | grep -E "\.worker_[0-9]+" | awk '{print $1}' | xargs -I{} screen -X -S {} quit

# Start the runserver
#screen -dmS "worker_server" bash -lc "conda activate '$VENV_PATH' && cd '$PROJECT_DIR' && python manage.py runserver"

# Loop to launch each worker in a separate detached screen session
for ((i = 1; i <= n; i++)); do
  SESSION_NAME="worker_$i"
  screen -dmS "$SESSION_NAME" bash -lc "conda activate '$VENV_PATH' && cd '$PROJECT_DIR' && python manage.py start_worker"
done

echo "$n workers started in detached screen sessions with air_env activated."