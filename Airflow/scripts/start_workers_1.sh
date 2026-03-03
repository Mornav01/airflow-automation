#!/bin/bash

set -euo pipefail

n=${1:-1}

PROJECT_DIR="/Users/ndaicnsarizona/Desktop/deidentification/deIdentification"
VENV_PATH="air_env"


echo "Cleaning up existing worker sessions..."

# Kill existing worker screens
screen -ls | grep -E "\.worker_[0-9]+" | awk '{print $1}' | xargs -r -I{} screen -X -S {} quit || true

echo "Starting worker_server..."


for ((i = 1; i <= n; i++)); do
    SESSION_NAME="worker_$i"

    echo "Starting $SESSION_NAME..."

    screen -dmS "$SESSION_NAME" bash -lc "
        conda activate '$VENV_PATH' &&
        cd '$PROJECT_DIR' &&
        python manage.py start_worker
    "

    sleep 1

    if ! screen -list | grep $SESSION_NAME; then
        echo "Failed to start $SESSION_NAME"
        exit 1
    fi
done

echo "$n workers started successfully."