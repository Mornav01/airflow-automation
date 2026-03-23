#!/bin/bash

# Number of workers to launch (defaults to 1)
n=${1:-1}

# Paths
PROJECT_DIR="/Users/ndaicnsarizona/Desktop/deidentification_polars_2/deidentification_v2/deIdentification/"
VENV_PATH="airenv"

# Loop to launch each worker in a new Terminal window
for ((i = 1; i <= n; i++)); do
  osascript <<EOF
tell application "Terminal"
     do script "cd '$PROJECT_DIR' && conda activate $VENV_PATH && python3 manage.py start_worker"
end tell
EOF
  sleep 1  # Optional: delay between opening terminals
done

echo "$n workers started in separate Terminal windows with airenv activated."
