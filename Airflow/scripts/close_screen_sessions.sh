#!/bin/bash

set -euo pipefail

echo "Closing all existing worker screen sessions..."

screen -ls | grep -E "\.worker_[0-9]+" | awk '{print $1}' | xargs -r -I{} screen -X -S {} quit || true

echo "All worker screen sessions closed."