#!/bin/bash
set -e

#################################
# LOAD ENV FILE
#################################
ENV_FILE=$1
if [ -z "$ENV_FILE" ]; then
  echo "❌ Usage: ./setup_instance.sh env/dev.env"
  exit 1
fi

echo "🌍 Loading env: $ENV_FILE"
set -a
source "$ENV_FILE"
set +a

#################################
# ACTIVATE CONDA ENV (ROBUST)
#################################
if ! command -v conda &> /dev/null; then
  echo "❌ Conda not found in PATH"
  exit 1
fi

# Proper way to load conda in scripts
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate air_env   # change if your env name is different

python --version
which airflow

#################################
# AUTO COMPUTE AIRFLOW_HOME
#################################
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

export AIRFLOW_HOME="$REPO_ROOT/Airflow"

echo "🏠 AIRFLOW_HOME = $AIRFLOW_HOME"

#################################
# SAFETY: FORBID SQLITE
#################################
if [[ "$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" == *"sqlite"* ]]; then
  echo "❌ FATAL: SQLite is not allowed"
  echo "DB = $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
  exit 1
fi

#################################
# CREATE DIR STRUCTURE
#################################
mkdir -p \
  "$AIRFLOW_HOME/dags" \
  "$AIRFLOW_HOME/plugins" \
  "$AIRFLOW_HOME/logs"

#################################
# SHOW ACTIVE CONFIG
#################################

pip install apache-airflow-providers-microsoft-mssql
pip install apache-airflow-providers-mysql
pip install apache-airflow-providers-redis
pip install apache-airflow-providers-celery


echo "🧠 Active DB:"
airflow config get-value database sql_alchemy_conn


#################################
# INIT / MIGRATE DB
#################################
echo "🗄 Migrating Airflow DB (Postgres)"
echo "🗄 Upgrading Airflow DB"
airflow db migrate

#################################
# CREATE DEFAULT CONNECTIONS
#################################
airflow connections create-default-connections

#################################
# CREATE ADMIN USER
#################################

#airflow db-manager create-user \
#  --username admin \
#  --password admin \
#  --firstname Admin \
#  --lastname User \
#  --role Admin \
#  --email admin@example.com
#  || true

#################################
# FINAL CHECK
#################################
airflow db check

echo ""
echo "🎉 Airflow is fully configured"
echo "🏠 AIRFLOW_HOME = $AIRFLOW_HOME"
echo "🌐 UI will be on port $AIRFLOW__WEBSERVER__PORT"
