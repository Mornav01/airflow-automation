#!/bin/bash
set -e

############################
# CONFIG
############################
CONDA_ENV="air_env"
#AIRFLOW_VERSION="2.10.2"
AIRFLOW_VERSION="3.1.7"

echo "🚀 Installing Airflow"

############################
# LOAD CONDA (REQUIRED)
############################
source "$(conda info --base)/etc/profile.d/conda.sh"

############################
# ACTIVATE ENV
############################
conda activate "$CONDA_ENV"

python --version

################################
# INSTALL AIRFLOW
################################
pip install --upgrade pip setuptools wheel

pip install psycopg2==2.9.10
pip install asyncpg

PYTHON_VERSION=$(python --version | cut -d ' ' -f2 | cut -d '.' -f1-2)
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install "apache-airflow[celery,redis]==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

airflow version

echo "✅ Airflow installed in conda env: $CONDA_ENV"
