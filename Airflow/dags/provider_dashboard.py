from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_DIR = "/Users/ndaijwm/Documents/udm/de_udm/dashboards"

with DAG(
    dag_id="provider_dashboard_mssql",
    start_date=datetime(2026, 4, 9, 6, 30),  # 12:00 PM IST in UTC
    schedule=None,  # run only once
    catchup=False,
    tags=["dashboard", "mssql", "reporting"],
) as dag:

    run_dashboard_sql = BashOperator(
        task_id="run_dashboard_table_mssql",
        bash_command="python3 dashboard_table_mssql_copy.py",
        cwd=PROJECT_DIR,
    )