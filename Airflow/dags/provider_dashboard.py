from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

IST = pendulum.timezone("Asia/Kolkata")

PROJECT_DIR = "/Users/ndai/Documents/udm/de_udm/dashboards"

with DAG(
    dag_id="provider_dashboard",
    start_date=datetime(2026, 4, 9, 12, 0, tzinfo=IST),  # 12 PM IST
    schedule=None,#"0 12 * * *",  # every day at 12:00 IST
    catchup=False,
    tags=["dashboard", "mssql", "reporting"],
) as dag:

    run_dashboard_sql = BashOperator(
        task_id="run_dashboard_table_mssql",
        bash_command="python3 dashboard_table_mssql.py",
        cwd=PROJECT_DIR,
    )