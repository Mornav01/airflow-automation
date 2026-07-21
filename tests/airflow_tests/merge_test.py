import os
from airflow_app.services.merge.merge_engine import MergeEngine

def run_merge(**context):
    engine = MergeEngine(
        db_type="mssql",   # or "mssql"
        main_conn=f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1435/mobiledoc_nov_22_11",
        diff_conn=f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1435/mobiledoc_difftestkaran",
        diffmerged_conn=f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1435/mobiledoc_diffmergeddifftest"
        )

    result = engine.run_table(
        table="family_2",
        pk_cols=[],
        default_date="2025-09-28 00:00:00"
    )
    print(result)

    if not result["success"]:
        raise Exception(result["error"])

run_merge()