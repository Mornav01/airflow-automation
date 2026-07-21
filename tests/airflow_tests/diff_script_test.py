import os
from airflow_app.services.diff_prodedure_execution import DiffService
import sqlalchemy
from sqlalchemy import text

# ---------- CONFIG (from your cycle_metadata) ----------

CORE_DB = f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1435/mobiledoc_nov_22_11"
DUMP_DB = f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1435/mobiledoc_06012026"
DIFF_DB = f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1435/mobiledoc_difftestkaran"

TABLE_NAME = "family_2"


# ---------- RUN DIFF ----------

print("Running diff...")
DiffService.run_diff(
    core_conn_str=CORE_DB,
    dump_conn_str=DUMP_DB,
    diff_conn_str=DIFF_DB,
    table_name=TABLE_NAME
)
print("Diff completed")


# ---------- VERIFY RESULT ----------

engine = sqlalchemy.create_engine(DIFF_DB)

with engine.begin() as conn:
    result = conn.execute(text(f"""
        SELECT diff_type, COUNT(*) cnt
        FROM dbo.{TABLE_NAME}
        GROUP BY diff_type
    """))

    print("\nDiff summary:")
    for row in result:
        print(row)
