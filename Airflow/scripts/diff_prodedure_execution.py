import sqlalchemy
from sqlalchemy import text
from urllib.parse import urlparse


class DiffService:

    @staticmethod
    def _extract_db_name(conn_str: str) -> str:
        """
        Extract DB name from SQLAlchemy connection string
        """
        parsed = urlparse(conn_str)
        return parsed.path.lstrip("/")  # /dbname -> dbname

    @staticmethod
    def run_diff(core_conn_str: str, dump_conn_str: str, diff_conn_str: str, table_name: str):
        """
        Calls sp_run_diff in core DB.
        """

        dump_db = DiffService._extract_db_name(dump_conn_str)
        diff_db = DiffService._extract_db_name(diff_conn_str)

        engine = sqlalchemy.create_engine(core_conn_str)

        sql = text("""
            EXEC sp_run_diff 
                @table_name = :table,
                @dump_db = :dump_db,
                @diff_db = :diff_db
        """)
        try:
            with engine.begin() as conn:
                conn.execute(
                    sql,
                    {
                        "table": table_name,
                        "dump_db": dump_db,
                        "diff_db": diff_db
                    }
                )
            return {
                "success": True,
                "table": table_name,
                "message": "Diff Calculated Successfully"
            }
        
        except Exception as e:
            return {
                "success": False,
                "table": table_name,
                "error": str(e)
            }
            
    
