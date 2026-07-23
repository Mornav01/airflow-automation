"""
db_url — the ONE place that turns credential primitives into a connection URL.

THE RULE (follow this everywhere in the codebase):

  1. Store every password / username in .env RAW and LITERAL — exactly what the
     database expects. NEVER percent-encode in .env (no `%40` for `@`, etc.).
  2. NEVER build a connection URL with an f-string, string concatenation, or a
     hand-rolled `.replace('@', '%40')`, and NEVER call `quote_plus` yourself.
  3. ALWAYS build URLs with `make_conn_url(...)` (or `sqlalchemy.engine.URL.create`
     directly in scripts that can't import this module). SQLAlchemy does the
     percent-encoding exactly once, so a password containing @ : / % \\ etc. works
     everywhere with no manual escaping and no double-encoding.

Raw-DBAPI dict configs (e.g. pymssql/pymysql `connect(password=...)`) take the
RAW password directly — no URL, so no encoding. The raw-in-.env rule keeps those
correct too, which pre-encoding (`%40`) would silently break.
"""
from typing import Optional, Union

from sqlalchemy.engine import URL


def make_conn_url(
    drivername: str,
    *,
    username: str = "",
    password: str = "",
    host: str = "",
    port: Optional[Union[str, int]] = None,
    database: str = "",
    query: Optional[dict] = None,
    as_string: bool = True,
):
    """Build a correctly-escaped SQLAlchemy connection URL from RAW primitives.

    Pass the literal values straight from .env — URL.create() percent-encodes
    every field, so no manual quote_plus / pre-encoding is needed.

    Returns a rendered string by default (drop-in for the existing conn-string
    constants that are also used for string ops like ``.rsplit('/')``); pass
    ``as_string=False`` to get the URL object (also accepted by create_engine).
    """
    url = URL.create(
        drivername,
        username=username or None,
        password=password or None,
        host=host or None,
        port=int(port) if str(port or "").strip() else None,
        database=database or None,
        query=query or {},
    )
    return url.render_as_string(hide_password=False) if as_string else url


def mssql_odbc_query(odbc_driver: str = "ODBC Driver 17 for SQL Server") -> dict:
    """Standard pyodbc query params. Pass the driver name WITH spaces — URL.create
    encodes it; do NOT pre-replace spaces with '+'."""
    return {"driver": odbc_driver, "TrustServerCertificate": "yes"}
