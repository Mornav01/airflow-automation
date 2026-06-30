"""
shared — utilities shared across all pipeline DAGs (MIND, eCW, …).

Importing this package as a side-effect loads the project .env file so that
os.environ.get() calls in shared.notifications and shared.tasks work correctly
even when the Airflow worker process doesn't inherit the shell environment.

dags/shared/ sits at the same directory depth as dags/mind/ and dags/ecw/, so
../../.env always resolves to Airflow/.env — the same path used in the individual
DAG files.
"""
import logging
import os

logger = logging.getLogger(__name__)


def _load_env_file(path: str) -> None:
    try:
        with open(path) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip().strip('"').strip("'")
                os.environ.setdefault(_key, _val)
    except FileNotFoundError:
        logger.warning("No .env file found at %s", path)


_load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env"))
