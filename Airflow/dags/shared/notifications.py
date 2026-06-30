"""
shared.notifications — Google Chat Cards V2 webhook alerts for all pipeline DAGs.

Reads GOOGLE_CHAT_WEBHOOK_URL lazily at call time (not at import time) so the
value is always fresh from the environment, regardless of import order.
"""
from __future__ import annotations

import json
import logging
import os
import traceback as _traceback
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

_STATUS_ICON = {
    "SUCCESS":         "✅",
    "FAILED":          "❌",
    "RETRY":           "🔄",
    "SKIPPED":         "⏭️",
    "UPSTREAM_FAILED": "🚫",
}


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"


def _send_webhook(context: dict, status: str) -> None:
    webhook_url = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("GOOGLE_CHAT_WEBHOOK_URL not configured — skipping notification.")
        return

    dag     = context.get("dag")
    ti      = context.get("task_instance")
    dag_run = context.get("dag_run")
    task    = context.get("task")
    exc     = context.get("exception")

    dag_id   = dag.dag_id if dag else "unknown"
    task_id  = ti.task_id if ti else "unknown"
    run_id   = (dag_run.run_id if dag_run else context.get("run_id", "unknown"))
    log_url  = getattr(ti, "log_url", None) or ""
    hostname = getattr(ti, "hostname", None) or "—"

    # Attempt counters: try_number is 1-based; max_tries = retries setting (0 = no retries)
    try_number = getattr(ti, "try_number", 1) if ti else 1
    max_tries  = (getattr(ti, "max_tries", 0) + 1) if ti else 1

    # Duration
    duration_str = "—"
    if ti and getattr(ti, "start_date", None):
        end   = getattr(ti, "end_date", None) or datetime.now(timezone.utc)
        start = ti.start_date
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if hasattr(end, "tzinfo") and end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        duration_str = _format_duration((end - start).total_seconds())

    exec_date_str = "—"
    if dag_run:
        ed = getattr(dag_run, "logical_date", None) or getattr(dag_run, "execution_date", None)
        if ed:
            exec_date_str = ed.strftime("%Y-%m-%d %H:%M UTC")

    start_str = "—"
    if ti and getattr(ti, "start_date", None):
        start_str = ti.start_date.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Exception: message + traceback tail extracted from the live exception object
    exc_text = ""
    if exc:
        exc_header = f"<b>{type(exc).__name__}:</b> {str(exc)[:400]}"
        tb = getattr(exc, "__traceback__", None)
        tb_str = ""
        if tb:
            lines = "".join(_traceback.format_tb(tb)).strip().split("\n")
            tb_str = "\n".join(lines[-14:])[:900]
        exc_text = exc_header + (f"\n<pre>{tb_str}</pre>" if tb_str else "")

    # Next retry delay (only relevant for RETRY events)
    retry_str = ""
    if status == "RETRY" and task:
        rd = getattr(task, "retry_delay", None)
        if rd:
            retry_str = f"⏱ Next attempt in {_format_duration(rd.total_seconds())}"

    icon = _STATUS_ICON.get(status, "ℹ️")

    def _dt(label: str, value: str) -> dict:
        return {"decoratedText": {"topLabel": label, "text": value, "wrapText": True}}

    core_widgets = [
        _dt("DAG",       dag_id),
        _dt("Task",      f"<b>{task_id}</b>"),
        _dt("Run ID",    run_id),
        _dt("Exec Date", exec_date_str),
        _dt("Attempt",   f"{try_number} of {max_tries}"),
        _dt("Started",   start_str),
        _dt("Duration",  duration_str),
        _dt("Worker",    hostname),
    ]

    sections: list = [{"header": "Task Details", "widgets": core_widgets}]

    if exc_text:
        sections.append({
            "header":      "Error Details",
            "collapsible": True,
            "widgets":     [{"textParagraph": {"text": exc_text}}],
        })

    if retry_str:
        sections.append({"widgets": [{"textParagraph": {"text": retry_str}}]})

    if log_url:
        sections.append({
            "widgets": [{
                "buttonList": {
                    "buttons": [{
                        "text":    "Open Task Logs",
                        "onClick": {"openLink": {"url": log_url}},
                    }]
                }
            }]
        })

    payload = {
        "cardsV2": [{
            "cardId": f"{dag_id}__{task_id}__{status}",
            "card": {
                "header": {
                    "title":    f"{icon} {status} — {task_id}",
                    "subtitle": f"DAG: {dag_id}",
                },
                "sections": sections,
            },
        }]
    }

    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.error("Failed to send GChat webhook alert: %s", e)


def on_success_callback(context):
    _send_webhook(context, "SUCCESS")


def on_failure_callback(context):
    _send_webhook(context, "FAILED")


def on_retry_callback(context):
    _send_webhook(context, "RETRY")


default_args = {
    "owner":             "airflow",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           0,
    "retry_delay":       timedelta(minutes=5),
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback,
    "on_retry_callback":   on_retry_callback,
}
