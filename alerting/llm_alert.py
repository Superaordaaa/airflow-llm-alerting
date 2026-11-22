"""Airflow callbacks that explain task failures in plain language with a local LLM.

Flow: task fails (or runs slow) -> collect error + context -> ask a local model via
Ollama to explain it -> send the explanation to Telegram. If the LLM is unreachable,
the raw error is sent instead, so an alert is never lost.

Configuration (environment variables):
    OLLAMA_URL           default http://localhost:11434
    OLLAMA_MODEL         default qwen2.5
    TELEGRAM_BOT_TOKEN   required to send alerts
    TELEGRAM_CHAT_ID     required to send alerts
    SLOW_TASK_SECONDS    default 1800, threshold for the slow-task alert
"""
from __future__ import annotations

import logging
import os
import traceback

import requests

log = logging.getLogger(__name__)

PROMPT = """You are helping a data engineer triage an Airflow alert.
Explain in plain language, in at most 5 short lines:
1. What went wrong
2. The most likely cause
3. What to check first

DAG: {dag_id}
Task: {task_id}
Try: {try_number}
Event: {event}

Error:
{error}
"""


def _cfg(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def explain(error: str, dag_id: str, task_id: str, try_number: int, event: str,
            timeout: int = 60) -> str | None:
    """Ask the local LLM for an explanation. Returns None if the LLM is unavailable."""
    url = _cfg("OLLAMA_URL", "http://localhost:11434").rstrip("/") + "/api/generate"
    prompt = PROMPT.format(dag_id=dag_id, task_id=task_id, try_number=try_number,
                           event=event, error=error[-4000:])
    try:
        r = requests.post(url, json={"model": _cfg("OLLAMA_MODEL", "qwen2.5"),
                                     "prompt": prompt, "stream": False}, timeout=timeout)
        r.raise_for_status()
        return (r.json().get("response") or "").strip() or None
    except (requests.RequestException, ValueError) as e:
        log.warning("LLM unavailable, sending raw error: %s", e)
        return None


def send_telegram(text: str, timeout: int = 15) -> bool:
    token, chat_id = _cfg("TELEGRAM_BOT_TOKEN"), _cfg("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; alert not sent")
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": text[:4000]}, timeout=timeout)
        r.raise_for_status()
        return True
    except requests.RequestException as e:
        log.error("Failed to send Telegram alert: %s", e)
        return False


def build_message(header: str, dag_id: str, task_id: str, explanation: str | None,
                  error: str, log_url: str | None) -> str:
    body = explanation if explanation else "LLM unavailable. Raw error:\n" + error[-1500:]
    parts = [header, f"DAG: {dag_id}", f"Task: {task_id}", "", body]
    if log_url:
        parts += ["", f"Log: {log_url}"]
    return "\n".join(parts)


def _error_text(context: dict) -> str:
    exc = context.get("exception")
    if exc is None:
        return "No exception in context."
    if isinstance(exc, BaseException):
        return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return str(exc)


def on_failure(context: dict) -> None:
    """Use as `on_failure_callback` on a DAG or task."""
    ti = context["task_instance"]
    error = _error_text(context)
    explanation = explain(error, ti.dag_id, ti.task_id, ti.try_number, "task failed")
    send_telegram(build_message("❌ Task failed", ti.dag_id, ti.task_id, explanation,
                                error, getattr(ti, "log_url", None)))


def on_success_check_slow(context: dict) -> None:
    """Use as `on_success_callback`: alert when a task succeeded but took too long."""
    ti = context["task_instance"]
    limit = float(_cfg("SLOW_TASK_SECONDS", "1800"))
    duration = getattr(ti, "duration", None)
    if duration is None or duration <= limit:
        return
    error = f"Task succeeded but took {duration:.0f}s (threshold {limit:.0f}s)."
    explanation = explain(error, ti.dag_id, ti.task_id, ti.try_number, "task slow")
    send_telegram(build_message("🐢 Task slow", ti.dag_id, ti.task_id, explanation,
                                error, getattr(ti, "log_url", None)))
