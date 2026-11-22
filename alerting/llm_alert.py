"""Airflow callbacks that explain task failures in plain language with a local LLM.

Flow when a task fails (or runs slow):
1. Send the raw alert to Telegram right away (DAG, task, run, error tail, log link).
   It never waits on the LLM, so it is never lost.
2. Ask a local model via Ollama for a short likely-cause explanation and send it as a
   second message, labelled as model output that can be wrong.
3. Per-DAG cooldown: after an alert, the same DAG/event stays quiet for
   ALERT_COOLDOWN_MINUTES, so a flapping DAG doesn't flood the chat.

Configuration (environment variables):
    OLLAMA_URL              default http://localhost:11434
    OLLAMA_MODEL            default qwen2.5
    TELEGRAM_BOT_TOKEN      required to send alerts
    TELEGRAM_CHAT_ID        required to send alerts
    SLOW_TASK_SECONDS       default 1800, threshold for the slow-task alert
    ALERT_COOLDOWN_MINUTES  default 60, set 0 to disable
    ALERT_STATE_FILE        default <temp dir>/airflow_llm_alert_state.json
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import traceback
from pathlib import Path

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

DISCLAIMER = "Written by {model} on our own server. It can be wrong; the error in the alert above is the source of truth."


def _cfg(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name) or default  # empty values fall back to the default


# ---------- cooldown ----------

def _state_file() -> Path:
    return Path(_cfg("ALERT_STATE_FILE", os.path.join(tempfile.gettempdir(),
                                                     "airflow_llm_alert_state.json")))


def _load_state() -> dict:
    try:
        return json.loads(_state_file().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def in_cooldown(key: str, now: float | None = None) -> bool:
    minutes = float(_cfg("ALERT_COOLDOWN_MINUTES", "60"))
    if minutes <= 0:
        return False
    last = _load_state().get(key)
    return last is not None and ((now or time.time()) - last) < minutes * 60


def mark_alerted(key: str, now: float | None = None) -> None:
    state = _load_state()
    state[key] = now or time.time()
    try:
        _state_file().write_text(json.dumps(state), encoding="utf-8")
    except OSError as e:
        log.warning("Could not write alert state: %s", e)


# ---------- LLM + Telegram ----------

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
        log.warning("LLM unavailable, raw alert only: %s", e)
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


# ---------- messages ----------

def build_alert(header: str, dag_id: str, task_id: str, run_id: str | None, error: str,
                log_url: str | None) -> str:
    parts = [header, f"DAG: {dag_id}", f"Task: {task_id}"]
    if run_id:
        parts.append(f"Run: {run_id}")
    parts += ["", error[-1500:]]
    if log_url:
        parts += ["", f"Log: {log_url}"]
    minutes = float(_cfg("ALERT_COOLDOWN_MINUTES", "60"))
    if minutes > 0:
        parts += ["", f"Next alert for this DAG in {minutes:.0f} min at the earliest."]
    return "\n".join(parts)


def build_explanation(dag_id: str, task_id: str, explanation: str) -> str:
    model = _cfg("OLLAMA_MODEL", "qwen2.5")
    return "\n".join([f"🤖 Likely cause · {dag_id}", DISCLAIMER.format(model=model), "",
                      f"{task_id}: {explanation}"])


def _error_text(context: dict) -> str:
    exc = context.get("exception")
    if exc is None:
        return "No exception in context."
    if isinstance(exc, BaseException):
        return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return str(exc)


def _alert(context: dict, event: str, header: str, error: str) -> None:
    ti = context["task_instance"]
    key = f"{ti.dag_id}:{event}"
    if in_cooldown(key):
        log.info("Alert for %s suppressed (cooldown)", key)
        return
    run_id = getattr(ti, "run_id", None) or context.get("run_id")
    send_telegram(build_alert(header, ti.dag_id, ti.task_id, run_id, error,
                              getattr(ti, "log_url", None)))
    mark_alerted(key)
    explanation = explain(error, ti.dag_id, ti.task_id, ti.try_number, event)
    if explanation:
        send_telegram(build_explanation(ti.dag_id, ti.task_id, explanation))


# ---------- Airflow callbacks ----------

def on_failure(context: dict) -> None:
    """Use as `on_failure_callback` on a DAG or task."""
    _alert(context, "task failed", "❌ Task failed", _error_text(context))


def on_success_check_slow(context: dict) -> None:
    """Use as `on_success_callback`: alert when a task succeeded but took too long."""
    ti = context["task_instance"]
    limit = float(_cfg("SLOW_TASK_SECONDS", "1800"))
    duration = getattr(ti, "duration", None)
    if duration is None or duration <= limit:
        return
    _alert(context, "task slow", "🐢 Task slow",
           f"Task succeeded but took {duration:.0f}s (threshold {limit:.0f}s).")
