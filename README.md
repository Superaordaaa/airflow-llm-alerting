# airflow-llm-alerting

When an Airflow task fails or runs slow, a **local LLM (Ollama, Qwen 2.5)** turns the raw error into a short plain-language explanation and sends it to **Telegram**. You see what broke and where to look first, without opening the logs.

This is a clean demo of a pattern I run in production as a Data Engineer, rewritten from scratch with no company code or configuration.

```
Airflow task fails / runs slow
        │  on_failure_callback / on_success_callback
        ▼
collect error + DAG/task context
        │
        ▼
Ollama (local, free) ── unavailable? ──► send raw error instead
        │
        ▼
Telegram: what went wrong · likely cause · what to check first
```

## Why
- Raw tracebacks are slow to read, especially at night or on a phone.
- A local model keeps error data on your own server and costs nothing per call.
- The fallback means an alert is never lost just because the LLM is down.

## Setup
1. Install and start [Ollama](https://ollama.com), then pull the model:
   ```bash
   ollama pull qwen2.5
   ```
2. Create a Telegram bot with @BotFather and get your chat ID.
3. Copy `alerting/` into your Airflow `dags/` folder (or onto `PYTHONPATH`), install `requests`, and set the variables from `.env.example`.
4. Attach the callbacks:
   ```python
   from alerting import on_failure, on_success_check_slow

   default_args = {
       "on_failure_callback": on_failure,
       "on_success_callback": on_success_check_slow,  # alerts if duration > SLOW_TASK_SECONDS
   }
   ```
5. Trigger `dags/example_alerting_dag.py`. Its task fails on purpose, so you can see the alert end to end.

## Configuration
| Variable | Default | Purpose |
|---|---|---|
| `OLLAMA_URL` | `http://localhost:11434` | Ollama server |
| `OLLAMA_MODEL` | `qwen2.5` | Any model you have pulled |
| `TELEGRAM_BOT_TOKEN` | — | Bot token |
| `TELEGRAM_CHAT_ID` | — | Chat or group to alert |
| `SLOW_TASK_SECONDS` | `1800` | Threshold for the slow-task alert |

## Tests
These tests need no Airflow, Ollama, or Telegram, because the HTTP calls are mocked:
```bash
python -m unittest discover -s tests -v
```

## Notes
- The LLM output is a triage hint, not a diagnosis. The log link is always included.
- Errors are trimmed before being sent to the model and to Telegram, so messages stay short.

## License
MIT
