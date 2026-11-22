"""Tests run without Airflow, Ollama, or Telegram: HTTP calls are mocked."""
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from alerting import llm_alert  # noqa: E402


def ti(duration=None):
    return SimpleNamespace(dag_id="d", task_id="t", try_number=1,
                           log_url="http://airflow/log", duration=duration)


def ok(payload):
    r = mock.Mock()
    r.raise_for_status.return_value = None
    r.json.return_value = payload
    return r


ENV = {"TELEGRAM_BOT_TOKEN": "x", "TELEGRAM_CHAT_ID": "1", "SLOW_TASK_SECONDS": "60"}


@mock.patch.dict(os.environ, ENV)
class TestAlerts(unittest.TestCase):
    @mock.patch("alerting.llm_alert.requests.post")
    def test_failure_sends_llm_explanation(self, post):
        post.side_effect = [ok({"response": "Amount is None in row 2."}), ok({})]
        llm_alert.on_failure({"task_instance": ti(), "exception": TypeError("bad")})
        text = post.call_args_list[1].kwargs["json"]["text"]
        self.assertIn("Amount is None in row 2.", text)
        self.assertIn("Log: http://airflow/log", text)

    @mock.patch("alerting.llm_alert.requests.post")
    def test_llm_down_falls_back_to_raw_error(self, post):
        post.side_effect = [requests.ConnectionError("refused"), ok({})]
        llm_alert.on_failure({"task_instance": ti(), "exception": ValueError("boom")})
        text = post.call_args_list[1].kwargs["json"]["text"]
        self.assertIn("LLM unavailable", text)
        self.assertIn("boom", text)

    @mock.patch("alerting.llm_alert.requests.post")
    def test_fast_task_sends_nothing(self, post):
        llm_alert.on_success_check_slow({"task_instance": ti(duration=10)})
        post.assert_not_called()

    @mock.patch("alerting.llm_alert.requests.post")
    def test_slow_task_alerts(self, post):
        post.side_effect = [ok({"response": "Upstream table grew."}), ok({})]
        llm_alert.on_success_check_slow({"task_instance": ti(duration=300)})
        self.assertIn("Task slow", post.call_args_list[1].kwargs["json"]["text"])

    @mock.patch("alerting.llm_alert.requests.post")
    def test_no_telegram_config_does_not_crash(self, post):
        with mock.patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": ""}):
            self.assertFalse(llm_alert.send_telegram("hi"))
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
