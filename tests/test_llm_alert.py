"""Tests run without Airflow, Ollama, or Telegram: HTTP calls are mocked."""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from alerting import llm_alert  # noqa: E402


def ti(duration=None, dag_id="d"):
    return SimpleNamespace(dag_id=dag_id, task_id="t", try_number=1, run_id="scheduled__x",
                           log_url="http://airflow/log", duration=duration)


def ok(payload):
    r = mock.Mock()
    r.raise_for_status.return_value = None
    r.json.return_value = payload
    return r


def texts(post):
    return [c.kwargs["json"]["text"] for c in post.call_args_list if "text" in c.kwargs.get("json", {})]


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        env = {"TELEGRAM_BOT_TOKEN": "x", "TELEGRAM_CHAT_ID": "1", "SLOW_TASK_SECONDS": "60",
               "ALERT_COOLDOWN_MINUTES": "60",
               "ALERT_STATE_FILE": os.path.join(self.tmp.name, "state.json")}
        self.env = mock.patch.dict(os.environ, env)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()


class TestAlerts(Base):
    @mock.patch("alerting.llm_alert.requests.post")
    def test_raw_alert_first_then_llm_explanation(self, post):
        post.side_effect = [ok({}), ok({"response": "Amount is None in row 2."}), ok({})]
        llm_alert.on_failure({"task_instance": ti(), "exception": TypeError("bad")})
        alert, explanation = texts(post)
        self.assertIn("Task failed", alert)
        self.assertIn("bad", alert)
        self.assertIn("Log: http://airflow/log", alert)
        self.assertIn("Run: scheduled__x", alert)
        self.assertIn("Amount is None in row 2.", explanation)
        self.assertIn("can be wrong", explanation)

    @mock.patch("alerting.llm_alert.requests.post")
    def test_llm_down_still_sends_raw_alert(self, post):
        post.side_effect = [ok({}), requests.ConnectionError("refused")]
        llm_alert.on_failure({"task_instance": ti(), "exception": ValueError("boom")})
        sent = texts(post)
        self.assertEqual(len(sent), 1)
        self.assertIn("boom", sent[0])

    @mock.patch("alerting.llm_alert.requests.post")
    def test_cooldown_suppresses_repeat_alerts_per_dag(self, post):
        post.return_value = ok({"response": "x"})
        llm_alert.on_failure({"task_instance": ti(), "exception": ValueError("1")})
        calls_after_first = post.call_count
        llm_alert.on_failure({"task_instance": ti(), "exception": ValueError("2")})
        self.assertEqual(post.call_count, calls_after_first)       # same DAG: suppressed
        llm_alert.on_failure({"task_instance": ti(dag_id="other"), "exception": ValueError("3")})
        self.assertGreater(post.call_count, calls_after_first)     # other DAG: alerts

    @mock.patch("alerting.llm_alert.requests.post")
    def test_cooldown_disabled(self, post):
        post.return_value = ok({"response": "x"})
        with mock.patch.dict(os.environ, {"ALERT_COOLDOWN_MINUTES": "0"}):
            llm_alert.on_failure({"task_instance": ti(), "exception": ValueError("1")})
            first = post.call_count
            llm_alert.on_failure({"task_instance": ti(), "exception": ValueError("2")})
        self.assertEqual(post.call_count, 2 * first)

    def test_empty_state_file_var_uses_default(self):
        with mock.patch.dict(os.environ, {"ALERT_STATE_FILE": ""}):
            self.assertTrue(str(llm_alert._state_file()).endswith("airflow_llm_alert_state.json"))

    def test_cooldown_expires(self):
        llm_alert.mark_alerted("d:task failed", now=1000)
        self.assertTrue(llm_alert.in_cooldown("d:task failed", now=1000 + 59 * 60))
        self.assertFalse(llm_alert.in_cooldown("d:task failed", now=1000 + 61 * 60))

    @mock.patch("alerting.llm_alert.requests.post")
    def test_fast_task_sends_nothing(self, post):
        llm_alert.on_success_check_slow({"task_instance": ti(duration=10)})
        post.assert_not_called()

    @mock.patch("alerting.llm_alert.requests.post")
    def test_slow_task_alerts(self, post):
        post.side_effect = [ok({}), ok({"response": "Upstream table grew."}), ok({})]
        llm_alert.on_success_check_slow({"task_instance": ti(duration=300)})
        self.assertIn("Task slow", texts(post)[0])

    @mock.patch("alerting.llm_alert.requests.post")
    def test_no_telegram_config_does_not_crash(self, post):
        with mock.patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": ""}):
            self.assertFalse(llm_alert.send_telegram("hi"))
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
