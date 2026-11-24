"""Runs that wait for an agent: `07-agent-review.json` through `RunManager`.

The run manager is used directly, with a fresh SQLite file per test; the
same flow over HTTP is in `test_system_http.py`.
"""
import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import app.stages  # noqa: F401 - registers the stages
from app.plans import policy_for
from app.runs import RunManager, RunNotWaiting

ROOT = Path(__file__).resolve().parents[1]
REVIEW = json.loads((ROOT / "pipelines" / "07-agent-review.json").read_text("utf-8"))


def settled(run, limit=5.0):
    """Wait until the run's thread is done with this stretch."""
    deadline = time.monotonic() + limit
    while run.status == "running" or (run.thread and run.thread.is_alive()):
        if time.monotonic() > deadline:
            raise AssertionError(f"run still {run.status}")
        time.sleep(0.01)
    return run


class WaitingRunTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.db = Path(folder.name) / "runs.db"
        self.runs = RunManager(self.db)

    def start(self, plan="basic", **payload):
        body = {"pipeline": REVIEW, "vars": {"ticket_id": "T-1001"}, **payload}
        return settled(self.runs.start(body, policy_for(plan), owner="agent", plan=plan))

    def test_the_draft_waits_for_the_agent_without_taking_a_slot(self):
        run = self.start()
        self.assertEqual(run.status, "waiting")
        self.assertEqual(run.state()["waiting_for"]["input"], "review")
        self.assertEqual(run.state()["waiting_for"]["node"], "review")
        self.assertEqual(self.runs._active_locked(), [])
        self.assertEqual(run.bus._items[-1]["type"], "waiting")

    def test_approved_as_edited_and_sent(self):
        run = self.start()
        self.runs.answer(run, "review", {"verdict": "approve", "text": "Fixed it for you."})
        settled(run)
        self.assertEqual(run.status, "finished")
        self.assertEqual(run.result, {"status": "answered"})
        self.assertEqual(run.artifacts["reply"], "Fixed it for you.")
        self.assertIn("reply_sent", [e["type"] for e in run.bus._items])

    def test_approved_as_drafted(self):
        run = self.start()
        draft = run.session.context.get_var("reply")
        self.runs.answer(run, "review", {"verdict": "approve"})
        self.assertEqual(settled(run).artifacts["reply"], draft)

    def test_taken_over_by_the_agent(self):
        run = self.start()
        self.runs.answer(run, "review", {"verdict": "take over"})
        settled(run)
        self.assertEqual(run.result, {"status": "escalated"})

    def test_one_answer_per_wait(self):
        run = self.start()
        with self.assertRaisesRegex(RunNotWaiting, "waits for 'review', not 'approval'"):
            self.runs.answer(run, "approval", {})
        self.runs.answer(run, "review", {"verdict": "approve"})
        with self.assertRaisesRegex(RunNotWaiting, "not waiting"):
            self.runs.answer(run, "review", {"verdict": "take over"})
        self.assertEqual(settled(run).result, {"status": "answered"})

    def test_a_restart_loses_nothing(self):
        run = self.start()
        spent = run.session.budget.report()["kb_lookups"]

        after_restart = RunManager(self.db)
        self.assertEqual(asyncio.run(after_restart.recover()), 1)
        again = after_restart.get(run.id, owner="agent")
        self.assertEqual(again.status, "waiting")
        self.assertIsNone(after_restart.get(run.id, owner="someone else"))

        after_restart.answer(again, "review", {"verdict": "approve"})
        settled(again)
        self.assertEqual(again.result, {"status": "answered"})
        self.assertEqual(again.session.budget.report()["kb_lookups"], spent)

    def test_nobody_looked_in_a_day(self):
        run = self.start()
        self.assertEqual(asyncio.run(self.runs.wake_due()), [])  # not yet
        later = time.time() + 86_401
        # a day later, for the store and for the core alike
        with mock.patch("stageflow.core.session.time.time", return_value=later):
            woken = asyncio.run(self.runs.wake_due(now=later))
            self.assertEqual(woken, [run.id])
            settled(run)
        self.assertEqual(run.result, {"status": "escalated"})
        self.assertIn("wait_timed_out", [e["type"] for e in run.bus._items])

    def test_a_waiting_run_can_be_stopped_but_not_stepped(self):
        run = self.start()
        with self.assertRaises(RunNotWaiting):
            self.runs.control(run, {"action": "step"})
        self.runs.control(run, {"action": "stop"})
        self.assertEqual(settled(run).status, "stopped")
        saved = asyncio.run(self.runs.store.load(run.id))
        self.assertEqual(saved.state, "finished")

    def test_a_run_that_waits_carries_no_secrets(self):
        with self.assertRaisesRegex(ValueError, "cannot carry secrets"):
            self.runs.start({"pipeline": REVIEW, "secrets": {"names": ["api_key"]}},
                            policy_for("basic"), plan="basic")

    def test_runs_without_a_wait_never_touch_the_disk(self):
        rules = json.loads((ROOT / "pipelines" / "06-rules-only.json").read_text("utf-8"))
        run = settled(self.runs.start({"pipeline": rules, "vars": {"ticket_id": "T-1001"}},
                                      policy_for("basic")))
        self.assertEqual(run.status, "finished")
        self.assertFalse(self.db.exists())


if __name__ == "__main__":
    unittest.main()
