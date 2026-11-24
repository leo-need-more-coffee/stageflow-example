"""Unit: the backend's own pieces — credentials, plans, secrets, the event log.

Each class here is tested on its own, without an HTTP server or a run. The
end-to-end behaviour is in `test_system_http.py`.
"""
import os
import threading
import unittest
from unittest import mock

from starlette.requests import Request

from app import auth
from app.events import EventBus, jsonable, sse_lines, telemetry_to_dict
from app.plans import PLANS, PLUMBING, RULES_ONLY, known, plan_names, policy_for
from app.runs import RunManager
from app.secrets import MASK, Masker, env_secret_names, env_secret_value


def request_with(header: str | None) -> Request:
    headers = [(auth.AUTH_HEADER.lower().encode(), header.encode())] if header else []
    return Request({"type": "http", "headers": headers})


class TokenTableTests(unittest.TestCase):
    def test_pairs_with_known_plans_only(self):
        got = auth._parse_tokens(" a:basic, b:pro ,c:gold, :pro, d:, e")
        self.assertEqual(got, {"a": "basic", "b": "pro"})

    def test_nothing_configured(self):
        self.assertEqual(auth._parse_tokens(""), {})


class CredentialTests(unittest.TestCase):
    def test_bearer_or_bare(self):
        self.assertEqual(auth.credential(request_with("Bearer abc")), "abc")
        self.assertEqual(auth.credential(request_with("bearer   abc ")), "abc")
        self.assertEqual(auth.credential(request_with("abc")), "abc")

    def test_another_scheme_is_kept_whole(self):
        self.assertEqual(auth.credential(request_with("Basic abc")), "Basic abc")

    def test_absent_or_blank(self):
        self.assertIsNone(auth.credential(request_with(None)))
        self.assertIsNone(auth.credential(request_with("   ")))


class CallerTests(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.multiple(auth, TOKENS={"t1": "basic", "t2": "pro"},
                                      ENABLED=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_the_plan_comes_from_the_token(self):
        self.assertEqual(auth.caller_plan(request_with("Bearer t2")), "pro")

    def test_no_token_or_an_unknown_one_is_401(self):
        from fastapi import HTTPException
        for header in (None, "nope"):
            with self.subTest(header=header):
                with self.assertRaises(HTTPException) as caught:
                    auth.caller_plan(request_with(header))
                self.assertEqual(caught.exception.status_code, 401)

    def test_the_caller_id_is_a_digest_of_the_token(self):
        one = auth.caller_id(request_with("t1"), "basic")
        self.assertEqual(one, auth.caller_id(request_with("Bearer t1"), "basic"))
        self.assertNotEqual(one, auth.caller_id(request_with("t2"), "pro"))
        self.assertEqual(len(one), 64)
        self.assertNotIn("t1", one)

    def test_without_tokens_everyone_is_full_and_nobody(self):
        with mock.patch.multiple(auth, TOKENS={}, ENABLED=False):
            self.assertEqual(auth.caller_plan(request_with(None)), "full")
            self.assertIsNone(auth.caller_id(request_with(None), "full"))

    def test_where_a_plan_came_from(self):
        self.assertEqual(auth.source_of("pro"), "query")
        self.assertEqual(auth.source_of(None), "token")


class PlanTests(unittest.TestCase):
    def test_every_stage_a_plan_names_is_registered(self):
        # a typo in a plan's list would make a stage silently unavailable
        from stageflow import get_stages
        import app.stages  # noqa: F401 - registers them
        missing = (RULES_ONLY | PLUMBING) - set(get_stages())
        self.assertEqual(missing, set())

    def test_names(self):
        self.assertEqual(plan_names(), ["basic", "full", "pro"])
        self.assertTrue(known("pro"))
        self.assertFalse(known("gold"))
        self.assertFalse(known(None))
        self.assertIs(policy_for("basic"), PLANS["basic"])

    def test_basic_has_no_model_and_pro_does(self):
        self.assertFalse(policy_for("basic").allows_stage("LlmTriageStage"))
        self.assertTrue(policy_for("pro").allows_stage("LlmTriageStage"))
        self.assertFalse(policy_for("basic").allows_node_type("map"))


class SecretTests(unittest.TestCase):
    ENV = {"SF_SECRETS": "PLAIN_KEY,ABSENT", "PLAIN_KEY": "plain-value",
           "SF_SECRET_PREFIXED": "prefixed-value", "UNRELATED": "do-not-leak"}

    def test_names_only_the_declared_ones(self):
        with mock.patch.dict(os.environ, self.ENV, clear=True):
            self.assertEqual(env_secret_names(), ["PLAIN_KEY", "PREFIXED"])
            self.assertEqual(env_secret_value("PREFIXED"), "prefixed-value")
            self.assertEqual(env_secret_value("PLAIN_KEY"), "plain-value")

    def test_a_run_cannot_pull_an_undeclared_variable(self):
        payload = {"vars": {"x": 1},
                   "secrets": {"env": ["PREFIXED", "UNRELATED"], "names": ["PREFIXED"]}}
        with mock.patch.dict(os.environ, self.ENV, clear=True):
            frame, mask = RunManager._starting_frame(payload)
        self.assertEqual(frame, {"x": 1, "PREFIXED": "prefixed-value"})
        self.assertEqual(mask({"PREFIXED": "prefixed-value"}), {"PREFIXED": MASK})


class MaskerTests(unittest.TestCase):
    def setUp(self):
        self.mask = Masker(names={"api_key"}, values={"sk-secret-123", "ab"})

    def test_by_name_and_by_value(self):
        data = {"api_key": "whatever", "copy": "sk-secret-123", "fine": "hello"}
        self.assertEqual(self.mask(data),
                         {"api_key": MASK, "copy": MASK, "fine": "hello"})

    def test_nested(self):
        data = {"args": [{"key": "sk-secret-123"}, ("sk-secret-123", 1)]}
        self.assertEqual(self.mask(data), {"args": [{"key": MASK}, [MASK, 1]]})

    def test_a_value_inside_a_longer_string(self):
        # an error message or a header echoed back carries the key inside it
        got = self.mask({"error": "401 for Authorization: Bearer sk-secret-123"})
        self.assertEqual(got, {"error": f"401 for Authorization: Bearer {MASK}"})

    def test_short_values_are_not_masked(self):
        self.assertEqual(self.mask("ab"), "ab")
        self.assertEqual(self.mask("about"), "about")

    def test_nothing_to_hide_is_falsy(self):
        self.assertFalse(Masker(set(), set()))
        self.assertFalse(Masker({""}, {"abc"}))
        self.assertTrue(Masker({"x"}, set()))


class EventTests(unittest.TestCase):
    def test_jsonable(self):
        self.assertEqual(jsonable({1: (1, {2}), "o": object}),
                         {"1": [1, "{2}"], "o": repr(object)})

    def test_telemetry_shape(self):
        event = mock.Mock(type="stage_started", stage_id="n", session_id="s",
                          payload=None)
        self.assertEqual(telemetry_to_dict(event),
                         {"type": "stage_started", "node": "n", "session": "s",
                          "payload": {}})

    def test_events_are_numbered_and_masked(self):
        bus = EventBus(mask=Masker({"k"}, set()))
        bus.push({"type": "a", "k": "secret"})
        bus.push({"type": "b"})
        bus.close()
        self.assertEqual(list(bus.follow()),
                         [{"type": "a", "k": MASK, "index": 0}, {"type": "b", "index": 1}])
        self.assertEqual(bus.size, 2)

    def test_a_late_subscriber_reads_from_where_it_asks(self):
        bus = EventBus()
        for i in range(5):
            bus.push({"type": "e", "i": i})
        bus.close()
        self.assertEqual([e["i"] for e in bus.follow(start=3)], [3, 4])

    def test_follow_waits_for_new_events_and_ends_with_the_run(self):
        bus = EventBus()
        seen = []
        reader = threading.Thread(target=lambda: seen.extend(bus.follow(timeout=1)))
        reader.start()
        bus.push({"type": "x"})
        bus.close()
        reader.join(2)
        self.assertFalse(reader.is_alive())
        self.assertEqual([e["type"] for e in seen], ["x"])

    def test_keep_alive_when_nothing_happens(self):
        bus = EventBus()
        frames = sse_lines(bus)
        with mock.patch.object(bus._cond, "wait", return_value=False):
            self.assertEqual(next(frames), b": keep-alive\n\n")
        bus.push({"type": "x", "text": "привет"})
        self.assertEqual(next(frames),
                         'data: {"type": "x", "text": "привет", "index": 0}\n\n'.encode())


if __name__ == "__main__":
    unittest.main()
