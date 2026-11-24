"""System: the backend as the editor sees it — a server on a port, over HTTP.

A real `uvicorn` is started in its own process with tokens in its
environment, and everything below goes through sockets: the questions asked
before a run, a run started and followed over the SSE stream, a debugging
session stepped by commands, and the things a second tenant must not be able
to do. Nothing is imported from `app`; this is a client.
"""
import json
import os
import socket
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASIC, PRO, OTHER = "tok-basic", "tok-pro", "tok-other"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Client:
    def __init__(self, base: str, token: str | None):
        self.base = base
        self.token = token

    def call(self, method: str, path: str, body=None, headers=None):
        """(status, decoded JSON or text, headers) — errors included."""
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data=data, method=method)
        if body is not None:
            request.add_header("Content-Type", "application/json")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        for name, value in (headers or {}).items():
            request.add_header(name, value)
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status, raw, got = response.status, response.read(), response.headers
        except urllib.error.HTTPError as exc:
            with exc:
                status, raw, got = exc.code, exc.read(), exc.headers
        text = raw.decode()
        try:
            return status, json.loads(text), got
        except ValueError:
            return status, text, got

    def get(self, path, **kw):
        return self.call("GET", path, **kw)

    def post(self, path, body, **kw):
        return self.call("POST", path, body=body, **kw)

    def events(self, run_id: str, until: str = "finished", limit: float = 20) -> list[dict]:
        """Read the SSE stream of a run until an event of type `until`."""
        request = urllib.request.Request(f"{self.base}/api/run/{run_id}/events")
        request.add_header("Authorization", f"Bearer {self.token}")
        seen = []
        deadline = time.monotonic() + limit
        with urllib.request.urlopen(request, timeout=limit) as stream:
            for line in stream:
                if time.monotonic() > deadline:
                    break
                line = line.decode().strip()
                if not line.startswith("data: "):
                    continue
                event = json.loads(line[len("data: "):])
                seen.append(event)
                if event["type"] in (until, "failed"):
                    break
        return seen


def rules_only() -> dict:
    return json.loads((ROOT / "pipelines" / "06-rules-only.json").read_text("utf-8"))


class BackendOverHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        port = free_port()
        env = {**os.environ,
               "SF_TOKENS": f"{BASIC}:basic,{PRO}:pro,{OTHER}:pro",
               "SF_PORT": str(port), "SF_HOST": "127.0.0.1",
               "SF_ALLOW_ORIGIN": "https://editor.example",
               "SF_MAX_RUNS_PER_CALLER": "2", "SF_MAX_ACTIVE_RUNS": "6"}
        cls.server = subprocess.Popen(
            [sys.executable, "main.py"], cwd=ROOT, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        cls.base = f"http://127.0.0.1:{port}"
        for _ in range(200):
            try:
                urllib.request.urlopen(cls.base + "/openapi.json", timeout=1)
                break
            except (urllib.error.URLError, ConnectionError):
                if cls.server.poll() is not None:
                    raise RuntimeError(cls.server.stdout.read())
                time.sleep(0.05)
        else:
            cls.server.kill()
            raise RuntimeError("the server did not come up")
        cls.basic = Client(cls.base, BASIC)
        cls.pro = Client(cls.base, PRO)
        cls.other = Client(cls.base, OTHER)
        cls.anonymous = Client(cls.base, None)

    @classmethod
    def tearDownClass(cls):
        cls.server.terminate()
        try:
            cls.server.wait(10)
        except subprocess.TimeoutExpired:  # pragma: no cover
            cls.server.kill()
        cls.server.stdout.close()

    def start(self, client, mode="run", ticket="T-1001", pipeline=None, **extra):
        status, body, _ = client.post("/api/run", {
            "pipeline": pipeline or rules_only(), "mode": mode,
            "vars": {"ticket_id": ticket}, **extra})
        return status, body

    def stop(self, client, run_id):
        client.post(f"/api/run/{run_id}/control", {"action": "stop"})

    # ------------------------------------------------- before a run

    def test_a_credential_is_required(self):
        status, body, _ = self.anonymous.get("/api/meta")
        self.assertEqual(status, 401)
        self.assertIn("no credentials", body["error"])
        status, _, _ = Client(self.base, "forged").get("/api/stages")
        self.assertEqual(status, 401)

    def test_meta_answers_for_the_caller_or_for_a_plan_asked_about(self):
        status, own, _ = self.basic.get("/api/meta")
        self.assertEqual((status, own["plan"], own["plan_source"]), (200, "basic", "token"))
        _, shown, _ = self.basic.get("/api/meta?plan=pro")
        self.assertEqual((shown["plan"], shown["plan_source"]), ("pro", "query"))
        status, body, _ = self.basic.get("/api/meta?plan=gold")
        self.assertEqual(status, 400)

    def test_the_palette_is_narrowed_by_the_plan(self):
        _, basic, _ = self.basic.get("/api/stages")
        _, pro, _ = self.pro.get("/api/stages")
        self.assertNotIn("LlmTriageStage", basic["stages"])
        self.assertIn("LoadTicketStage", basic["stages"])
        self.assertLess(set(basic["stages"]), set(pro["stages"]))

    def test_cors_lets_the_editor_origin_in(self):
        status, _, headers = self.anonymous.call("OPTIONS", "/api/run", headers={
            "Origin": "https://editor.example",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type,authorization"})
        self.assertEqual(status, 200)
        self.assertEqual(headers["access-control-allow-origin"], "https://editor.example")

    def test_static_pipelines_are_served_uncached(self):
        status, body, headers = self.anonymous.get("/pipelines/06-rules-only.json")
        self.assertEqual(status, 200)
        self.assertEqual(body["nodes"], rules_only()["nodes"])
        self.assertEqual(headers["cache-control"], "no-store")

    # ------------------------------------------------- a run

    def test_a_run_from_start_to_finish_over_the_stream(self):
        status, started = self.start(self.basic)
        self.assertEqual(status, 201)
        events = self.basic.events(started["id"])
        types = [e["type"] for e in events]
        self.assertEqual(types[-1], "finished", events[-1])
        self.assertIn("node_enter", types)
        self.assertEqual([e["index"] for e in events], list(range(len(events))))
        _, state, _ = self.basic.get(f"/api/run/{started['id']}")
        self.assertEqual(state["status"], "finished")
        self.assertGreater(state["meters"]["steps"], 0)
        self.assertEqual(state["limits"]["steps"], 200, "the basic plan's ceiling")

    def test_a_debugging_session_moves_only_when_told(self):
        _, started = self.start(self.basic, mode="step")
        run_id = started["id"]
        try:
            state = self._wait(self.basic, run_id, lambda s: s.get("waiting"))
            first = state["node"]
            time.sleep(0.2)
            _, still, _ = self.basic.get(f"/api/run/{run_id}")
            self.assertEqual(still["node"], first, "nothing moves without a command")

            self.basic.post(f"/api/run/{run_id}/control", {"action": "step"})
            moved = self._wait(self.basic, run_id,
                               lambda s: s.get("waiting") and s["node"] != first)
            self.assertNotEqual(moved["node"], first)

            self.basic.post(f"/api/run/{run_id}/control", {"action": "resume"})
            done = self._wait(self.basic, run_id, lambda s: s["status"] != "running")
            self.assertEqual(done["status"], "finished")
        finally:
            self.stop(self.basic, run_id)

    def test_a_bad_command_is_a_clear_400(self):
        _, started = self.start(self.basic, mode="step")
        try:
            status, body, _ = self.basic.post(f"/api/run/{started['id']}/control",
                                              {"action": "dance"})
            self.assertEqual(status, 400)
            self.assertIn("action", body["error"])
        finally:
            self.stop(self.basic, started["id"])

    # ------------------------------------------------- what is refused

    def test_a_graph_drawn_for_another_plan_is_refused_by_name(self):
        status, body = self.start(self.basic, plan="pro")
        self.assertEqual(status, 403)
        self.assertIn("'pro'", body["error"])

    def test_a_stage_outside_the_plan_is_refused_before_running(self):
        graph = json.loads((ROOT / "pipelines" / "01-triage.json").read_text("utf-8"))
        status, body = self.start(self.basic, pipeline=graph)
        self.assertEqual(status, 400)
        self.assertIn("not allowed by the policy", body["error"])

    def test_another_tenant_cannot_see_or_drive_a_run(self):
        _, started = self.start(self.pro, mode="step")
        run_id = started["id"]
        try:
            for method, path, body in (
                ("GET", f"/api/run/{run_id}", None),
                ("GET", f"/api/run/{run_id}/events", None),
                ("POST", f"/api/run/{run_id}/control", {"action": "stop"}),
                ("POST", f"/api/run/{run_id}/vars", {"set": {"x": 1}}),
            ):
                with self.subTest(method=method, path=path):
                    status, answer, _ = self.other.call(method, path, body=body)
                    self.assertEqual(status, 404)
                    self.assertEqual(answer["error"], "run not found")
            _, mine, _ = self.pro.get(f"/api/run/{run_id}")
            self.assertEqual(mine["status"], "running", "the run was not touched")
        finally:
            self.stop(self.pro, run_id)

    def test_one_credential_may_hold_two_runs(self):
        held = []
        try:
            for _ in range(2):
                status, started = self.start(self.basic, mode="step")
                self.assertEqual(status, 201)
                held.append(started["id"])
            status, body = self.start(self.basic, mode="step")
            self.assertEqual(status, 429)
            self.assertIn("may have 2", body["error"])
            status, other = self.start(self.pro, mode="step")
            self.assertEqual(status, 201, "the limit is per credential")
            held.append(("pro", other["id"]))
        finally:
            for run in held:
                if isinstance(run, tuple):
                    self.stop(self.pro, run[1])
                else:
                    self.stop(self.basic, run)
            for run in held:
                client, run_id = ((self.pro, run[1]) if isinstance(run, tuple)
                                  else (self.basic, run))
                self._wait(client, run_id, lambda s: s["status"] != "running")

    # ------------------------------------------------- helpers

    def _wait(self, client, run_id, condition, limit=10.0):
        deadline = time.monotonic() + limit
        while time.monotonic() < deadline:
            _, state, _ = client.get(f"/api/run/{run_id}")
            if condition(state):
                return state
            time.sleep(0.02)
        self.fail(f"run {run_id} never reached the state; last: {state}")


if __name__ == "__main__":
    unittest.main()
