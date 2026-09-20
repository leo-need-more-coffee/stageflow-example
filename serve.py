"""An example StageFlow backend for the StageFlow editor.

    python serve.py [port]

The editor (github.com/leo-need-more-coffee/stageflow-ui) is pure static
front-end: it holds no stage registry and executes nothing. Everything it
needs it asks a backend for, over the URL the user types on the connection
screen. This is such a backend, in about two hundred lines:

  * ``GET /api/stages`` — live stage specs from the StageFlow core;
  * ``GET /api/secrets`` — NAMES of the secrets in the server environment
    (``SF_SECRETS=A,B`` and/or ``SF_SECRET_A=...``); values are never handed
    out — the run substitutes them;
  * ``/api/run…`` — running a pipeline with step debugging (``runner_api.py``);
  * the static files of this folder — the SVG icons the demo stages refer to
    and the ready-made pipelines in ``pipelines/``.

Pipelines are executed by the REAL core: a second implementation of the
semantics in JavaScript would mean the debugger shows something other than
what actually happens.

The editor is served from a different origin, so every answer carries CORS
headers (``SF_ALLOW_ORIGIN`` narrows them down from the default ``*``).

It also registers a few demo stages whose icons are links to the SVG files in
./icons — to show that `icon` in a docstring takes more than a glyph. Next
door, in ``llm_stages.py``, live stages that really do call the OpenAI API and
the network: the demo pipeline ``pipelines/llm-demo-pipeline.json`` is built on
them.
"""
import importlib.util
import json
import os
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parent

#: Which origin the editor is served from. ``*`` by default: this is a local
#: example backend, and the editor may be opened from a file, a container or a
#: colleague's machine. Narrow it down with SF_ALLOW_ORIGIN when the backend
#: stops being a local example.
ALLOW_ORIGIN = os.environ.get("SF_ALLOW_ORIGIN", "*")

#: What to bind to. Loopback by default — an example backend with a run API
#: has no business being reachable from the network without being asked.
HOST = os.environ.get("SF_HOST", "127.0.0.1")

# StageFlow core: an installed package (pip install stageflow-framework) wins;
# a sibling checkout is the fallback, so both repositories cloned side by side
# run without installing anything.
if importlib.util.find_spec("stageflow") is None:
    sys.path.insert(0, str(ROOT.parent / "stageflow"))

from stageflow import BaseStage, register_stage  # noqa: E402

from runner_api import RunManager, env_secret_names, sse_lines  # noqa: E402

# Real stages (OpenAI API and HTTP) — registered by the import itself.
# Without the `openai` SDK the editor must still open: the llm_stages stages
# simply will not be in the registry, and the demo pipeline built on them will
# not run.
try:
    import llm_stages  # noqa: E402,F401
except ImportError as exc:  # pragma: no cover - depends on the environment
    print(f"LLM stages disabled ({exc}); install the SDK: pip install openai")

RUNS = RunManager()


@register_stage("HttpRequestStage")
class HttpRequestStage(BaseStage):
    """
    description: "Demo stage: the icon is a link to an SVG file"
    icon: "/icons/globe.svg"
    icon_mono: true
    arguments:
      url:
        type: string
        description: "Request address"
    outputs:
      body:
        type: any
        description: "Response body"
    """

    category = "demo.io"

    async def run(self):
        self.set_outputs({"body": {"demo": True}})


@register_stage("QueryDbStage")
class QueryDbStage(BaseStage):
    """
    description: "Demo stage: an SVG icon from a local folder"
    icon: "/icons/database.svg"
    icon_mono: true
    arguments:
      query:
        type: string
        description: "SQL query"
    outputs:
      rows:
        type: list
        description: "Result rows"
    """

    category = "demo.io"

    async def run(self):
        self.set_outputs({"rows": []})


@register_stage("LlmCompleteStage")
class LlmCompleteStage(BaseStage):
    """
    description: "Demo stage: own icon and an explicit accent color"
    icon: "/icons/sparkles.svg"
    icon_mono: true
    color: "#c084fc"
    arguments:
      prompt:
        type: string
        description: "Prompt for the model"
    outputs:
      completion:
        type: string
        description: "Model answer"
    """

    category = "demo.llm"

    async def run(self):
        self.set_outputs({"completion": ""})


class Handler(SimpleHTTPRequestHandler):
    extensions_map = {**SimpleHTTPRequestHandler.extensions_map, ".svg": "image/svg+xml"}

    def end_headers(self):  # noqa: N802 - SimpleHTTPRequestHandler API
        # The editor lives on another origin (its own static server), so
        # without these headers every fetch and the SSE stream would be blocked
        # by the browser before reaching us.
        self.send_header("Access-Control-Allow-Origin", ALLOW_ORIGIN)
        self.send_header("Vary", "Origin")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_OPTIONS(self):  # noqa: N802 - SimpleHTTPRequestHandler API
        # the preflight of a POST with a JSON body: without an answer to it the
        # browser never sends the run request itself
        self.send_response(204)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ----------------------------------------------------------------- GET

    def do_GET(self):  # noqa: N802 - SimpleHTTPRequestHandler API
        path = urlsplit(self.path).path
        if path == "/api/stages":
            return self._stages()
        if path == "/api/secrets":
            # names ONLY: the values of environment secrets stay on the server
            # and are substituted into the starting frame of a run (see
            # runner_api.py)
            return self._json({"names": env_secret_names(), "source": "env"})
        if path.startswith("/api/run/"):
            parts = path[len("/api/run/"):].split("/")
            run = RUNS.get(parts[0])
            if run is None:
                return self._json({"error": "run not found"}, 404)
            if len(parts) == 1:
                return self._json(run.state())
            if parts[1:] == ["events"]:
                query = parse_qs(urlsplit(self.path).query)
                return self._events(run, int((query.get("from") or ["0"])[0]))
            return self._json({"error": "unknown path"}, 404)
        return super().do_GET()

    # ---------------------------------------------------------------- POST

    def do_POST(self):  # noqa: N802 - SimpleHTTPRequestHandler API
        path = urlsplit(self.path).path
        try:
            if path == "/api/run":
                run = RUNS.start(self._body())
                return self._json({"id": run.id, "state": run.state()}, 201)
            if path.startswith("/api/run/"):
                run_id, _, action = path[len("/api/run/"):].partition("/")
                run = RUNS.get(run_id)
                if run is None:
                    return self._json({"error": "run not found"}, 404)
                if action == "control":
                    return self._json(RUNS.control(run, self._body()))
                if action == "vars":
                    return self._json(RUNS.set_vars(run, self._body()))
            return self._json({"error": "unknown path"}, 404)
        except Exception as exc:  # noqa: BLE001 - an answer instead of a traceback
            # a broken pipeline description or command is an answer to the
            # client, not a server failure: the editor shows it in the status bar
            return self._json({"error": f"{type(exc).__name__}: {exc}"}, 400)

    # ------------------------------------------------------------- answers

    def _stages(self):
        from stageflow import get_stages
        self._json({"stages": {name: cls.get_specs() for name, cls in get_stages().items()}})

    def _json(self, payload, code=200):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        return json.loads(self.rfile.read(length) or b"{}")

    def _events(self, run, start: int):
        """Event stream of a run (SSE). The connection lives until the run ends
        or the client goes away."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.end_headers()
        try:
            for chunk in sse_lines(run.bus, start):
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # the client closed the tab — business as usual

    def log_message(self, fmt, *args):  # noqa: N802 - SimpleHTTPRequestHandler API
        pass  # the event stream would otherwise flood the console


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("SF_PORT", 8765))
    print(f"StageFlow example backend: http://{HOST}:{port}")
    print("Type that address on the connection screen of the editor.")
    ThreadingHTTPServer(
        (HOST, port), partial(Handler, directory=str(ROOT))
    ).serve_forever()
