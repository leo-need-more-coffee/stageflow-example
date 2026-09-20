"""Running a pipeline with step debugging: one run = one debug session.

The editor does not execute pipelines itself, and should not: the execution
semantics live in the StageFlow core (CEL, types, try/except, parallel), and a
second implementation in JavaScript would drift — debugging would show
something other than what actually happens. So the UI is just a client: the
run happens in a real ``Session``, and the core debugger (``StepDebugger``)
exposes the stop point, the frame and the ability to accept edits.

Every run gets a thread with its own event loop: the HTTP handlers live in the
server's, so commands go through the thread-safe methods of the debugger and
events come back through :class:`app.events.EventBus`.
"""
from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from stageflow import Context, Pipeline, Session, StepDebugger

from app.events import EventBus, jsonable, telemetry_to_dict
from app.secrets import Masker, env_secret_names, env_secret_value

MAX_RUNS = 8  # how many runs we keep in memory (debugging, not production)


@dataclass
class Run:
    """One run: a session in its own thread plus the debugger."""

    id: str
    session: Session
    debugger: StepDebugger
    bus: EventBus
    status: str = "running"          # running | finished | failed | stopped
    result: dict | None = None
    artifacts: dict | None = None
    error: str | None = None
    started: float = field(default_factory=time.monotonic)
    thread: threading.Thread | None = None
    mask: Any = None                 # scrubs secrets out of state and events

    @property
    def done(self) -> bool:
        return self.status != "running"

    def state(self) -> dict:
        # only the content of the run is masked — frame, result, artifacts:
        # the bookkeeping fields of the state must not fall under a secret
        # name, even if someone called their key `status`
        mask = self.mask or (lambda value, key=None: value)
        debug = dict(self.debugger.state)
        if "vars" in debug:
            debug["vars"] = mask(debug["vars"])
        return {
            "id": self.id,
            "status": self.status,
            "result": mask(self.result),
            "artifacts": mask(self.artifacts),
            "error": self.error,
            "events": self.bus.size,
            **debug,
        }


class RunManager:
    """Keeps the runs and creates new ones. One process — one manager."""

    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}
        self._lock = threading.Lock()
        self._counter = 0

    # -------------------------------------------------------------- create

    def start(self, payload: dict) -> Run:
        pipeline = Pipeline.from_dict(payload.get("pipeline") or {})
        pipeline.validate()  # description errors answer right away, not in a thread

        mode = "step" if payload.get("mode") == "step" else "run"
        delay = float(payload.get("delay") or 0)
        start_vars, mask = self._starting_frame(payload)

        with self._lock:
            self._counter += 1
            run_id = f"run-{self._counter}"
            self._evict_locked()

        bus = EventBus(mask=mask)
        debugger = StepDebugger(mode=mode, delay=delay, on_event=bus.push)
        session = Session(
            id=run_id,
            pipeline=pipeline,
            context=Context(vars=start_vars),
            event_handler=lambda event: bus.push(telemetry_to_dict(event)),
            debugger=debugger,
        )
        run = Run(id=run_id, session=session, debugger=debugger, bus=bus, mask=mask)
        run.thread = threading.Thread(target=self._execute, args=(run,), daemon=True)

        with self._lock:
            self._runs[run_id] = run
        run.thread.start()
        return run

    def get(self, run_id: str) -> Run | None:
        with self._lock:
            return self._runs.get(run_id)

    @staticmethod
    def _starting_frame(payload: dict) -> tuple[dict, Masker | None]:
        """The variables a run starts with, and what to hide on the way back.

        Values from the server environment are substituted here and never
        reach the browser at all — the page sends names only. Local secrets
        (typed into the editor) arrive as ordinary starting variables; the
        server does not store them, but knows they are secret and scrubs them
        out of the events.
        """
        secrets = payload.get("secrets") or {}
        start_vars = dict(payload.get("vars") or {})
        # only names the server itself declared a secret: a request must not be
        # able to read an arbitrary environment variable back out of the frame
        allowed = set(env_secret_names())
        for name in secrets.get("env") or []:
            value = env_secret_value(str(name)) if str(name) in allowed else None
            if value is not None:
                start_vars[str(name)] = value

        names = {str(n) for n in (secrets.get("names") or [])}
        masker = Masker(
            names=names,
            values={start_vars[n] for n in names if isinstance(start_vars.get(n), str)},
        )
        return start_vars, masker or None

    def _evict_locked(self) -> None:
        """Keep only the latest runs: debug sessions pile up quickly."""
        finished = [r for r in self._runs.values() if r.done]
        finished.sort(key=lambda r: r.started)
        while len(self._runs) - len(finished) + 1 > MAX_RUNS and finished:
            self._runs.pop(finished.pop(0).id, None)

    # ------------------------------------------------------------- control

    def control(self, run: Run, payload: dict) -> dict:
        action = payload.get("action")
        if action == "step":
            run.debugger.step(int(payload.get("count") or 1))
        elif action == "resume":
            run.debugger.resume()
        elif action == "pause":
            run.debugger.pause()
        elif action == "stop":
            run.session.stop()
            run.debugger.resume()  # release the step wait, or the stop never lands
        elif action == "delay":
            run.debugger.set_delay(float(payload.get("delay") or 0))
        else:
            raise ValueError(f"Unknown action: {action!r}")
        return run.state()

    def set_vars(self, run: Run, payload: dict) -> dict:
        values = payload.get("set") or {}
        drop = payload.get("drop") or []
        if not isinstance(values, dict) or not isinstance(drop, list):
            raise ValueError("expected {\"set\": {...}, \"drop\": [...]}")
        run.debugger.set_vars(values, drop)
        return run.state()

    # ------------------------------------------------------------- the run

    @staticmethod
    def _execute(run: Run) -> None:
        """The body of the run thread: its own event loop for the whole run."""
        try:
            result = asyncio.run(run.session.run())
            run.result = result.result
            run.artifacts = jsonable(result.artifacts)
            stopped = (result.result or {}).get("result") == "stopped"
            run.status = "stopped" if stopped else "finished"
            run.bus.push({
                "type": "finished",
                "status": run.status,
                "result": run.result,
                "artifacts": run.artifacts,
                "vars": jsonable(dict(result.context.vars)),
            })
        except Exception as exc:  # noqa: BLE001 - a failed run is a result too
            run.status = "failed"
            run.error = f"{type(exc).__name__}: {exc}"
            run.bus.push({"type": "failed", "error": run.error, "node": run.debugger.node})
        finally:
            run.bus.close()
