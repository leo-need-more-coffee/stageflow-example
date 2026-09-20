"""HTTP API for running a pipeline with step debugging.

The editor does not execute pipelines itself, and should not: the execution
semantics live in the StageFlow core (CEL, types, try/except, parallel), and a
second implementation in JavaScript would drift — debugging would show
something other than what actually happens. So the UI here is just a client:
the run happens in a real ``Session``, and the core debugger (``StepDebugger``)
exposes the stop point, the frame and the ability to accept edits.

API (JSON, one run = one "debug session"):

    POST   /api/run                {pipeline, vars, mode, delay, secrets} -> {id, state}
    GET    /api/run/<id>           -> run state
    GET    /api/run/<id>/events    -> event stream (SSE), ?from=N — from the Nth
    POST   /api/run/<id>/control   {action: step|resume|pause|stop, count, delay}
    POST   /api/run/<id>/vars      {set: {...}, drop: [...]} -> state

The events in the stream are what a debugger needs to show: `node_enter` /
`node_exit` with the frame, `paused` (stopped, waiting for a step), session
telemetry (`stage_started`, `stage_failed`, `condition_evaluated`, …),
`var_set` / `var_rejected`, `finished` with the result and artifacts, or
`failed` with the error.

The execution thread is separate, with its own event loop: HTTP handlers live
in the server threads, so commands go through the thread-safe methods of the
debugger, and events through :class:`EventBus`.
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from stageflow import Context, Pipeline, Session, StepDebugger

MAX_RUNS = 8  # how many runs we keep in memory (debugging, not production)

SECRET_MASK = "•" * 8  # the same the editor shows (secrets.js)

#: Where the server takes secrets from: comma-separated names in ``SF_SECRETS``
#: and/or variables prefixed with ``SF_SECRET_`` (the name is what follows the
#: prefix). An explicit list rather than "the whole environment": handing the
#: browser a list of every variable on the machine is a bad idea by itself.
SECRET_PREFIX = "SF_SECRET_"


def env_secret_names() -> list[str]:
    """Names of the secrets available to the server. Names only: values stay here."""
    names = {
        part.strip()
        for part in (os.environ.get("SF_SECRETS") or "").split(",")
        if part.strip() and part.strip() in os.environ
    }
    names.update(
        key[len(SECRET_PREFIX):]
        for key in os.environ
        if key.startswith(SECRET_PREFIX) and key != SECRET_PREFIX
    )
    return sorted(names)


def env_secret_value(name: str) -> str | None:
    """Value of an environment secret — under the prefixed name or as it is.

    Only ever called for a name the server itself declared a secret (see
    ``RunManager.start``): otherwise a run request could name any variable of
    the server environment and read it back out of the starting frame.
    """
    prefixed = os.environ.get(SECRET_PREFIX + name)
    return prefixed if prefixed is not None else os.environ.get(name)


class SecretMasker:
    """Scrubs secret values out of everything that goes to the browser.

    The run sees the key when it has to — a stage needs it — but the debugger
    shows the FRAME and the event log, and without this the key would be back
    in the page with the very first `node_enter`, and from there in a
    screenshot. We hide by two signs: by variable name (in the frame
    dictionaries) and by the value itself (stage arguments in telemetry — the
    name there is already a different one).
    """

    def __init__(self, names: set[str], values: set[str]) -> None:
        self._names = {n for n in names if n}
        # an empty string would match everything, a one-character key almost so
        self._values = {v for v in values if isinstance(v, str) and len(v) >= 4}

    def __bool__(self) -> bool:
        return bool(self._names or self._values)

    def __call__(self, data: Any, key: str | None = None) -> Any:
        if key is not None and key in self._names:
            return SECRET_MASK
        if isinstance(data, str):
            return SECRET_MASK if data in self._values else data
        if isinstance(data, dict):
            return {k: self(v, k if isinstance(k, str) else None) for k, v in data.items()}
        if isinstance(data, (list, tuple)):
            return [self(item) for item in data]
        return data


class EventBus:
    """The event log of a run, with subscribers.

    A log rather than a plain queue: a subscriber arrives over a separate HTTP
    request after the start, and without history it would miss the beginning —
    and in debugging the beginning is the interesting part.
    """

    def __init__(self, mask: Any = None) -> None:
        self._items: list[dict] = []
        self._cond = threading.Condition()
        self._closed = False
        self.mask = mask

    @property
    def size(self) -> int:
        return len(self._items)

    def push(self, event: dict) -> None:
        if self.mask:
            event = self.mask(event)
        with self._cond:
            # the event number is part of the event itself: a subscriber can
            # read the stream from where it broke off (`?from=N`) without
            # consulting anything else
            self._items.append({**event, "index": len(self._items)})
            self._cond.notify_all()

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def history(self, start: int = 0) -> list[dict]:
        with self._cond:
            return self._items[start:]

    def follow(self, start: int = 0, timeout: float = 30.0):
        """Generator: yields events from ``start``, waits for new ones, ends
        together with the run. ``timeout`` is how often to yield "nothing" so
        that the connection is not taken for a hung one."""
        index = start
        while True:
            with self._cond:
                while index >= len(self._items) and not self._closed:
                    if not self._cond.wait(timeout):
                        break
                if index >= len(self._items):
                    if self._closed:
                        return
                    yield None  # keep-alive
                    continue
                batch = self._items[index:]
                index = len(self._items)
            for event in batch:
                yield event


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

        # Secrets: values from the server environment are substituted here and
        # never reach the browser at all — the page sends names only. Local
        # ones (typed into the editor) arrive as ordinary starting variables;
        # the server does not store them, but knows they are secret and scrubs
        # them out of the events.
        secrets = payload.get("secrets") or {}
        start_vars = dict(payload.get("vars") or {})
        # only names the server itself declared a secret: a request must not be
        # able to read an arbitrary environment variable back out of the frame
        allowed_env = set(env_secret_names())
        for name in secrets.get("env") or []:
            if str(name) not in allowed_env:
                continue
            value = env_secret_value(str(name))
            if value is not None:
                start_vars[str(name)] = value
        secret_names = {str(n) for n in (secrets.get("names") or [])}
        masker = SecretMasker(
            names=secret_names,
            values={start_vars[n] for n in secret_names if isinstance(start_vars.get(n), str)},
        )
        context = Context(vars=start_vars)

        with self._lock:
            self._counter += 1
            run_id = f"run-{self._counter}"
            self._evict_locked()

        bus = EventBus(mask=masker if masker else None)
        debugger = StepDebugger(mode=mode, delay=delay, on_event=bus.push)
        session = Session(
            id=run_id,
            pipeline=pipeline,
            context=context,
            event_handler=lambda event: bus.push(_event_to_dict(event)),
            debugger=debugger,
        )
        run = Run(id=run_id, session=session, debugger=debugger, bus=bus,
                  mask=masker if masker else None)
        run.thread = threading.Thread(target=_run_session, args=(run,), daemon=True)

        with self._lock:
            self._runs[run_id] = run
        run.thread.start()
        return run

    def get(self, run_id: str) -> Run | None:
        with self._lock:
            return self._runs.get(run_id)

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


def _event_to_dict(event: Any) -> dict:
    """Session telemetry in a shape fit for the event stream."""
    return {
        "type": event.type,
        "node": event.stage_id,
        "session": event.session_id,
        "payload": event.payload or {},
    }


def _run_session(run: Run) -> None:
    """The body of the run thread: its own event loop for the whole run."""
    try:
        result = asyncio.run(run.session.run())
        run.result = result.result
        run.artifacts = _jsonable(result.artifacts)
        run.status = "stopped" if (result.result or {}).get("result") == "stopped" else "finished"
        run.bus.push({
            "type": "finished",
            "status": run.status,
            "result": run.result,
            "artifacts": run.artifacts,
            "vars": _jsonable(dict(result.context.vars)),
        })
    except Exception as exc:  # noqa: BLE001 - a failed run is a result too
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}"
        run.bus.push({
            "type": "failed",
            "error": run.error,
            "node": run.debugger.node,
        })
    finally:
        run.bus.close()


def _jsonable(value: Any) -> Any:
    """Frame values can be anything (a stage object, a set) — what survives
    JSON goes into the event stream as it is, the rest as a string."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def sse_lines(bus: EventBus, start: int = 0):
    """SSE frames for the event stream: ``data: {...}`` or a heartbeat comment."""
    for event in bus.follow(start):
        if event is None:
            yield b": keep-alive\n\n"
            continue
        payload = json.dumps(_jsonable(event), ensure_ascii=False)
        yield f"data: {payload}\n\n".encode()
