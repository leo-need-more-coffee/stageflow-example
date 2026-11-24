"""Runs with step debugging: one run is one debug session.

Pipelines run in the real core (`Session` with a `StepDebugger`), not in the
editor, so the debugger shows actual behaviour.

Each run has its own thread and event loop. HTTP handlers run in the server's
loop and use the debugger's thread-safe methods; events come back through
`app.events.EventBus`.
"""
from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from stageflow import Context, Pipeline, Session, Policy, StepDebugger

from app import config
from app.events import EventBus, jsonable, telemetry_to_dict
from app.secrets import Masker, env_secret_names, env_secret_value

MAX_RUNS = 8  # finished runs kept in memory


class RunLimitReached(Exception):
    """Too many runs in progress, for the process or for this caller."""


@dataclass
class Run:
    """One run: a session running in its own thread, with its debugger."""

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
    mask: Any = None                 # hides secrets in state and events
    #: Digest of the credential that started it (`app.auth.caller_id`), or
    #: None without authentication.
    owner: str | None = None
    #: When it was last started, stepped or edited.
    touched: float = field(default_factory=time.monotonic)
    #: Stopped for being idle; not counted while its thread finishes.
    expired: bool = False

    @property
    def done(self) -> bool:
        return self.status != "running"

    def state(self) -> dict:
        # Mask only the run's data (frame, result, artifacts), never the
        # bookkeeping fields, even if a secret is named `status`.
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
            # Spent meters next to their limits, so a client can show "12 of
            # 100" and tell which limit stopped a run. Gauge limits use the
            # report's `peak_*` names.
            "meters": self.session.budget.report(),
            "limits": {
                **self.session.budget.limits.counters,
                **{f"peak_{name}": value
                   for name, value in self.session.budget.limits.gauges.items()},
            },
            **debug,
        }


class RunManager:
    """Creates and keeps the runs. One per process."""

    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}
        self._lock = threading.Lock()
        self._counter = 0

    # --------------------------------------------------------------- create

    def start(self, payload: dict, policy: Policy, owner: str | None = None) -> Run:
        """Start a run.

        `policy` is the caller's, resolved from the request in `app/api.py`,
        never from the payload. `owner` is the caller id; only that caller can
        see and control the run. Raises RunLimitReached when too many runs are
        in progress.
        """
        pipeline = Pipeline.from_dict(payload.get("pipeline") or {})
        # Validation and policy errors are reported in the response, before
        # a thread is started.
        pipeline.validate(policy)

        mode = "step" if payload.get("mode") == "step" else "run"
        delay = float(payload.get("delay") or 0)
        start_vars, mask = self._starting_frame(payload)

        with self._lock:
            self._expire_idle_locked()
            self._check_room_locked(owner)
            self._counter += 1
            run_id = f"run-{self._counter}"
            self._evict_locked()
            run = self._create_locked(run_id, pipeline, policy, mode, delay,
                                      start_vars, mask, owner)
        run.thread.start()
        return run

    def _create_locked(self, run_id, pipeline, policy, mode, delay,
                       start_vars, mask, owner) -> Run:
        """Create the run. Called under the same lock as the limit check, so
        two concurrent requests cannot both take the last slot."""
        bus = EventBus(mask=mask)
        debugger = StepDebugger(mode=mode, delay=delay, on_event=bus.push)
        session = Session(
            id=run_id,
            pipeline=pipeline,
            context=Context(vars=start_vars),
            event_handler=lambda event: bus.push(telemetry_to_dict(event)),
            debugger=debugger,
            policy=policy,
        )
        run = Run(id=run_id, session=session, debugger=debugger, bus=bus, mask=mask,
                  owner=owner)
        run.thread = threading.Thread(target=self._execute, args=(run,), daemon=True)
        self._runs[run_id] = run
        return run

    def get(self, run_id: str, owner: str | None = None) -> Run | None:
        """The run if it belongs to `owner`; otherwise None, as if it did not
        exist."""
        with self._lock:
            run = self._runs.get(run_id)
        if run is None or run.owner != owner:
            return None
        return run

    # --------------------------------------------------------------- limits

    def _active_locked(self) -> list[Run]:
        return [r for r in self._runs.values() if not r.done and not r.expired]

    def _check_room_locked(self, owner: str | None) -> None:
        active = self._active_locked()
        if len(active) >= config.MAX_ACTIVE_RUNS:
            raise RunLimitReached(
                f"{len(active)} runs are going already, and this backend runs "
                f"{config.MAX_ACTIVE_RUNS} at once; try again when one ends"
            )
        if owner is not None:
            mine = sum(1 for r in active if r.owner == owner)
            if mine >= config.MAX_RUNS_PER_CALLER:
                raise RunLimitReached(
                    f"these credentials have {mine} runs going, and may have "
                    f"{config.MAX_RUNS_PER_CALLER}; stop one or let it finish"
                )

    def _expire_idle_locked(self) -> None:
        """Stop step-by-step runs that have had no command for too long.
        Checked when a run is about to start, so no timer thread is needed."""
        now = time.monotonic()
        for run in self._active_locked():
            if run.debugger.waiting and now - run.touched > config.RUN_IDLE_SECONDS:
                run.expired = True
                run.session.stop()
                run.debugger.resume()  # wake the step wait so the stop takes effect

    @staticmethod
    def _starting_frame(payload: dict) -> tuple[dict, Masker | None]:
        """The starting variables of a run and a masker for its output.

        Server-side secrets are substituted here and never sent to the
        browser (the client sends their names). Secrets typed in the editor
        arrive as ordinary variables; they are not stored, only masked in the
        events.
        """
        secrets = payload.get("secrets") or {}
        start_vars = dict(payload.get("vars") or {})
        # Only secrets the server declared: a request must not be able to read
        # arbitrary environment variables.
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
        """Drop the oldest finished runs to keep at most MAX_RUNS in memory."""
        finished = [r for r in self._runs.values() if r.done]
        finished.sort(key=lambda r: r.started)
        while len(self._runs) - len(finished) + 1 > MAX_RUNS and finished:
            self._runs.pop(finished.pop(0).id, None)

    # -------------------------------------------------------------- control

    def control(self, run: Run, payload: dict) -> dict:
        run.touched = time.monotonic()
        action = payload.get("action")
        if action == "step":
            run.debugger.step(int(payload.get("count") or 1))
        elif action == "resume":
            run.debugger.resume()
        elif action == "pause":
            run.debugger.pause()
        elif action == "stop":
            run.session.stop()
            run.debugger.resume()  # wake the step wait so the stop takes effect
        elif action == "delay":
            run.debugger.set_delay(float(payload.get("delay") or 0))
        else:
            raise ValueError(f"Unknown action: {action!r}")
        return run.state()

    def set_vars(self, run: Run, payload: dict) -> dict:
        run.touched = time.monotonic()
        values = payload.get("set") or {}
        drop = payload.get("drop") or []
        if not isinstance(values, dict) or not isinstance(drop, list):
            raise ValueError("expected {\"set\": {...}, \"drop\": [...]}")
        run.debugger.set_vars(values, drop)
        return run.state()

    # ------------------------------------------------------------------ run

    @staticmethod
    def _execute(run: Run) -> None:
        """The run thread: one event loop for the whole run."""
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
