"""Runs with step debugging: one run is one debug session.

Pipelines run in the real core (`Session` with a `StepDebugger`), not in the
editor, so the debugger shows actual behaviour.

Each run has its own thread and event loop. HTTP handlers run in the server's
loop and use the debugger's thread-safe methods; events come back through
`app.events.EventBus`.

A pipeline with a `wait` node runs with a checkpoint store (SQLite, see
`config.RUNS_DB`). When it reaches the wait, the run is saved, its thread
ends, and its status is `waiting`; it takes no run slot while it waits.
`answer` continues it — in a new thread, from the store — and `recover`
brings waiting runs back after a restart.
"""
from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from stageflow import Context, Pipeline, Policy, Session, SQLiteStore, StepDebugger

from app import config
from app.events import EventBus, jsonable, telemetry_to_dict
from app.plans import policy_for
from app.secrets import Masker, env_secret_names, env_secret_value

MAX_RUNS = 8  # finished runs kept in memory


class RunLimitReached(Exception):
    """Too many runs in progress, for the process or for this caller."""


class RunNotWaiting(Exception):
    """An answer for a run that is not waiting, or not for that input."""


def has_wait(pipeline: Pipeline) -> bool:
    return any(node.type == "wait" for node in pipeline.nodes)


class Ledger:
    """Who owns each saved run and on which plan it runs: what the core's
    checkpoint does not keep, because it is the host's business. A table in
    the same SQLite file as the checkpoints."""

    def __init__(self, path: Path) -> None:
        self.path = str(path)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS example_runs "
                       "(run_id TEXT PRIMARY KEY, owner TEXT, plan TEXT NOT NULL)")

    def add(self, run_id: str, owner: str | None, plan: str) -> None:
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO example_runs VALUES (?, ?, ?)", (run_id, owner, plan))

    def all(self) -> list[tuple[str, str | None, str]]:
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute("SELECT run_id, owner, plan FROM example_runs").fetchall()


@dataclass
class Run:
    """One run: a session running in its own thread, with its debugger."""

    id: str
    session: Session
    debugger: StepDebugger
    bus: EventBus
    status: str = "running"          # running | waiting | finished | failed | stopped
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
    #: The plan it runs on; a waiting run is continued under it.
    plan: str | None = None
    #: Saved to the store when it waits (its pipeline has a `wait` node).
    durable: bool = False
    #: While waiting: {"node", "input", "deadline"}.
    waiting: dict | None = None

    @property
    def done(self) -> bool:
        """Not executing: finished, or saved and waiting."""
        return self.status != "running"

    @property
    def over(self) -> bool:
        return self.status in ("finished", "failed", "stopped")

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
            # Not `waiting`: the debugger's state uses that for its step wait.
            "waiting_for": self.waiting,
            "durable": self.durable,
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

    def __init__(self, db_path: Path | None = None) -> None:
        self._runs: dict[str, Run] = {}
        self._lock = threading.Lock()
        self._db_path = db_path
        self._store: SQLiteStore | None = None
        self._ledger: Ledger | None = None

    @property
    def store(self) -> SQLiteStore:
        """The checkpoint store, opened on first use."""
        if self._store is None:
            path = self._db_path or config.RUNS_DB
            self._store, self._ledger = SQLiteStore(path), Ledger(path)
        return self._store

    @property
    def ledger(self) -> Ledger:
        self.store  # noqa: B018 - opens both
        return self._ledger

    # --------------------------------------------------------------- create

    def start(self, payload: dict, policy: Policy, owner: str | None = None,
              plan: str | None = None) -> Run:
        """Start a run.

        `policy` is the caller's, resolved from the request in `app/api.py`,
        never from the payload; `plan` is its name, kept for a run that
        waits. `owner` is the caller id; only that caller can see and control
        the run. Raises RunLimitReached when too many runs are in progress.
        """
        pipeline = Pipeline.from_dict(payload.get("pipeline") or {})
        # Validation and policy errors are reported in the response, before
        # a thread is started.
        pipeline.validate(policy)

        durable = has_wait(pipeline)
        secrets = payload.get("secrets") or {}
        if durable and (secrets.get("env") or secrets.get("names")):
            # The frame of a waiting run is written to disk, and a secret in
            # it would be too.
            raise ValueError("a pipeline with a wait node is saved to disk while it "
                             "waits, so it cannot carry secrets; take them out of it")
        if durable and plan is None:
            raise ValueError("a run that waits needs its plan to be continued")

        mode = "step" if payload.get("mode") == "step" else "run"
        delay = float(payload.get("delay") or 0)
        start_vars, mask = self._starting_frame(payload)

        with self._lock:
            self._expire_idle_locked()
            self._check_room_locked(owner)
            # Unique across restarts: saved runs keep their ids.
            run_id = f"run-{uuid.uuid4().hex[:12]}"
            self._evict_locked()
            run = self._create_locked(run_id, pipeline, policy, mode, delay,
                                      start_vars, mask, owner, durable)
            run.plan = plan
        if durable:
            self.ledger.add(run_id, owner, plan)
        run.thread.start()
        return run

    def _create_locked(self, run_id, pipeline, policy, mode, delay,
                       start_vars, mask, owner, durable=False) -> Run:
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
            store=self.store if durable else None,
        )
        run = Run(id=run_id, session=session, debugger=debugger, bus=bus, mask=mask,
                  owner=owner, durable=durable)
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
        """Drop the oldest finished runs to keep at most MAX_RUNS in memory.
        Waiting runs stay: they are what someone will come back to."""
        finished = [r for r in self._runs.values() if r.over]
        finished.sort(key=lambda r: r.started)
        while len(self._runs) - len(finished) + 1 > MAX_RUNS and finished:
            self._runs.pop(finished.pop(0).id, None)

    # -------------------------------------------------------------- control

    def control(self, run: Run, payload: dict) -> dict:
        run.touched = time.monotonic()
        action = payload.get("action")
        if run.status == "waiting":
            # Nothing is executing; only a stop means something, and it ends
            # the saved run too.
            if action != "stop":
                raise RunNotWaiting(f"the run is waiting for '{run.waiting['input']}'; "
                                    "answer it or stop it")
            self._continue(run, stop=True)
            return run.state()
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

    # -------------------------------------------------------------- waiting

    def answer(self, run: Run, input_type: str, payload: dict) -> dict:
        """Give a waiting run the input it waits for and continue it."""
        self._continue(run, input_type=input_type, payload=payload)
        return run.state()

    def _continue(self, run: Run, input_type: str | None = None,
                  payload: dict | None = None, stop: bool = False) -> None:
        """Continue a waiting run from the store, in a new thread: with an
        input, with none (its deadline has passed), or to stop it."""
        with self._lock:
            if run.status != "waiting":
                raise RunNotWaiting(f"the run is {run.status}, not waiting")
            expected = run.waiting["input"]
            if input_type is not None and input_type != expected:
                raise RunNotWaiting(f"the run waits for '{expected}', not '{input_type}'")
            # Set under the lock: a second answer to the same run is refused.
            run.status, run.waiting = "running", None
        run.touched = time.monotonic()
        run.debugger = StepDebugger(mode="run", on_event=run.bus.push)
        run.bus.reopen()

        async def restored() -> Session:
            session = await Session.restore(
                self.store, run.id, policy=policy_for(run.plan),
                event_handler=lambda event: run.bus.push(telemetry_to_dict(event)),
            )
            session.debugger = run.debugger
            if stop:
                session.stop()
            elif input_type is not None:
                await session.input(input_type, payload or {})
            run.session = session
            return session

        run.thread = threading.Thread(target=self._execute, args=(run, restored),
                                      daemon=True)
        run.thread.start()

    async def wake_due(self, now: float | None = None) -> list[str]:
        """Continue the waiting runs whose deadline has passed: each takes its
        `on_timeout` road. Called periodically by the server."""
        woken = []
        for run_id in await self.store.due(now):
            with self._lock:
                run = self._runs.get(run_id)
            if run is not None and run.status == "waiting":
                self._continue(run)
                woken.append(run_id)
        return woken

    async def recover(self) -> int:
        """After a restart: bring back the runs that were waiting."""
        found = 0
        for run_id, owner, plan in self.ledger.all():
            saved = await self.store.load(run_id)
            if saved is None or saved.state != "waiting" or run_id in self._runs:
                continue
            session = await Session.restore(self.store, run_id, policy=policy_for(plan))
            waiting = {k: saved.waiting_for[k] for k in ("node", "input", "deadline")}
            bus = EventBus()
            run = Run(id=run_id, session=session, debugger=StepDebugger(mode="run"),
                      bus=bus, owner=owner, plan=plan, durable=True, status="waiting",
                      waiting=waiting)
            bus.push({"type": "waiting", **waiting})
            bus.close()
            with self._lock:
                self._runs[run_id] = run
            found += 1
        return found

    # ------------------------------------------------------------------ run

    @staticmethod
    def _execute(run: Run,
                 prepare: Callable[[], Awaitable[Session]] | None = None) -> None:
        """The run thread: one event loop for the whole run, or for this
        stretch of it, up to the next wait."""
        async def go():
            session = await prepare() if prepare else run.session
            return await session.run()

        try:
            result = asyncio.run(go())
            if result.waiting:
                run.waiting = {k: result.waiting[k] for k in ("node", "input", "deadline")}
                run.status = "waiting"
                run.bus.push({"type": "waiting", **run.waiting})
                return
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
