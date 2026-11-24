"""The HTTP API the editor uses.

Before a run: `/meta` (what this backend supports), `/stages` (stage specs)
and `/secrets` (names of server-side secrets). During a run: `/run` to start
one, then state, events (SSE), control commands and variable edits.

Answers depend on the caller's plan (see `app.auth`). `/meta` and `/stages`
accept `?plan=` to show what another plan allows. A run always uses the plan
of the caller's credential.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.auth import CallerId, CallerPlan, caller_plan, source_of
from app.events import sse_lines
from app.exceptions import ClientErrorRoute
from app.runs import Run, RunLimitReached, RunManager
from app.schemas import ControlRequest, RunRequest, VarsRequest
from app.plans import known, plan_names, policy_for
from app.secrets import env_secret_names

# Every endpoint requires a credential (when tokens are configured), and a
# run is only visible to the credential that started it (`current_run`).
router = APIRouter(prefix="/api", route_class=ClientErrorRoute,
                   dependencies=[Depends(caller_plan)])

#: One run manager per process.
runs = RunManager()


def current_run(run_id: str, caller: CallerId) -> Run:
    # Another caller's run is reported as not found.
    run = runs.get(run_id, owner=caller)
    if run is None:
        raise HTTPException(404, "run not found")
    return run


CurrentRun = Annotated[Run, Depends(current_run)]
FromEvent = Annotated[int, Query(alias="from", ge=0, description="read on from the Nth event")]
#: `?plan=`: show another plan's allowance. Not checked against the caller,
#: since it only affects what is shown, not what runs.
ShownPlan = Annotated[
    str | None,
    Query(alias="plan", description="answer about this plan instead of the caller's"),
]


#: Version of this backend's HTTP API (not of the core). Bumped only for
#: breaking changes; new endpoints or fields do not change it.
API_VERSION = 1


@router.get("/meta", summary="What this backend is and what the caller may use")
async def meta(caller: CallerPlan, plan: ShownPlan = None) -> dict:
    """What the client needs before drawing: supported node types, stages,
    locales and limits for the caller's plan (or for `?plan=`).

    `node_types` comes from the core's registry narrowed by the plan, so the
    client can grey out what it cannot use. `plan_source` says whether the
    answer is for the caller's own plan or a requested one. `plans` lists the
    plans a client may ask about.
    """
    from stageflow import capabilities

    if plan is not None and not known(plan):
        raise HTTPException(400, f"no such plan: '{plan}'")
    shown = plan or caller
    policy = policy_for(shown)
    return {"api": API_VERSION, "plan": shown, "plan_source": source_of(plan),
            "plans": plan_names(), **capabilities(policy),
            "limits": _limits_of(policy)}


def _limits_of(policy) -> dict:
    """The plan's limits, so a client can warn before a run."""
    limits = policy.limits
    return {
        "counters": dict(limits.counters),
        "gauges": dict(limits.gauges),
        "max_retries": limits.max_retries,
        "max_delay_seconds": limits.max_delay_seconds,
    }


@router.get("/stages", summary="Specs of the stages the caller may use")
async def stages(caller: CallerPlan, plan: ShownPlan = None) -> dict:
    """Specs of the stages the plan allows (`?plan=` as in `/meta`), so the
    palette only offers what a run would accept."""
    from stageflow import get_stages

    if plan is not None and not known(plan):
        raise HTTPException(400, f"no such plan: '{plan}'")
    policy = policy_for(plan or caller)
    return {"stages": {name: cls.get_specs()
                       for name, cls in get_stages().items()
                       if policy.allows_stage(name)}}


@router.get("/secrets", summary="NAMES of the secrets in the environment")
async def secrets() -> dict:
    # Names only; values are substituted on the server (see app/secrets.py).
    return {"names": env_secret_names(), "source": "env"}


@router.post("/run", status_code=201, summary="Start a run")
async def start_run(body: RunRequest, caller: CallerPlan, who: CallerId = None) -> dict:
    """Start a run on the caller's plan.

    `body.plan` is the plan the client drew against, not a choice: if it
    differs from the caller's plan, the run is refused with a message that
    names both plans.
    """
    if body.plan is not None and body.plan != caller:
        raise HTTPException(
            403,
            f"this graph was prepared for plan '{body.plan}', and these "
            f"credentials are on '{caller}'",
        )
    try:
        run = runs.start(body.model_dump(), policy_for(caller), owner=who)
    except RunLimitReached as exc:
        raise HTTPException(429, str(exc)) from exc
    return {"id": run.id, "state": run.state()}


@router.get("/run/{run_id}", summary="State of a run")
async def run_state(run: CurrentRun) -> dict:
    return run.state()


@router.get("/run/{run_id}/events", summary="Event stream of a run (SSE)")
async def run_events(run: CurrentRun, start: FromEvent = 0) -> StreamingResponse:
    """The run's events as SSE, until the run ends or the client disconnects.
    `?from=N` resumes the stream from event N."""
    return StreamingResponse(
        sse_lines(run.bus, start),
        media_type="text/event-stream; charset=utf-8",
        # Tell proxies not to buffer the stream.
        headers={"X-Accel-Buffering": "no"},
    )


@router.post("/run/{run_id}/control", summary="Step, resume, pause, stop")
async def control_run(run: CurrentRun, body: ControlRequest) -> dict:
    return runs.control(run, body.model_dump())


@router.post("/run/{run_id}/vars", summary="Write or drop frame variables")
async def set_run_vars(run: CurrentRun, body: VarsRequest) -> dict:
    return runs.set_vars(run, body.model_dump())
