"""What the editor asks the backend for.

Before a run: what this backend can do at all (``/meta``), the stages it may
put on the canvas (``/stages``) and the names of the keys a run can be given
(``/secrets``). During one: a run it starts and then drives a node at a time
(``/run…``), reading the event stream as it goes.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.events import sse_lines
from app.exceptions import ClientErrorRoute
from app.runs import Run, RunManager
from app.schemas import ControlRequest, RunRequest, VarsRequest
from app.plans import PLAN, current_policy
from app.secrets import env_secret_names

router = APIRouter(prefix="/api", route_class=ClientErrorRoute)

#: One process — one manager (see :class:`app.runs.RunManager`).
runs = RunManager()


def current_run(run_id: str) -> Run:
    run = runs.get(run_id)
    if run is None:
        raise HTTPException(404, "run not found")
    return run


CurrentRun = Annotated[Run, Depends(current_run)]
FromEvent = Annotated[int, Query(alias="from", ge=0, description="read on from the Nth event")]


#: The version of the HTTP contract of this backend — the endpoints and the
#: shapes on this page, not the version of the core behind it. It goes up only
#: when a client that speaks the old one would break; adding an endpoint or a
#: field does not move it.
API_VERSION = 1


@router.get("/meta", summary="What this backend is and what the caller may use")
async def meta() -> dict:
    """What a client needs before it draws anything.

    An editor is built against one version of the core and then pointed here.
    Rather than have it keep a table of which release grew which node type, the
    core is asked directly — ``node_types`` is its registry, so a name absent
    from it is exactly a name a run would reject.

    Narrowed by the plan this process serves (``app/plans.py``), because an
    editor needs to know what *this* caller may draw. Whether a node type is
    missing because the core is older or because the plan is narrower is not a
    distinction it has to make.
    """
    from stageflow import capabilities

    policy = current_policy()
    return {"api": API_VERSION, "plan": PLAN, **capabilities(policy),
            "limits": _limits_of(policy)}


def _limits_of(policy) -> dict:
    """The numbers, so a client can warn before a run instead of after."""
    limits = policy.limits
    return {
        "counters": dict(limits.counters),
        "gauges": dict(limits.gauges),
        "max_retries": limits.max_retries,
        "max_delay_seconds": limits.max_delay_seconds,
    }


@router.get("/stages", summary="Specs of the stages the caller may use")
async def stages() -> dict:
    """Only what the plan allows.

    Sending the specs of a stage the plan forbids would have the editor draw
    it in the palette and the run refuse it — the mismatch the whole policy
    exists to avoid, reintroduced one endpoint later.
    """
    from stageflow import get_stages

    policy = current_policy()
    return {"stages": {name: cls.get_specs()
                       for name, cls in get_stages().items()
                       if policy.allows_stage(name)}}


@router.get("/secrets", summary="NAMES of the secrets in the environment")
async def secrets() -> dict:
    # names ONLY: the values stay on the server and are substituted into the
    # starting frame of a run (see app/secrets.py)
    return {"names": env_secret_names(), "source": "env"}


@router.post("/run", status_code=201, summary="Start a run")
async def start_run(body: RunRequest) -> dict:
    run = runs.start(body.model_dump())
    return {"id": run.id, "state": run.state()}


@router.get("/run/{run_id}", summary="State of a run")
async def run_state(run: CurrentRun) -> dict:
    return run.state()


@router.get("/run/{run_id}/events", summary="Event stream of a run (SSE)")
async def run_events(run: CurrentRun, start: FromEvent = 0) -> StreamingResponse:
    """The connection lives until the run ends or the client goes away.
    ``?from=N`` reads the log on from the Nth event: a subscriber that broke
    off does not lose the beginning, and in debugging the beginning is the
    interesting part."""
    return StreamingResponse(
        sse_lines(run.bus, start),
        media_type="text/event-stream; charset=utf-8",
        # a proxy that buffers would hold the tokens back until the run ends
        headers={"X-Accel-Buffering": "no"},
    )


@router.post("/run/{run_id}/control", summary="Step, resume, pause, stop")
async def control_run(run: CurrentRun, body: ControlRequest) -> dict:
    return runs.control(run, body.model_dump())


@router.post("/run/{run_id}/vars", summary="Write or drop frame variables")
async def set_run_vars(run: CurrentRun, body: VarsRequest) -> dict:
    return runs.set_vars(run, body.model_dump())
