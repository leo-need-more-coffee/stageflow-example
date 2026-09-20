"""What the editor asks the backend for.

Before a run: the stages it may put on the canvas (``/stages``) and the names
of the keys a run can be given (``/secrets``). During one: a run it starts and
then drives a node at a time (``/run…``), reading the event stream as it goes.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.events import sse_lines
from app.exceptions import ClientErrorRoute
from app.runs import Run, RunManager
from app.schemas import ControlRequest, RunRequest, VarsRequest
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


@router.get("/stages", summary="Specs of every registered stage")
async def stages() -> dict:
    from stageflow import get_stages
    return {"stages": {name: cls.get_specs() for name, cls in get_stages().items()}}


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
