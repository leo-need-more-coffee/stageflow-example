"""What the editor asks the backend for.

Before a run: what this backend can do at all (``/meta``), the stages it may
put on the canvas (``/stages``) and the names of the keys a run can be given
(``/secrets``). During one: a run it starts and then drives a node at a time
(``/run…``), reading the event stream as it goes.

Everything here is answered *for a caller* — see :mod:`app.auth`. The two
questions before a run take an optional ``?plan=`` and answer about that plan
whether or not the caller is on it, because a client has to be able to show
what a plan would allow. A run takes no such parameter: it goes on the plan
the credential resolves to, and says so if the two disagree.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.auth import CallerPlan, caller_plan, source_of
from app.events import sse_lines
from app.exceptions import ClientErrorRoute
from app.runs import Run, RunManager
from app.schemas import ControlRequest, RunRequest, VarsRequest
from app.plans import known, plan_names, policy_for
from app.secrets import env_secret_names

# The credential is demanded on every endpoint rather than on the run alone:
# a run is driven by four of them, and a stranger who may step and stop it is
# not meaningfully less of a stranger for not having started it. With no
# tokens configured the dependency lets everyone through (`app/auth.py`).
router = APIRouter(prefix="/api", route_class=ClientErrorRoute,
                   dependencies=[Depends(caller_plan)])

#: One process — one manager (see :class:`app.runs.RunManager`).
runs = RunManager()


def current_run(run_id: str) -> Run:
    run = runs.get(run_id)
    if run is None:
        raise HTTPException(404, "run not found")
    return run


CurrentRun = Annotated[Run, Depends(current_run)]
FromEvent = Annotated[int, Query(alias="from", ge=0, description="read on from the Nth event")]
#: "Answer about this plan" — a request to be *shown* something, not a claim
#: to be on it. Unverified on purpose; what a run may do is decided elsewhere.
ShownPlan = Annotated[
    str | None,
    Query(alias="plan", description="answer about this plan instead of the caller's"),
]


#: The version of the HTTP contract of this backend — the endpoints and the
#: shapes on this page, not the version of the core behind it. It goes up only
#: when a client that speaks the old one would break; adding an endpoint or a
#: field does not move it.
API_VERSION = 1


@router.get("/meta", summary="What this backend is and what the caller may use")
async def meta(caller: CallerPlan, plan: ShownPlan = None) -> dict:
    """What a client needs before it draws anything.

    An editor is built against one version of the core and then pointed here.
    Rather than have it keep a table of which release grew which node type, the
    core is asked directly — ``node_types`` is its registry, so a name absent
    from it is exactly a name a run would reject.

    Narrowed by a plan (``app/plans.py``), because an editor needs to know
    what may be drawn. Whether a node type is missing because the core is
    older or because the plan is narrower is not a distinction it has to make
    — which is why ``plan_source`` is here: the client says *why* something is
    greyed out, and says it without guessing.

    By default the plan is the caller's own. ``?plan=basic`` answers about
    that one instead, for anyone, unverified: a client showing "what would I
    lose on the cheaper tier" is asking a question, not claiming a tier.
    ``plans`` is the list to offer, so that the names are not hard-coded into
    a client that cannot know them.
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
    """The numbers, so a client can warn before a run instead of after."""
    limits = policy.limits
    return {
        "counters": dict(limits.counters),
        "gauges": dict(limits.gauges),
        "max_retries": limits.max_retries,
        "max_delay_seconds": limits.max_delay_seconds,
    }


@router.get("/stages", summary="Specs of the stages the caller may use")
async def stages(caller: CallerPlan, plan: ShownPlan = None) -> dict:
    """Only what the plan allows.

    Sending the specs of a stage the plan forbids would have the editor draw
    it in the palette and the run refuse it — the mismatch the whole policy
    exists to avoid, reintroduced one endpoint later.

    ``?plan=`` as in ``/meta``, and for the same reason: a palette shown for
    a plan has to come from the same answer as the limits shown beside it.
    """
    from stageflow import get_stages

    if plan is not None and not known(plan):
        raise HTTPException(400, f"no such plan: '{plan}'")
    policy = policy_for(plan or caller)
    return {"stages": {name: cls.get_specs()
                       for name, cls in get_stages().items()
                       if policy.allows_stage(name)}}


@router.get("/secrets", summary="NAMES of the secrets in the environment")
async def secrets() -> dict:
    # names ONLY: the values stay on the server and are substituted into the
    # starting frame of a run (see app/secrets.py)
    return {"names": env_secret_names(), "source": "env"}


@router.post("/run", status_code=201, summary="Start a run")
async def start_run(body: RunRequest, caller: CallerPlan) -> dict:
    """Starts a run on the caller's plan — and only on it.

    ``body.plan`` is not a choice: it is what the client *drew against*, and
    saying it lets the disagreement be named. Without it a client that had
    been previewing a wider plan would get a pile of validation errors about
    individual stages and no hint that the plan is the reason.
    """
    if body.plan is not None and body.plan != caller:
        raise HTTPException(
            403,
            f"this graph was prepared for plan '{body.plan}', and these "
            f"credentials are on '{caller}'",
        )
    run = runs.start(body.model_dump(), policy_for(caller))
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
