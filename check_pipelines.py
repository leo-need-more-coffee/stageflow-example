"""Checks the pipelines in ``pipelines/`` the way the core would.

    python check_pipelines.py

Two things, and both of them are what breaks in a demo nobody runs:

  * every pipeline is valid — the ids resolve, the stages exist, the arguments
    the stages declared are there;
  * the full one really runs without an API key, taking the `except` road to
    the keyword rules. That is the promise the README makes, and it is the
    easiest one to break by editing a stage;
  * ``/api/meta`` answers, and answers about the core that is actually
    installed — that is what an editor decides by;
  * the plan a client may *ask to be shown* and the plan a run is *executed*
    under stay two different things — the whole point of `app/auth.py`, and a
    thing one refactor of the dependency graph would quietly undo.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import app.stages  # noqa: E402,F401 - registers the stages

from stageflow import Context, Pipeline, Session  # noqa: E402
from stageflow.exceptions import PipelineValidationError  # noqa: E402


def check_valid() -> list[str]:
    """Every shipped pipeline is well formed, and allowed by the plan.

    A plan narrower than the default is *supposed* to refuse some of these —
    that is what a plan is — so a refusal is only a problem on the plan this
    repository ships with, which is also the one CI runs.
    """
    from app.plans import DEFAULT_PLAN as PLAN, policy_for

    policy = policy_for(PLAN)
    problems = []
    for path in sorted((ROOT / "pipelines").glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        try:
            Pipeline.from_dict(data).validate(policy)
            print(f"  ok    {path.name}")
        except PipelineValidationError as exc:
            if PLAN == "full":
                problems.append(f"{path.name}: {exc}")
                print(f"  FAIL  {path.name}: {exc}")
            else:
                print(f"  --    {path.name}: not on plan '{PLAN}'")
        except Exception as exc:  # noqa: BLE001 - the report is the point
            problems.append(f"{path.name}: {type(exc).__name__}: {exc}")
            print(f"  FAIL  {path.name}: {exc}")
    return problems


async def check_runs_without_key() -> list[str]:
    problems = []
    path = ROOT / "pipelines" / "04-support-bot.json"
    pipeline = Pipeline.from_dict(json.loads(path.read_text(encoding="utf-8")))
    for ticket_id, expected in [("T-1001", "answered"), ("T-1006", "escalated")]:
        session = Session(id=f"check-{ticket_id}", pipeline=pipeline,
                          context=Context(vars={"ticket_id": ticket_id}))
        result = await session.run()
        status = (result.result or {}).get("status")
        if status == expected:
            print(f"  ok    {path.name} {ticket_id} -> {status}")
        else:
            problems.append(f"{ticket_id}: expected {expected}, got {status}")
            print(f"  FAIL  {path.name} {ticket_id} -> {status} (expected {expected})")
    return problems


async def check_meta() -> list[str]:
    """The answer an editor plans by, checked against this process.

    Two promises live here and both are cheap to break in a way nothing else
    notices. The endpoint would still return 200 with a stale hand-written
    list, and the editor would go on offering a node this backend cannot run;
    and /api/stages would still return 200 with stages the plan forbids, so
    the editor would draw them and the run would refuse them — the very
    mismatch the policy exists to remove, one endpoint later.
    """
    from stageflow import capabilities, get_stages
    from stageflow.core.nodes import get_node_types

    from app.api import API_VERSION, meta, stages
    from app.plans import DEFAULT_PLAN as PLAN, policy_for

    problems = []
    policy = policy_for(PLAN)
    answer = await meta(caller=PLAN)

    if answer.get("api") != API_VERSION or answer.get("plan") != PLAN:
        problems.append(f"/api/meta: api/plan wrong: {answer}")
    if answer.get("node_types") != capabilities(policy)["node_types"]:
        problems.append("/api/meta: node_types is not what the policy allows")
    allowed = {name for name in get_stages() if policy.allows_stage(name)}
    served = set((await stages(caller=PLAN))["stages"])
    if served != allowed:
        problems.append(f"/api/stages served {sorted(served - allowed)} "
                        f"the plan forbids, or dropped {sorted(allowed - served)}")
    forbidden = [t for t in get_node_types() if not policy.allows_node_type(t)]
    if set(answer["node_types"]) & set(forbidden):
        problems.append("/api/meta lists a node type the plan forbids")

    if problems:
        for problem in problems:
            print(f"  FAIL  {problem}")
        return problems
    print(f"  ok    /api/meta  plan={PLAN} api={answer['api']} "
          f"core={answer['stageflow']} nodes={len(answer['node_types'])} "
          f"stages={answer['stages']}")
    return problems


async def check_plan_is_not_a_claim() -> list[str]:
    """Asking to be shown a plan is free; being run on one is not.

    The thing worth a check is the asymmetry, because it is invisible in the
    code that breaks it. Wire ``?plan=`` into the run — one line, and an
    obvious-looking one — and every ceiling in this repository becomes a
    query parameter.
    """
    from fastapi import HTTPException
    from starlette.requests import Request

    from app import auth
    from app.api import meta, start_run
    from app.plans import policy_for
    from app.schemas import RunRequest

    def request(header: str | None = None) -> Request:
        headers = [(auth.AUTH_HEADER.lower().encode(), header.encode())] if header else []
        return Request({"type": "http", "headers": headers})

    problems = []

    # 1. a narrow caller may look at a wide plan, and is told it is a what-if
    shown = await meta(caller="basic", plan="pro")
    if shown["plan"] != "pro" or shown["plan_source"] != "query":
        problems.append(f"/api/meta?plan=pro did not answer about pro: {shown['plan']}")
    if shown["limits"]["counters"] != policy_for("pro").limits.counters:
        problems.append("/api/meta?plan=pro answered with someone else's limits")
    own = await meta(caller="basic")
    if own["plan"] != "basic" or own["plan_source"] == "query":
        problems.append("/api/meta without ?plan= did not answer about the caller")

    # 2. …and cannot run on it
    try:
        await start_run(RunRequest(plan="pro", pipeline={}), caller="basic")
        problems.append("a graph drawn for 'pro' was accepted from a 'basic' caller")
    except HTTPException as exc:
        if exc.status_code != 403:
            problems.append(f"the plan mismatch answered {exc.status_code}, not 403")

    # 3. the credential decides, and only it
    tokens, enabled = auth.TOKENS, auth.ENABLED
    auth.TOKENS, auth.ENABLED = {"tok-pro": "pro"}, True
    try:
        cases = [(None, 401), ("nonsense", 401), ("tok-pro", "pro"),
                 ("Bearer tok-pro", "pro")]
        for header, expected in cases:
            try:
                got = auth.caller_plan(request(header))
            except HTTPException as exc:
                got = exc.status_code
            if got != expected:
                problems.append(f"credential {header!r}: expected {expected}, got {got}")
    finally:
        auth.TOKENS, auth.ENABLED = tokens, enabled

    if problems:
        for problem in problems:
            print(f"  FAIL  {problem}")
        return problems
    print("  ok    ?plan= shows, the credential decides, a mismatch is refused")
    return problems


def main() -> int:
    print("the backend describes itself:")
    problems = asyncio.run(check_meta())
    print("a plan is shown to anyone and run for no one:")
    problems += asyncio.run(check_plan_is_not_a_claim())
    print("pipelines are valid:")
    problems += check_valid()
    print("the full pipeline runs without an API key:")
    problems += asyncio.run(check_runs_without_key())

    if problems:
        print(f"\n{len(problems)} problem(s)")
        return 1
    print("\nall good")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
