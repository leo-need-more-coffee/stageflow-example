"""Checks for the shipped pipelines and the backend's main promises.

    python check_pipelines.py

- every pipeline validates;
- the full pipeline runs without an API key, falling back to keyword rules;
- `/api/meta` reports the installed core;
- a plan can be shown on request but a run uses the caller's own plan;
- the plans work: `basic` runs its pipeline, refuses others, and its meters
  are charged and enforced;
- a run is only visible to its owner, and the number of runs is limited.
"""
from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import app.stages  # noqa: E402,F401 - registers the stages

from stageflow import Context, Pipeline, Session  # noqa: E402


def check_valid() -> list[str]:
    """Every shipped pipeline is valid on the open plan (what a caller gets
    without credentials). Narrower plans are checked separately."""
    from app.plans import OPEN_PLAN, policy_for

    policy = policy_for(OPEN_PLAN)
    problems = []
    for path in sorted((ROOT / "pipelines").glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        try:
            Pipeline.from_dict(data).validate(policy)
            print(f"  ok    {path.name}")
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
    """`/api/meta` reports the installed core's node types, and `/api/stages`
    only lists stages the plan allows."""
    from stageflow import capabilities, get_stages
    from stageflow.core.nodes import get_node_types

    from app.api import API_VERSION, meta, stages
    from app.plans import OPEN_PLAN, policy_for

    problems = []
    policy = policy_for(OPEN_PLAN)
    answer = await meta(caller=OPEN_PLAN)

    if answer.get("api") != API_VERSION or answer.get("plan") != OPEN_PLAN:
        problems.append(f"/api/meta: api/plan wrong: {answer}")
    if answer.get("node_types") != capabilities(policy)["node_types"]:
        problems.append("/api/meta: node_types is not what the policy allows")
    allowed = {name for name in get_stages() if policy.allows_stage(name)}
    served = set((await stages(caller=OPEN_PLAN))["stages"])
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
    print(f"  ok    /api/meta  plan={OPEN_PLAN} api={answer['api']} "
          f"core={answer['stageflow']} nodes={len(answer['node_types'])} "
          f"stages={answer['stages']}")
    return problems


async def check_plan_is_not_a_claim() -> list[str]:
    """`?plan=` changes what is shown, never what a run is allowed to do."""
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

    # 1. A caller may look at a wider plan and is told it is a what-if.
    shown = await meta(caller="basic", plan="pro")
    if shown["plan"] != "pro" or shown["plan_source"] != "query":
        problems.append(f"/api/meta?plan=pro did not answer about pro: {shown['plan']}")
    if shown["limits"]["counters"] != policy_for("pro").limits.counters:
        problems.append("/api/meta?plan=pro answered with someone else's limits")
    own = await meta(caller="basic")
    if own["plan"] != "basic" or own["plan_source"] == "query":
        problems.append("/api/meta without ?plan= did not answer about the caller")

    # 2. ...but cannot run on it.
    try:
        await start_run(RunRequest(plan="pro", pipeline={}), caller="basic")
        problems.append("a graph drawn for 'pro' was accepted from a 'basic' caller")
    except HTTPException as exc:
        if exc.status_code != 403:
            problems.append(f"the plan mismatch answered {exc.status_code}, not 403")

    # 3. The credential decides the plan.
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


async def check_plans_are_real() -> list[str]:
    """The plans have an effect: every stage they name exists, `basic` runs
    its own pipeline and refuses one it lacks, and its meters are charged
    and enforced."""
    from stageflow import Context, Policy, Session, get_stages
    from stageflow.exceptions import PipelineValidationError

    from app.plans import PLANS, policy_for

    problems = []

    # 1. Every stage named in a plan exists.
    registered = set(get_stages())
    for name, policy in PLANS.items():
        missing = sorted((policy.stages or set()) - registered)
        if missing:
            problems.append(f"plan '{name}' allows stages nobody registered: {missing}")

    def load(filename):
        path = ROOT / "pipelines" / filename
        return Pipeline.from_dict(json.loads(path.read_text(encoding="utf-8")))

    # 2. `basic` runs its own pipeline and refuses the one needing `map`.
    basic = policy_for("basic")
    try:
        load("06-rules-only.json").validate(basic)
    except PipelineValidationError as exc:
        problems.append(f"the basic plan cannot run 06-rules-only: {exc}")
    try:
        load("05-batch-triage.json").validate(basic)
        problems.append("the basic plan accepted 05-batch-triage, which needs a map node")
    except PipelineValidationError:
        pass

    # 3. Meters are charged, and a ceiling stops a run.
    session = Session(id="plan-basic", pipeline=load("06-rules-only.json"),
                      context=Context(vars={"ticket_id": "T-1001"}), policy=basic)
    result = await session.run()
    for meter in ("kb_lookups", "replies_sent"):
        if not result.meters.get(meter):
            problems.append(f"nothing charged '{meter}', which the basic plan limits")

    broke = Policy(
        stages=basic.stages, node_types=basic.node_types,
        limits=replace(basic.limits, counters={**basic.limits.counters, "kb_lookups": 0}),
    )
    stopped = await Session(id="plan-ceiling", pipeline=load("06-rules-only.json"),
                            context=Context(vars={"ticket_id": "T-1001"}),
                            policy=broke).run()
    if (stopped.result or {}).get("status") != "budget_exceeded":
        problems.append(f"a zero ceiling on kb_lookups did not stop the run: {stopped.result}")

    if problems:
        for problem in problems:
            print(f"  FAIL  {problem}")
        return problems
    print(f"  ok    basic runs 06-rules-only, refuses 05-batch-triage, "
          f"spends {result.meters['kb_lookups']:.0f} kb_lookups and stops at zero")
    return problems


async def check_runs_belong_to_callers() -> list[str]:
    """Another tenant's run is not found, and the run limits hold."""
    from fastapi import HTTPException

    from app import api, auth, config
    from app.runs import RunManager
    from app.schemas import RunRequest

    pipeline = json.loads((ROOT / "pipelines" / "06-rules-only.json").read_text("utf-8"))
    body = RunRequest(pipeline=pipeline, mode="step", vars={"ticket_id": "T-1001"})
    problems: list[str] = []

    saved = (auth.TOKENS, auth.ENABLED, api.runs, config.MAX_ACTIVE_RUNS,
             config.MAX_RUNS_PER_CALLER, config.RUN_IDLE_SECONDS)
    auth.TOKENS, auth.ENABLED = {"tok-a": "basic", "tok-b": "basic"}, True
    api.runs = RunManager()
    config.MAX_ACTIVE_RUNS, config.MAX_RUNS_PER_CALLER = 3, 2
    alice, bob = "a" * 64, "b" * 64
    try:
        started = await api.start_run(body, caller="basic", who=alice)
        run_id = started["id"]
        try:
            api.current_run(run_id, caller=bob)
            problems.append("another credential could open somebody's run")
        except HTTPException as exc:
            if exc.status_code != 404:
                problems.append(f"somebody else's run answered {exc.status_code}, not 404")
        if api.current_run(run_id, caller=alice).id != run_id:
            problems.append("the owner could not open their own run")

        async def refused(who: str, why: str) -> None:
            try:
                await api.start_run(body, caller="basic", who=who)
                problems.append(f"{why} was accepted")
            except HTTPException as exc:
                if exc.status_code != 429:
                    problems.append(f"{why} answered {exc.status_code}, not 429")

        await api.start_run(body, caller="basic", who=alice)
        await refused(alice, "a third run for one credential")
        await api.start_run(body, caller="basic", who=bob)
        await refused(bob, "a fourth run for the process")

        # All runs are waiting for a step; idle ones must give way.
        for _ in range(100):
            if all(r.debugger.waiting for r in api.runs._active_locked()):
                break
            await asyncio.sleep(0.02)
        config.RUN_IDLE_SECONDS = 0
        try:
            await api.start_run(body, caller="basic", who=bob)
        except HTTPException as exc:
            problems.append(f"idle debug runs still held their places: {exc.detail}")
    finally:
        for run in list(api.runs._runs.values()):
            run.session.stop()
            run.debugger.resume()
        (auth.TOKENS, auth.ENABLED, api.runs, config.MAX_ACTIVE_RUNS,
         config.MAX_RUNS_PER_CALLER, config.RUN_IDLE_SECONDS) = saved

    for problem in problems:
        print(f"  FAIL  {problem}")
    if not problems:
        print("  ok    a run is its owner's, two per credential, "
              "three at once, and an idle one gives way")
    return problems


def main() -> int:
    print("the backend describes itself:")
    problems = asyncio.run(check_meta())
    print("a plan is shown to anyone and run for no one:")
    problems += asyncio.run(check_plan_is_not_a_claim())
    print("pipelines are valid:")
    problems += check_valid()
    print("the plans are real:")
    problems += asyncio.run(check_plans_are_real())
    print("the full pipeline runs without an API key:")
    problems += asyncio.run(check_runs_without_key())
    print("runs belong to whoever started them:")
    problems += asyncio.run(check_runs_belong_to_callers())

    if problems:
        print(f"\n{len(problems)} problem(s)")
        return 1
    print("\nall good")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
