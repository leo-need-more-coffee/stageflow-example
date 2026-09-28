"""Checks the pipelines in ``pipelines/`` the way the core would.

    python check_pipelines.py

Two things, and both of them are what breaks in a demo nobody runs:

  * every pipeline is valid — the ids resolve, the stages exist, the arguments
    the stages declared are there;
  * the full one really runs without an API key, taking the `except` road to
    the keyword rules. That is the promise the README makes, and it is the
    easiest one to break by editing a stage;
  * ``/api/meta`` answers, and answers about the core that is actually
    installed — that is what an editor decides by.
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


def check_valid() -> list[str]:
    problems = []
    for path in sorted((ROOT / "pipelines").glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        try:
            Pipeline.from_dict(data).validate()
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
    """The answer an editor plans by, checked against the core in this process.

    The endpoint is cheap to break in a way nothing else notices: it would
    still return 200 with a stale hand-written list, and the editor would go
    on offering a node this backend cannot run.
    """
    from stageflow import capabilities
    from stageflow.core.nodes import get_node_types

    from app.api import API_VERSION, meta

    problems = []
    answer = await meta()
    expected = {"api": API_VERSION, **capabilities()}
    if answer != expected:
        problems.append(f"/api/meta answered {answer}, expected {expected}")
        print(f"  FAIL  /api/meta {answer}")
        return problems
    if answer["node_types"] != sorted(get_node_types()):
        problems.append("/api/meta node_types is not the registry")
        print("  FAIL  /api/meta node_types is not the registry")
        return problems
    print(f"  ok    /api/meta  api={answer['api']} core={answer['stageflow']} "
          f"nodes={len(answer['node_types'])} stages={answer['stages']}")
    return problems


def main() -> int:
    print("the backend describes itself:")
    problems = asyncio.run(check_meta())
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
