"""Checks the pipelines in ``pipelines/`` the way the core would.

    python check_pipelines.py

Two things, and both of them are what breaks in a demo nobody runs:

  * every pipeline is valid — the ids resolve, the stages exist, the arguments
    the stages declared are there;
  * the full one really runs without an API key, taking the `except` road to
    the keyword rules. That is the promise the README makes, and it is the
    easiest one to break by editing a stage.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import serve  # noqa: E402,F401 - registers the stages

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


def main() -> int:
    print("pipelines are valid:")
    problems = check_valid()
    print("the full pipeline runs without an API key:")
    problems += asyncio.run(check_runs_without_key())

    if problems:
        print(f"\n{len(problems)} problem(s)")
        return 1
    print("\nall good")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
