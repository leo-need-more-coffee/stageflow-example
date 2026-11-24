"""Request bodies of the run API, validated by pydantic so that a bad
command gets a clear error message."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class Secrets(BaseModel):
    """Secret names, never values. `env`: server-side secrets to substitute;
    `names`: all secret variables of the run, masked in the events."""

    env: list[str] = []
    names: list[str] = []


class RunRequest(BaseModel):
    #: The plan the client drew against (not a choice of plan: the run uses
    #: the caller's). Used to explain a refusal by plan.
    plan: str | None = None
    pipeline: dict[str, Any] = {}
    vars: dict[str, Any] = {}
    mode: Literal["run", "step"] = "run"
    delay: float = Field(0, ge=0, description="pause between nodes, seconds")
    secrets: Secrets | None = None


class ControlRequest(BaseModel):
    action: Literal["step", "resume", "pause", "stop", "delay"]
    count: int = Field(1, ge=1, description="how many nodes a `step` walks")
    delay: float = Field(0, ge=0)


class VarsRequest(BaseModel):
    set: dict[str, Any] = {}
    drop: list[str] = []
