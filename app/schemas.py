"""The bodies of the run API.

Declared rather than picked apart with ``dict.get``: the editor's commands are
a small fixed set, and a wrong one should come back as one clear message
instead of an exception from somewhere inside the core.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class Secrets(BaseModel):
    """Names, never values: ``env`` are the ones the server declared its own
    and substitutes itself, ``names`` is every secret of the run — those are
    what gets scrubbed out of the events."""

    env: list[str] = []
    names: list[str] = []


class RunRequest(BaseModel):
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
