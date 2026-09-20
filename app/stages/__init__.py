"""The stages of the bot: importing this package registers them all.

``support`` is the prepared part — tickets, the knowledge base, replies — and
has no dependencies. ``llm`` needs the `openai` SDK, and without it the rest
must still work: a pipeline built on rules does not care, and the full one
falls back to it through an `except`. :data:`LLM_IMPORT_ERROR` is how the
caller learns those two stages are missing.
"""
from __future__ import annotations

from app.stages import support  # noqa: F401 - registers its stages

LLM_IMPORT_ERROR: str | None = None

try:
    from app.stages import llm  # noqa: F401 - registers its stages
except ImportError as exc:  # pragma: no cover - depends on the environment
    LLM_IMPORT_ERROR = str(exc)
