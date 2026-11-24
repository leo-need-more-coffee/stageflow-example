"""Secrets: which ones the server has, and masking them in the output.

The server only gives out secret names and substitutes the values itself.
A pipeline reads a secret as an ordinary variable.
"""
from __future__ import annotations

import os
from typing import Any

#: Secrets are the names listed in `SF_SECRETS` plus variables prefixed with
#: `SF_SECRET_` (the name is the rest). Never the whole environment.
SECRET_PREFIX = "SF_SECRET_"

MASK = "•" * 8  # same as the editor (secrets.js)


def env_secret_names() -> list[str]:
    """Names of the secrets available on the server."""
    names = {
        part.strip()
        for part in (os.environ.get("SF_SECRETS") or "").split(",")
        if part.strip() and part.strip() in os.environ
    }
    names.update(
        key[len(SECRET_PREFIX):]
        for key in os.environ
        if key.startswith(SECRET_PREFIX) and key != SECRET_PREFIX
    )
    return sorted(names)


def env_secret_value(name: str) -> str | None:
    """A secret's value, from `SF_SECRET_<name>` or `<name>`. Only call it for
    names from `env_secret_names()` (see `RunManager._starting_frame`)."""
    prefixed = os.environ.get(SECRET_PREFIX + name)
    return prefixed if prefixed is not None else os.environ.get(name)


class Masker:
    """Masks secrets in everything sent to the browser.

    A stage may need the secret, but the debugger shows the frame and the
    events. Values are masked by variable name (in the frame) and by value
    (anywhere, e.g. in stage arguments in telemetry).
    """

    def __init__(self, names: set[str], values: set[str]) -> None:
        self._names = {n for n in names if n}
        # Very short values would match too much text.
        self._values = {v for v in values if isinstance(v, str) and len(v) >= 4}

    def __bool__(self) -> bool:
        return bool(self._names or self._values)

    def __call__(self, data: Any, key: str | None = None) -> Any:
        if key is not None and key in self._names:
            return MASK
        if isinstance(data, str):
            # Also inside longer strings (error messages, echoed headers).
            # Longest first, so a secret containing another is masked whole.
            for value in sorted(self._values, key=len, reverse=True):
                if value in data:
                    data = data.replace(value, MASK)
            return data
        if isinstance(data, dict):
            return {k: self(v, k if isinstance(k, str) else None) for k, v in data.items()}
        if isinstance(data, (list, tuple)):
            return [self(item) for item in data]
        return data
