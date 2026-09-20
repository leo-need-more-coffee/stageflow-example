"""The secrets of a run: which ones the server has, and hiding them again.

The server hands out NAMES and substitutes VALUES itself. A page that could
ask for the value of a variable would only have to name ``AWS_SECRET_KEY`` to
get it, and the editor does not need it: a pipeline reads the key as an
ordinary variable, and the run is where the substitution happens.
"""
from __future__ import annotations

import os
from typing import Any

#: Where the server takes secrets from: comma-separated names in ``SF_SECRETS``
#: and/or variables prefixed with ``SF_SECRET_`` (the name is what follows the
#: prefix). An explicit list rather than "the whole environment": handing the
#: browser a list of every variable on the machine is a bad idea by itself.
SECRET_PREFIX = "SF_SECRET_"

MASK = "•" * 8  # the same the editor shows (secrets.js)


def env_secret_names() -> list[str]:
    """Names of the secrets available to the server. Names only: values stay here."""
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
    """Value of an environment secret — under the prefixed name or as it is.

    Only ever called for a name the server itself declared a secret (see
    :meth:`app.runs.RunManager.start`): otherwise a run request could name any
    variable of the server environment and read it back out of the starting
    frame.
    """
    prefixed = os.environ.get(SECRET_PREFIX + name)
    return prefixed if prefixed is not None else os.environ.get(name)


class Masker:
    """Scrubs secret values out of everything that goes to the browser.

    The run sees the key when it has to — a stage needs it — but the debugger
    shows the FRAME and the event log, and without this the key would be back
    in the page with the very first `node_enter`, and from there in a
    screenshot. We hide by two signs: by variable name (in the frame
    dictionaries) and by the value itself (stage arguments in telemetry — the
    name there is already a different one).
    """

    def __init__(self, names: set[str], values: set[str]) -> None:
        self._names = {n for n in names if n}
        # an empty string would match everything, a one-character key almost so
        self._values = {v for v in values if isinstance(v, str) and len(v) >= 4}

    def __bool__(self) -> bool:
        return bool(self._names or self._values)

    def __call__(self, data: Any, key: str | None = None) -> Any:
        if key is not None and key in self._names:
            return MASK
        if isinstance(data, str):
            return MASK if data in self._values else data
        if isinstance(data, dict):
            return {k: self(v, k if isinstance(k, str) else None) for k, v in data.items()}
        if isinstance(data, (list, tuple)):
            return [self(item) for item in data]
        return data
