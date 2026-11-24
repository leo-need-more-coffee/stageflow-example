"""Where the files are and what the environment asks for."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: The prepared situations the stages work on. Read on every call, so editing
#: one of them and running the pipeline again needs no restart.
DATA_DIR = ROOT / "data"

#: The folders the editor may ask for over HTTP: the icons the stage specs
#: refer to by an absolute path, the ready-made pipelines and the fixtures.
PUBLIC_DIRS = ("icons", "pipelines", "data")

#: What to bind to. Loopback by default — an example backend with a run API
#: has no business being reachable from the network without being asked.
HOST = os.environ.get("SF_HOST", "127.0.0.1")
PORT = int(os.environ.get("SF_PORT", 8765))

#: Which origin the editor is served from. ``*`` by default: this is a local
#: example backend, and the editor may be opened from a file, a container or a
#: colleague's machine. Narrow it down when the backend stops being one.
ALLOW_ORIGIN = os.environ.get("SF_ALLOW_ORIGIN", "*")

#: How many runs may be going at once in this process, across everybody. A run
#: is a thread with its own event loop, and a plan's ceilings bound what one
#: run spends — not how many of them somebody starts.
MAX_ACTIVE_RUNS = int(os.environ.get("SF_MAX_ACTIVE_RUNS", 4))

#: How many of those one credential may hold. Only when tokens are configured:
#: without them every caller is the same anonymous one, and the process-wide
#: number above is the only one that means anything.
MAX_RUNS_PER_CALLER = int(os.environ.get("SF_MAX_RUNS_PER_CALLER", 2))

#: A step-by-step run that has been waiting for a command this long is
#: stopped. A debugging session left open in a closed tab would otherwise hold
#: its place in the limits above for as long as the process lives.
RUN_IDLE_SECONDS = float(os.environ.get("SF_RUN_IDLE_SECONDS", 600))
