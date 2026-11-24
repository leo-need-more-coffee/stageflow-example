"""Paths and settings from the environment."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: Prepared tickets and the knowledge base. Read on every call, so edits take
#: effect without a restart.
DATA_DIR = ROOT / "data"

#: Folders served over HTTP: stage icons, sample pipelines and fixtures.
PUBLIC_DIRS = ("icons", "pipelines", "data")

#: Bind address. Loopback by default.
HOST = os.environ.get("SF_HOST", "127.0.0.1")
PORT = int(os.environ.get("SF_PORT", 8765))

#: Allowed CORS origin. `*` by default for a local example; restrict it when
#: deploying.
ALLOW_ORIGIN = os.environ.get("SF_ALLOW_ORIGIN", "*")

#: Maximum runs in progress in this process. Each run is a thread with its own
#: event loop; plans limit what a run spends, not how many runs exist.
MAX_ACTIVE_RUNS = int(os.environ.get("SF_MAX_ACTIVE_RUNS", 4))

#: Maximum runs in progress per credential (only with SF_TOKENS).
MAX_RUNS_PER_CALLER = int(os.environ.get("SF_MAX_RUNS_PER_CALLER", 2))

#: A step-by-step run with no command for this long is stopped, so debugging
#: sessions left in closed tabs do not hold run slots forever.
RUN_IDLE_SECONDS = float(os.environ.get("SF_RUN_IDLE_SECONDS", 600))

#: Where runs that wait (a pipeline with a `wait` node) are saved, so that
#: they outlive a restart. SQLite; created on first use.
RUNS_DB = Path(os.environ.get("SF_RUNS_DB", ROOT / "runs.db"))

#: How often waiting runs are checked for a passed deadline (`timeout` on a
#: `wait` node), in seconds.
DUE_CHECK_SECONDS = float(os.environ.get("SF_DUE_CHECK_SECONDS", 60))
