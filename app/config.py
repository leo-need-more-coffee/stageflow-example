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
