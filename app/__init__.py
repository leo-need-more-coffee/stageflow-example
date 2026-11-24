"""The support bot backend (the FastAPI app is built in ``main.py``).

Importing this package locates the StageFlow core: the installed package if
there is one, otherwise a sibling ``../stageflow`` checkout.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

if importlib.util.find_spec("stageflow") is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "stageflow"))
