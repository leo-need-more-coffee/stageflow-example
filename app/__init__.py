"""The support bot backend: the FastAPI app is assembled in ``main.py``.

The one thing that happens on importing the package is finding the StageFlow
core, and it has to happen before anything imports it: an installed package
(``pip install stageflow-framework``) wins, a sibling checkout is the fallback,
so two repositories cloned side by side run without installing anything.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

if importlib.util.find_spec("stageflow") is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "stageflow"))
