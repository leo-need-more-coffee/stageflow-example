"""A support bot as a StageFlow backend for the StageFlow editor.

    python main.py [port]            # or: uvicorn main:app --reload

The editor (github.com/leo-need-more-coffee/stageflow-ui) is pure static
front-end: it holds no stage registry and executes nothing. Everything it
needs it asks a backend for, over the URL the user types on the connection
screen. This is such a backend — a support bot whose stages take a prepared
ticket, look the answer up in a knowledge base and either write a reply or
hand the ticket to a human.

What it answers is in ``app/api.py``, and ``/docs`` lists it; the pipelines
are executed by the REAL core (``app/runs.py``), because a second
implementation of the semantics in JavaScript would mean the debugger shows
something other than what actually happens.
"""
from __future__ import annotations

import sys

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException
from starlette.middleware.cors import CORSMiddleware

from app import config
from app.api import router
from app.exceptions import http_error, invalid_body
from app.middleware import NoStore
from app.stages import LLM_IMPORT_ERROR

if LLM_IMPORT_ERROR:  # pragma: no cover - depends on the environment
    print(f"LLM stages disabled ({LLM_IMPORT_ERROR}); install the SDK: pip install openai")

app = FastAPI(
    title="StageFlow example backend",
    description="A support bot for the StageFlow editor: stage specs, secret names, runs.",
    version="1.0.0",
)
app.add_middleware(NoStore)
# The editor lives on another origin, so without these headers every fetch and
# the SSE stream would be blocked by the browser before reaching us; the
# middleware answers the preflight of a POST with a JSON body too.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.ALLOW_ORIGIN],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
    max_age=86400,
)
app.add_exception_handler(HTTPException, http_error)
app.add_exception_handler(RequestValidationError, invalid_body)
app.include_router(router)


@app.get("/", include_in_schema=False)
async def index() -> RedirectResponse:
    """Whoever typed the address into a browser instead of into the editor
    wanted to see what it answers."""
    return RedirectResponse("/docs")


# A stage names its icon by an absolute path (`/icons/ticket.svg`), which the
# editor resolves against the backend URL — so the icons are served from here,
# and the pipelines and the fixtures alongside them. Only those three folders:
# the source of the backend is not the editor's business.
for name in config.PUBLIC_DIRS:
    app.mount(f"/{name}", StaticFiles(directory=config.ROOT / name), name=name)


def main() -> None:
    import uvicorn

    port = int(sys.argv[1]) if len(sys.argv) > 1 else config.PORT
    print(f"StageFlow example backend: http://{config.HOST}:{port}")
    print("Type that address on the connection screen of the editor.")
    print(f"What it answers: http://{config.HOST}:{port}/docs")
    # access_log off: the event stream would otherwise flood the console
    uvicorn.run(app, host=config.HOST, port=port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
