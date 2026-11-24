"""A support bot served as a StageFlow backend for the StageFlow editor.

    python main.py [port]            # or: uvicorn main:app --reload

The editor (github.com/leo-need-more-coffee/stageflow-ui) is a static page:
it has no stages and runs nothing itself. It gets stage specs from a backend
and asks the backend to run pipelines. This is such a backend: a support bot
that loads a ticket, searches a knowledge base, and either replies or hands
the ticket to a person.

Endpoints are in ``app/api.py`` (also listed at ``/docs``). Runs use the real
StageFlow core (``app/runs.py``), so the debugger shows actual behaviour.
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
from app.auth import AUTH_HEADER
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
# The editor runs on another origin, so the browser needs CORS headers for
# every request, the SSE stream and the preflight of JSON POSTs.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.ALLOW_ORIGIN],
    allow_methods=["GET", "POST", "OPTIONS"],
    # Include the credential header, whatever this deployment calls it.
    allow_headers=["Content-Type", AUTH_HEADER],
    # The hosted editor (https) calling this backend on 127.0.0.1 is a
    # "private network" request in Chrome; without this the preflight fails.
    allow_private_network=True,
    max_age=86400,
)
app.add_exception_handler(HTTPException, http_error)
app.add_exception_handler(RequestValidationError, invalid_body)
app.include_router(router)


@app.get("/", include_in_schema=False)
async def index() -> RedirectResponse:
    """Opening the backend URL in a browser shows the API docs."""
    return RedirectResponse("/docs")


# Stage specs refer to icons by absolute path (`/icons/ticket.svg`), resolved
# against the backend URL. Only these folders are served, not the source.
for name in config.PUBLIC_DIRS:
    app.mount(f"/{name}", StaticFiles(directory=config.ROOT / name), name=name)


def main() -> None:
    import uvicorn

    port = int(sys.argv[1]) if len(sys.argv) > 1 else config.PORT
    print(f"StageFlow example backend: http://{config.HOST}:{port}")
    print("Type that address on the connection screen of the editor.")
    print(f"What it answers: http://{config.HOST}:{port}/docs")
    # No access log: the event stream would flood the console.
    uvicorn.run(app, host=config.HOST, port=port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
