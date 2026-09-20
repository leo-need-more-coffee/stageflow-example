"""Failures in the shape the editor reads.

Every answer that is not a result is ``{"error": "…"}`` with a 4xx: that is
the field the editor shows in its status bar (``runner.js``), and a broken
pipeline description or command is the client's mistake, not a server failure.
"""
from __future__ import annotations

from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.exceptions import HTTPException as StarletteHTTPException


def http_error(_request, exc: StarletteHTTPException) -> JSONResponse:
    return JSONResponse({"error": exc.detail}, exc.status_code, exc.headers)


def invalid_body(_request, exc: RequestValidationError) -> JSONResponse:
    # one message and a 400, the same shape as any other client mistake —
    # rather than a list of pydantic locations to unpick in the status bar
    first = exc.errors()[0]
    where = ".".join(str(part) for part in first["loc"][1:])
    return JSONResponse({"error": f"{where}: {first['msg']}" if where else first["msg"]}, 400)


class ClientErrorRoute(APIRoute):
    """Answers with a 400 where the core raised, keeping the handlers
    themselves free of try/except."""

    def get_route_handler(self):
        handler = super().get_route_handler()

        async def guarded(request):
            try:
                return await handler(request)
            except (HTTPException, RequestValidationError):
                raise  # already an answer
            except Exception as exc:  # noqa: BLE001 - an answer instead of a traceback
                raise HTTPException(400, f"{type(exc).__name__}: {exc}") from exc

        return guarded
