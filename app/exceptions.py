"""Error responses in the format the editor shows.

Every error is `{"error": "..."}` with a 4xx status: an invalid pipeline or
command is a client error, not a server failure.
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
    # One message and a 400, instead of a list of pydantic locations.
    first = exc.errors()[0]
    where = ".".join(str(part) for part in first["loc"][1:])
    return JSONResponse({"error": f"{where}: {first['msg']}" if where else first["msg"]}, 400)


class ClientErrorRoute(APIRoute):
    """Turns exceptions raised by the core into 400 responses, so the
    handlers need no try/except."""

    def get_route_handler(self):
        handler = super().get_route_handler()

        async def guarded(request):
            try:
                return await handler(request)
            except (HTTPException, RequestValidationError):
                raise  # already an HTTP response
            except Exception as exc:  # noqa: BLE001 - an answer instead of a traceback
                raise HTTPException(400, f"{type(exc).__name__}: {exc}") from exc

        return guarded
