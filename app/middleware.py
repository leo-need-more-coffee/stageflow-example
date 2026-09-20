"""Middleware: one header, on every answer."""
from __future__ import annotations

from typing import Any

from starlette.datastructures import MutableHeaders


class NoStore:
    """``Cache-Control: no-store`` on every answer, the static files included.

    An edited pipeline or fixture must never be served from the browser cache:
    the files are read on every run, and that is the point of them.

    A plain ASGI middleware and not ``@app.middleware("http")`` on purpose:
    the latter pumps the response body through a queue, and the event stream
    has to reach the browser chunk by chunk — a token at a time.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def with_header(message):
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message).setdefault("cache-control", "no-store")
            await send(message)

        await self.app(scope, receive, with_header)
