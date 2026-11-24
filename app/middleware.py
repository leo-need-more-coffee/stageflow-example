"""Middleware that adds `Cache-Control: no-store` to every response."""
from __future__ import annotations

from typing import Any

from starlette.datastructures import MutableHeaders


class NoStore:
    """Adds `Cache-Control: no-store` to every response, static files
    included, so edited pipelines and fixtures are never served from cache.

    Plain ASGI rather than `@app.middleware("http")`, which buffers the body
    and would break the event stream.
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
