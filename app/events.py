"""The event log of a run and the SSE stream that sends it to the browser.

Events include `node_enter`/`node_exit` with the frame, `paused`, session
telemetry (`stage_started`, `condition_evaluated`, ...), `var_set`/
`var_rejected`, and finally `finished` or `failed`.
"""
from __future__ import annotations

import json
import threading
from typing import Any


def jsonable(value: Any) -> Any:
    """Convert a value for JSON: anything JSON cannot hold becomes its repr."""
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def telemetry_to_dict(event: Any) -> dict:
    """A session event as an event-stream dict."""
    return {
        "type": event.type,
        "node": event.stage_id,
        "session": event.session_id,
        "payload": event.payload or {},
    }


class EventBus:
    """A run's event log with subscribers. Keeps the full history, because a
    subscriber connects after the run has started and must see the start."""

    def __init__(self, mask: Any = None) -> None:
        self._items: list[dict] = []
        self._cond = threading.Condition()
        self._closed = False
        self.mask = mask

    @property
    def size(self) -> int:
        return len(self._items)

    def push(self, event: dict) -> None:
        if self.mask:
            event = self.mask(event)
        with self._cond:
            # Each event carries its index, so a client can resume with
            # `?from=N`.
            self._items.append({**event, "index": len(self._items)})
            self._cond.notify_all()

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def follow(self, start: int = 0, timeout: float = 30.0):
        """Yield events from `start`, waiting for new ones, until the run ends.
        Yields None every `timeout` seconds as a keep-alive."""
        index = start
        while True:
            with self._cond:
                while index >= len(self._items) and not self._closed:
                    if not self._cond.wait(timeout):
                        break
                if index >= len(self._items):
                    if self._closed:
                        return
                    yield None  # keep-alive
                    continue
                batch = self._items[index:]
                index = len(self._items)
            for event in batch:
                yield event


def sse_lines(bus: EventBus, start: int = 0):
    """SSE frames: `data: {...}` per event, or a keep-alive comment."""
    for event in bus.follow(start):
        if event is None:
            yield b": keep-alive\n\n"
            continue
        yield f"data: {json.dumps(jsonable(event), ensure_ascii=False)}\n\n".encode()
