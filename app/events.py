"""The event log of a run and its way out to the browser.

The events are what a debugger needs to show: ``node_enter`` / ``node_exit``
with the frame, ``paused`` (stopped, waiting for a step), session telemetry
(``stage_started``, ``stage_failed``, ``condition_evaluated``, …), ``var_set``
/ ``var_rejected``, ``finished`` with the result and artifacts, or ``failed``
with the error.
"""
from __future__ import annotations

import json
import threading
from typing import Any


def jsonable(value: Any) -> Any:
    """Frame values can be anything (a stage object, a set) — what survives
    JSON goes into the event stream as it is, the rest as a string."""
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def telemetry_to_dict(event: Any) -> dict:
    """Session telemetry in a shape fit for the event stream."""
    return {
        "type": event.type,
        "node": event.stage_id,
        "session": event.session_id,
        "payload": event.payload or {},
    }


class EventBus:
    """The event log of a run, with subscribers.

    A log rather than a plain queue: a subscriber arrives over a separate HTTP
    request after the start, and without history it would miss the beginning —
    and in debugging the beginning is the interesting part.
    """

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
            # the event number is part of the event itself: a subscriber can
            # read the stream from where it broke off (`?from=N`) without
            # consulting anything else
            self._items.append({**event, "index": len(self._items)})
            self._cond.notify_all()

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def follow(self, start: int = 0, timeout: float = 30.0):
        """Generator: yields events from ``start``, waits for new ones, ends
        together with the run. ``timeout`` is how often to yield ``None`` —
        "nothing yet" — so that the connection is not taken for a hung one."""
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
    """SSE frames for the event stream: ``data: {...}`` or a heartbeat comment."""
    for event in bus.follow(start):
        if event is None:
            yield b": keep-alive\n\n"
            continue
        yield f"data: {json.dumps(jsonable(event), ensure_ascii=False)}\n\n".encode()
