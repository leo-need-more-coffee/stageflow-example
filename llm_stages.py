"""Stages that really do call the OpenAI API (and the network).

The demo stages in ``serve.py`` are stubs: they show that an icon can be a link
to an SVG file, and do nothing else. Here it is the other way round: real Chat
Completions calls through the official ``openai`` SDK and a real HTTP request.
That is what the editor was built for — to show that a graph assembled with the
mouse runs on a live core and does live work.

Why Chat Completions and not Responses: ``/v1/chat/completions`` is implemented
by every OpenAI-compatible gateway and proxy through which the API is usually
reachable, while ``/v1/responses`` is not. The ``base_url`` argument (or the
``OPENAI_BASE_URL`` environment variable) points requests at such a gateway;
the same argument lets a pipeline be tested without a real key — against a
local stub.

A stage gets the key as an ORDINARY ARGUMENT, ``api_key``: in a pipeline it
comes from a variable (the secret store substitutes it at start — see the docs
on secrets), and only the name stays in the JSON. If the argument is not set,
the server environment works (``OPENAI_API_KEY``) — the SDK reads it itself.

Errors are deliberately split into separate classes: ``LlmRateLimited``,
``LlmOverloaded``, ``LlmTimeout``, ``LlmRefused``, ``LlmAuthError``. This is
not pedantry — in a pipeline they become branches: `retry` on a node repeats
429 and 5xx, `except` of a `try` block takes a refusal and a bad key down
different roads. A single bare ``Exception`` would not let such a graph be
drawn at all.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import urllib.error
import urllib.parse
import urllib.request

import openai

from stageflow import BaseStage, EventSpec, register_stage

#: The default model. Changed by the `model` argument right in the node, or by
#: the OPENAI_MODEL environment variable — gateways and proxies each have their
#: own catalogue, and hardcoding one identifier into a pipeline is pointless.
DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")

LLM_ICON = "/icons/sparkles.svg"

#: The streaming event: a chunk of the answer goes out AS SOON AS it arrives
#: from the model, not after the stage has finished talking. The answer is
#: written in front of you — which gives the debugger not beauty but feedback:
#: you can see the node works, see WHAT it answers, and see it before it is
#: done.
#:
#: The ``stream: true`` field is a CONTRACT WITH THE EDITOR, not decoration:
#: the debug panel shows the stream of any node that sends chunks with such a
#: payload, and knows nothing about LLMs. The event name (``llm_delta``) is the
#: stage's own business; another streaming stage (a transcription, a build log,
#: a row-by-row export) gets the same panel under whatever name it likes.
#: ``label`` is the column heading, also from the stage: it is the stage that
#: knows what the text is.
STREAM_LABEL = "Model answer"

LLM_EVENTS = [
    EventSpec("llm_delta", "A chunk of the model answer, as soon as it arrived",
              payload_schema={"stream": bool, "text": str, "index": int, "label": str}),
    EventSpec("llm_completed", "The answer is finished: length and token usage",
              payload_schema={"chars": int, "input_tokens": int, "output_tokens": int}),
]


def _delta(stage: BaseStage, text: str, index: int) -> None:
    stage.emit("llm_delta", {"stream": True, "text": text, "index": index,
                             "label": STREAM_LABEL})


# --------------------------------------------------------------- errors

class LlmAuthError(RuntimeError):
    """There is no key, or it was not accepted."""


class LlmRateLimited(RuntimeError):
    """429: too often, or the quota ran out. Cured by a retry with a pause."""


class LlmOverloaded(RuntimeError):
    """5xx on the API side. Also cured by a retry."""


class LlmTimeout(RuntimeError):
    """The answer did not arrive within the allotted time."""


class LlmRefused(RuntimeError):
    """The model refused to answer (a refusal or a content filter)."""


class LlmTruncated(RuntimeError):
    """The answer was cut off by the token limit — it cannot be read as whole."""


class LlmModelNotFound(RuntimeError):
    """This key or gateway has no such model."""


class LlmBadRequest(RuntimeError):
    """A bad request: wrong parameter, input too long, malformed schema."""


class LlmUnavailable(RuntimeError):
    """The API cannot be reached: network, proxy, DNS."""


class HttpStatusError(RuntimeError):
    """The response arrived, but with an error code."""


class HttpTimeout(RuntimeError):
    """The server did not answer within the allotted time."""


class HttpUnavailable(RuntimeError):
    """The connection was not established."""


# --------------------------------------------------------------- client

_clients: dict[tuple[str | None, str | None], openai.AsyncOpenAI] = {}


def _client(api_key: str | None, base_url: str | None) -> openai.AsyncOpenAI:
    """A client per (key, address) pair.

    The cache is not a micro-optimisation: a pipeline of a dozen nodes would
    create a dozen HTTP pools, and parallel branches would lose connection
    reuse exactly where they run at the same time.
    """
    key = (api_key or None, base_url or None)
    if key not in _clients:
        kwargs: dict = {"max_retries": 0}  # retries belong to the node, they show on the graph
        if api_key:
            kwargs["api_key"] = api_key
        if base_url:
            kwargs["base_url"] = base_url
        _clients[key] = openai.AsyncOpenAI(**kwargs)
    return _clients[key]


def _resolve_key(args: dict) -> str | None:
    key = (args.get("api_key") or "").strip() or None
    if key:
        return key
    if os.environ.get("OPENAI_API_KEY"):
        return None  # the SDK takes it itself — the key never passes through the pipeline
    raise LlmAuthError(
        "No OpenAI API key: set the 'api_key' argument (a variable from the "
        "secret store, File -> Secrets...) or the OPENAI_API_KEY environment "
        "variable of the server"
    )


@contextlib.asynccontextmanager
async def _errors_mapped(model: str):
    """SDK errors into stage classes.

    A context manager rather than a wrapper around a single call: when
    streaming, half of the errors arrive not on the request but in the middle
    of iterating over the chunks, and they have to be caught there too.
    """
    try:
        yield
    except openai.AuthenticationError as exc:
        raise LlmAuthError(f"key not accepted: {exc}") from exc
    except openai.PermissionDeniedError as exc:
        raise LlmAuthError(f"key lacks permissions: {exc}") from exc
    except openai.NotFoundError as exc:
        raise LlmModelNotFound(f"model {model!r} is unavailable: {exc}") from exc
    except openai.RateLimitError as exc:
        raise LlmRateLimited(f"too often, or the quota ran out: {exc}") from exc
    except openai.BadRequestError as exc:
        raise LlmBadRequest(str(exc)) from exc
    except openai.APITimeoutError as exc:
        raise LlmTimeout(f"the answer did not arrive in time: {exc}") from exc
    except openai.APIConnectionError as exc:
        raise LlmUnavailable(f"the API cannot be reached: {exc}") from exc
    except openai.APIStatusError as exc:
        if exc.status_code >= 500:
            raise LlmOverloaded(f"the API is overloaded ({exc.status_code})") from exc
        raise LlmBadRequest(f"HTTP {exc.status_code}: {exc}") from exc


def _check_finish(finish: str | None, refusal: str) -> None:
    """A refusal and a truncation arrive as a successful response, not as an
    exception: such an answer cannot be read as an ordinary one."""
    if refusal:
        raise LlmRefused(f"the model refused: {refusal}")
    if finish == "content_filter":
        raise LlmRefused("the answer was stopped by the content filter")
    if finish == "length":
        raise LlmTruncated("the answer was cut off by max_tokens — raise the limit")


async def _complete(stage: BaseStage, args: dict, **request) -> tuple[str, dict]:
    """A Chat Completions call. Returns (text, token usage).

    Streaming by default: every chunk goes out as an ``llm_delta`` event as
    soon as it arrives, so the answer is visible as it is being written rather
    than appearing whole half a minute later. The ``stream: false`` argument
    turns streaming off (some gateways cannot do it) — the stage then emits a
    single ``llm_delta`` with the whole text, so that both roads look the same
    to the UI.
    """
    api_key = _resolve_key(args)
    client = _client(api_key, (args.get("base_url") or "").strip() or None)
    model = request.get("model") or DEFAULT_MODEL

    effort = (args.get("effort") or "").strip()
    if effort:
        # reasoning models only: ordinary ones do not accept it, so by default
        # we do not send it at all
        request["reasoning_effort"] = effort

    streaming = args.get("stream")
    streaming = True if streaming is None else bool(streaming)
    if not streaming:
        async with _errors_mapped(model):
            response = await _send(client, request)
        choice = response.choices[0]
        _check_finish(choice.finish_reason, getattr(choice.message, "refusal", None) or "")
        text = (choice.message.content or "").strip()
        usage = _usage(getattr(response, "usage", None))
        _delta(stage, text, 1)
        stage.emit("llm_completed", {"chars": len(text), **usage})
        return text, usage

    request = {**request, "stream": True, "stream_options": {"include_usage": True}}
    pieces: list[str] = []
    refusal: list[str] = []
    finish: str | None = None
    usage_obj = None
    index = 0

    async with _errors_mapped(model):
        stream = await _send(client, request)
        async for chunk in stream:
            if getattr(chunk, "usage", None):
                usage_obj = chunk.usage  # the last frame when include_usage is on
            if not chunk.choices:
                continue
            choice = chunk.choices[0]
            if choice.finish_reason:
                finish = choice.finish_reason
            delta = choice.delta
            if getattr(delta, "refusal", None):
                refusal.append(delta.refusal)
            piece = getattr(delta, "content", None)
            if not piece:
                continue
            pieces.append(piece)
            index += 1
            # an event per chunk is exactly what "written token by token" means
            _delta(stage, piece, index)

    _check_finish(finish, "".join(refusal))
    text = "".join(pieces).strip()
    usage = _usage(usage_obj)
    stage.emit("llm_completed", {"chars": len(text), **usage})
    return text, usage


async def _send(client: openai.AsyncOpenAI, request: dict):
    """A request adjusted for the age of the gateway.

    New models require ``max_completion_tokens`` and complain about
    ``max_tokens``; old gateways are the other way round and do not know the
    new name; many compatible servers have never heard of ``stream_options`` at
    all. One attempt does not cover that, so on a rejection over EXACTLY such a
    parameter we try once more without it: otherwise the pipeline would work
    with only half of the compatible servers.
    """
    attempt = dict(request)
    for _ in range(2):
        try:
            return await client.chat.completions.create(**attempt)
        except openai.BadRequestError as exc:
            message = str(exc)
            if "max_completion_tokens" in message and "max_completion_tokens" in attempt:
                attempt["max_tokens"] = attempt.pop("max_completion_tokens")
                continue
            if "stream_options" in message and "stream_options" in attempt:
                attempt.pop("stream_options")
                continue
            raise
    return await client.chat.completions.create(**attempt)


def _messages(system: str | None, prompt: str) -> list[dict]:
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    return messages


def _usage(usage) -> dict:
    """Token usage; a gateway may not send it — then zeros, not a crash."""
    return {
        "input_tokens": getattr(usage, "prompt_tokens", 0) or 0,
        "output_tokens": getattr(usage, "completion_tokens", 0) or 0,
    }


def _parse_json(text: str) -> dict:
    """The format is guaranteed by the schema, but a broken answer must fail
    with a clear error rather than a KeyError somewhere further down the graph."""
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise LlmBadRequest(f"the answer did not parse as JSON: {exc}") from exc


# --------------------------------------------------------------- stages

@register_stage("LlmMessageStage")
class LlmMessageStage(BaseStage):
    """
    description: "A request to an OpenAI model: a prompt in, the answer text out"
    icon: "/icons/sparkles.svg"
    icon_mono: true
    color: "#c084fc"
    arguments:
      prompt:
        type: string
        description: "The user message"
      system:
        type: string
        optional: true
        description: "System prompt: the role and the rules of the answer"
      model:
        type: string
        optional: true
        default: "gpt-4o"
        description: "Model identifier (each gateway has its own)"
      effort:
        type: string
        optional: true
        description: "reasoning_effort for reasoning models: minimal | low | medium | high"
      max_tokens:
        type: int
        optional: true
        default: 4096
        description: "Limit on the answer length"
      temperature:
        type: float
        optional: true
        description: "Spread of the answer; empty — the model default"
      api_key:
        type: string
        optional: true
        description: "API key; empty — taken from the server environment"
      base_url:
        type: string
        optional: true
        description: "A custom API address (proxy, compatible gateway)"
      stream:
        type: bool
        optional: true
        default: true
        description: "Write the answer as a stream, a chunk per event"
    outputs:
      text:
        type: string
        description: "The model answer"
      input_tokens:
        type: int
        description: "Input tokens"
      output_tokens:
        type: int
        description: "Output tokens"
    """

    category = "llm"
    timeout = 300  # reasoning models happily think for minutes
    allowed_events = LLM_EVENTS

    async def run(self):
        args = self.get_arguments()
        request = {
            "model": args.get("model") or DEFAULT_MODEL,
            "messages": _messages(args.get("system"), str(args.get("prompt") or "")),
            "max_completion_tokens": int(args.get("max_tokens") or 4096),
        }
        if args.get("temperature") is not None:
            request["temperature"] = float(args["temperature"])
        text, usage = await _complete(self, args, **request)
        self.set_outputs({"text": text, **usage})


@register_stage("LlmJsonStage")
class LlmJsonStage(BaseStage):
    """
    description: "An answer that follows a JSON schema: an object, not text"
    icon: "/icons/sparkles.svg"
    icon_mono: true
    color: "#a78bfa"
    arguments:
      prompt:
        type: string
        description: "What to extract or decide"
      schema:
        type: dict
        description: "JSON Schema of the answer (an object with properties)"
      system:
        type: string
        optional: true
        description: "System prompt"
      strict:
        type: bool
        optional: true
        default: true
        description: "Strict schema: the answer is guaranteed to match it"
      model:
        type: string
        optional: true
        default: "gpt-4o"
        description: "Model identifier"
      effort:
        type: string
        optional: true
        description: "reasoning_effort for reasoning models"
      max_tokens:
        type: int
        optional: true
        default: 4096
        description: "Limit on the answer length"
      api_key:
        type: string
        optional: true
        description: "API key; empty — taken from the server environment"
      base_url:
        type: string
        optional: true
        description: "A custom API address"
      stream:
        type: bool
        optional: true
        default: true
        description: "Write the answer as a stream, a chunk per event"
    outputs:
      data:
        type: dict
        description: "The parsed answer object"
      input_tokens:
        type: int
        description: "Input tokens"
      output_tokens:
        type: int
        description: "Output tokens"
    """

    category = "llm"
    timeout = 300
    allowed_events = LLM_EVENTS

    async def run(self):
        args = self.get_arguments()
        schema = args.get("schema")
        if not isinstance(schema, dict):
            raise LlmBadRequest("LlmJsonStage: the 'schema' argument must be a JSON schema")
        strict = args.get("strict")
        text, usage = await _complete(
            self,
            args,
            model=args.get("model") or DEFAULT_MODEL,
            messages=_messages(args.get("system"), str(args.get("prompt") or "")),
            max_completion_tokens=int(args.get("max_tokens") or 4096),
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "result",
                    "schema": schema,
                    "strict": True if strict is None else bool(strict),
                },
            },
        )
        self.set_outputs({"data": _parse_json(text), **usage})


@register_stage("LlmClassifyStage")
class LlmClassifyStage(BaseStage):
    """
    description: "Assign a text to one of the labels — an input for switch"
    icon: "/icons/sparkles.svg"
    icon_mono: true
    color: "#8b5cf6"
    arguments:
      text:
        type: string
        description: "What is being classified"
      labels:
        type: list
        description: "Allowed labels (the answer will be one of them)"
      criteria:
        type: string
        optional: true
        description: "What to go by when choosing"
      model:
        type: string
        optional: true
        default: "gpt-4o"
        description: "Model identifier"
      effort:
        type: string
        optional: true
        description: "reasoning_effort for reasoning models"
      api_key:
        type: string
        optional: true
        description: "API key; empty — taken from the server environment"
      base_url:
        type: string
        optional: true
        description: "A custom API address"
    outputs:
      label:
        type: string
        description: "The chosen label — exactly one of labels"
      reason:
        type: string
        description: "A short justification of the choice"
    """

    category = "llm"
    timeout = 120
    allowed_events = LLM_EVENTS

    async def run(self):
        args = self.get_arguments()
        labels = args.get("labels")
        if not isinstance(labels, list) or not labels:
            raise LlmBadRequest("LlmClassifyStage: 'labels' must be a non-empty list")
        labels = [str(label) for label in labels]
        criteria = args.get("criteria") or "Pick the most suitable label."
        text, _usage_ = await _complete(
            self,
            args,
            model=args.get("model") or DEFAULT_MODEL,
            messages=_messages(
                "You are a classifier. Answer strictly according to the schema.",
                f"{criteria}\n\nText:\n{args.get('text') or ''}",
            ),
            max_completion_tokens=1024,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "classification",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        # the enum in the schema is not a hint but a guarantee:
                        # a switch in the graph counts on a finite set of
                        # branches, and a label that is "roughly right" would
                        # break the routing silently
                        "properties": {
                            "label": {"type": "string", "enum": labels},
                            "reason": {"type": "string"},
                        },
                        "required": ["label", "reason"],
                        "additionalProperties": False,
                    },
                },
            },
        )
        data = _parse_json(text)
        self.set_outputs({
            "label": str(data.get("label", "")),
            "reason": str(data.get("reason", "")),
        })


@register_stage("HttpGetStage")
class HttpGetStage(BaseStage):
    """
    description: "A real GET request: status, body and parsed JSON"
    icon: "/icons/globe.svg"
    icon_mono: true
    arguments:
      url:
        type: string
        description: "Request address"
      headers:
        type: dict
        optional: true
        description: "Additional headers"
      timeout:
        type: float
        optional: true
        default: 15
        description: "How long to wait for the answer, seconds"
      max_bytes:
        type: int
        optional: true
        default: 200000
        description: "How much of the body to read: a whole answer may not fit the frame"
    outputs:
      status:
        type: int
        description: "HTTP status code"
      body:
        type: string
        description: "Response body (truncated to max_bytes)"
      json:
        type: any
        description: "Parsed JSON, or null if the body is not JSON"
    """

    category = "net"
    timeout = 60

    async def run(self):
        args = self.get_arguments()
        url = str(args.get("url") or "")
        if not url.startswith(("http://", "https://")):
            raise HttpUnavailable(f"HttpGetStage: expected an http(s) address, got {url!r}")
        headers = args.get("headers") or {}
        wait = float(args.get("timeout") or 15)
        limit = int(args.get("max_bytes") or 200_000)

        # the address is often assembled from a variable ("…?q=' + vars.topic"),
        # and the topic may well be non-Latin: without percent encoding urllib
        # fails on the first non-ASCII character
        safe_url = urllib.parse.quote(url, safe=":/?#[]@!$&'()*+,;=%~")

        def fetch() -> tuple[int, str]:
            request = urllib.request.Request(
                safe_url, headers={str(k): str(v) for k, v in headers.items()})
            with urllib.request.urlopen(request, timeout=wait) as response:
                return response.status, response.read(limit).decode("utf-8", "replace")

        # urllib is synchronous and the session is asynchronous: without a
        # separate thread the request would freeze the whole run, neighbouring
        # parallel branches included
        try:
            status, body = await asyncio.to_thread(fetch)
        except urllib.error.HTTPError as exc:
            raise HttpStatusError(f"{url}: HTTP {exc.code}") from exc
        except TimeoutError as exc:
            raise HttpTimeout(f"{url}: no answer within {wait} s") from exc
        except urllib.error.URLError as exc:
            reason = exc.reason
            if isinstance(reason, TimeoutError):
                raise HttpTimeout(f"{url}: no answer within {wait} s") from exc
            raise HttpUnavailable(f"{url}: {reason}") from exc

        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = None
        self.set_outputs({"status": status, "body": body, "json": parsed})
