"""The two stages that call a language model: triage and reply.

They use Chat Completions (`/v1/chat/completions`), which every
OpenAI-compatible gateway, proxy and local stub implements; `base_url`
selects one.

The key is the `api_key` argument, usually a secret variable from the
editor, so only its name appears in the pipeline JSON. If it is empty, the
SDK reads `OPENAI_API_KEY` from the environment.

Errors are split into four classes so the graph can route on them:
`LlmAuthError` (no key: fall back to rules), `LlmRateLimited` and
`LlmUnavailable` (worth a `retry`), `LlmBadAnswer` (the answer did not match
the schema).

These stages use both halves of the budget: `reserve` in the spec is checked
before the call, and `charge()` afterwards records the actual usage reported
by the provider.
"""
from __future__ import annotations

import asyncio
import json
import os

import openai

from stageflow import BaseStage, EventSpec, register_stage

#: Default model; overridable per node and via the environment, since each
#: gateway has its own model names.
DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

TOPICS = ["billing", "technical", "account", "product", "other"]
URGENCIES = ["low", "normal", "high"]
MOODS = ["calm", "annoyed", "angry"]


class LlmAuthError(RuntimeError):
    """No API key, or it was rejected."""


class LlmRateLimited(RuntimeError):
    """429: rate limit or quota. A retry with a delay may help."""


class LlmUnavailable(RuntimeError):
    """The API is unreachable or returned 5xx. A retry may help."""


class LlmBadAnswer(RuntimeError):
    """The model's answer did not match what was asked for."""


_clients: dict[tuple[object, str | None, str | None], openai.AsyncOpenAI] = {}


def _client(api_key: str | None, base_url: str | None) -> openai.AsyncOpenAI:
    """A cached client per (key, base URL, event loop).

    The loop is part of the key because each run has its own thread and loop
    (see ``app/runs.py``) and the SDK binds its connection pool to a loop.
    Sharing a client across runs would fail with "Event loop is closed".
    """
    loop = asyncio.get_running_loop()
    for stale in [k for k in _clients if k[0] is not loop and k[0].is_closed()]:
        _clients.pop(stale, None)  # that run and its loop are gone
    key = (loop, api_key or None, base_url or None)
    if key not in _clients:
        # No SDK retries: retries belong on the graph, where they are visible.
        kwargs: dict = {"max_retries": 0}
        if api_key:
            kwargs["api_key"] = api_key
        if base_url:
            kwargs["base_url"] = base_url
        _clients[key] = openai.AsyncOpenAI(**kwargs)
    return _clients[key]


def _resolve_key(args: dict) -> str | None:
    key = (args.get("api_key") or "").strip() or None
    if key or os.environ.get("OPENAI_API_KEY"):
        return key
    raise LlmAuthError(
        "no OpenAI API key: set the 'api_key' argument (a variable from the "
        "editor's secret store) or OPENAI_API_KEY in the server environment"
    )


def _tokens_used(usage, messages: list[dict], answer: str) -> int:
    """Tokens used by a call: the provider's `usage` if reported, otherwise
    an estimate of four characters per token (so a token limit still works
    with gateways that do not report usage)."""
    total = getattr(usage, "total_tokens", None)
    if total:
        return int(total)
    text = "".join(str(m.get("content") or "") for m in messages) + answer
    return max(1, len(text) // 4)


async def _chat(stage: BaseStage, args: dict, messages: list[dict],
                schema: dict | None = None, on_chunk=None) -> str:
    """Make one model call: error mapping, optional streaming and charging.

    `schema` makes the answer a JSON object of that shape; `on_chunk`
    receives text as it arrives. A call that was sent is always charged; a
    failed call keeps its stage's reservation.
    """
    client = _client(_resolve_key(args), (args.get("base_url") or "").strip() or None)
    request: dict = {
        "model": args.get("model") or DEFAULT_MODEL,
        "messages": messages,
    }
    if schema is not None:
        request["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": "answer", "strict": True, "schema": schema},
        }

    pieces: list[str] = []
    usage = None
    try:
        if on_chunk is None:
            answer = await client.chat.completions.create(**request)
            usage = answer.usage
            pieces.append(answer.choices[0].message.content or "")
        else:
            # Ask for usage in the final stream chunk.
            stream = await client.chat.completions.create(
                **request, stream=True, stream_options={"include_usage": True})
            async for chunk in stream:
                if getattr(chunk, "usage", None):
                    usage = chunk.usage
                if not chunk.choices:
                    continue
                piece = getattr(chunk.choices[0].delta, "content", None)
                if not piece:
                    continue
                pieces.append(piece)
                on_chunk(piece)
    except openai.AuthenticationError as exc:
        raise LlmAuthError(f"the key was not accepted: {exc}") from exc
    except openai.PermissionDeniedError as exc:
        raise LlmAuthError(f"the key lacks permissions: {exc}") from exc
    except openai.RateLimitError as exc:
        raise LlmRateLimited(str(exc)) from exc
    except openai.APIConnectionError as exc:
        raise LlmUnavailable(f"the API cannot be reached: {exc}") from exc
    except openai.APIStatusError as exc:
        if exc.status_code >= 500:
            raise LlmUnavailable(f"the API answered {exc.status_code}") from exc
        raise LlmBadAnswer(f"HTTP {exc.status_code}: {exc}") from exc

    text = "".join(pieces).strip()
    # Charge before checking for an empty answer: it was paid for anyway.
    stage.charge(llm_calls=1, tokens=_tokens_used(usage, messages, text))
    if not text:
        raise LlmBadAnswer("the model returned an empty answer")
    return text


@register_stage("LlmTriageStage")
class LlmTriageStage(BaseStage):
    """
    description: "Reads the ticket: topic, urgency, mood, one-line summary"
    icon: "/icons/sparkles.svg"
    icon_mono: true
    color: "#c084fc"
    reserve:
      llm_calls: 1
      tokens: "size(args.text) / 4 + 300"
    arguments:
      text:
        type: string
        description: "The customer's message"
      subject:
        type: string
        optional: true
        description: "The subject line, if there is one"
      model:
        type: string
        optional: true
        description: "Model identifier (every gateway has its own)"
      api_key:
        type: string
        optional: true
        description: "API key; empty — from the server environment"
      base_url:
        type: string
        optional: true
        description: "A custom API address (proxy, gateway, local stub)"
    outputs:
      topic:
        type: string
        description: "billing | technical | account | product | other"
      urgency:
        type: string
        description: "low | normal | high"
      mood:
        type: string
        description: "calm | annoyed | angry"
      summary:
        type: string
        description: "What the ticket is about, in one sentence"
    """

    category = "support.triage"
    timeout = 60

    #: Strict enums: the graph routes on `topic` with a `switch`, so values
    #: must match exactly.
    SCHEMA = {
        "type": "object",
        "properties": {
            "topic": {"type": "string", "enum": TOPICS},
            "urgency": {"type": "string", "enum": URGENCIES},
            "mood": {"type": "string", "enum": MOODS},
            "summary": {"type": "string"},
        },
        "required": ["topic", "urgency", "mood", "summary"],
        "additionalProperties": False,
    }

    async def run(self):
        args = self.get_arguments()
        subject = str(args.get("subject") or "")
        text = str(args.get("text") or "")
        answer = await _chat(self, args, [
            {"role": "system", "content":
                "You triage support tickets. Answer strictly by the schema. "
                "The summary is one sentence, no more than 15 words."},
            {"role": "user", "content": f"Subject: {subject}\n\nMessage:\n{text}"},
        ], schema=self.SCHEMA)

        try:
            data = json.loads(answer)
        except json.JSONDecodeError as exc:
            raise LlmBadAnswer(f"the answer is not JSON: {exc}") from exc
        if data.get("topic") not in TOPICS:
            raise LlmBadAnswer(f"unknown topic {data.get('topic')!r}")

        self.set_outputs({
            "topic": data["topic"],
            "urgency": data.get("urgency", "normal"),
            "mood": data.get("mood", "calm"),
            "summary": str(data.get("summary", "")).strip(),
        })


@register_stage("LlmReplyStage")
class LlmReplyStage(BaseStage):
    """
    description: "Writes the reply from the knowledge base article — as a stream"
    icon: "/icons/sparkles.svg"
    icon_mono: true
    color: "#a78bfa"
    reserve:
      llm_calls: 1
      tokens: "(size(args.text) + size(args.article)) / 4 + 600"
    arguments:
      text:
        type: string
        description: "The customer's message"
      article:
        type: string
        description: "The knowledge base answer to build on"
      name:
        type: string
        optional: true
        description: "Who to greet"
      model:
        type: string
        optional: true
        description: "Model identifier"
      api_key:
        type: string
        optional: true
        description: "API key; empty — from the server environment"
      base_url:
        type: string
        optional: true
        description: "A custom API address"
    outputs:
      reply:
        type: string
        description: "The finished reply"
      chars:
        type: int
        description: "Its length"
    """

    category = "support.reply"
    timeout = 120
    allowed_events = [
        EventSpec("reply_chunk", "A piece of the reply, as soon as it arrived",
                  payload_schema={"stream": bool, "text": str, "label": str}),
    ]

    async def run(self):
        args = self.get_arguments()
        name = str(args.get("name") or "there")
        # Forward chunks as they arrive, so the debug panel shows the reply
        # being written.
        reply = await _chat(self, args, [
            {"role": "system", "content":
                "You are a support agent. Answer the customer using ONLY the "
                "facts from the knowledge base article. Be warm, concrete and "
                "short: three or four sentences. No lists, no headings."},
            {"role": "user", "content":
                f"Customer name: {name}\n\n"
                f"Their message:\n{args.get('text') or ''}\n\n"
                f"Knowledge base article:\n{args.get('article') or ''}"},
        ], on_chunk=lambda piece: self.emit("reply_chunk", {
            "stream": True, "text": piece, "label": "Reply",
        }))
        self.set_outputs({"reply": reply, "chars": len(reply)})
