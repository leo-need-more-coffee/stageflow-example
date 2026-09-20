"""The two stages of the bot that actually call a language model.

Everything else in this example is prepared data (see ``support_stages.py``).
These two are the live part: they read the ticket the way a person would —
deciding what it is about and how angry it sounds — and write the reply.

Chat Completions rather than Responses: `/v1/chat/completions` is implemented
by every OpenAI-compatible gateway, proxy and local stub, and the `base_url`
argument points the stages at one. That is also how this example is tried
without a real key (see the README).

The key arrives as an ordinary `api_key` argument: in a pipeline that is a
variable from the editor's secret store, so only the name is in the JSON. With
the argument empty the SDK reads `OPENAI_API_KEY` from the environment and the
key never passes through the pipeline at all.

Four error classes, not one: in a graph they become roads. `LlmAuthError` means
"no key — fall back to the rules", `LlmRateLimited` and `LlmUnavailable` are
what a `retry` repeats, `LlmBadAnswer` means the model answered something the
schema did not allow. A single bare `Exception` would not let such a graph be
drawn.
"""
from __future__ import annotations

import json
import os

import openai

from stageflow import BaseStage, EventSpec, register_stage

#: The default model. Gateways each have their own catalogue, so it is an
#: argument of the node and an environment variable rather than a constant.
DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

TOPICS = ["billing", "technical", "account", "product", "other"]
URGENCIES = ["low", "normal", "high"]
MOODS = ["calm", "annoyed", "angry"]


class LlmAuthError(RuntimeError):
    """There is no key, or it was not accepted."""


class LlmRateLimited(RuntimeError):
    """429: too often, or the quota ran out. A retry with a pause helps."""


class LlmUnavailable(RuntimeError):
    """The API cannot be reached, or it answered 5xx. A retry helps too."""


class LlmBadAnswer(RuntimeError):
    """The model answered, but not with what was asked for."""


_clients: dict[tuple[str | None, str | None], openai.AsyncOpenAI] = {}


def _client(api_key: str | None, base_url: str | None) -> openai.AsyncOpenAI:
    """One client per (key, address). A pipeline of a dozen nodes would
    otherwise open a dozen connection pools."""
    key = (api_key or None, base_url or None)
    if key not in _clients:
        # retries are the node's business — on the graph they are visible
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


async def _chat(stage: BaseStage, args: dict, messages: list[dict],
                schema: dict | None = None, on_chunk=None) -> str:
    """One call, all the error mapping, and the streaming — in one place.

    `schema` turns the answer into a JSON object of a known shape; `on_chunk`
    receives the text as it arrives, so a stage can type it out instead of
    waiting for the whole answer.
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
    try:
        if on_chunk is None:
            answer = await client.chat.completions.create(**request)
            pieces.append(answer.choices[0].message.content or "")
        else:
            stream = await client.chat.completions.create(**request, stream=True)
            async for chunk in stream:
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
    category: "support.triage"
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

    #: The enums are not a hint but a guarantee: the graph routes on `topic`
    #: with a `switch`, and a label that is "roughly right" would break the
    #: routing silently.
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
    category: "support.reply"
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
        # the chunks go out as they arrive: the editor's debug panel shows the
        # answer being written instead of a node that sits still for half a
        # minute and then finishes
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
