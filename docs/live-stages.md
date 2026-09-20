# Live stages: the OpenAI API and the network

The demo stages in `serve.py` are stubs: they show that an `icon` can be a link
to an SVG file, and do nothing else. `llm_stages.py` is the opposite: real Chat
Completions calls through the official `openai` SDK and a real HTTP request.
That is what the editor was built for — a graph assembled with the mouse runs
on a live core and does live work.

Chat Completions and not Responses: `/v1/chat/completions` is implemented by
every OpenAI-compatible gateway and proxy through which the API is usually
reachable — `/v1/responses` is not. For the same reason a stage can fall back
from `max_completion_tokens` to `max_tokens`: new models require the first
name, old gateways know only the second, and one attempt does not cover that.

| stage | what it does | outputs |
|---|---|---|
| `LlmMessageStage` | a request to a model: a prompt (plus a system one) in, the answer as a stream | `text`, `input_tokens`, `output_tokens` |
| `LlmJsonStage` | an answer that follows a JSON schema — an object, not text | `data`, the tokens |
| `LlmClassifyStage` | one label out of a list (an `enum` in the schema) — an input for a `switch` | `label`, `reason` |
| `HttpGetStage` | a real GET: the status, the body, the parsed JSON | `status`, `body`, `json` |

**The key arrives as an ordinary `api_key` argument** — in a pipeline that is
the `OPENAI_API_KEY` variable from the secret store, so only the name stays in
the JSON (see the editor's documentation on secrets). An empty argument means "take it from the
server environment": the key then does not pass through the pipeline at all —
the SDK reads `OPENAI_API_KEY` itself. The `base_url` argument (or
`OPENAI_BASE_URL` on the server) points the requests at your own gateway or
proxy — the same argument lets a pipeline be tested without a real key, against
a local Chat Completions stub. The model is the `model` argument of the node
(`gpt-4o` by default, overridden by `OPENAI_MODEL`): every gateway has its own
catalogue, and hardcoding one identifier into a pipeline is pointless.

**The errors are split into classes** — `LlmAuthError`, `LlmRateLimited`,
`LlmOverloaded`, `LlmTimeout`, `LlmRefused`, `LlmTruncated`,
`LlmModelNotFound`, `LlmBadRequest`, `LlmUnavailable`, `HttpStatusError`,
`HttpTimeout`, `HttpUnavailable`. This is not pedantry: in a graph they become
roads. A node's `retry` repeats 429 and 5xx with a pause, the `except` of a
`try` block takes "no key" and "the model refused" down different branches. A
single bare `Exception` would not let such a graph be drawn. A refusal arrives
as a successful HTTP 200 — as a `refusal` field or `finish_reason:
"content_filter"` — so it is checked BEFORE the text is read; an answer cut off
by the limit (`finish_reason: "length"`) is a separate error too, not "just a
short answer".

## The answer is written as it is written

The stages call the model **as a stream** and hand every chunk that arrives
outwards AT ONCE — as an `llm_delta` event (plus `llm_completed` at the end,
with the length and the token usage). The events are declared as a contract of
the stage (`allowed_events`), so this is ordinary StageFlow telemetry rather
than a separate channel: it travels the same way as `node_enter` and
`stage_started` — through the session, the run bus and SSE. What that gives the
debugger is not beauty: it is visible that the node is alive, and visible WHAT
it answers, without waiting for it to finish.

**The editor meanwhile knows nothing about LLMs.** It must not know the event
names of the demo stages: those live in `llm_stages.py` next to the server, not
in the editor core, and tomorrow the stream will be written by a different
stage — a transcription, a build log, a row-by-row export. So a stream is
**any event with such a payload**:

```json
{"stream": true, "text": "a chunk", "index": 7, "label": "Model answer"}
```

`stream: true` means "this is a chunk of text a node is writing right now",
`label` is what to call the column (the stage knows, not the panel). The name
of the event itself does not matter. This is verified directly: a
`FakeTranscribeStage` that sends a `chunk` event and never mentions a model
gets the same panel with the heading "Transcription".

In the debug panel the stream has a column of its own — with the node name in
the heading and a blinking cursor while the chunks keep coming. Every node has
its own text: three `parallel` branches write at the same time, and the panel
shows the one that wrote last. A repeated entry into a node (a `retry`) clears
the text — otherwise the second attempt would be appended to the unfinished
answer of the first. The cursor goes out when the node is left: no separate
"stream closed" is needed, the end of a node is the end of its stream.

**The stream deliberately goes past the common `change` event.** There are
hundreds of chunks per answer, and `change` redraws the graph, the palette and
the toolbar whole: the tab would lie down on the thirtieth token. So `Runner`
accumulates the text itself and sends a separate `stream` event, on which the
panel patches ONE DOM node. The chunks do not get into the event log either —
they would crowd out everything meaningful (the log limit is 200 entries).
Verified on a run: 39 stream events caused not one extra redraw of the canvas.

The `stream: false` argument turns streaming off — some gateways cannot do it.
The stage then emits a single chunk with the whole text: both roads look the
same to the panel.

`effort` (that is, `reasoning_effort`) is not sent at all by default: ordinary
models do not accept it and answer 400. Set it in a node only when working with
a reasoning model.

## The demo pipeline: `pipelines/llm-demo-pipeline.json`

Opened in the editor through "File" → "Import JSON…" → from a file (or fetched from this server at `/pipelines/llm-demo-pipeline.json`). 26 nodes, a "topic
overview":

1. `plan` (`LlmJsonStage`) breaks the topic into three sub-questions and
   decides whether it is heavy or light — the answer follows a strict schema
   (`json_schema`), so the decision can be checked by an expression rather than
   by parsing text;
2. `route` (`switch`) sends it down one of two roads by `vars.plan.difficulty`;
3. the heavy one is a `parallel` of four branches: three questions go to the
   model at the same time, the fourth goes to GitHub for repositories on the
   topic (`HttpGetStage` plus a `retry` and a nested `try` of its own: the
   overview must not be cancelled because GitHub did not answer);
4. `merge` and `tally` bring the answers together and count the tokens with a
   CEL expression;
5. `polish` is a subpipeline with a frame of its own: a template plus an
   editorial pass over the answer (`inputs` / `artifact_outputs` are the only
   boundary of data visibility);
6. `done` hands over the artifacts: the report, the plan, the sources, the
   token usage.

The error handling is two nested `try` blocks, and every road has an end of its
own: `guard` covers the planner (no key → `no_key` → the `no_key` terminal; a
refusal from the model → a spare plan that leaves THE SAME set of variables),
`work` covers the whole answering phase (they did not come together →
`degrade` → the `partial` terminal). So the pipeline ends meaningfully in four
cases: `ok`, `partial`, `no_key` and a failure — and all four have been
verified by running them.

To run it: `pip install openai`, then the key — either in "File" → "Secrets…"
under the name `OPENAI_API_KEY`, or through this server's environment
(`SF_SECRET_OPENAI_API_KEY=sk-… python serve.py`) — and "Run" → "Run". A pace
of 0.5–1 s makes the run watchable: it is visible how three branches go to the
model at the same time and how the answers converge back.
