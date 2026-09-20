# StageFlow example: a support bot

A working backend for the
[StageFlow editor](https://github.com/leo-need-more-coffee/stageflow-ui) and
four pipelines to open in it.

The editor is static front-end: it draws and debugs a graph but holds no stages
and executes nothing. This repository is what it talks to — a support bot
whose stages take a prepared ticket, decide what it is about, look the answer
up in a knowledge base and either write a reply or hand the ticket to a human.

Everything the bot works on is prepared data in `data/`, so a run gives the
same result every time. The one live part is the language model: it reads the
ticket (topic, urgency, mood) and writes the reply.

## Running it

```bash
pip install -r requirements.txt
python main.py                   # http://127.0.0.1:8765
```

The backend is a FastAPI application, so the usual way works too — and it is
the one to use with `--reload` while editing a stage:

```bash
uvicorn main:app --port 8765 --reload
```

Either way `http://127.0.0.1:8765/docs` describes every endpoint below and
lets you start a run straight from the page, without the editor.

Then open the editor and type `http://127.0.0.1:8765` on its connection
screen. "File" → "Import JSON…" opens one of the pipelines from `pipelines/`;
"Run" → "Debug step by step" walks it a node at a time.

The key for the model is given to the server, not to the pipeline:

```bash
SF_SECRET_OPENAI_API_KEY=sk-… python main.py
```

The editor then sees only the NAME `OPENAI_API_KEY`, and the pipelines read it
as an ordinary variable. (`OPENAI_API_KEY=sk-… python main.py` works too — the
SDK picks it up itself.)

**Without a key** the fourth pipeline still runs end to end: the model failing
is a road on the graph, and it leads to the keyword rules. The first three stop
at the triage node with `LlmAuthError` — which is worth stepping through once,
it is the clearest thing the debugger shows.

| variable | what it does |
|---|---|
| `SF_PORT`, `SF_HOST` | where to listen (default `127.0.0.1:8765`); `python main.py 9000` overrides the port |
| `SF_SECRETS`, `SF_SECRET_<NAME>` | the secrets available to a run; the editor only ever sees their names |
| `SF_ALLOW_ORIGIN` | the origin allowed by CORS (default `*`) |
| `OPENAI_API_KEY`, `OPENAI_BASE_URL`, `OPENAI_MODEL` | read by the two model stages |

`OPENAI_BASE_URL` points the stages at any OpenAI-compatible gateway, proxy or
local stub — that is also how to try the first three pipelines without a real
key.

## The files

```
main.py                 the FastAPI app: middleware, error handlers, statics
app/api.py              what the editor asks for — every endpoint
app/schemas.py          the bodies of the run API
app/runs.py             a run: a real Session in its own thread, with the debugger
app/events.py           the event log of a run and the SSE stream out of it
app/secrets.py          which secrets the server has, and hiding them again
app/config.py           where the files are, what the environment asks for
app/stages/support.py   the stages that read the prepared data
app/stages/llm.py       the two that call a model
check_pipelines.py      what CI runs: the pipelines are valid, the full one runs
```

## The four pipelines

They are the same bot at four sizes. Each one adds exactly one idea to the
previous, so the graph stays readable while it grows.

| file | nodes | what is new |
|---|---|---|
| `01-triage.json` | 4 | a straight line: load a ticket, let the model read it |
| `02-auto-reply.json` | 10 | a knowledge base search and the first `condition`: an answer, or a human |
| `03-routing.json` | 13 | `parallel` (the customer and the article at once) and a `switch` on urgency |
| `04-support-bot.json` | 16 + 5 | `try`/`except` down to the keyword rules, `retry` on a rate limit, and a subpipeline that writes the reply |

The fourth one is the one to look at in the editor: the `try` block frames the
nodes whose errors it catches, the `parallel` frames its branches, and the
subpipeline is a graph of its own behind one node.

## The stages

`app/stages/support.py` — the prepared part, no network:

| stage | what it does |
|---|---|
| `LoadTicketStage` | takes a ticket out of `data/tickets.json` |
| `LoadCustomerStage` | the customer's plan and promised answer time |
| `ClassifyByRulesStage` | topic and urgency by keywords — the road taken when the model is unavailable |
| `SearchKnowledgeStage` | finds an article in `data/knowledge.json`, or says it found nothing |
| `RenderReplyStage` | fills `{ticket_id}` and friends in an article |
| `SendReplyStage` | sends the reply — and types it out word by word, so the editor shows it arriving |
| `EscalateStage` | creates a task for a human and says why |

`app/stages/llm.py` — the two that call a model:

| stage | what it does |
|---|---|
| `LlmTriageStage` | topic, urgency, mood and a one-line summary, by a strict JSON schema |
| `LlmReplyStage` | writes the reply from the article, streaming it as it arrives |

The strict schema is not decoration: the graph routes on `topic` with a
`switch`, and a label that is only roughly right would break the routing
silently.

Four error classes (`LlmAuthError`, `LlmRateLimited`, `LlmUnavailable`,
`LlmBadAnswer`) exist for the same reason — in a graph they become roads: a
`retry` repeats the rate limit, an `except` sends "no key" to the rules.

## The data

| file | what is in it |
|---|---|
| `data/tickets.json` | seven situations: a double charge, a stuck dashboard, a lockout, a rate limit, a feature request, an angry third reminder, a how-to |
| `data/customers.json` | five customers with plans and answer times |
| `data/knowledge.json` | six knowledge base articles plus the "nothing matched" fallback |

Editing these is the fastest way to try another situation — the files are read
on every run, so there is nothing to restart.

## What it answers

```
GET    /api/stages             the specs of every registered stage
GET    /api/secrets            the NAMES of the secrets in the environment
POST   /api/run                {pipeline, vars, mode: "run"|"step", delay} -> {id, state}
GET    /api/run/<id>           the state of the run
GET    /api/run/<id>/events    the event stream (SSE), ?from=N — read on from the Nth
POST   /api/run/<id>/control   {action: "step"|"resume"|"pause"|"stop"|"delay", count, delay}
POST   /api/run/<id>/vars      {set: {...}, drop: [...]}
```

Plus `GET /docs` (the same list, interactive — `/` redirects there) and three
folders of static files: the icons the stages refer to (`icons/`), the
pipelines and the fixtures. Only those three — the source of the backend is not
the editor's business. Every answer carries CORS headers, the editor being
served from another origin, and `Cache-Control: no-store`, so an edited
pipeline is never served from the browser cache.

The bodies are declared as models (`app/schemas.py`), and a body they reject
comes back the same way a broken pipeline does: `400` with `{"error": "…"}`,
which is the field the editor shows in its status bar.

A pipeline is executed by the real core, in a real `Session` with the core
debugger, so the editor's step debugger shows what actually happens rather than
a second implementation of the semantics in JavaScript.

`stages.json` is a snapshot of the specs, handy for opening the editor without
a running backend. To refresh it after changing a stage:

```bash
python -c "import app.stages, json; from stageflow import get_stages; \
print(json.dumps({'stages': {n: c.get_specs() for n, c in get_stages().items()}}, ensure_ascii=False, indent=2))" > stages.json
```

## Checking it after an edit

```bash
python check_pipelines.py
```

Validates every pipeline the way the core does and runs the full one without a
key, both roads: an answered ticket and an escalated one.

## Writing your own backend

There is nothing special about this one — the editor needs the endpoints above,
the same event stream and CORS headers; FastAPI here is a convenience, not a
requirement of the protocol, and `app/api.py` is the whole of it. The stage
specs are whatever `get_specs()` of the core returns; an icon given as an
absolute path (`/icons/ticket.svg`) is resolved by the editor against the
backend URL, so the icons are served from here.

## License

MIT — see [LICENSE](LICENSE).
