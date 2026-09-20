# StageFlow example backend

A runnable example of a backend for the
[StageFlow editor](https://github.com/leo-need-more-coffee/stageflow-ui), and a
set of demo pipelines for it.

The editor itself is pure static front-end: it holds no stage registry and
executes nothing. Everything it needs it asks a backend for, over the URL typed
on its connection screen. This repository is that backend — in about two
hundred lines of Python on top of the
[StageFlow core](https://github.com/leo-need-more-coffee/stageflow).

## Running it

```bash
pip install -r requirements.txt   # the core; openai is optional
python serve.py                   # http://127.0.0.1:8765
```

Then open the editor and type `http://127.0.0.1:8765` on its connection
screen.

Keys are handed to the server through the environment — they never travel
through the pipeline JSON:

```bash
SF_SECRET_OPENAI_API_KEY=sk-… python serve.py     # or SF_SECRETS=OPENAI_API_KEY
```

| variable | what it does |
|---|---|
| `SF_PORT` | the port (the default is 8765; a positional argument wins) |
| `SF_HOST` | what to bind to (the default is `127.0.0.1`) |
| `SF_SECRETS`, `SF_SECRET_<NAME>` | the secrets available to a run; the editor only ever sees the names |
| `SF_ALLOW_ORIGIN` | the origin allowed by CORS (the default is `*`) |
| `OPENAI_API_KEY`, `OPENAI_BASE_URL`, `OPENAI_MODEL` | read by the live stages of `llm_stages.py` |

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

Plus the static files of this folder: the SVG icons the demo stages refer to
(`icons/`) and the ready-made pipelines (`pipelines/`). Every answer carries
CORS headers — the editor is served from another origin.

A pipeline is executed by the REAL core, in a real `Session` with the core
debugger (`StepDebugger`): a second implementation of the semantics in
JavaScript would mean the debugger shows something other than what actually
happens.

## What is inside

| file | what it is |
|---|---|
| `serve.py` | the HTTP server, CORS, and three demo stages whose icons are links to SVG files |
| `runner_api.py` | the run API: a `Session` in a thread of its own, the event bus, the SSE stream, the secret masker |
| `llm_stages.py` | live stages: real OpenAI API calls and a real HTTP request, with the errors split into classes |
| `pipelines/demo-pipeline.json` | a small graph: `condition` + `parallel` + a `try` block, on the demo stages |
| `pipelines/llm-demo-pipeline.json` | 26 nodes on the live stages: a plan, parallel answers, a merge (see [docs/live-stages.md](docs/live-stages.md)) |
| `stages.json` | a snapshot of the specs — handy for opening the editor without a running backend |

To refresh the snapshot after changing the stages:

```bash
python -c "import sys; sys.path.insert(0, '.'); import serve; \
import json; from stageflow import get_stages; \
print(json.dumps({'stages': {n: c.get_specs() for n, c in get_stages().items()}}, ensure_ascii=False, indent=2))" > stages.json
```

The `import serve` is required: the demo stages of the server and `llm_stages`
are registered by it, and without them the snapshot comes out incomplete.

## Writing your own backend

There is nothing special about this one. The editor needs exactly the seven
endpoints listed above, the same event stream, and CORS headers. The stage
specs are whatever `get_specs()` of the core returns:
`{"stages": {"StageName": {"stage_name": …, "description": …, "category": …,
"icon": …, "arguments": [...], "outputs": [...]}}}`. An icon given as an
absolute path (`/icons/globe.svg`) is resolved by the editor against the
backend URL, so the icons are served from here.

## Documentation

- [Live stages](docs/live-stages.md) — the OpenAI API and HTTP stages,
  streaming answers, the demo pipeline

## License

MIT — see [LICENSE](LICENSE).
