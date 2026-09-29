"""Who is calling, and therefore which plan the run is on.

Deliberately *here* and not in the core. StageFlow knows `Policy` — what a
caller may compose and how much it may spend — and knows nothing about
tokens, headers, sessions or tenants, because every platform already has its
own answer to those and would have to fight the framework's. The core takes a
policy; producing one out of an HTTP request is this file, forty lines of it,
and a real backend replaces it with its own without touching anything else.

What the example demonstrates is not the token scheme (it is the crudest one
possible) but **where the check happens**. A client may ask to be *shown* any
plan it likes — ``/api/meta?plan=pro`` — and gets an honest answer about what
that plan allows without having to be entitled to it: drawing is not running,
and a name is not a permission. The plan a run is *executed* under is never
taken from the request that way; it is resolved here, from the credential,
and a graph drawn against a wider plan is refused rather than quietly
narrowed.

Configuring it::

    SF_TOKENS="demo-basic:basic,demo-pro:pro" python main.py

Nothing configured — no tokens, no authentication: the example stays an
example that starts and works. Configure one token and every endpoint starts
demanding one, which is the behaviour to point at.
"""
from __future__ import annotations

import os
from typing import Annotated

from fastapi import Depends, HTTPException, Request

from app.plans import DEFAULT_PLAN, known

#: Which header the credential arrives in. A setting rather than a constant
#: because backends disagree: `Authorization: Bearer …`, `X-Api-Key: …`, a
#: gateway header put there by something upstream.
AUTH_HEADER = os.environ.get("SF_AUTH_HEADER", "Authorization")


def _parse_tokens(raw: str) -> dict[str, str]:
    """``"tok-a:basic, tok-b:pro"`` -> ``{"tok-a": "basic", "tok-b": "pro"}``."""
    tokens: dict[str, str] = {}
    for pair in raw.split(","):
        token, _, plan = pair.strip().partition(":")
        if token and known(plan):
            tokens[token] = plan
    return tokens


TOKENS: dict[str, str] = _parse_tokens(os.environ.get("SF_TOKENS", ""))

#: Whether this process tells callers apart at all.
ENABLED = bool(TOKENS)


def credential(request: Request) -> str | None:
    """The token out of the header, with an optional scheme in front of it.

    ``Bearer abc`` and a bare ``abc`` both give ``abc``: which of the two a
    client sends is a matter of habit, and refusing one of them teaches
    nothing.
    """
    raw = (request.headers.get(AUTH_HEADER) or "").strip()
    if not raw:
        return None
    scheme, _, rest = raw.partition(" ")
    return rest.strip() if rest.strip() and scheme.lower() == "bearer" else raw


def caller_plan(request: Request) -> str:
    """The plan this request is entitled to. The authoritative answer."""
    if not ENABLED:
        return DEFAULT_PLAN
    token = credential(request)
    if token is None:
        raise HTTPException(401, f"no credentials: send a token in the "
                                 f"'{AUTH_HEADER}' header")
    plan = TOKENS.get(token)
    if plan is None:
        raise HTTPException(401, "the credentials were not recognised")
    return plan


#: How the plan in an answer was arrived at, so that a client can say whether
#: it is looking at its own allowance or at a what-if.
def source_of(requested: str | None) -> str:
    if requested:
        return "query"
    return "token" if ENABLED else "default"


CallerPlan = Annotated[str, Depends(caller_plan)]
