"""Authentication: which plan a request runs on.

The core only knows `Policy`; tokens and tenants are the backend's concern,
so this module turns a request into a plan. A real backend would replace it.

The token scheme is deliberately simple. What matters is where the check
happens: a client may ask to *see* any plan (`/api/meta?plan=pro`), but a
run always uses the plan of the caller's credential.

Configuration::

    SF_TOKENS="demo-basic:basic,demo-pro:pro" python main.py

Without tokens there is no authentication and every caller gets `full`.
"""
from __future__ import annotations

import hashlib
import os
from typing import Annotated

from fastapi import Depends, HTTPException, Request

from app.plans import OPEN_PLAN, known

#: Header that carries the credential (`Authorization`, `X-Api-Key`, ...).
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

#: Whether authentication is enabled.
ENABLED = bool(TOKENS)


def credential(request: Request) -> str | None:
    """The token from the header; `Bearer abc` and `abc` both give `abc`."""
    raw = (request.headers.get(AUTH_HEADER) or "").strip()
    if not raw:
        return None
    scheme, _, rest = raw.partition(" ")
    return rest.strip() if rest.strip() and scheme.lower() == "bearer" else raw


def caller_plan(request: Request) -> str:
    """The plan this request is entitled to."""
    if not ENABLED:
        return OPEN_PLAN
    token = credential(request)
    if token is None:
        raise HTTPException(401, f"no credentials: send a token in the "
                                 f"'{AUTH_HEADER}' header")
    plan = TOKENS.get(token)
    if plan is None:
        raise HTTPException(401, "the credentials were not recognised")
    return plan


#: Where the plan in a response came from: `query`, `token` or `open`.
def source_of(requested: str | None) -> str:
    if requested:
        return "query"
    return "token" if ENABLED else "open"


CallerPlan = Annotated[str, Depends(caller_plan)]


def caller_id(request: Request, _plan: CallerPlan) -> str | None:
    """The caller's identity for run ownership: a SHA-256 digest of the token
    (so the run table does not store credentials). None when authentication
    is off, in which case runs are shared."""
    if not ENABLED:
        return None
    token = credential(request) or ""
    return hashlib.sha256(token.encode()).hexdigest()


CallerId = Annotated[str | None, Depends(caller_id)]
