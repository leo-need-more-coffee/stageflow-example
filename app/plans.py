"""The plans this backend serves, and the policy each one is.

A real platform looks a tenant up, finds their subscription and builds the
policy from it. This is an example backend, so the plans are a table and the
tenant is whoever presents a token (``app/auth.py``) — the point being what a
plan *is* and where it is checked, not where it was stored.

The mapping lives here and not in the core on purpose. The core knows
`Policy`; the words "plan", "basic" and "pro" are the platform's business, and
so is the price list that turns the meters in a result into an invoice.
"""
from __future__ import annotations

import os

from stageflow import Limits, Policy

#: Everything this backend registers, with room to work. The default: an
#: example nobody has restricted should behave like an example.
FULL = Policy()

PLANS: dict[str, Policy] = {
    "full": FULL,
    "basic": Policy(
        # no model calls at all: the keyword rules are in, the LLM is not
        stages={"LoadTicketStage", "ClassifyByRulesStage", "SearchKbStage",
                "TemplateStage", "SetValueStage", "ConcatStage"},
        node_types={"entry", "stage", "condition", "switch", "terminal"},
        limits=Limits(
            counters={"seconds": 15, "steps": 200, "iterations": 50},
            gauges={"concurrency": 2, "depth": 2, "frame_bytes": 200_000},
            max_retries=2,
            max_delay_seconds=2,
        ),
    ),
    "pro": Policy(
        node_types=None,  # every type the core has
        limits=Limits(
            counters={"seconds": 120, "steps": 5_000, "iterations": 2_000,
                      "tokens": 200_000, "llm_calls": 100},
            gauges={"concurrency": 8, "depth": 4, "frame_bytes": 4_000_000},
            max_retries=5,
            max_delay_seconds=30,
        ),
    ),
}

#: Who a caller is when nobody has been told apart: no tokens configured, or a
#: request that carries none where none are demanded.
DEFAULT_PLAN = os.environ.get("SF_PLAN", "full")


def plan_names() -> list[str]:
    """The plans a client may ask to be shown. Answered to anyone: a name is
    not a permission, and a client that cannot see the list cannot offer it."""
    return sorted(PLANS)


def known(name: str | None) -> bool:
    return name in PLANS


def policy_for(name: str | None) -> Policy:
    """The policy of a plan; an unknown name falls back to the default one.

    Falling back rather than raising, because this is also what answers for
    ``SF_PLAN=typo``: a misspelt environment variable should start the example
    rather than make every endpoint 500.
    """
    return PLANS.get(name or "", PLANS.get(DEFAULT_PLAN, FULL))
