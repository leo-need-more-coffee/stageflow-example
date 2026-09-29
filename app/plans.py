"""Two plans, so that the policy is something you can see rather than read about.

A real platform looks a tenant up, finds their subscription and builds the
policy from it. This is an example backend with no tenants at all, so the plan
is an environment variable — the point being what a plan *is*, not where it
came from:

    SF_PLAN=basic python main.py

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

#: Which plan this process serves. One process, one plan, because there is
#: nobody to tell apart here; a real platform picks per request.
PLAN = os.environ.get("SF_PLAN", "full")


def current_policy() -> Policy:
    return PLANS.get(PLAN, FULL)
