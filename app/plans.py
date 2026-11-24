"""The plans this backend offers, each a `Policy`.

A real platform would build the policy from a tenant's subscription; here
plans are a table and the tenant is whoever presents a token (`app.auth`).
Plan names and pricing belong to the backend, not the core.
"""
from __future__ import annotations

from stageflow import Limits, Policy

#: Stages that need no model or network (everything in `app/stages/support.py`).
#: Listed explicitly so that newly registered stages are not added by accident.
RULES_ONLY = {
    "LoadTicketStage", "LoadCustomerStage", "ClassifyByRulesStage",
    "SearchKnowledgeStage", "RenderReplyStage", "SendReplyStage",
    "EscalateStage",
}

#: The core's basic data stages; every plan needs them to wire a graph.
PLUMBING = {"SetValueStage", "CopyValueStage", "ConcatStage", "TemplateStage"}

PLANS: dict[str, Policy] = {
    # Everything this backend registers, with generous limits.
    "full": Policy(),
    "basic": Policy(
        # No model calls: keyword rules only. `06-rules-only.json` is the
        # pipeline built for this plan.
        stages=RULES_ONLY | PLUMBING,
        node_types={"entry", "stage", "condition", "switch", "terminal"},
        limits=Limits(
            counters={"seconds": 15, "steps": 200, "iterations": 50,
                      # Business meters, which a support product would bill by.
                      "kb_lookups": 20, "replies_sent": 1, "escalations": 1},
            gauges={"concurrency": 2, "depth": 2, "frame_bytes": 200_000},
            max_retries=2,
            max_delay_seconds=2,
        ),
    ),
    "pro": Policy(
        node_types=None,  # all node types
        limits=Limits(
            counters={"seconds": 120, "steps": 5_000, "iterations": 2_000,
                      "tokens": 200_000, "llm_calls": 100,
                      "kb_lookups": 2_000, "replies_sent": 200,
                      "escalations": 50},
            gauges={"concurrency": 8, "depth": 4, "frame_bytes": 4_000_000},
            max_retries=5,
            max_delay_seconds=30,
        ),
    ),
}

#: The plan used when authentication is off (no tokens configured).
OPEN_PLAN = "full"


def plan_names() -> list[str]:
    """Plan names a client may ask about (public: a name grants nothing)."""
    return sorted(PLANS)


def known(name: str | None) -> bool:
    return name in PLANS


def policy_for(name: str) -> Policy:
    """The policy of a plan. Callers only pass known names, so an unknown
    one raises KeyError."""
    return PLANS[name]
