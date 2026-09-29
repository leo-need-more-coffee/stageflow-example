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

from stageflow import Limits, Policy

#: The stages of the bot that need no model and no network: everything in
#: ``app/stages/support.py``. Named as a list rather than "everything except
#: the LLM ones", because a plan that is defined by subtraction grows a hole
#: the day somebody registers a stage.
RULES_ONLY = {
    "LoadTicketStage", "LoadCustomerStage", "ClassifyByRulesStage",
    "SearchKnowledgeStage", "RenderReplyStage", "SendReplyStage",
    "EscalateStage",
}

#: The core's own small stages — setting a variable, concatenating two. Every
#: plan gets them: they are the plumbing a graph is wired with, and a tier
#: that cannot set a variable is not a cheaper tier but a broken one.
PLUMBING = {"SetValueStage", "CopyValueStage", "ConcatStage", "TemplateStage"}

PLANS: dict[str, Policy] = {
    # everything this backend registers, with room to work
    "full": Policy(),
    "basic": Policy(
        # no model calls at all: the keyword rules are in, the LLM is not.
        # `06-rules-only.json` is the pipeline this plan is built around —
        # a tier that can run nothing at all teaches nothing about tiers
        stages=RULES_ONLY | PLUMBING,
        node_types={"entry", "stage", "condition", "switch", "terminal"},
        limits=Limits(
            counters={"seconds": 15, "steps": 200, "iterations": 50,
                      # the business meters, which is what a support plan
                      # would really be sold by
                      "kb_lookups": 20, "replies_sent": 1, "escalations": 1},
            gauges={"concurrency": 2, "depth": 2, "frame_bytes": 200_000},
            max_retries=2,
            max_delay_seconds=2,
        ),
    ),
    "pro": Policy(
        node_types=None,  # every type the core has
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

#: Who a caller is when this process tells nobody apart — no tokens
#: configured (``app/auth.py``). The unrestricted one, because an example
#: nobody has restricted should behave like an example.
OPEN_PLAN = "full"


def plan_names() -> list[str]:
    """The plans a client may ask to be shown. Answered to anyone: a name is
    not a permission, and a client that cannot see the list cannot offer it."""
    return sorted(PLANS)


def known(name: str | None) -> bool:
    return name in PLANS


def policy_for(name: str) -> Policy:
    """The policy of a plan. The name must be one — every caller either got
    it from :func:`known` or from the token table, which drops what it does
    not recognise, so an unknown one here is a bug and says so."""
    return PLANS[name]
