"""Stages of the support bot: tickets, customers, the knowledge base, replies.

All data comes from ``data/`` (tickets, customers, knowledge base). Nothing
uses the network or randomness, so every run of a pipeline gives the same
result, which makes the demo easy to debug and explain.

Each stage does one thing. Decisions are made in the graph (`condition`,
`switch`, `try`), where they are visible, not inside stages.

The model-based stages are in ``llm.py``.

Most stages are free. Three charge business units instead of seconds or
bytes: `kb_lookups`, `replies_sent`, `escalations`. Meter names are up to the
host; pricing them is a separate concern.
"""
from __future__ import annotations

import json

from stageflow import BaseStage, EventSpec, register_stage

from app.config import DATA_DIR


class TicketNotFound(LookupError):
    """No such ticket in the fixtures."""


class CustomerNotFound(LookupError):
    """No such customer in the fixtures."""


def _load(name: str) -> dict:
    """Read a fixture file. Not cached, so edits take effect without a restart."""
    with open(DATA_DIR / name, encoding="utf-8") as fh:
        return json.load(fh)


# ------------------------------------------------------------------ loading

@register_stage("LoadTicketStage")
class LoadTicketStage(BaseStage):
    """
    description: "Takes a prepared ticket out of data/tickets.json"
    icon: "/icons/ticket.svg"
    icon_mono: true
    arguments:
      ticket_id:
        type: string
        optional: true
        description: "Ticket id (T-1001); empty — the first one in the file"
    outputs:
      ticket:
        type: dict
        description: "The whole ticket"
      text:
        type: string
        description: "The customer's message"
      subject:
        type: string
        description: "The subject line"
      customer_id:
        type: string
        description: "Who wrote it"
    """

    category = "support.data"

    async def run(self):
        wanted = (self.get_arguments().get("ticket_id") or "").strip()
        tickets = _load("tickets.json")["tickets"]
        if not wanted:
            ticket = tickets[0]
        else:
            ticket = next((t for t in tickets if t["id"] == wanted), None)
        if ticket is None:
            raise TicketNotFound(
                f"no ticket {wanted!r}; the file has: "
                + ", ".join(t["id"] for t in tickets)
            )
        self.set_outputs({
            "ticket": ticket,
            "text": ticket["text"],
            "subject": ticket["subject"],
            "customer_id": ticket["customer_id"],
        })


@register_stage("LoadCustomerStage")
class LoadCustomerStage(BaseStage):
    """
    description: "The customer's plan and how fast they are owed an answer"
    icon: "/icons/user.svg"
    icon_mono: true
    arguments:
      customer_id:
        type: string
        description: "Customer id (C-77)"
    outputs:
      customer:
        type: dict
        description: "The whole customer record"
      name:
        type: string
        description: "Who to address in the reply"
      plan:
        type: string
        description: "free | pro | enterprise"
      reply_within_hours:
        type: int
        description: "The promised answer time for that plan"
    """

    category = "support.data"

    async def run(self):
        wanted = str(self.get_arguments().get("customer_id") or "").strip()
        customers = _load("customers.json")["customers"]
        customer = customers.get(wanted)
        if customer is None:
            raise CustomerNotFound(f"no customer {wanted!r}")
        self.set_outputs({
            "customer": {**customer, "id": wanted},
            "name": customer["name"],
            "plan": customer["plan"],
            "reply_within_hours": customer["reply_within_hours"],
        })


# ---------------------------------------------------------- triage by rules

@register_stage("ClassifyByRulesStage")
class ClassifyByRulesStage(BaseStage):
    """
    description: "Topic and urgency by keywords — no model, no network"
    icon: "/icons/tag.svg"
    icon_mono: true
    arguments:
      text:
        type: string
        description: "The customer's message"
      subject:
        type: string
        optional: true
        description: "The subject line — often the clearest words are there"
    outputs:
      topic:
        type: string
        description: "billing | technical | account | product | other"
      urgency:
        type: string
        description: "low | normal | high"
      matched:
        type: list
        description: "The words the decision was made on — so it can be argued with"
    """

    category = "support.triage"

    #: Keywords per topic. Simple on purpose: this is the fallback when the
    #: model is unavailable, and it should be predictable.
    TOPICS = {
        "billing": ["charge", "charged", "refund", "invoice", "payment", "price", "subscription"],
        "technical": ["error", "429", "loading", "spinning", "stuck", "broken", "crash",
                      "api", "timeout", "dashboard", "migration"],
        "account": ["login", "log in", "password", "email", "locked", "access", "account"],
        "product": ["how do i", "export", "feature", "dark mode", "csv", "theme"],
    }
    URGENT = ["cancelling", "cancel", "blocking", "urgent", "asap", "third week", "nobody replied"]

    async def run(self):
        args = self.get_arguments()
        # Search the subject as well as the message.
        text = f"{args.get('subject') or ''} {args.get('text') or ''}".lower()
        scores = {
            topic: [word for word in words if word in text]
            for topic, words in self.TOPICS.items()
        }
        topic, matched = max(scores.items(), key=lambda kv: len(kv[1]))
        urgent = [word for word in self.URGENT if word in text]
        self.set_outputs({
            "topic": topic if matched else "other",
            "urgency": "high" if urgent else "normal",
            "matched": matched + urgent,
        })


# ----------------------------------------------------------- knowledge base

@register_stage("SearchKnowledgeStage")
class SearchKnowledgeStage(BaseStage):
    """
    description: "Looks for an article in the knowledge base by topic and words"
    icon: "/icons/book.svg"
    icon_mono: true
    arguments:
      text:
        type: string
        description: "The customer's message"
      topic:
        type: string
        optional: true
        description: "Narrows the search down to one topic"
    outputs:
      found:
        type: bool
        description: "Whether anything matched — the graph branches on this"
      article:
        type: dict
        description: "The article, or the fallback one when nothing matched"
      answer:
        type: string
        description: "The article text, with {placeholders} still in it"
      article_id:
        type: string
        description: "Which article it was"
    """

    category = "support.data"

    async def run(self):
        args = self.get_arguments()
        text = str(args.get("text") or "").lower()
        topic = (args.get("topic") or "").strip()

        base = _load("knowledge.json")
        pool = [a for a in base["articles"] if not topic or a["topic"] == topic]
        # An article scores one hit per tag found in the message; the best
        # wins, and zero hits means no match.
        ranked = sorted(
            ((sum(tag in text for tag in a["tags"]), a) for a in pool),
            key=lambda pair: pair[0],
            reverse=True,
        )
        hits, article = ranked[0] if ranked else (0, base["fallback"])
        if not hits:
            article = base["fallback"]
        # Charged, not reserved: a lookup always costs the same and is cheap.
        self.charge(kb_lookups=1)
        self.set_outputs({
            "found": bool(hits),
            "article": article,
            "answer": article["answer"],
            "article_id": article["id"],
        })


# ------------------------------------------------------------------ replies

@register_stage("RenderReplyStage")
class RenderReplyStage(BaseStage):
    """
    description: "Fills the placeholders of an answer: {ticket_id}, {name}, …"
    icon: "/icons/pen.svg"
    icon_mono: true
    arguments:
      answer:
        type: string
        description: "The text with {placeholders}"
      "*":
        type: any
        optional: true
        description: "Anything else becomes a placeholder value"
    outputs:
      reply:
        type: string
        description: "The finished reply"
      missing:
        type: list
        description: "Placeholders nothing was given for — they stay as they are"
    """

    category = "support.reply"

    async def run(self):
        args = self.get_arguments()
        template = str(args.pop("answer", ""))
        values = {k: v for k, v in args.items() if isinstance(v, (str, int, float))}

        missing = []
        out = []
        rest = template
        while "{" in rest:
            head, _, tail = rest.partition("{")
            name, closed, tail = tail.partition("}")
            out.append(head)
            if not closed:  # a lone brace stays as is
                out.append("{" + name)
                rest = ""
                break
            if name in values:
                out.append(str(values[name]))
            else:
                missing.append(name)
                out.append("{" + name + "}")
            rest = tail
        out.append(rest)

        self.set_outputs({"reply": "".join(out), "missing": missing})


@register_stage("SendReplyStage")
class SendReplyStage(BaseStage):
    """
    description: "Sends the reply to the customer — and types it out as it goes"
    icon: "/icons/send.svg"
    icon_mono: true
    arguments:
      reply:
        type: string
        description: "What to send"
      ticket_id:
        type: string
        optional: true
        description: "Which ticket it answers"
      channel:
        type: string
        optional: true
        default: "email"
        description: "email | chat"
    outputs:
      sent:
        type: bool
        description: "Always true — a failure is an exception, not a flag"
      chars:
        type: int
        description: "How long the reply was"
    """

    category = "support.reply"
    allowed_events = [
        EventSpec("reply_chunk", "A piece of the reply as it is typed out",
                  payload_schema={"stream": bool, "text": str, "label": str}),
        EventSpec("reply_sent", "The reply left for the customer",
                  payload_schema={"ticket_id": str, "channel": str, "chars": int}),
    ]

    async def run(self):
        args = self.get_arguments()
        reply = str(args.get("reply") or "")
        ticket_id = str(args.get("ticket_id") or "")
        channel = str(args.get("channel") or "email")

        # Send the reply word by word. Chunks with `stream: true` are shown as
        # a stream in the editor's debug panel, for any stage.
        for word in reply.split(" "):
            self.emit("reply_chunk", {
                "stream": True, "text": word + " ", "label": "Reply",
            })

        self.emit("reply_sent", {
            "ticket_id": ticket_id, "channel": channel, "chars": len(reply),
        })
        # Charged after the fact: the size of the reply is not known before.
        self.charge(replies_sent=1, reply_chars=len(reply))
        self.set_outputs({"sent": True, "chars": len(reply)})


@register_stage("EscalateStage")
class EscalateStage(BaseStage):
    """
    description: "Hands the ticket to a human and says why"
    icon: "/icons/escalate.svg"
    icon_mono: true
    arguments:
      ticket_id:
        type: string
        description: "Which ticket"
      reason:
        type: string
        description: "Why a person is needed"
      queue:
        type: string
        optional: true
        default: "tier-2"
        description: "Which queue it goes into"
    outputs:
      task_id:
        type: string
        description: "The id of the task created for the human"
      queue:
        type: string
        description: "Where it landed"
    """

    category = "support.reply"
    allowed_events = [
        EventSpec("escalated", "The ticket went to a human",
                  payload_schema={"ticket_id": str, "queue": str, "reason": str}),
    ]

    async def run(self):
        args = self.get_arguments()
        ticket_id = str(args.get("ticket_id") or "T-????")
        queue = str(args.get("queue") or "tier-2")
        reason = str(args.get("reason") or "no reason given")
        # Derived from the ticket id, not random, so runs are reproducible.
        task_id = f"HUMAN-{ticket_id.split('-')[-1]}"
        self.emit("escalated", {"ticket_id": ticket_id, "queue": queue, "reason": reason})
        # A person's time is the most expensive resource this bot uses.
        self.charge(escalations=1)
        self.set_outputs({"task_id": task_id, "queue": queue})
