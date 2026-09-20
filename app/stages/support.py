"""The stages of a support bot: tickets, the knowledge base, replies.

Everything these stages work with is prepared data in ``data/``: the tickets
(``tickets.json``), the customers (``customers.json``) and the knowledge base
(``knowledge.json``). Nothing here goes to the network, nothing here is random
— a pipeline built on them gives the same answer on every run, which is what a
demo needs: a debugger showing a different graph on every step is impossible to
talk about.

The stages are deliberately small and single-purpose. That is not neatness for
its own sake: a node on the canvas is a stage, and a stage that both looks
something up and decides what to do with it turns into a node whose meaning has
to be guessed from its name. The decisions live in the graph — `condition`,
`switch`, `try` — where they can be seen.

The one thing that does go to a live service is classification, and that lives
apart, in ``llm.py``.
"""
from __future__ import annotations

import json

from stageflow import BaseStage, EventSpec, register_stage

from app.config import DATA_DIR


class TicketNotFound(LookupError):
    """There is no such ticket in the fixtures."""


class CustomerNotFound(LookupError):
    """There is no such customer in the fixtures."""


def _load(name: str) -> dict:
    """The fixtures are read on every call on purpose: editing a JSON file and
    running the pipeline again is the fastest way to try another situation, and
    a cache would mean restarting the server for it."""
    with open(DATA_DIR / name, encoding="utf-8") as fh:
        return json.load(fh)


# ----------------------------------------------------------------- loading

@register_stage("LoadTicketStage")
class LoadTicketStage(BaseStage):
    """
    description: "Takes a prepared ticket out of data/tickets.json"
    icon: "/icons/ticket.svg"
    icon_mono: true
    category: "support.data"
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
    category: "support.data"
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


# ------------------------------------------------------------- triage by rules

@register_stage("ClassifyByRulesStage")
class ClassifyByRulesStage(BaseStage):
    """
    description: "Topic and urgency by keywords — no model, no network"
    icon: "/icons/tag.svg"
    icon_mono: true
    category: "support.triage"
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

    #: Keywords per topic. Crude on purpose: this is the road a graph takes when
    #: the model is unavailable, and it has to be predictable rather than clever.
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
        # the subject is searched too: "Dashboard stuck on loading" says more
        # than the message that follows it
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


# ---------------------------------------------------------- knowledge base

@register_stage("SearchKnowledgeStage")
class SearchKnowledgeStage(BaseStage):
    """
    description: "Looks for an article in the knowledge base by topic and words"
    icon: "/icons/book.svg"
    icon_mono: true
    category: "support.data"
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
        # a hit is a tag occurring in the message; the article with the most
        # hits wins, and zero hits means "nothing matched" rather than "the
        # first one will do"
        ranked = sorted(
            ((sum(tag in text for tag in a["tags"]), a) for a in pool),
            key=lambda pair: pair[0],
            reverse=True,
        )
        hits, article = ranked[0] if ranked else (0, base["fallback"])
        if not hits:
            article = base["fallback"]
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
    category: "support.reply"
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
            if not closed:  # a lone brace is just a brace
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
    category: "support.reply"
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

        # The reply is typed out word by word rather than handed over whole.
        # The `stream: true` payload is a contract with the editor: its debug
        # panel shows the stream of any node that sends chunks like this, and
        # knows nothing about support bots or language models.
        for word in reply.split(" "):
            self.emit("reply_chunk", {
                "stream": True, "text": word + " ", "label": "Reply",
            })

        self.emit("reply_sent", {
            "ticket_id": ticket_id, "channel": channel, "chars": len(reply),
        })
        self.set_outputs({"sent": True, "chars": len(reply)})


@register_stage("EscalateStage")
class EscalateStage(BaseStage):
    """
    description: "Hands the ticket to a human and says why"
    icon: "/icons/escalate.svg"
    icon_mono: true
    category: "support.reply"
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
        # derived from the ticket id rather than random: a demo that prints a
        # different number every run is a demo nobody can point at
        task_id = f"HUMAN-{ticket_id.split('-')[-1]}"
        self.emit("escalated", {"ticket_id": ticket_id, "queue": queue, "reason": reason})
        self.set_outputs({"task_id": task_id, "queue": queue})
