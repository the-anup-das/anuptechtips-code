"""The request handler around the bot: a kill switch, a cache that can't serve an answer
from an old policy, and an audit row (plus its event) for every reply.
"""
import hashlib
import json

import psycopg
import redis
from psycopg.types.json import Jsonb

from bot import HANDOFF_REPLY, answer, retrieve
from eligibility import Ticket
from models import Clause, Outcome

KILL_SWITCH = "chatbot:kill"    # SET chatbot:kill 1 sends every conversation to a human
CACHE_TTL = 3600                # seconds
AUDIT_TOPIC = "chatbot.audit"


def _question_hash(question: str) -> str:
    return hashlib.sha256(" ".join(question.lower().split()).encode()).hexdigest()[:32]


# Instead of this: the key is the question alone, so an edited clause keeps being quoted
# from the cache until the TTL runs out.
def cache_key_naive(question: str, clauses: list[Clause]) -> str:
    return f"chatbot:answer:{_question_hash(question)}"


# Use this: the key also carries every clause version the answer was drafted from. Publish
# a new version of any of them and the old answer simply stops matching.
def cache_key(question: str, clauses: list[Clause]) -> str:
    versions = ",".join(f"{c.clause_id}@{c.version}" for c in clauses)
    stamp = hashlib.sha256(versions.encode()).hexdigest()[:16]
    return f"chatbot:answer:{_question_hash(question)}:{stamp}"


def record(conn: psycopg.Connection, conversation_id: str, question: str, out: Outcome,
           topic: str = AUDIT_TOPIC) -> str:
    """Write the audit row and its outbox event in one transaction: both or neither."""
    checks = {"problems": list(out.problems), "tier2": out.tier2, "cached": out.cached}
    cited = [list(ref) for ref in out.cited]
    draft = out.draft.text if out.draft else None
    with conn.transaction():
        audit_id, created_at = conn.execute(
            "INSERT INTO answer_audit (conversation_id, question, intent, action, stage, "
            "draft, reply, cited, checks) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "RETURNING id, created_at",
            (conversation_id, question, out.intent, out.action, out.stage, draft, out.reply,
             Jsonb(cited), Jsonb(checks))).fetchone()
        event = {"audit_id": str(audit_id), "conversation_id": conversation_id,
                 "question": question, "intent": out.intent, "action": out.action,
                 "stage": out.stage, "draft": draft, "reply": out.reply, "cited": cited,
                 "checks": checks, "created_at": created_at.isoformat()}
        conn.execute("INSERT INTO outbox (topic, key, payload) VALUES (%s, %s, %s)",
                     (topic, conversation_id, Jsonb(event)))
    return str(audit_id)


def handle(conn: psycopg.Connection, r: redis.Redis, llm, conversation_id: str, question: str,
           ticket: Ticket | None = None, key_for=cache_key, topic: str = AUDIT_TOPIC) -> Outcome:
    """Answer one customer message. Every path ends in an audit row."""
    try:
        killed = r.get(KILL_SWITCH)
    except redis.RedisError:
        killed = b"unreachable"         # can't read the switch: fail closed, to a human
    if killed:
        out = Outcome("handoff", "kill_switch", HANDOFF_REPLY)
    else:
        clauses = retrieve(conn, question)
        key = key_for(question, clauses)
        hit = r.get(key)
        if hit:
            cached = json.loads(hit)
            out = Outcome("ship", "shipped", cached["reply"], cached["intent"],
                          cited=tuple(map(tuple, cached["cited"])), cached=True)
        else:
            out = answer(llm, question, clauses, ticket)
            if out.stage == "shipped":  # never cache a hand-off or a decision about one ticket
                r.set(key, json.dumps({"reply": out.reply, "intent": out.intent,
                                       "cited": out.cited}), ex=CACHE_TTL)
    record(conn, conversation_id, question, out, topic)
    return out
