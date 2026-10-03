"""A gate between an agent and its tools. It runs outside the model, so the model can't
read it, edit it or argue with it. Redis holds the approvals, the freeze and the switches."""
import hashlib
import json
import pathlib
import time
from collections.abc import Callable
from typing import Any

import redis

READ, WRITE, DESTRUCTIVE = "read", "write", "destructive"
KNOWN = {"select": READ, "show": READ, "insert": WRITE, "update": WRITE}
APPROVAL_TTL = 900  # seconds: an approval nobody uses is gone after 15 minutes
BUCKET = (pathlib.Path(__file__).parent / "token_bucket.lua").read_text()


class Denied(Exception):
    """The gate said no. `digest` names the call a human would have to approve."""

    def __init__(self, reason: str, digest: str = ""):
        super().__init__(reason)
        self.reason, self.digest = reason, digest


def classify(sql: str) -> str:
    """The first keyword decides. Anything the table doesn't know is destructive."""
    words = sql.split(None, 1)
    return KNOWN.get(words[0].lower(), DESTRUCTIVE) if words else DESTRUCTIVE


def call_digest(agent: str, env: str, sql: str, max_rows: int | None = None) -> str:
    """SHA-256 of the exact call, so an approval for one statement fits no other."""
    blob = json.dumps([agent, env, sql, max_rows], separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


class ToolGate:
    def __init__(self, r: redis.Redis, execute: Callable[[str, str, str, int | None], Any],
                 audit: Callable[[dict], None] = lambda event: None,
                 rate: float = 1 / 60, burst: int = 5):
        self.r, self.execute, self.audit = r, execute, audit
        self.rate, self.burst = rate, burst  # destructive calls: `burst` at once, then `rate`/s
        self._bucket = r.register_script(BUCKET)

    def approve(self, digest: str, approver: str) -> bool:
        """The human's side. NX: a second approver can't overwrite the first."""
        return bool(self.r.set(f"approval:{digest}", approver, nx=True, ex=APPROVAL_TTL))

    def _decide(self, agent: str, env: str, sql: str, kind: str,
                max_rows: int | None) -> tuple[str, str]:
        if self.r.exists(f"killswitch:{agent}"):
            return "kill switch", ""
        if kind == DESTRUCTIVE:
            allowed, _wait_ms = self._bucket(keys=[f"bucket:{agent}"],
                                             args=[self.rate, self.burst, 1])
            if not allowed:
                return "rate limit", ""
        if env == "prod" and kind != READ:  # production is denied by default
            if self.r.exists("freeze:prod"):
                return "change freeze", ""
            digest = call_digest(agent, env, sql, max_rows)
            if self.r.getdel(f"approval:{digest}") is None:  # GETDEL: an approval works once
                return "approval required", digest
        return "", ""

    def call(self, agent: str, env: str, sql: str, max_rows: int | None = None) -> Any:
        kind = classify(sql)
        reason, digest = self._decide(agent, env, sql, kind, max_rows)
        self.audit({"agent": agent, "env": env, "kind": kind, "sql": sql,
                    "decision": reason or "allowed", "ts": time.time()})
        if reason:
            raise Denied(reason, digest)
        return self.execute(env, kind, sql, max_rows)
