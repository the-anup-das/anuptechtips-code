"""The gate: classification, deny-by-default production, single-use approvals, the change
freeze, the kill switch, the rate limit, the audit trail and what happens without Redis."""
import time

import pytest
import redis
from psycopg.errors import InsufficientPrivilege

import gate as gate_module
import lab
from executor import Executor, PlanExceeded
from gate import DESTRUCTIVE, READ, WRITE, Denied, ToolGate, call_digest, classify

DROP = "DROP TABLE prod.executives, prod.companies"


class Recorder:
    """Stands in for the executor when a test only cares whether a call got through."""

    def __init__(self):
        self.ran = []

    def __call__(self, env, kind, sql, max_rows=None):
        self.ran.append((env, kind, sql))
        return "ran"


@pytest.fixture
def ran():
    return Recorder()


@pytest.fixture
def gate(r, ran):
    return ToolGate(r, ran)


@pytest.mark.parametrize("sql, kind", [
    ("SELECT 1", READ), ("  select * from prod.executives", READ), ("SHOW search_path", READ),
    ("INSERT INTO t VALUES (1)", WRITE), ("update t set a = 1", WRITE),
    ("DELETE FROM t", DESTRUCTIVE), ("DROP TABLE t", DESTRUCTIVE), ("TRUNCATE t", DESTRUCTIVE),
    ("ALTER TABLE t DROP COLUMN a", DESTRUCTIVE), ("GRANT ALL ON t TO PUBLIC", DESTRUCTIVE),
    ("WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone", DESTRUCTIVE),
    ("EXPLAIN ANALYZE DELETE FROM t", DESTRUCTIVE), ("/* hi */ SELECT 1", DESTRUCTIVE),
    ("CALL cleanup()", DESTRUCTIVE), ("", DESTRUCTIVE), ("   ", DESTRUCTIVE),
])
def test_unknown_means_destructive(sql, kind):
    assert classify(sql) == kind


def test_reads_on_production_need_no_approval(gate, ran):
    assert gate.call("a1", "prod", "SELECT 1") == "ran"
    assert ran.ran == [("prod", READ, "SELECT 1")]


@pytest.mark.parametrize("sql", [DROP, "DELETE FROM prod.executives WHERE id = 1",
                                 "UPDATE prod.executives SET name = 'x' WHERE id = 1",
                                 "INSERT INTO prod.companies VALUES (9999, 'x')"])
def test_production_changes_are_denied_by_default(gate, ran, sql):
    with pytest.raises(Denied) as denied:
        gate.call("a1", "prod", sql)
    assert denied.value.reason == "approval required"
    assert denied.value.digest == call_digest("a1", "prod", sql)
    assert ran.ran == []


def test_staging_needs_no_approval(gate, ran):
    gate.call("a1", "staging", "DELETE FROM staging.inbox WHERE id = 1")
    assert len(ran.ran) == 1


def test_an_approval_works_exactly_once(gate, ran, r):
    digest = call_digest("a1", "prod", DROP)
    assert gate.approve(digest, "anup") is True
    assert 890 < r.ttl(f"approval:{digest}") <= 900
    assert gate.call("a1", "prod", DROP) == "ran"
    with pytest.raises(Denied, match="approval required"):
        gate.call("a1", "prod", DROP)  # the retry, or the agent trying its luck again
    assert len(ran.ran) == 1


def test_an_approval_fits_only_the_call_it_was_given_for(gate, ran):
    gate.approve(call_digest("a1", "prod", DROP), "anup")
    for agent, env, sql, max_rows in (
            ("a1", "prod", "DROP TABLE prod.executives", None),  # a different statement
            ("a2", "prod", DROP, None),                          # a different agent
            ("a1", "prod", DROP, 10)):                           # a different plan
        with pytest.raises(Denied, match="approval required"):
            gate.call(agent, env, sql, max_rows)
    assert ran.ran == []
    assert gate.call("a1", "prod", DROP) == "ran"  # the approved call is still good


def test_an_expired_approval_fails(gate, ran, monkeypatch):
    monkeypatch.setattr(gate_module, "APPROVAL_TTL", 1)
    gate.approve(call_digest("a1", "prod", DROP), "anup")
    time.sleep(1.3)
    with pytest.raises(Denied, match="approval required"):
        gate.call("a1", "prod", DROP)
    assert ran.ran == []


def test_a_second_approver_cannot_overwrite_the_first(gate, r):
    digest = call_digest("a1", "prod", DROP)
    assert gate.approve(digest, "anup") is True
    assert gate.approve(digest, "someone-else") is False
    assert r.get(f"approval:{digest}") == b"anup"


def test_a_change_freeze_beats_an_approval(gate, ran, r):
    r.set("freeze:prod", "release week", ex=60)
    gate.approve(call_digest("a1", "prod", DROP), "anup")
    with pytest.raises(Denied, match="change freeze"):
        gate.call("a1", "prod", DROP)
    assert gate.call("a1", "prod", "SELECT 1") == "ran"       # reads still work
    assert gate.call("a1", "staging", "DELETE FROM t") == "ran"  # so does staging
    r.delete("freeze:prod")
    assert gate.call("a1", "prod", DROP) == "ran"  # the approval survived the freeze


def test_the_kill_switch_stops_everything_for_that_agent(gate, ran, r):
    r.set("killswitch:a1", "stopped by hand")
    for env, sql in (("prod", "SELECT 1"), ("staging", "DELETE FROM t"), ("prod", DROP)):
        with pytest.raises(Denied, match="kill switch"):
            gate.call("a1", env, sql)
    assert ran.ran == []
    assert gate.call("a2", "prod", "SELECT 1") == "ran"  # another agent is not affected


def test_destructive_calls_are_rate_limited(gate, ran):
    for i in range(5):
        gate.call("a1", "staging", f"DELETE FROM staging.inbox WHERE id = {i}")
    with pytest.raises(Denied, match="rate limit"):
        gate.call("a1", "staging", "DELETE FROM staging.inbox WHERE id = 6")
    assert len(ran.ran) == 5
    for _ in range(50):  # reads and writes don't spend tokens
        gate.call("a1", "staging", "SELECT 1")
        gate.call("a1", "staging", "INSERT INTO t VALUES (1)")
    assert gate.call("a2", "staging", "DELETE FROM t") == "ran"  # one bucket per agent


def test_the_bucket_refills(r, ran):
    fast = ToolGate(r, ran, rate=10, burst=2)  # 10 tokens a second for the test
    fast.call("a1", "staging", "DELETE FROM t")
    fast.call("a1", "staging", "DELETE FROM t")
    with pytest.raises(Denied, match="rate limit"):
        fast.call("a1", "staging", "DELETE FROM t")
    time.sleep(0.25)
    assert fast.call("a1", "staging", "DELETE FROM t") == "ran"


def test_a_denied_attempt_still_costs_a_token(gate, ran):
    for _ in range(5):
        with pytest.raises(Denied, match="approval required"):
            gate.call("a1", "prod", DROP)
    with pytest.raises(Denied, match="rate limit"):
        gate.call("a1", "prod", DROP)


def test_every_call_is_audited_allowed_or_not(r, ran):
    events = []
    gate = ToolGate(r, ran, audit=events.append)
    gate.call("a1", "prod", "SELECT 1")
    with pytest.raises(Denied):
        gate.call("a1", "prod", DROP)
    assert [(e["agent"], e["env"], e["kind"], e["decision"]) for e in events] == [
        ("a1", "prod", READ, "allowed"), ("a1", "prod", DESTRUCTIVE, "approval required")]
    assert events[1]["sql"] == DROP and events[1]["ts"] <= time.time()


def test_the_gate_fails_closed_when_redis_is_down(ran):
    dead = redis.Redis(host="localhost", port=1, socket_connect_timeout=0.2)
    gate = ToolGate(dead, ran)
    with pytest.raises(redis.RedisError):  # refused or timed out: either way, nothing runs
        gate.call("a1", "prod", "SELECT 1")
    assert ran.ran == []


# ---- the gate in front of the real executor and the real roles ----

@pytest.fixture
def real_gate(db, r, connect):
    def build(soft_delete: bool) -> ToolGate:
        writer = connect(lab.OPERATOR if soft_delete else lab.OWNER)
        return ToolGate(r, Executor(connect(lab.AGENT), writer, soft_delete))
    return build


def approve_and_call(gate: ToolGate, sql: str, max_rows=None):
    gate.approve(call_digest("a1", "prod", sql, max_rows), "anup")
    return gate.call("a1", "prod", sql, max_rows)


def test_without_soft_delete_an_approved_drop_is_a_real_drop(real_gate, connect):
    approve_and_call(real_gate(soft_delete=False), DROP)
    owner = connect()
    assert lab.original_rows(owner, "executives") == 0
    assert owner.execute("SELECT count(*) FROM trash.manifest").fetchone() == (0,)


def test_with_soft_delete_an_approved_drop_moves_the_tables_to_the_trash(real_gate, connect):
    approve_and_call(real_gate(soft_delete=True), DROP)
    owner = connect()
    assert owner.execute("SELECT to_regclass('prod.executives')").fetchone() == (None,)
    assert lab.original_rows(owner, "executives", schema="trash") == 1206
    assert lab.original_rows(owner, "companies", schema="trash") == 1196


def test_a_statement_that_touches_more_rows_than_planned_is_rolled_back(real_gate, connect):
    gate = real_gate(soft_delete=True)
    sql = "DELETE FROM prod.executives WHERE id < 100"  # the plan said 3 rows
    with pytest.raises(PlanExceeded, match="99 rows, the approved plan said 3"):
        approve_and_call(gate, sql, max_rows=3)
    assert lab.original_rows(connect(), "executives") == 1206


def test_a_statement_within_its_plan_commits(real_gate, connect):
    sql = "DELETE FROM prod.executives WHERE id IN (1, 2, 3)"
    assert approve_and_call(real_gate(soft_delete=True), sql, max_rows=3).rowcount == 3
    assert lab.original_rows(connect(), "executives") == 1203


def test_lying_about_the_environment_gets_the_staging_credential(real_gate, connect):
    """A call labelled staging skips the approval, and runs with a role that can't touch
    production. The label picks the credential, so the lie only hurts the liar."""
    with pytest.raises(InsufficientPrivilege, match="must be owner of table executives"):
        real_gate(soft_delete=True).call("a1", "staging", "DROP TABLE prod.executives")
    assert lab.original_rows(connect(), "executives") == 1206


def test_other_approved_ddl_fails_once_deletes_are_soft(real_gate, connect):
    """Only the plain DROP TABLE is rewritten. TRUNCATE and friends reach Postgres as the
    operator, which owns nothing."""
    for sql in ("TRUNCATE prod.executives", "DROP TABLE prod.executives CASCADE",
                "DROP SCHEMA prod CASCADE"):
        with pytest.raises(Exception) as refused:
            approve_and_call(real_gate(soft_delete=True), sql)
        assert not isinstance(refused.value, Denied)
    assert lab.original_rows(connect(), "executives") == 1206
