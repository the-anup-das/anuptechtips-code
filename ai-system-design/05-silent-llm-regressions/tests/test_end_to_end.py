"""Each injected bug, through the production path, into the slice counters: the sliced alert
names the slice, and the global alert stays quiet."""
import numpy as np

from evals import SYSTEM_PROMPT, evaluate
from fake_model import Model, Serving, approx_top_k
from pipeline import record
from probes import by_suite
from serving import Fleet, bucket, serve, traffic
from slices import DIMENSIONS, degraded_slices

MODEL = Model()
BASELINE = evaluate(MODEL, SYSTEM_PROMPT, [f"b{i}" for i in range(20)],
                    probes=by_suite("known_answer"))["known_answer"].value


def run(conn, fleet: Fleet, ticks: int, users: int = 500) -> list[dict]:
    events = []
    for tick in range(ticks):
        events += serve(fleet, MODEL, SYSTEM_PROMPT, traffic(tick, users=users), tick)
    record(conn, events)
    return events


def alerts(conn, metric: str = "passed", baseline: float = BASELINE) -> dict[str, list]:
    found = {dim: degraded_slices(conn, "known_answer", baseline, by=(dim,), metric=metric)
             for dim in DIMENSIONS}
    found["global"] = degraded_slices(conn, "known_answer", baseline, by=(), metric=metric)
    return {name: [a.key for a in hits] for name, hits in found.items() if hits}


def test_healthy_fleet_raises_no_alert_on_any_dimension(conn):
    events = run(conn, Fleet(), ticks=5)
    assert len(events) == 10_000 and {e["pool"] for e in events} == {"standard"}
    assert alerts(conn) == {}
    assert alerts(conn, metric="script_ok", baseline=0.98) == {}


def test_sticky_misrouting_shows_up_in_the_pool_slice_only(conn):
    fleet = Fleet(misroute_share=0.008)
    events = run(conn, fleet, ticks=5)
    lost = [e for e in events if e["pool"] == "long-context"]
    assert 0.003 < len(lost) / len(events) < 0.015
    assert len(lost) % 4 == 0             # sticky: a misrouted session loses all 4 of its requests
    assert alerts(conn) == {"pool": [("long-context",)]}


def test_route_is_sticky_per_session():
    fleet = Fleet(misroute_share=0.5)
    first = {s: fleet.route(s) for s in (f"s{i}" for i in range(200))}
    fleet.misroute_share = 0.0            # the bug is fixed, but live sessions stay where they are
    assert {s: fleet.route(s) for s in first} == first
    assert set(first.values()) == {"standard", "long-context"}
    assert fleet.route("a-new-session") == "standard"


def test_top_k_bug_shows_up_in_the_big_batch_slices_only(conn):
    run(conn, Fleet(hw_b=Serving(top_k=approx_top_k)), ticks=8)
    found = alerts(conn)
    assert "global" not in found and "pool" not in found
    hits = degraded_slices(conn, "known_answer", BASELINE, by=("hardware", "batch_bucket"))
    assert sorted(a.key for a in hits) == [("hw-b", "33-64"), ("hw-b", "9-32")]


def test_script_corruption_is_pinned_to_its_hardware_while_the_pass_rate_holds(conn):
    run(conn, Fleet(hw_b=Serving(script_boost=12.0)), ticks=5)
    assert alerts(conn) == {}                                 # the known-answer eval stays green
    found = alerts(conn, metric="script_ok", baseline=0.98)   # 2% of healthy replies are flagged
    assert found["hardware"] == [("hw-b",)]                   # the slice says where: hw-a is clean


def test_fp16_pick_changes_replies_without_changing_the_pass_rate(conn):
    events = run(conn, Fleet(hw_b=Serving(dtype=np.float16)), ticks=3)
    healthy = run_plain(3)
    assert sum(e["passed"] for e in events) == sum(e["passed"] for e in healthy)
    assert alerts(conn) == {}


def run_plain(ticks: int) -> list[dict]:
    fleet, events = Fleet(), []
    for tick in range(ticks):
        events += serve(fleet, MODEL, SYSTEM_PROMPT, traffic(tick), tick)
    return events


def test_batch_buckets():
    assert [bucket(n) for n in (1, 2, 8, 9, 32, 33, 64)] == [
        "1", "2-8", "2-8", "9-32", "9-32", "33-64", "33-64"]
