"""The Wilson-bound check: a degraded slice is flagged, the global rate and tiny slices aren't."""
import uuid

import pytest

from detectors import scripts, unexpected_scripts
from pipeline import record
from slices import degraded_slices, wilson_bounds

BASELINE = 0.96


def events(n: int, passed: int, tick: int = 0, **slice_) -> list[dict]:
    base = {"tick": tick, "suite": "known_answer", "pool": "standard", "hardware": "hw-a",
            "batch_bucket": "2-8", "model_version": "model-1", "prompt_version": "p1",
            "client_version": "client-1", "script_ok": True}
    return [{**base, **slice_, "event_id": str(uuid.uuid4()), "passed": i < passed}
            for i in range(n)]


def test_wilson_bounds_match_known_values():
    low, high = wilson_bounds(45, 50, z=1.96)
    assert round(low, 3) == 0.786 and round(high, 3) == 0.957
    assert wilson_bounds(0, 0) == (0.0, 1.0)
    low, high = wilson_bounds(10, 10)
    assert high == pytest.approx(1.0) and low < 1.0     # 10 of 10 doesn't prove 100%
    low, high = wilson_bounds(0, 10)
    assert low == pytest.approx(0.0) and 0.4 < high < 0.5


def test_degraded_slice_is_flagged_while_the_global_rate_is_not(conn):
    record(conn, events(4960, passed=4762))                             # 96.0% on the right pool
    record(conn, events(40, passed=28, pool="long-context"))            # 70% for 0.8% of traffic
    alerts = degraded_slices(conn, "known_answer", BASELINE, by=("pool",))
    assert [(a.key, a.passed, a.total) for a in alerts] == [(("long-context",), 28, 40)]
    assert alerts[0].upper < BASELINE - 0.01
    # all traffic as one slice: 4790 of 5000 = 95.8%, nowhere near an alert
    assert degraded_slices(conn, "known_answer", BASELINE, by=()) == []


def test_small_slices_and_healthy_slices_are_not_flagged(conn):
    record(conn, events(2000, passed=1920))
    record(conn, events(12, passed=6, pool="long-context"))      # 50%, but only 12 replies
    record(conn, events(400, passed=376, hardware="hw-b"))       # 94%: lower, within chance
    assert degraded_slices(conn, "known_answer", BASELINE, by=("pool",)) == []
    assert degraded_slices(conn, "known_answer", BASELINE, by=("hardware",)) == []
    assert degraded_slices(conn, "known_answer", BASELINE, by=("pool",), min_n=10) != []


def test_global_alert_fires_once_the_drop_is_big_enough(conn):
    record(conn, events(4200, passed=4032))                             # 96%
    record(conn, events(800, passed=560, pool="long-context"))          # 70% for 16% of traffic
    [alert] = degraded_slices(conn, "known_answer", BASELINE, by=())
    assert alert.key == () and alert.total == 5000


def test_the_window_and_two_dimensions_at_once(conn):
    record(conn, events(300, passed=288, tick=1))
    record(conn, events(300, passed=200, tick=5, hardware="hw-b", batch_bucket="33-64"))
    assert degraded_slices(conn, "known_answer", BASELINE, by=("hardware",), ticks=(0, 4)) == []
    [alert] = degraded_slices(conn, "known_answer", BASELINE, by=("hardware", "batch_bucket"),
                              ticks=(3, 5))
    assert alert.key == ("hw-b", "33-64")


def test_script_metric_and_unknown_columns(conn):
    bad = events(200, passed=200, hardware="hw-b")
    for e in bad[:30]:
        e["script_ok"] = False                                  # 15% of replies flagged
    record(conn, events(2000, passed=2000) + bad)
    [alert] = degraded_slices(conn, "known_answer", 0.98, by=("hardware",), metric="script_ok")
    assert alert.key == ("hw-b",)
    with pytest.raises(ValueError):
        degraded_slices(conn, "known_answer", BASELINE, by=("pool; DROP TABLE slice_stats",))
    with pytest.raises(ValueError):
        degraded_slices(conn, "known_answer", BASELINE, metric="total")


def test_script_detector():
    assert scripts("Fix the bug in parse(), v2.") == {"LATIN"}
    assert unexpected_scripts("What does this return?", "It returns ครับ twice") == {"THAI"}
    assert unexpected_scripts("What does this return?", "It returns 数据") == {"CJK"}
    assert unexpected_scripts("What does this return?", "It returns 42, i.e. [1, 2]!") == set()
    assert unexpected_scripts("这个函数返回什么?", "它返回 42") == set()      # same script as the prompt
    # a translation request is flagged too: the detector can't tell, so alert on the rate
    assert unexpected_scripts("Translate 'thanks' for our Thai users.", "ขอบคุณ") == {"THAI"}
