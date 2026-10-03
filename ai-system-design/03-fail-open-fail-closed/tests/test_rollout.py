"""rollout() and its health gate: first with fakes, then with twelve proxies on Kafka."""
import functools

import lab
from config_holder import ConfigHolder, NaiveHolder, ScoreZeroHolder
from rollout import GLOBAL, health, rollout

GOOD, BAD = ["good"], ["bad"]


def recorder():
    published = []
    return published, lambda cohort, version, names: published.append((cohort, version, names))


def gate(bad_versions=(), bad_cohorts=(), silent=False):
    def verdict(cohort, version):
        if silent:
            return None
        return version not in bad_versions and cohort not in bad_cohorts
    return verdict


FAST = dict(soak=0.05, poll=0.01)


def test_a_bad_file_stops_at_cohort_0():
    published, publish = recorder()
    assert rollout(2, BAD, GOOD, publish, gate(bad_versions={2}), **FAST) == 0
    assert published == [(0, 2, BAD), (0, 3, GOOD)]      # one cohort got it, then got v1 back


def test_a_good_file_reaches_every_cohort_in_order():
    published, publish = recorder()
    assert rollout(2, GOOD, GOOD, publish, gate(), **FAST) == 3
    assert published == [(0, 2, GOOD), (1, 2, GOOD), (2, 2, GOOD)]


def test_no_data_by_the_deadline_counts_as_unhealthy():
    published, publish = recorder()
    assert rollout(2, GOOD, GOOD, publish, gate(silent=True), **FAST) == 0
    assert published == [(0, 2, GOOD), (0, 3, GOOD)]


def test_early_good_news_does_not_cut_the_soak_short():
    verdicts = iter([True, True, False])
    published, publish = recorder()
    passed = rollout(2, BAD, GOOD, publish, lambda cohort, version: next(verdicts), **FAST)
    assert passed == 0 and published[-1] == (0, 3, GOOD)


def test_a_failure_at_the_second_stage_rolls_back_both_cohorts():
    published, publish = recorder()
    assert rollout(2, BAD, GOOD, publish, gate(bad_cohorts={1}), **FAST) == 1
    assert published == [(0, 2, BAD), (1, 2, BAD), (0, 3, GOOD), (1, 3, GOOD)]


def test_a_global_push_reaches_everyone_before_the_gate_can_say_no():
    published, publish = recorder()
    assert rollout(2, BAD, GOOD, publish, gate(bad_versions={2}), stages=GLOBAL, **FAST) == 0
    assert published[:3] == [(0, 2, BAD), (1, 2, BAD), (2, 2, BAD)]
    assert published[3:] == [(0, 3, GOOD), (1, 3, GOOD), (2, 3, GOOD)]


def test_with_no_good_file_there_is_nothing_to_roll_back_to():
    published, publish = recorder()
    assert rollout(1, BAD, None, publish, gate(bad_versions={1}), **FAST) == 0
    assert published == [(0, 1, BAD)]


# ---------------------------------------------------------------- the gate, on Redis counters
def counters(r, **fields):
    r.hset(lab.health_key("t", 0, 2), mapping=fields)
    return health(r, "t", 0, 2)


def test_the_gate_waits_for_enough_requests(r):
    assert health(r, "t", 0, 2) is None
    assert counters(r, ok=19) is None
    assert counters(r, ok=18, blocked=2) is True


def test_the_gate_fails_on_errors_rejections_and_a_jump_in_blocks(r):
    assert counters(r, ok=98, err=2) is False            # 2% of requests failed
    r.flushdb()
    assert counters(r, rejected=1) is False              # a proxy refused the file
    r.flushdb()
    assert counters(r, ok=40, blocked=60) is False       # nothing failed, most of it blocked
    assert health(r, "t", 0, 2, max_blocked=1.0) is True  # an errors-only gate would pass it


# ---------------------------------------------------------------- twelve proxies on Kafka
def errors(proxy) -> int:
    return proxy.statuses[500]


def test_staged_rollout_stops_the_doubled_file_at_one_proxy(fleet, files):
    good, bad = files
    f = fleet(NaiveHolder)
    assert rollout(2, bad, good, f.publish, f.gate) == 0
    f.wait_for(lambda proxy: proxy.holder.version in (1, 3))
    f.stop()
    canary, rest = f.proxies[0], f.proxies[1:]
    assert errors(canary) > 0 and canary.holder.version == 3
    assert all(errors(proxy) == 0 and proxy.holder.seen == 1 for proxy in rest)


def test_a_global_push_takes_down_all_twelve(fleet, files):
    good, bad = files
    f = fleet(NaiveHolder)
    assert rollout(2, bad, good, f.publish, f.gate, stages=GLOBAL) == 0
    f.wait_for(lambda proxy: proxy.holder.version == 3)
    f.stop()
    assert all(errors(proxy) > 0 for proxy in f.proxies)


def test_hardened_proxies_serve_stale_and_never_error(fleet, files):
    good, bad = files
    f = fleet(ConfigHolder)
    assert rollout(2, bad, good, f.publish, f.gate, stages=GLOBAL) == 0
    f.wait_for(lambda proxy: proxy.holder.version == 3)
    f.stop()
    for proxy in f.proxies:
        assert not any(status >= 500 for status in proxy.statuses)   # no 5xx anywhere
        assert proxy.humans_blocked == 0 and proxy.bots_passed == 0
        assert proxy.holder.rejected == 1
    assert sum(proxy.stale for proxy in f.proxies) > 0   # served on v1 until the rollback came


def test_a_good_file_reaches_all_twelve(fleet, files):
    good, _ = files
    f = fleet(NaiveHolder)
    assert rollout(2, good[:50], good, f.publish, f.gate, soak=0.4) == 3
    f.wait_for(lambda proxy: proxy.holder.version == 2)
    f.stop()
    assert all(errors(proxy) == 0 for proxy in f.proxies)


def test_an_errors_only_gate_promotes_the_file_that_fails_wrong(fleet, files, r):
    good, bad = files
    f = fleet(ScoreZeroHolder)
    assert rollout(2, bad, good, f.publish, f.gate) == 0             # the block rate stops it
    f.wait_for(lambda proxy: proxy.holder.version in (1, 3))
    errors_only = functools.partial(health, r, f.run, max_blocked=1.0)
    assert rollout(4, bad, good, f.publish, errors_only, soak=0.4) == 3
    f.wait_for(lambda proxy: proxy.holder.version == 4)
    f.stop()
    assert all(errors(proxy) == 0 for proxy in f.proxies)            # not one 5xx...
    assert all(proxy.humans_blocked > 0 for proxy in f.proxies)      # ...and humans blocked
