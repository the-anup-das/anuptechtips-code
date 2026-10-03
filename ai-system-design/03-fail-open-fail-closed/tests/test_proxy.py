"""One proxy, driven by hand: it reads files from Kafka and reports what it served to Redis."""
import time
import uuid

import lab
from config_holder import ConfigHolder, NaiveHolder
from proxy import Proxy
from rollout import health, kafka_publisher


def make(holder, on_unknown="open"):
    """One proxy in cohort 0, its run id and a publisher for that run."""
    run = uuid.uuid4().hex[:8]
    proxy = Proxy("proxy-test", 0, holder, run, lab.topic_ends()[0], on_unknown)
    return proxy, run, kafka_publisher(run)


def take(proxy, messages: int) -> None:
    deadline = time.monotonic() + 10
    while messages:
        assert time.monotonic() < deadline, "the message never arrived"
        messages -= proxy.poll(0.05)


def test_a_proxy_applies_files_in_order_and_counts_what_it_served(files, r):
    good, bad = files
    proxy, run, publish = make(NaiveHolder())
    publish(0, 1, good)
    take(proxy, 1)
    proxy.serve(100)
    assert proxy.statuses == {200: 90, 403: 10}           # every tenth request is a bot
    publish(0, 2, bad)
    take(proxy, 1)
    proxy.serve(100)
    assert proxy.statuses[500] == 100
    proxy.close()
    assert health(r, run, 0, 1) is True
    assert health(r, run, 0, 2) is False
    assert r.hgetall(lab.health_key(run, 0, 2)) == {b"err": b"100"}


def test_a_rejection_reaches_the_gate_before_any_request_does(files, r):
    good, bad = files
    proxy, run, publish = make(ConfigHolder())
    publish(0, 1, good)
    publish(0, 2, bad)
    take(proxy, 2)
    assert health(r, run, 0, 2) is False                  # nothing served or flushed yet
    proxy.serve(50)
    assert proxy.stale == 50 and proxy.statuses == {200: 45, 403: 5}
    proxy.close()


def test_with_no_good_file_the_answer_is_the_one_chosen_in_advance(files, r):
    _, bad = files
    for on_unknown, status in (("open", 200), ("closed", 503)):
        proxy, _, publish = make(ConfigHolder(), on_unknown)
        publish(0, 1, bad)
        take(proxy, 1)
        proxy.serve(100)
        assert proxy.statuses == {status: 100}
        assert proxy.bots_passed == (10 if on_unknown == "open" else 0)
        proxy.close()


def test_a_proxy_ignores_files_published_for_another_fleet(files, r):
    good, bad = files
    proxy, _, publish = make(NaiveHolder())
    kafka_publisher("another-run")(0, 7, bad)             # same topic, someone else's file
    publish(0, 1, good)
    take(proxy, 1)
    assert (proxy.holder.version, proxy.holder.names) == (1, good)
    proxy.close()
