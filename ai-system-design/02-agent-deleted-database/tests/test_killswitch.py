"""The kill switch: a Kafka consumer trips it, a human can trip it, and the gate obeys it."""
import json
import pathlib
import subprocess
import sys
import threading
import time

import pytest
from confluent_kafka import Producer

import killswitch
import lab
from executor import Executor
from gate import Denied, ToolGate


@pytest.fixture
def producer():
    kafka = Producer({"bootstrap.servers": lab.KAFKA})
    yield kafka
    kafka.flush(5)


@pytest.fixture
def watchdog(r, topic):
    """The watchdog on a fresh topic, running once its partition is assigned."""
    stopped, ready = threading.Event(), threading.Event()
    thread = threading.Thread(target=killswitch.watch, daemon=True, kwargs=dict(
        r=r, topic=topic, group=topic, stopped=stopped, ready=ready))
    thread.start()
    assert ready.wait(30), "the watchdog never got its partition"
    yield
    stopped.set()
    thread.join(10)


def send(producer, topic, agent, kind="destructive", ts=None, n=1, step=0.0):
    start = time.time() if ts is None else ts
    for i in range(n):
        event = {"agent": agent, "env": "staging", "kind": kind, "sql": "DELETE ...",
                 "decision": "allowed", "ts": start + i * step}
        producer.produce(topic, key=agent, value=json.dumps(event))
    producer.flush(5)


def wait_for(check, timeout=15.0):
    deadline = time.monotonic() + timeout
    while not (value := check()) and time.monotonic() < deadline:
        time.sleep(0.02)
    return value


def trip_canary(producer, topic, r):
    """The topic has one partition, so once the canary's switch is thrown the watchdog
    has also read everything that was sent before it."""
    send(producer, topic, "canary", n=21)
    assert wait_for(lambda: r.get("killswitch:canary"))


def test_the_21st_destructive_call_in_10_seconds_trips_the_switch(r, topic, producer, watchdog):
    send(producer, topic, "a1", n=20)
    trip_canary(producer, topic, r)
    assert r.get("killswitch:a1") is None
    send(producer, topic, "a1", n=1)
    assert wait_for(lambda: r.get("killswitch:a1")) == b"21 destructive calls in 10 s"


def test_reads_and_writes_never_trip_it(r, topic, producer, watchdog):
    send(producer, topic, "a1", kind="read", n=200)
    send(producer, topic, "a1", kind="write", n=200)
    trip_canary(producer, topic, r)
    assert r.get("killswitch:a1") is None


def test_slow_deletes_do_not_trip_it(r, topic, producer, watchdog):
    send(producer, topic, "a1", n=60, ts=time.time() - 120, step=1.0)  # one a second
    trip_canary(producer, topic, r)
    assert r.get("killswitch:a1") is None


def test_the_switch_is_thrown_once_and_later_events_do_not_change_it(r, topic, producer,
                                                                    watchdog):
    send(producer, topic, "a1", n=21)
    assert wait_for(lambda: r.get("killswitch:a1")) == b"21 destructive calls in 10 s"
    send(producer, topic, "a1", n=50)
    trip_canary(producer, topic, r)
    assert r.get("killswitch:a1") == b"21 destructive calls in 10 s"


def test_a_human_can_stop_and_clear_an_agent(r):
    ran = []
    gate = ToolGate(r, lambda env, kind, sql, max_rows: ran.append(sql))
    killswitch.stop(r, "a1", "from my phone")
    with pytest.raises(Denied, match="kill switch"):
        gate.call("a1", "staging", "SELECT 1")
    assert r.get("killswitch:a1") == b"from my phone"
    killswitch.clear(r, "a1")
    gate.call("a1", "staging", "SELECT 1")
    assert ran == ["SELECT 1"]


def test_the_switch_works_from_the_command_line(r):
    folder = pathlib.Path(killswitch.__file__).parent
    subprocess.run([sys.executable, "killswitch.py", "stop", "a1", "from", "my", "phone"],
                   cwd=folder, check=True)
    assert r.get("killswitch:a1") == b"from my phone"
    subprocess.run([sys.executable, "killswitch.py", "clear", "a1"], cwd=folder, check=True)
    assert r.get("killswitch:a1") is None


def test_a_200_delete_speedrun_is_cut_short(db, r, topic, producer, watchdog, connect):
    """End to end: gate -> Kafka -> watchdog -> Redis -> gate. The bucket is opened wide so
    the watchdog is the only thing in the way."""
    executor = Executor(connect(lab.AGENT), connect(lab.OPERATOR), soft_delete=True)
    gate = ToolGate(r, executor, audit=killswitch.audit_to_kafka(producer, topic),
                    rate=1000, burst=1000)
    reasons = []
    for email_id in range(1, 201):
        try:
            gate.call("speedrunner", "staging", f"DELETE FROM staging.inbox WHERE id = {email_id}")
            time.sleep(0.005)  # a model that needs 5 ms to pick the next email
        except Denied as denied:
            reasons.append(denied.reason)
    left = connect().execute("SELECT count(*) FROM staging.inbox").fetchone()[0]
    assert set(reasons) == {"kill switch"}
    assert 21 <= 200 - left < 200 and len(reasons) == left
