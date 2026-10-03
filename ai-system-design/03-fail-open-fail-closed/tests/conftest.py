"""Fixtures: the Postgres database `failmodes`, Redis DB 8 and the failmodes.* Kafka topics
from the Docker services in reliable-python-services/docker-compose.yml."""
import functools
import time
import uuid

import pytest

import lab
from generator import feature_names, grant_r0
from proxy import ProxyThread, start_fleet
from rollout import health, kafka_publisher


@pytest.fixture(scope="session")
def database():
    conn = lab.connect()
    yield conn
    conn.close()


@pytest.fixture
def pg(database):
    lab.reset_schema(database)      # every test starts "before the permission change"
    return database


@pytest.fixture
def r():
    client = lab.redis_client()
    client.flushdb()                # DB 8 belongs to this post
    yield client
    client.close()


@pytest.fixture(scope="session")
def files(database):
    """The two files the generator produces: before the grant (60 names) and after (120)."""
    lab.reset_schema(database)
    good = feature_names(database, "feature_gen")
    grant_r0(database, "feature_gen")
    bad = feature_names(database, "feature_gen")
    lab.reset_schema(database)
    return good, bad


class Fleet:
    """Twelve proxies on real Kafka topics, already serving version 1 (the good file)."""

    def __init__(self, holder_factory, r, good, on_unknown="open", first_file=True):
        self.run = uuid.uuid4().hex[:8]
        self.publish = kafka_publisher(self.run)
        self.gate = functools.partial(health, r, self.run)
        self.proxies = start_fleet(holder_factory, self.run, lab.topic_ends(), on_unknown,
                                   epoch=time.perf_counter())
        self.threads = [ProxyThread(proxy) for proxy in self.proxies]
        for thread in self.threads:
            thread.start()
        if first_file:
            for cohort in range(len(lab.COHORTS)):
                self.publish(cohort, 1, good)
            self.wait_for(lambda proxy: proxy.holder.version == 1)
        for thread in self.threads:
            thread.serving.set()

    def wait_for(self, condition, timeout=10.0):
        deadline = time.perf_counter() + timeout
        while not all(condition(proxy) for proxy in self.proxies):
            assert time.perf_counter() < deadline, "the proxies never got there"
            time.sleep(0.01)

    def stop(self):
        for thread in self.threads:
            thread.stop()


@pytest.fixture
def fleet(r, files):
    """fleet(holder_factory) starts twelve proxies; they are stopped after the test."""
    started = []

    def start(holder_factory, **kwargs):
        started.append(Fleet(holder_factory, r, files[0], **kwargs))
        return started[-1]

    yield start
    for one in started:
        one.stop()
