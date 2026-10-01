"""Run consume.run(), but die hard right after the DB commit of one chosen event, before
its offset is committed. A marker file makes it crash only on the first attempt.

    python tests/crash_once.py <variant b|c> <topic> <group> <crash_seq> <marker_file>
"""
import logging
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import consume  # noqa: E402
from worker import plain_once  # noqa: E402

variant, topic, group, crash_seq, marker = sys.argv[1:6]
real = consume.handle_once if variant == "c" else plain_once


def handle_then_maybe_die(conn, event_id, event, apply=consume.credit_account):
    ran = real(conn, event_id, event, apply)
    marker_path = pathlib.Path(marker)
    if event["seq"] == int(crash_seq) and not marker_path.exists():
        marker_path.write_text("crashed")
        os._exit(1)  # no finally, no close(), no offset commit
    return ran


consume.handle_once = handle_then_maybe_die
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
consume.run(topic, group, **{"session.timeout.ms": 6000})
