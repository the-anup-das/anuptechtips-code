"""M4: the flapping. Four roles stand for four database nodes that receive the permission
change one at a time. The generator's query runs once per tick as one node picked at random
(all 30 queries run first, with a grant before ticks 7, 13, 19 and 25). The files are then
replayed, one every 5 s, to three fleets of twelve proxies at the same moment:

  naive, global push       every file to all twelve at once, no gate (the incident's shape)
  naive, staged rollout    cohort by cohort behind the health gate, with rollback
  hardened, global push    ConfigHolder consumers; every file to all twelve at once, no gate

30 ticks per run, 5 runs with seeds 1 to 5. Each run takes the measure lock on its own, and a
finished run is checkpointed, so a stopped script carries on where it was.

Writes results/m4_flapping_ticks.csv (what the generator produced at each tick),
m4_flapping_series.csv (proxies failing every 100 ms, seed 1 only) and
m4_flapping_summary.json.
"""
import csv
import functools
import json
import logging
import os
import pathlib
import random
import statistics
import sys
import threading
import time
import uuid

import lab
from config_holder import ConfigHolder, NaiveHolder
from feature_file import validate_feature_file
from generator import feature_names, grant_r0
from proxy import ProxyThread, start_fleet
from rollout import STAGED, health, kafka_publisher, rollout

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))                 # the series folder
from common.measure_lock import measuring  # noqa: E402

SEEDS = [1, 2, 3, 4, 5][:int(os.environ.get("RUNS", "5"))]
TICKS = 30
TICK_SECONDS = float(os.environ.get("TICK_SECONDS", "5"))
GRANT_AT = {7: 1, 13: 2, 19: 3, 25: 4}               # tick -> the node that gets the grant then
LANES = [("naive, global push", NaiveHolder, False),
         ("naive, staged rollout", NaiveHolder, True),
         ("hardened, global push", ConfigHolder, False)]


def generate_ticks(conn, seed: int) -> tuple[list[str], list[dict]]:
    """Run the generator's real query once per tick, granting node by node on schedule."""
    lab.reset_schema(conn)
    rng = random.Random(seed)
    in_service = feature_names(conn, "feature_node_1")      # the file before any change
    previous, granted, ticks = in_service, 0, []
    for tick in range(1, TICKS + 1):
        if tick in GRANT_AT:
            grant_r0(conn, f"feature_node_{GRANT_AT[tick]}")
            granted += 1
        node = rng.randint(1, 4)
        names = feature_names(conn, f"feature_node_{node}")
        problems = validate_feature_file(names, previous)
        ticks.append({"seed": seed, "tick": tick, "nodes_with_grant": granted, "node": node,
                      "rows": len(names), "producer_check": "rejected" if problems else "ok",
                      "names": names})
        if not problems:
            previous = names
    lab.reset_schema(conn)
    return in_service, ticks


class Lane(threading.Thread):
    """One fleet of twelve proxies and the controller that feeds it the tick files."""

    def __init__(self, name: str, holder, staged: bool, in_service: list[str], ticks: list[dict],
                 r, epoch: float):
        super().__init__(daemon=True, name=name)
        self.staged, self.ticks, self.epoch = staged, ticks, epoch
        self.run_id = uuid.uuid4().hex[:8]
        self.publish = kafka_publisher(self.run_id)
        self.gate = functools.partial(health, r, self.run_id)
        self.fleet = start_fleet(holder, self.run_id, lab.topic_ends(), epoch=epoch)
        self.threads = [ProxyThread(proxy) for proxy in self.fleet]
        self.version, self.last_good, self.halted = 1, in_service, 0
        for thread in self.threads:
            thread.start()
        for cohort in range(len(lab.COHORTS)):
            self.publish(cohort, 1, in_service)
        while not all(proxy.holder.version == 1 for proxy in self.fleet):
            time.sleep(0.005)

    def run(self) -> None:
        time.sleep(max(0.0, self.epoch - time.perf_counter()))
        for thread in self.threads:
            thread.serving.set()
        for tick in self.ticks:
            time.sleep(max(0.0, self.epoch + (tick["tick"] - 1) * TICK_SECONDS
                           - time.perf_counter()))
            self.version += 1
            if not self.staged:                       # no gate: the file goes to everyone
                for cohort in range(len(lab.COHORTS)):
                    self.publish(cohort, self.version, tick["names"])
            elif rollout(self.version, tick["names"], self.last_good, self.publish,
                         self.gate, stages=STAGED) == len(STAGED):
                self.last_good = tick["names"]
            else:
                self.version += 1                     # the rollback used the next number
                self.halted += 1
        time.sleep(max(0.0, self.epoch + TICKS * TICK_SECONDS - time.perf_counter()))
        self.ended = time.monotonic()
        for thread in self.threads:
            thread.stop()

    def series(self) -> list[dict]:
        """Every 100 ms: how many proxies answered at least one request with a 5xx."""
        rows = []
        for slot in range(int(TICKS * TICK_SECONDS * 10)):
            rows.append({
                "lane": self.name, "second": slot / 10,
                "proxies_failing": sum(proxy.errors_by_slot[slot] > 0 for proxy in self.fleet),
                "requests": sum(proxy.served_by_slot[slot] for proxy in self.fleet),
                "failed_requests": sum(proxy.errors_by_slot[slot] for proxy in self.fleet),
            })
        return rows

    def summary(self) -> dict:
        series = self.series()
        requests = sum(row["requests"] for row in series)
        failed = sum(row["failed_requests"] for row in series)
        ages = [self.ended - proxy.holder.applied_at for proxy in self.fleet
                if hasattr(proxy.holder, "applied_at")]
        return {
            "requests": requests, "failed_requests": failed,
            "failed_share_pct": round(100 * failed / requests, 2),
            "max_proxies_failing_at_once": max(row["proxies_failing"] for row in series),
            "seconds_with_all_12_failing": sum(row["proxies_failing"] == 12 for row in series) / 10,
            "seconds_with_any_failing": sum(row["proxies_failing"] > 0 for row in series) / 10,
            "proxies_that_ever_failed": sum(proxy.statuses[500] > 0 for proxy in self.fleet),
            "rollouts_halted": self.halted,
            "files_rejected_per_proxy": max(proxy.holder.rejected for proxy in self.fleet),
            "stale_serves": sum(proxy.stale for proxy in self.fleet),
            "oldest_file_in_service_seconds": round(max(ages), 1) if ages else "",
        }


def one_seed(conn, r, seed: int) -> tuple[list[dict], dict, list[dict]]:
    in_service, ticks = generate_ticks(conn, seed)
    # a short poll, so a queue of other benchmarks on this machine can't starve this one
    with measuring("fail-open-flapping", poll=0.0005):
        epoch = time.perf_counter() + 3.0             # leave time to start three fleets
        lanes = [Lane(name, holder, staged, in_service, ticks, r, epoch)
                 for name, holder, staged in LANES]
        for lane in lanes:
            lane.start()
        for lane in lanes:
            lane.join()
    return ticks, {lane.name: lane.summary() for lane in lanes}, \
        [row for lane in lanes for row in lane.series()]


def main() -> None:
    logging.getLogger("config").setLevel(logging.ERROR)
    r = lab.redis_client()
    out = HERE / "results"
    out.mkdir(exist_ok=True)
    # Each seed waits for the machine-wide measure lock, so a full run can take a long time.
    # Finished seeds are kept in a checkpoint; a restarted run picks up after them.
    checkpoint = out / "m4_flapping_checkpoint.json"
    done = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    all_ticks, summaries, first_series = [], {}, []
    with lab.connect() as conn:
        for seed in SEEDS:
            if str(seed) in done:
                ticks, summary, series = done[str(seed)]
                print(f"seed {seed}: taken from the checkpoint", flush=True)
            else:
                ticks, summary, series = one_seed(conn, r, seed)
                ticks = [{k: v for k, v in tick.items() if k != "names"} for tick in ticks]
                done[str(seed)] = [ticks, summary, series if seed == SEEDS[0] else []]
                checkpoint.write_text(json.dumps(done))
            all_ticks += ticks
            summaries[seed] = summary
            first_series = first_series or series
            bad = sum(tick["rows"] > 60 for tick in ticks)
            print(f"seed {seed}: {bad} of {TICKS} files doubled", flush=True)
            for lane, s in summary.items():
                print(f"    {lane:24} failed {s['failed_requests']:>6} of {s['requests']} "
                      f"({s['failed_share_pct']}%), all 12 failing for "
                      f"{s['seconds_with_all_12_failing']} s, any failing for "
                      f"{s['seconds_with_any_failing']} s", flush=True)

    with open(out / "m4_flapping_ticks.csv", "w", newline="") as f:
        fields = [k for k in all_ticks[0] if k != "names"]
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_ticks)
    with open(out / "m4_flapping_series.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(first_series[0]))
        writer.writeheader()
        writer.writerows(first_series)

    medians = {}
    for lane, *_ in LANES:
        medians[lane] = {}
        for metric in summaries[SEEDS[0]][lane]:
            values = [summaries[seed][lane][metric] for seed in SEEDS]
            if all(value != "" for value in values):
                medians[lane][metric] = {"median": statistics.median(values),
                                         "min": min(values), "max": max(values)}
    doubled = [sum(t["rows"] > 60 for t in all_ticks if t["seed"] == seed) for seed in SEEDS]
    (out / "m4_flapping_summary.json").write_text(json.dumps({
        "seeds": SEEDS, "ticks": TICKS, "tick_seconds": TICK_SECONDS,
        "grant_at_tick": GRANT_AT, "proxies": sum(lab.COHORTS),
        "requests_per_second_per_proxy": 100,
        "doubled_files_per_seed": dict(zip(SEEDS, doubled)),
        "files_the_producer_check_rejects_per_seed": dict(zip(SEEDS, [
            sum(t["producer_check"] == "rejected" for t in all_ticks if t["seed"] == seed)
            for seed in SEEDS])),
        "per_seed": summaries, "across_seeds": medians}, indent=2))
    checkpoint.unlink()


if __name__ == "__main__":
    main()
