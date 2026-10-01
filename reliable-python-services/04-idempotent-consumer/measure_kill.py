"""M1: kill the consumer hard 20 times mid-stream, then count duplicate and lost side effects.

    python measure_kill.py --variant c --runs 5

Per run: 10,000 events on a 6-partition topic. The consumer runs as a subprocess and is
killed with Popen.kill(), which on Windows is TerminateProcess: no cleanup, no offset
commit, no consumer.close(). Each life lasts a random 1-8 s after its first side effect.
Every handler sleeps 10 ms (a stand-in for real work) so the stream outlasts 20 kills.
session.timeout.ms=6000 so the group notices a dead member in seconds, not 45 s.
Correctness counts only, so no measure lock.
"""
import argparse
import json
import random
import time
import uuid

import harness as h

N_EVENTS = 10_000
KILLS = 20
LIFE_S = (1.0, 8.0)
WORK_MS = 10


def wait_for_progress(dsn: str, since_id: int, timeout: float = 90) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if h.ledger_max_id(dsn) > since_id:
            return True
        time.sleep(0.02)
    return False


def one_run(variant: str, run: int, seed: int, n_kills: int = KILLS) -> dict:
    rng = random.Random(seed)
    schema = f"m1_{variant}_{run}"
    dsn = h.setup_schema(schema)
    topic = f"consumer.m1.{variant}.{run}.{uuid.uuid4().hex[:6]}"
    h.create_topic(topic, 6)
    h.produce_events(topic, N_EVENTS)
    log = h.HERE / "results" / "logs" / f"m1_{variant}_{run}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    args = ["--work-ms", str(WORK_MS), "--session-timeout-ms", "6000"]
    kills = []
    t_start = time.time()
    for k in range(n_kills):
        before = h.ledger_max_id(dsn)
        proc = h.start_worker(variant, topic, topic, dsn, log, *args)
        t_proc = time.time()
        if not wait_for_progress(dsn, before):
            proc.kill(); proc.wait()
            raise RuntimeError(f"no progress before kill {k}")
        t_first = time.time()
        life = rng.uniform(*LIFE_S)
        time.sleep(life)
        proc.kill()  # TerminateProcess on Windows
        proc.wait()
        e = h.effects(dsn)
        kills.append({"kill": k + 1, "startup_s": round(t_first - t_proc, 2),
                      "life_s": round(life, 2), "rows": e["rows"], "distinct": e["distinct"]})
        if e["distinct"] >= N_EVENTS:
            break
    # final drain: let one consumer finish, then stop it once it's idle and caught up
    proc = h.start_worker(variant, topic, topic, dsn, log, *args)
    last, stable_since = -1, time.time()
    while True:
        time.sleep(1)
        cur = h.ledger_max_id(dsn)
        if cur != last:
            last, stable_since = cur, time.time()
        if time.time() - stable_since > 8 and h.lag(topic, topic) == 0:
            break
    proc.kill(); proc.wait()
    e = h.effects(dsn)
    result = {
        "variant": variant, "run": run, "seed": seed, "events": N_EVENTS,
        "kills": len(kills), "duplicates": e["duplicates"],
        "lost": N_EVENTS - e["distinct"], "ledger_rows": e["rows"],
        "balance_total": e["balance_total"], "expected_balance": N_EVENTS * h.AMOUNT,
        "wall_s": round(time.time() - t_start, 1), "kill_log": kills,
    }
    h.delete_topics([topic])
    h.drop_schema(schema)
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", required=True, choices=["a1", "a2", "b", "c"])
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--kills", type=int, default=KILLS)
    ap.add_argument("--tag", default="")
    args = ap.parse_args()
    out = h.HERE / "results" / f"m1_kill_{args.variant}{args.tag}.json"
    results = []
    for run in range(1, args.runs + 1):
        r = one_run(args.variant, run, seed=1000 + run, n_kills=args.kills)  # same kill schedule for every variant
        results.append(r)
        print(json.dumps({k: v for k, v in r.items() if k != "kill_log"}), flush=True)
        out.write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    main()
