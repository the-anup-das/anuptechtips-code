"""The production path of the lab: sticky router -> server pool -> batch -> model -> score.
Every probe that goes through here becomes one event, tagged with the slice it landed in."""
import uuid
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field, replace

from detectors import unexpected_scripts
from fake_model import Model, Request, Serving, fingerprint, respond, unit
from probes import by_suite, render, score

BATCH_SIZES = (1, 8, 32, 64)        # how many requests share a forward pass, by server load
LONG_CONTEXT_NOISE = 1.0            # short requests on the long-context pool get worse replies
EVENTS = uuid.UUID("5f0c2a52-6d0e-4c53-9d59-0f6c9b1a7e10")   # namespace for event IDs


def bucket(batch_size: int) -> str:
    return "1" if batch_size == 1 else "2-8" if batch_size <= 8 else \
        "9-32" if batch_size <= 32 else "33-64"


@dataclass
class Fleet:
    """The serving path's knobs. Every default is the healthy setting."""
    misroute_share: float = 0.0     # routing bug: share of new sessions sent to the wrong pool
    hw_b: Serving = Serving()       # what hardware "hw-b" runs; kernel bugs get deployed here
    sticky: dict[str, str] = field(default_factory=dict)

    def route(self, session: str) -> str:
        """Sticky routing: a session keeps the pool its first request got."""
        if session not in self.sticky:
            lost = unit("route", session) < self.misroute_share
            self.sticky[session] = "long-context" if lost else "standard"
        return self.sticky[session]

    def serving(self, pool: str, hardware: str) -> Serving:
        base = self.hw_b if hardware == "hw-b" else Serving()
        return replace(base, noise=LONG_CONTEXT_NOISE) if pool == "long-context" else base


def traffic(tick: int, users: int = 500, per_session: int = 4,
            suite: str = "known_answer", prefix: str = "") -> list[Request]:
    """Each user opens one session per tick and sends a few probes in it."""
    probes = by_suite(suite)
    requests = []
    for user in range(users):
        session = f"{prefix}u{user:04d}-t{tick}"
        first = int(unit("pick", session) * len(probes))
        requests += [Request(probes[(first + 37 * j) % len(probes)], session)
                     for j in range(per_session)]
    return requests


def serve(fleet: Fleet, model: Model, system_prompt: Sequence[str], requests: Sequence[Request],
          tick: int, run: str = "lab", client_version: str = "client-1") -> list[dict]:
    """Route, batch, run the model and score. Returns one event per request."""
    groups = defaultdict(list)
    for req in requests:
        pool = fleet.route(req.session)
        hardware = "hw-b" if unit("hw", req.session) < 0.5 else "hw-a"
        size = BATCH_SIZES[int(unit("load", req.session) * len(BATCH_SIZES))]
        groups[pool, hardware, size].append(req)

    events = []
    for (pool, hardware, size), reqs in groups.items():
        serving = fleet.serving(pool, hardware)
        for i in range(0, len(reqs), size):
            batch = reqs[i:i + size]
            for req, tokens in zip(batch, respond(model, serving, system_prompt, batch)):
                name = f"{run}:{tick}:{req.session}:{req.probe.id}"   # a retry gets the same ID
                events.append({
                    "event_id": str(uuid.uuid5(EVENTS, name)),
                    "tick": tick, "suite": req.probe.suite,
                    "pool": pool, "hardware": hardware, "batch_bucket": bucket(len(batch)),
                    "model_version": model.version,
                    "prompt_version": fingerprint(system_prompt),
                    "client_version": client_version,
                    "passed": score(req.probe, tokens),
                    "script_ok": not unexpected_scripts(req.probe.prompt, render(tokens)),
                })
    return events
