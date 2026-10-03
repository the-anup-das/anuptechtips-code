"""M5: the delete drill. What does the index still serve after documents were edited,
re-permissioned and deleted at the source?

Source history (fixed seed): 2,000 documents of 5 sections each are created. Then 400 are
edited (2 of their 5 sections change), 100 lose the group "contractors" and 100 are deleted.
That is 2,600 events, each carrying the document's version.

Delivery is at least once and out of order: a change happens up to 300 ticks after its
document was created (one creation per tick), every event arrives up to 200 ticks late, and
one event in ten is delivered a second time, up to 400 ticks after the first. Each of the 20
runs uses a different delivery order.

Consumers:
  append_only      naive_indexer.append_only: upserts add chunks, deletes are ignored
  last_event_wins  naive_indexer.last_event_wins: replaces and deletes, trusts arrival order
  versioned        indexer.apply: version claim, versioned replace, tombstones
  versioned_lossy  indexer.apply, but 2% of the events never arrive at all;
                   then indexer.reconcile runs once (reported as versioned_lossy_reconciled)

After each run the drill searches the index with the post's SQL and counts:
  deleted_still_served   deleted documents that still come back for their own text
  edited_serving_old     edited documents whose replaced text still comes back
  revoked_still_readable documents the group "contractors" can still read after the revoke
  leftover_chunks        rows in `chunks` that aren't part of a live document's current version
The vectors come from embedder.HashingEmbedder, a deterministic stand-in for a model.
"""
import csv
import json
import pathlib
import random
import statistics
import sys

import harness
import naive_indexer
from embedder import HashingEmbedder
from indexer import Event, apply, reconcile
from query import Scope
from retrieval import connect, dense_search

HERE = pathlib.Path(__file__).resolve().parent
SCHEMA = "drill"
DOCS, SECTIONS, WORDS = 2000, 5, 12
EDITED, REVOKED, DELETED = 400, 100, 100
RUNS = 20
LOST_SHARE = 0.02
EVERYONE = Scope(harness.TENANT, ("all-staff", "contractors"))
CONTRACTOR = Scope(harness.TENANT, ("contractors",))
CONSUMERS = {"append_only": naive_indexer.append_only,
             "last_event_wins": naive_indexer.last_event_wins,
             "versioned": apply, "versioned_lossy": apply}
METRICS = ("deleted_still_served", "edited_serving_old", "revoked_still_readable",
           "leftover_chunks")


def section(doc: int, number: int, edition: int) -> str:
    """Deterministic filler text: made-up words, different for every section and edition."""
    rng = random.Random(f"{doc}/{number}/{edition}")
    return " ".join(f"w{rng.randrange(20000):05d}" for _ in range(WORDS))


def document(doc: int, version: int, kind: str) -> Event:
    doc_id = f"doc-{doc:04d}"
    if kind == "delete":
        return Event("delete", harness.TENANT, doc_id, version)
    # an edit rewrites sections 1 and 3; a revoke leaves the text alone
    text = "\n\n".join(
        f"## Section {n}\n\n{section(doc, n, 2 if kind == 'edit' and n in (1, 3) else 1)}"
        for n in range(SECTIONS))
    acl = ("all-staff",) if kind == "revoke" else ("all-staff", "contractors")
    return Event("upsert", harness.TENANT, doc_id, version, f"Document {doc}",
                 f"https://docs.example/{doc}", text, acl)


def chunk_text(doc: int, number: int, edition: int) -> str:
    """The chunk as the chunker stores it: title / section path: text."""
    return f"Document {doc} / Section {number}: {section(doc, number, edition)}"


def source_history() -> tuple[list[tuple[float, Event]], dict[str, list[int]]]:
    """(source time, event) pairs, plus which documents got which change."""
    rng = random.Random(1)
    changed = rng.sample(range(DOCS), EDITED + REVOKED + DELETED)
    plan = {"edit": changed[:EDITED], "revoke": changed[EDITED:EDITED + REVOKED],
            "delete": changed[EDITED + REVOKED:]}
    history = [(float(doc), document(doc, 1, "create")) for doc in range(DOCS)]
    for kind, docs in plan.items():
        history += [(doc + rng.uniform(1, 300), document(doc, 2, kind)) for doc in docs]
    return sorted(history, key=lambda pair: pair[0]), plan


def deliveries(history: list[tuple[float, Event]], seed: int, lost: float) -> list[Event]:
    """The order the consumer sees: late, sometimes twice, and with `lost` share missing."""
    rng = random.Random(seed)
    arrivals = []
    for when, event in history:
        if rng.random() < lost:
            continue
        first = when + rng.uniform(0, 200)
        arrivals.append((first, event))
        if rng.random() < 0.10:
            arrivals.append((first + rng.uniform(0, 400), event))
    return [event for _, event in sorted(arrivals, key=lambda pair: pair[0])]


def inspect(conn, embed, plan: dict[str, list[int]]) -> dict[str, int]:
    def served(doc: int, text: str, scope: Scope) -> list[str]:
        found = dense_search(conn, embed, text, scope, 5)
        return [c.text for c in found if c.id.startswith(f"doc-{doc:04d}#")]

    deleted = sum(bool(served(doc, chunk_text(doc, 0, 1), EVERYONE)) for doc in plan["delete"])
    stale = sum(chunk_text(doc, 1, 1) in served(doc, chunk_text(doc, 1, 1), EVERYONE)
                for doc in plan["edit"])
    readable = sum(bool(served(doc, chunk_text(doc, 0, 1), CONTRACTOR))
                   for doc in plan["revoke"])

    current = {f"doc-{doc:04d}": 1 for doc in range(DOCS)}
    current.update({f"doc-{doc:04d}": 2 for doc in plan["edit"] + plan["revoke"]})
    for doc in plan["delete"]:
        del current[f"doc-{doc:04d}"]
    leftover = sum(count for doc_id, version, count in conn.execute(
        "SELECT doc_id, version, count(*) FROM chunks GROUP BY 1, 2")
        if current.get(doc_id) != version)
    return dict(zip(METRICS, (deleted, stale, readable, leftover)))


def main() -> None:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else RUNS
    history, plan = source_history()
    events_by_doc = {}
    for _, event in history:   # the source's current state: the newest event per document
        events_by_doc[event.doc_id] = event
    live = {doc_id: e for doc_id, e in events_by_doc.items() if e.type == "upsert"}
    listing = {doc_id: e.version for doc_id, e in live.items()}

    rows = []
    for run in range(1, runs + 1):
        for name, consume in CONSUMERS.items():
            lossy = name == "versioned_lossy"
            delivered = deliveries(history, seed=run, lost=LOST_SHARE if lossy else 0.0)
            dsn = harness.setup_schema(SCHEMA)
            embed = HashingEmbedder()
            with connect(dsn) as conn:
                for event in delivered:
                    consume(conn, event, embed)
                counts = inspect(conn, embed, plan)
                rows.append({"run": run, "consumer": name, "delivered": len(delivered),
                             **counts})
                if lossy:
                    repaired = reconcile(conn, harness.TENANT, lambda: listing, live.__getitem__,
                                         embed)
                    rows.append({"run": run, "consumer": "versioned_lossy_reconciled",
                                 "delivered": len(delivered), **inspect(conn, embed, plan)})
                    print(f"run {run}: reconcile {repaired}")
            print(rows[-2] if lossy else rows[-1])
    harness.drop_schema(SCHEMA)

    summary = {}
    for name in [*CONSUMERS, "versioned_lossy_reconciled"]:
        mine = [row for row in rows if row["consumer"] == name]
        summary[name] = {metric: {"median": statistics.median(r[metric] for r in mine),
                                  "min": min(r[metric] for r in mine),
                                  "max": max(r[metric] for r in mine)} for metric in METRICS}
        summary[name]["runs_with_any_stale_answer"] = sum(
            any(r[m] for m in METRICS[:3]) for r in mine)

    results = HERE / "results"
    results.mkdir(exist_ok=True)
    with open(results / "m5_delete_drill_runs.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (results / "m5_delete_drill.json").write_text(json.dumps({
        "documents": DOCS, "sections_per_document": SECTIONS, "source_events": len(history),
        "edited": EDITED, "revoked": REVOKED, "deleted": DELETED, "runs": runs,
        "lost_share_in_lossy_variant": LOST_SHARE, "summary": summary}, indent=2) + "\n")
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
