"""The retrieval gate. Run it in CI on every change to chunking, embeddings, indexes or
search SQL: it fails when recall drops, or when a chunk comes back that the user can't read."""
import json

from harness import GOLDEN, TENANT
from query import Scope
from retrieval import hybrid_search

QUESTIONS = json.loads((GOLDEN / "golden_set.json").read_text())
BASELINE = json.loads((GOLDEN / "baseline.json").read_text())
MAX_RECALL_DROP = 0.02
K = 5


def recall_at_k(conn, embed) -> float:
    """Share of answerable questions whose answer text is in the top K chunks."""
    answerable = [q for q in QUESTIONS if q["kind"] == "answerable"]
    hits = 0
    for q in answerable:
        found = hybrid_search(conn, embed, q["question"], Scope(TENANT, tuple(q["groups"])), K)
        hits += any(c.id.startswith(q["doc_id"] + "#") and q["answer"] in c.text for c in found)
    return hits / len(answerable)


def restricted_chunks_returned(conn, embed) -> list[tuple[str, str]]:
    """(question, chunk id) for every chunk that came back and shouldn't have: the index's
    own ACL forbids it, or the golden set says this user must not get that document."""
    leaks = []
    for q in QUESTIONS:
        forbidden = {row[0] for row in conn.execute(
            "SELECT doc_id || '#' || position FROM chunks WHERE NOT acl && %s::text[]",
            (q["groups"],))}
        found = hybrid_search(conn, embed, q["question"], Scope(TENANT, tuple(q["groups"])), 50)
        off_limits = q["doc_id"] if q["kind"] == "restricted" else None
        leaks += [(q["question"], c.id) for c in found
                  if c.id in forbidden or c.id.split("#")[0] == off_limits]
    return leaks


def test_recall_has_not_dropped(golden_index, embed):
    recall = recall_at_k(golden_index, embed)
    floor = BASELINE["recall_at_5"] - MAX_RECALL_DROP
    assert recall >= floor, f"recall@{K} is {recall:.3f}, below the floor of {floor:.3f}"


def test_no_restricted_chunk_comes_back(golden_index, embed):
    assert restricted_chunks_returned(golden_index, embed) == []
