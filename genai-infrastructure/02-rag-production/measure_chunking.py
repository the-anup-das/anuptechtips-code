"""M6: does the way you chunk change what retrieval finds, and how much text it sends?

The handbook in golden/ (11 pages, 618 words) is indexed once per chunker with the post's
schema, consumer (indexer.apply) and search. Each of the 16 answerable golden questions is
asked with the user's groups, and a question counts as found at k when a chunk of the right
document that contains the whole answer text is in the top k.

Chunkers:
  structure             chunking.chunk: split on headings, whole paragraphs, title and
                        section path in front of every chunk (what the post recommends)
  structure_no_prefix   the same chunks without the title and section path
  fixed_40              headings dropped, fixed windows of 40 words
  fixed_40_title        fixed windows of 40 words, title in front
  fixed_40_overlap_20   fixed windows of 40 words, each overlapping the last by 20
  fixed_20              headings dropped, fixed windows of 20 words
  whole_page            one chunk per page

Also counted: answers cut in half (no chunk of the right document holds the whole answer
text) and the average number of words in the top 5 and the top 1, which is what the model
gets to read.

The embedder is embedder.HashingEmbedder, which matches words, not meaning, and the pages
are short (36 to 84 words). Treat the result as a comparison of chunkers on one small
corpus, not as recall you will see with a real model.

    python measure_chunking.py
"""
import json
import pathlib
import sys

import chunking
import harness
import indexer
from embedder import HashingEmbedder
from query import Scope
from retrieval import connect, dense_search, hybrid_search, keyword_search

HERE = pathlib.Path(__file__).resolve().parent
SCHEMA = "chunking"
QUESTIONS = [q for q in json.loads((harness.GOLDEN / "golden_set.json").read_text())
             if q["kind"] == "answerable"]


def fixed(size: int, overlap: int = 0, title_prefix: bool = False):
    """Fixed windows of words with the heading lines thrown away."""
    def chunker(title: str, text: str, **_) -> list[str]:
        words = " ".join(line for line in text.splitlines()
                         if not line.startswith("#")).split()
        step = size - overlap
        windows = [" ".join(words[i:i + size])
                   for i in range(0, max(len(words) - overlap, 1), step)]
        return [f"{title}: {w}" if title_prefix else w for w in windows]
    return chunker


def structure_no_prefix(title: str, text: str, **_) -> list[str]:
    return [c.split(": ", 1)[1] for c in chunking.chunk(title, text)]


def whole_page(title: str, text: str, **_) -> list[str]:
    return [f"{title}: {' '.join(text.split())}"]


CHUNKERS = {
    "structure": chunking.chunk,
    "structure_no_prefix": structure_no_prefix,
    "fixed_40": fixed(40),
    "fixed_40_title": fixed(40, title_prefix=True),
    "fixed_40_overlap_20": fixed(40, overlap=20),
    "fixed_20": fixed(20),
    "whole_page": whole_page,
}


def found(chunks, q) -> bool:
    return any(c.id.startswith(q["doc_id"] + "#") and q["answer"] in c.text for c in chunks)


def measure(conn, embed) -> dict:
    searches = {
        "hybrid": lambda q, k, s: hybrid_search(conn, embed, q["question"], s, k),
        "dense": lambda q, k, s: dense_search(conn, embed, q["question"], s, k),
        "keyword": lambda q, k, s: keyword_search(conn, q["question"], s, k),
    }
    row = {
        "chunks": conn.execute("SELECT count(*) FROM chunks").fetchone()[0],
        "answers_cut_in_half": sum(
            conn.execute("SELECT 1 FROM chunks WHERE doc_id = %s "
                         "AND position(%s in content) > 0 LIMIT 1",
                         (q["doc_id"], q["answer"])).fetchone() is None
            for q in QUESTIONS),
    }
    for name, search in searches.items():
        for k in (1, 3, 5):
            row[f"{name}_found_at_{k}"] = sum(
                found(search(q, k, Scope(harness.TENANT, tuple(q["groups"]))), q)
                for q in QUESTIONS)
    for k in (1, 5):
        words = [len(c.text.split()) for q in QUESTIONS for c in
                 hybrid_search(conn, embed, q["question"],
                               Scope(harness.TENANT, tuple(q["groups"])), k)]
        row[f"avg_words_top_{k}"] = round(sum(words) / len(QUESTIONS), 1)
    return row


def main() -> None:
    embed = HashingEmbedder()
    summary = {"questions": len(QUESTIONS), "chunkers": {}}
    try:
        for name, chunker in CHUNKERS.items():
            indexer.chunk = chunker  # apply() looks the chunker up at call time
            dsn = harness.setup_schema(SCHEMA)
            with connect(dsn) as conn:
                harness.index_corpus(conn, embed)
                summary["chunkers"][name] = measure(conn, embed)
    finally:
        indexer.chunk = chunking.chunk
        harness.drop_schema(SCHEMA)

    results = HERE / "results"
    results.mkdir(exist_ok=True)
    (results / "m6_chunking.json").write_text(json.dumps(summary, indent=1))
    print(f"{'chunker':<20} {'chunks':>6} {'cut':>4} {'@1':>3} {'@3':>3} {'@5':>3} "
          f"{'words@5':>8}   (hybrid, of {len(QUESTIONS)})")
    for name, r in summary["chunkers"].items():
        print(f"{name:<20} {r['chunks']:>6} {r['answers_cut_in_half']:>4} "
              f"{r['hybrid_found_at_1']:>3} {r['hybrid_found_at_3']:>3} "
              f"{r['hybrid_found_at_5']:>3} {r['avg_words_top_5']:>8}")


if __name__ == "__main__":
    sys.exit(main())
