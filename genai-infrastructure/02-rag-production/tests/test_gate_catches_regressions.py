"""A gate that can't fail is decoration. These tests break the pipeline on purpose and check
that the gate in test_retrieval_gate.py goes red."""
import pytest

import harness
import indexer
import retrieval
# Aliased, so pytest doesn't collect the two gate tests a second time from this module.
from test_retrieval_gate import recall_at_k, restricted_chunks_returned
from test_retrieval_gate import test_no_restricted_chunk_comes_back as leak_gate
from test_retrieval_gate import test_recall_has_not_dropped as recall_gate


def careless_chunker(title: str, text: str, size: int = 40) -> list[str]:
    """A refactor gone wrong: heading lines are thrown away as boilerplate and the rest is
    cut into fixed windows of words, so chunks lose their section path."""
    words = " ".join(line for line in text.splitlines() if not line.startswith("#")).split()
    return [" ".join(words[i:i + size]) for i in range(0, len(words), size)]


def test_the_healthy_pipeline_scores_the_stored_baseline(golden_index, embed):
    assert recall_at_k(golden_index, embed) == 1.0
    assert restricted_chunks_returned(golden_index, embed) == []


def test_a_broken_chunker_fails_the_recall_gate(conn, embed, monkeypatch):
    monkeypatch.setattr(indexer, "chunk", careless_chunker)
    harness.index_corpus(conn, embed)

    assert recall_at_k(conn, embed) == 0.875   # 14 of the 16 answers are still found
    with pytest.raises(AssertionError, match="recall@5 is 0.875, below the floor of 0.980"):
        recall_gate(conn, embed)


def test_a_search_that_lost_its_permission_filter_fails_the_leak_gate(golden_index, embed,
                                                                      monkeypatch):
    unfiltered = retrieval.DENSE.replace("AND acl && %(groups)s::text[]", "")
    assert unfiltered != retrieval.DENSE
    monkeypatch.setattr(retrieval, "DENSE", unfiltered)

    leaks = restricted_chunks_returned(golden_index, embed)
    assert ("What is the salary band for senior engineers?", "salary-bands#1") in leaks
    with pytest.raises(AssertionError):
        leak_gate(golden_index, embed)


def test_a_document_indexed_with_the_wrong_acl_fails_the_leak_gate(golden_index, embed):
    # The source says only HR may read the salary bands; a sync bug told the index "everyone".
    golden_index.execute("UPDATE chunks SET acl = '{all-staff}' WHERE doc_id = 'salary-bands'")

    # The search can't tell, because it trusts the ACL it was given. The gate can: the golden
    # set records who must NOT get this document.
    leaks = restricted_chunks_returned(golden_index, embed)
    assert sorted(leaks) == [("What is the salary band for senior engineers?", "salary-bands#0"),
                             ("What is the salary band for senior engineers?", "salary-bands#1"),
                             ("What is the salary band for senior engineers?", "salary-bands#2")]
    with pytest.raises(AssertionError):
        leak_gate(golden_index, embed)
