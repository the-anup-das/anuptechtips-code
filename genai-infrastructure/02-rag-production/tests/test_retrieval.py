"""The two searches against Postgres + pgvector: what they return, and for whom."""
import numpy as np

import synth
from harness import TENANT
from indexer import Event, apply
from query import Scope
from retrieval import DENSE, EXACT, connect, dense_search, hybrid_search, keyword_search

STAFF = Scope(TENANT, ("all-staff",))
ENGINEER = Scope(TENANT, ("all-staff", "engineering"))


def doc_ids(found) -> set[str]:
    return {chunk.id.split("#")[0] for chunk in found}


def test_connect_turns_on_iterative_scans_and_custom_plans(conn):
    conn.execute("SELECT '[1,2,3]'::vector")   # loads pgvector, which validates the setting
    assert conn.execute("SHOW hnsw.iterative_scan").fetchone() == ("relaxed_order",)
    assert conn.execute("SHOW plan_cache_mode").fetchone() == ("force_custom_plan",)


def test_dense_search_finds_the_chunk_and_returns_title_and_url(golden_index, embed):
    found = dense_search(golden_index, embed, "refund limit for EU customers", STAFF, 3)
    assert found[0].id.startswith("refund-policy#")
    assert found[0].title == "Refund policy"
    assert found[0].url == "https://handbook.example/refund-policy"
    assert found[0].text == ("Refund policy / EU customers: The limit is 30 days from delivery. "
                             "Money goes back to the original payment method within "
                             "5 business days.")


def test_keyword_search_finds_an_exact_code(golden_index):
    found = keyword_search(golden_index, "E1042", STAFF, 5)
    assert [chunk.id.split("#")[0] for chunk in found] == ["vpn-setup"]
    assert "certificate expired" in found[0].text


def test_keyword_search_matches_any_word_and_ranks_the_best_first(golden_index):
    found = keyword_search(golden_index, "How many characters must a password have?", STAFF, 5)
    assert found and "14 characters" in found[0].text


def test_keyword_search_with_only_stopwords_returns_nothing(golden_index):
    assert keyword_search(golden_index, "what is the", STAFF, 5) == []


def test_both_searches_only_return_what_the_groups_may_read(golden_index, embed):
    question = "What was the root cause of the payment outage?"
    for search in (lambda q, s: dense_search(golden_index, embed, q, s, 50),
                   lambda q, s: keyword_search(golden_index, q, s, 50),
                   lambda q, s: hybrid_search(golden_index, embed, q, s, 50)):
        assert "payment-outage-postmortem" not in doc_ids(search(question, STAFF))
        assert "payment-outage-postmortem" in doc_ids(search(question, ENGINEER))
        assert doc_ids(search(question, STAFF)) <= {
            "refund-policy", "parental-leave", "expense-policy", "vpn-setup", "security-policy",
            "holiday-calendar", "equipment-policy"}


def test_a_user_with_no_groups_gets_nothing(golden_index, embed):
    nobody = Scope(TENANT, ())
    assert dense_search(golden_index, embed, "refund limit", nobody, 10) == []
    assert keyword_search(golden_index, "refund limit", nobody, 10) == []


def test_another_tenant_sees_nothing(golden_index, embed):
    other = Scope("globex", ("all-staff", "engineering", "hr", "leadership"))
    assert hybrid_search(golden_index, embed, "refund limit for EU customers", other, 10) == []


def test_vectors_from_another_embedding_model_are_never_compared(conn, embed):
    apply(conn, Event("upsert", TENANT, "old", 1, "Refunds", "u", "The limit is 30 days.",
                      ("all-staff",)), embed)
    conn.execute("UPDATE chunks SET embed_model = 'previous-model-v0'")
    assert dense_search(conn, embed, "refund limit", STAFF, 5) == []


def test_dense_results_come_back_in_distance_order(golden_index, embed):
    question = "parental leave for contractors in Germany"
    found = dense_search(golden_index, embed, question, STAFF, 10)
    distance = dict(golden_index.execute(
        "SELECT doc_id || '#' || position, embedding <=> %s::vector FROM chunks",
        (embed([question])[0],)).fetchall())
    distances = [distance[chunk.id] for chunk in found]
    assert len(found) == 10 and distances == sorted(distances)
    assert distances[0] == min(d for cid, d in distance.items()
                               if not cid.startswith(("on-call", "payment", "salary", "board")))


def load_synthetic(conn, n: int, allowed_every: int) -> None:
    """n synthetic chunks; one in `allowed_every` is readable by group 'few'."""
    vectors = synth.clustered(n, seed=1)
    with conn.cursor() as cur, cur.copy(
            "COPY chunks (tenant_id, doc_id, version, chunk_hash, position, title, url, "
            "content, acl, embed_model, embedding) FROM STDIN") as copy:
        for i, vector in enumerate(vectors):
            acl = "{few,many}" if i % allowed_every == 0 else "{many}"
            copy.write_row((TENANT, f"d{i}", 1, f"{i:064x}", 0, "t", "u", f"chunk {i}", acl,
                            "synthetic", synth.to_text(vector)))
    conn.execute("ANALYZE chunks")


def test_a_filtered_search_comes_back_short_unless_iterative_scans_are_on(conn):
    load_synthetic(conn, n=5000, allowed_every=100)   # 1% of the chunks are allowed
    query = synth.to_text(synth.clustered(1, seed=2)[0])
    args = {"query": query, "tenant": TENANT, "groups": ["few"], "model": "synthetic", "k": 10}
    # On a table this small Postgres would rather filter first and sort exactly. Switch
    # those plans off to get the HNSW index scan it picks on a big table.
    conn.execute("SET enable_bitmapscan = off")
    conn.execute("SET enable_seqscan = off")

    conn.execute("SET hnsw.iterative_scan = off")  # pgvector's default
    assert len(conn.execute(DENSE, args).fetchall()) < 10

    conn.execute("SET hnsw.iterative_scan = relaxed_order")   # what connect() sets
    assert len(conn.execute(DENSE, args).fetchall()) == 10


def test_on_a_small_table_the_planner_filters_first_so_nothing_is_missing(conn):
    load_synthetic(conn, n=5000, allowed_every=100)
    query = synth.to_text(synth.clustered(1, seed=2)[0])
    args = {"query": query, "tenant": TENANT, "groups": ["few"], "model": "synthetic", "k": 10}
    conn.execute("SET hnsw.iterative_scan = off")

    plan = " ".join(row[0] for row in conn.execute("EXPLAIN " + DENSE, args).fetchall())
    assert "chunks_embedding" not in plan and "chunks_acl" in plan   # exact: filter, then sort
    assert len(conn.execute(DENSE, args).fetchall()) == 10           # the demo looks fine


def test_asking_for_more_rows_than_ef_search_needs_iterative_scans_too(conn):
    load_synthetic(conn, n=2000, allowed_every=1)
    query = synth.to_text(synth.clustered(1, seed=2)[0])
    args = {"query": query, "tenant": TENANT, "groups": ["many"], "model": "synthetic", "k": 50}

    conn.execute("SET hnsw.iterative_scan = off")
    assert len(conn.execute(DENSE, args).fetchall()) == 40    # hnsw.ef_search defaults to 40

    conn.execute("SET hnsw.iterative_scan = relaxed_order")
    assert len(conn.execute(DENSE, args).fetchall()) == 50


def test_the_exact_query_returns_a_full_page_where_the_hnsw_plan_comes_back_short(conn):
    load_synthetic(conn, n=5000, allowed_every=100)
    query = synth.to_text(synth.clustered(1, seed=2)[0])
    args = {"query": query, "tenant": TENANT, "groups": ["few"], "model": "synthetic", "k": 10}
    conn.execute("SET enable_bitmapscan = off")   # as above: get the plan a big table gets
    conn.execute("SET enable_seqscan = off")
    conn.execute("SET hnsw.iterative_scan = off")
    assert len(conn.execute(DENSE, args).fetchall()) < 10

    plan = " ".join(row[0] for row in conn.execute("EXPLAIN " + EXACT, args).fetchall())
    assert "chunks_embedding" not in plan   # the vector index can't order "distance + 0"
    assert len(conn.execute(EXACT, args).fetchall()) == 10


def test_the_exact_query_is_the_one_the_benchmark_measured():
    import measure_filtered_search
    assert EXACT == measure_filtered_search.EXACT
