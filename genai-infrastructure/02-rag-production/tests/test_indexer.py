"""The ingestion consumer against Postgres + pgvector: replays, late events, deletes,
permission changes, crashes and the reconciliation job."""
import random
import threading

import psycopg
import pytest

from harness import TENANT, orphan_chunks
from indexer import Event, apply, reconcile
from query import Scope
from retrieval import connect, dense_search

V1 = "## Meals\n\nThe daily limit is 60 euros.\n\n## Flights\n\nBook economy class."
V2 = "## Meals\n\nThe daily limit is 75 euros.\n\n## Flights\n\nBook economy class."


def upsert(version: int, text: str = V1, doc: str = "expense-policy",
           acl: tuple[str, ...] = ("all-staff",), tenant: str = TENANT) -> Event:
    return Event("upsert", tenant, doc, version, "Expense policy", f"https://x.example/{doc}",
                 text, acl)


def delete(version: int, doc: str = "expense-policy", tenant: str = TENANT) -> Event:
    return Event("delete", tenant, doc, version)


def chunks(conn) -> list[tuple]:
    return conn.execute(
        "SELECT doc_id, version, position, content, acl FROM chunks "
        "ORDER BY tenant_id, doc_id, position").fetchall()


def registry(conn) -> list[tuple]:
    return conn.execute(
        "SELECT doc_id, version, deleted FROM documents ORDER BY tenant_id, doc_id").fetchall()


def test_an_upsert_indexes_the_chunks_with_their_metadata(conn, embed):
    assert apply(conn, upsert(1), embed) == "indexed"
    assert chunks(conn) == [
        ("expense-policy", 1, 0, "Expense policy / Meals: The daily limit is 60 euros.",
         ["all-staff"]),
        ("expense-policy", 1, 1, "Expense policy / Flights: Book economy class.", ["all-staff"]),
    ]
    assert registry(conn) == [("expense-policy", 1, False)]
    model, has_keywords = conn.execute(
        "SELECT embed_model, tsv @@ to_tsquery('english', 'euro') FROM chunks "
        "WHERE position = 0").fetchone()
    assert model == embed.name and has_keywords


def test_a_replayed_event_changes_nothing_and_embeds_nothing(conn, embed):
    apply(conn, upsert(1), embed)
    before, calls = chunks(conn), embed.calls
    assert apply(conn, upsert(1), embed) == "skipped"
    assert chunks(conn) == before and embed.calls == calls


def test_a_new_version_replaces_the_old_one_in_full(conn, embed):
    apply(conn, upsert(1), embed)
    assert apply(conn, upsert(2, V2), embed) == "indexed"
    rows = chunks(conn)
    assert {version for _, version, _, _, _ in rows} == {2}
    assert [content for *_, content, _ in rows] == [
        "Expense policy / Meals: The daily limit is 75 euros.",
        "Expense policy / Flights: Book economy class."]


def test_a_shorter_new_version_leaves_no_chunks_behind(conn, embed):
    apply(conn, upsert(1), embed)
    apply(conn, upsert(2, "## Meals\n\nThe daily limit is 75 euros."), embed)
    assert len(chunks(conn)) == 1 and orphan_chunks(conn) == 0


def test_only_changed_chunks_are_embedded_again(conn, embed):
    apply(conn, upsert(1), embed)
    assert embed.calls == 2
    apply(conn, upsert(2, V2), embed)   # one of the two chunks changed
    assert embed.calls == 3


def test_a_late_older_event_is_skipped(conn, embed):
    apply(conn, upsert(2, V2), embed)
    assert apply(conn, upsert(1), embed) == "skipped"
    assert {version for _, version, *_ in chunks(conn)} == {2}
    assert registry(conn) == [("expense-policy", 2, False)]


def test_a_delete_removes_every_chunk_and_leaves_a_tombstone(conn, embed):
    apply(conn, upsert(1), embed)
    assert apply(conn, delete(2), embed) == "deleted"
    assert chunks(conn) == []
    assert registry(conn) == [("expense-policy", 2, True)]


def test_a_late_upsert_cannot_bring_a_deleted_document_back(conn, embed):
    apply(conn, upsert(1), embed)
    apply(conn, delete(3), embed)
    assert apply(conn, upsert(2, V2), embed) == "skipped"   # arrived after the delete
    assert chunks(conn) == []


def test_a_delete_that_arrives_first_still_blocks_the_older_upsert(conn, embed):
    assert apply(conn, delete(2), embed) == "deleted"       # the index never saw version 1
    assert apply(conn, upsert(1), embed) == "skipped"
    assert chunks(conn) == [] and registry(conn) == [("expense-policy", 2, True)]


def test_a_replayed_delete_is_skipped(conn, embed):
    apply(conn, upsert(1), embed)
    apply(conn, delete(2), embed)
    assert apply(conn, delete(2), embed) == "skipped"


def test_a_document_recreated_with_a_higher_version_is_indexed_again(conn, embed):
    apply(conn, upsert(1), embed)
    apply(conn, delete(2), embed)
    assert apply(conn, upsert(3, V2), embed) == "indexed"
    assert registry(conn) == [("expense-policy", 3, False)]
    assert len(chunks(conn)) == 2


def test_a_permission_change_rewrites_the_acl_without_embedding_anything(conn, embed):
    apply(conn, upsert(1, acl=("all-staff", "contractors")), embed)
    calls = embed.calls
    assert apply(conn, upsert(2, acl=("all-staff",)), embed) == "indexed"
    assert embed.calls == calls
    assert {tuple(acl) for *_, acl in chunks(conn)} == {("all-staff",)}


def test_revoked_access_closes_and_a_late_event_cannot_reopen_it(conn, embed):
    contractor = Scope(TENANT, ("contractors",))
    apply(conn, upsert(1, acl=("all-staff", "contractors")), embed)
    assert dense_search(conn, embed, "daily limit for meals", contractor, 5)

    apply(conn, upsert(3, acl=("all-staff",)), embed)                  # access revoked
    assert dense_search(conn, embed, "daily limit for meals", contractor, 5) == []
    apply(conn, upsert(2, acl=("all-staff", "contractors")), embed)    # an old event, late
    assert dense_search(conn, embed, "daily limit for meals", contractor, 5) == []


def test_tenants_do_not_share_documents(conn, embed):
    apply(conn, upsert(5, tenant="acme"), embed)
    apply(conn, upsert(1, V2, tenant="globex"), embed)   # same doc_id, lower version: fine
    assert conn.execute("SELECT tenant_id, version FROM documents ORDER BY 1").fetchall() == [
        ("acme", 5), ("globex", 1)]
    apply(conn, delete(6, tenant="acme"), embed)
    assert conn.execute("SELECT DISTINCT tenant_id FROM chunks").fetchall() == [("globex",)]


def test_a_failed_write_rolls_back_and_the_retry_succeeds(conn, embed):
    apply(conn, upsert(1), embed)
    before = chunks(conn)

    class BrokenEmbedder:
        name = embed.name

        def __call__(self, texts):
            return [[0.0, 1.0] for _ in texts]   # wrong dimension: the INSERT fails

    with pytest.raises(psycopg.errors.DataException):
        apply(conn, upsert(2, V2), BrokenEmbedder())
    assert chunks(conn) == before                           # the old version is still whole
    assert registry(conn) == [("expense-policy", 1, False)]  # and the claim rolled back too
    assert apply(conn, upsert(2, V2), embed) == "indexed"    # the redelivered event works


def test_an_embedding_outage_leaves_the_index_untouched(conn, embed):
    apply(conn, upsert(1), embed)
    before = chunks(conn)

    class DownEmbedder:
        name = embed.name

        def __call__(self, texts):
            raise TimeoutError("embedding API timed out")

    with pytest.raises(TimeoutError):
        apply(conn, upsert(2, V2), DownEmbedder())
    assert chunks(conn) == before and registry(conn) == [("expense-policy", 1, False)]


def history() -> list[Event]:
    """Three documents with edits, a permission change, a delete and a re-creation."""
    return [
        upsert(1, doc="a"), upsert(2, V2, doc="a"), upsert(3, V2, doc="a", acl=("hr",)),
        upsert(1, doc="b"), upsert(2, V2, doc="b"), delete(3, doc="b"),
        upsert(1, doc="c"), delete(2, doc="c"), upsert(3, V2, doc="c"),
    ]


def final_state(conn) -> tuple:
    return chunks(conn), registry(conn)


def test_any_delivery_order_with_duplicates_ends_in_the_same_state(dsn, embed):
    with connect(dsn) as conn:
        for event in history():
            apply(conn, event, embed)
        expected = final_state(conn)
        assert [doc for doc, *_ in expected[0]] == ["a", "a", "c", "c"]   # b is gone

    for seed in range(20):
        rng = random.Random(seed)
        delivered = history() + rng.sample(history(), 4)   # 4 events are delivered twice
        rng.shuffle(delivered)
        with connect(dsn) as conn:
            conn.execute("TRUNCATE chunks, documents")
            for event in delivered:
                apply(conn, event, embed)
            assert final_state(conn) == expected, f"seed {seed}"
            assert orphan_chunks(conn) == 0


def test_two_workers_applying_two_versions_at_once_end_on_the_newest(dsn, embed):
    with connect(dsn) as conn:
        apply(conn, upsert(1), embed)
    for _ in range(10):
        start = threading.Barrier(2)

        def work(event: Event) -> None:
            with connect(dsn) as c:
                start.wait()
                apply(c, event, embed)

        version = _ * 2 + 2
        threads = [threading.Thread(target=work, args=(upsert(version, V2),)),
                   threading.Thread(target=work, args=(upsert(version + 1, V1),))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        with connect(dsn) as conn:
            assert {v for _, v, *_ in chunks(conn)} == {version + 1}
            assert registry(conn) == [("expense-policy", version + 1, False)]
            assert orphan_chunks(conn) == 0


def test_reconcile_repairs_what_lost_events_left_behind(conn, embed):
    source = {"a": upsert(2, V2, doc="a"), "c": upsert(1, doc="c")}   # the source of truth
    apply(conn, upsert(1, doc="a"), embed)   # the event for a's version 2 never arrived
    apply(conn, upsert(1, doc="b"), embed)   # b was deleted at the source; that event was lost
    # c was created at the source, and its event was lost as well

    listing = {doc_id: event.version for doc_id, event in source.items()}
    result = reconcile(conn, TENANT, lambda: listing, source.__getitem__, embed)

    assert result == {"reindexed": 2, "deleted": 1}
    assert registry(conn) == [("a", 2, False), ("b", 2, True), ("c", 1, False)]
    assert {(doc, version) for doc, version, *_ in chunks(conn)} == {("a", 2), ("c", 1)}
    assert orphan_chunks(conn) == 0


def test_reconcile_on_an_index_that_is_in_step_does_nothing(conn, embed):
    apply(conn, upsert(1, doc="a"), embed)
    calls = embed.calls
    result = reconcile(conn, TENANT, lambda: {"a": 1}, lambda doc_id: upsert(1, doc=doc_id),
                       embed)
    assert result == {"reindexed": 0, "deleted": 0} and embed.calls == calls


def test_the_real_delete_event_arriving_after_reconcile_is_harmless(conn, embed):
    apply(conn, upsert(1, doc="b"), embed)
    reconcile(conn, TENANT, lambda: {}, lambda doc_id: upsert(1, doc=doc_id), embed)
    assert apply(conn, delete(7, doc="b"), embed) == "deleted"   # newer than the tombstone
    assert registry(conn) == [("b", 7, True)] and chunks(conn) == []


def test_a_document_created_while_reconcile_runs_is_not_tombstoned(conn, embed):
    source = {"a": upsert(1, doc="a")}
    apply(conn, source["a"], embed)

    def list_source() -> dict[str, int]:
        # Between reconcile's read of the index and this listing, "n" is created at the
        # source and its event reaches the index through the normal consumer.
        source["n"] = upsert(1, doc="n")
        apply(conn, source["n"], embed)
        return {doc_id: event.version for doc_id, event in source.items()}

    result = reconcile(conn, TENANT, list_source, source.__getitem__, embed)
    assert result == {"reindexed": 1, "deleted": 0}   # "n" looked behind; the re-index is a no-op
    assert registry(conn) == [("a", 1, False), ("n", 1, False)]
    assert apply(conn, upsert(2, V2, doc="n"), embed) == "indexed"   # its next version still lands
