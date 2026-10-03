"""What the two naive consumers get wrong. These tests pin the failures down, so the delete
drill's numbers have a small, readable counterpart."""
from harness import TENANT
from indexer import Event
from naive_indexer import append_only, last_event_wins
from query import Scope
from retrieval import dense_search

STAFF = Scope(TENANT, ("all-staff",))
V1 = "## Meals\n\nThe daily limit is 60 euros."
V2 = "## Meals\n\nThe daily limit is 75 euros."


def upsert(version: int, text: str) -> Event:
    return Event("upsert", TENANT, "expense-policy", version, "Expense policy", "u", text,
                 ("all-staff",))


DELETE = Event("delete", TENANT, "expense-policy", 3)


def texts(conn, embed) -> list[str]:
    return [chunk.text for chunk in dense_search(conn, embed, "daily limit for meals", STAFF, 5)]


def test_append_only_keeps_answering_from_a_deleted_document(conn, embed):
    append_only(conn, upsert(1, V1), embed)
    append_only(conn, DELETE, embed)
    assert texts(conn, embed) == ["Expense policy / Meals: The daily limit is 60 euros."]


def test_append_only_serves_the_old_and_the_new_version_side_by_side(conn, embed):
    append_only(conn, upsert(1, V1), embed)
    append_only(conn, upsert(2, V2), embed)
    assert sorted(texts(conn, embed)) == ["Expense policy / Meals: The daily limit is 60 euros.",
                                          "Expense policy / Meals: The daily limit is 75 euros."]


def test_last_event_wins_is_correct_when_events_arrive_in_order(conn, embed):
    last_event_wins(conn, upsert(1, V1), embed)
    last_event_wins(conn, upsert(2, V2), embed)
    assert texts(conn, embed) == ["Expense policy / Meals: The daily limit is 75 euros."]
    last_event_wins(conn, DELETE, embed)
    assert texts(conn, embed) == []


def test_last_event_wins_brings_a_deleted_document_back_when_an_upsert_is_late(conn, embed):
    last_event_wins(conn, upsert(1, V1), embed)
    last_event_wins(conn, DELETE, embed)
    last_event_wins(conn, upsert(2, V2), embed)   # sent before the delete, delivered after it
    assert texts(conn, embed) == ["Expense policy / Meals: The daily limit is 75 euros."]


def test_last_event_wins_rolls_a_document_back_when_an_old_event_is_redelivered(conn, embed):
    last_event_wins(conn, upsert(1, V1), embed)
    last_event_wins(conn, upsert(2, V2), embed)
    last_event_wins(conn, upsert(1, V1), embed)   # a redelivery of the first event
    assert texts(conn, embed) == ["Expense policy / Meals: The daily limit is 60 euros."]
