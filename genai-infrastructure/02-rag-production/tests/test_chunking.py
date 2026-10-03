from chunking import chunk

TEXT = """Customers can ask for their money back.

## EU customers

The limit is 30 days.

## US customers

The limit is 45 days.

### Alaska

Add 5 days for shipping.
"""


def test_every_chunk_starts_with_the_title_and_section_path():
    assert chunk("Refund policy", TEXT) == [
        "Refund policy: Customers can ask for their money back.",
        "Refund policy / EU customers: The limit is 30 days.",
        "Refund policy / US customers: The limit is 45 days.",
        "Refund policy / US customers / Alaska: Add 5 days for shipping.",
    ]


def test_a_sibling_heading_replaces_the_deeper_path():
    text = "## A\n\n### A1\n\none\n\n## B\n\ntwo"
    assert chunk("T", text) == ["T / A / A1: one", "T / B: two"]


def test_a_heading_right_above_its_text_still_counts():
    assert chunk("T", "## Meals\nThe daily limit is 60 euros.") == [
        "T / Meals: The daily limit is 60 euros."]


def test_paragraphs_are_packed_up_to_the_limit_and_never_split():
    text = "\n\n".join(["one two three four"] * 5)
    chunks = chunk("T", text, max_words=10)
    assert chunks == ["T: one two three four one two three four",
                      "T: one two three four one two three four",
                      "T: one two three four"]


def test_an_oversized_paragraph_is_packed_sentence_by_sentence():
    text = "First sentence here. Second sentence here. Third sentence here."
    assert chunk("T", text, max_words=6) == [
        "T: First sentence here. Second sentence here.", "T: Third sentence here."]


def test_empty_text_gives_no_chunks():
    assert chunk("T", "") == []
    assert chunk("T", "## Only a heading") == []
