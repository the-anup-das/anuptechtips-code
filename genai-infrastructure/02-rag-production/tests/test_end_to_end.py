"""The whole path against Postgres + pgvector: the handbook corpus goes in through the
consumer, a question goes through answer() with the real SQL searches. Only the reranker and
the LLM are fakes."""
import re
from functools import partial

from harness import TENANT, corpus_events
from indexer import Event, apply
from query import NOT_FOUND, Scope, answer
from retrieval import connect, dense_search, keyword_search

STAFF = Scope(TENANT, ("all-staff",))
HR = Scope(TENANT, ("all-staff", "hr"))


def word_overlap(question: str, texts: list[str]) -> list[float]:
    """A stand-in for a cross-encoder: counts the question's words that appear in the text."""
    words = set(re.findall(r"[a-z0-9]+", question.lower()))
    return [float(len(words & set(re.findall(r"[a-z0-9]+", text.lower())))) for text in texts]


class QuotingLLM:
    """Answers with the first source's text and cites it, unless told there is no answer."""

    def __init__(self, no_answer: bool = False) -> None:
        self.prompts: list[str] = []
        self.no_answer = no_answer

    def __call__(self, prompt: str) -> str:
        self.prompts.append(prompt)
        if self.no_answer:
            return "NO_ANSWER"
        first_source = prompt.split("[S1] ", 1)[1].split("\n\n", 1)[0]
        return f"{first_source.splitlines()[1]} [S1]"


def ask(dsn, embed, question: str, scope: Scope, llm, **kw):
    # Two connections: the two searches run at the same time
    with connect(dsn) as first, connect(dsn) as second:
        return answer(question, "", scope, llm, partial(keyword_search, first),
                      partial(dense_search, second, embed), word_overlap, **kw)


def test_a_question_is_answered_from_the_right_chunk_with_a_citation(dsn, golden_index, embed):
    llm = QuotingLLM()
    result = ask(dsn, embed, "What is the refund limit for EU customers?", STAFF, llm)
    assert result.text.startswith("Refund policy / EU customers: The limit is 30 days")
    assert result.citations_ok and result.sources[0].id.startswith("refund-policy#")
    assert result.sources[0].url == "https://handbook.example/refund-policy"
    assert set(result.timings) == {"keyword", "dense", "retrieve", "rerank", "generate"}


def test_a_restricted_document_never_reaches_the_prompt(dsn, golden_index, embed):
    question = "What is the salary band for senior engineers?"
    llm = QuotingLLM()
    result = ask(dsn, embed, question, STAFF, llm)
    assert len(llm.prompts) == 1 and "95,000" not in llm.prompts[0]
    assert "Salary bands" not in llm.prompts[0]
    assert all(not source.id.startswith("salary-bands#") for source in result.sources)

    llm = QuotingLLM()
    result = ask(dsn, embed, question, HR, llm, min_score=4)
    assert "95,000 to 120,000 euros" in llm.prompts[0]
    assert result.sources[0].id.startswith("salary-bands#") and result.citations_ok


def test_an_unanswerable_question_is_refused(dsn, golden_index, embed):
    result = ask(dsn, embed, "What is the dress code in the office?", STAFF,
                 QuotingLLM(no_answer=True))
    assert result.text == NOT_FOUND and result.sources == []


def test_a_deleted_document_stops_answering_at_once(dsn, golden_index, embed):
    question = "How much parental leave do contractors in Germany get?"
    before = ask(dsn, embed, question, STAFF, QuotingLLM())
    assert "8 weeks" in before.text

    apply(golden_index, Event("delete", TENANT, "parental-leave", 2), embed)
    llm = QuotingLLM()
    after = ask(dsn, embed, question, STAFF, llm)
    assert all("parental-leave" not in source.id for source in after.sources)
    assert all("8 weeks" not in prompt for prompt in llm.prompts)


def test_an_edited_document_answers_from_the_new_text_only(dsn, golden_index, embed):
    old = next(e for e in corpus_events() if e.doc_id == "expense-policy")
    new_text = old.text.replace("60 euros", "75 euros")
    apply(golden_index, Event("upsert", TENANT, old.doc_id, 2, old.title, old.url, new_text,
                              old.acl), embed)
    llm = QuotingLLM()
    result = ask(dsn, embed, "What is the daily limit for meals in the expense policy?", STAFF,
                 llm)
    assert "75 euros" in result.text
    assert "60 euros" not in llm.prompts[0]
