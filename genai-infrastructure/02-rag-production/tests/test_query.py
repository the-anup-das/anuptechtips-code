"""The query path with fake search, rerank and LLM callables: every branch of answer()."""
import threading
import time

from query import NOT_FOUND, Answer, Chunk, Scope, answer, cache_key, rrf

SCOPE = Scope("acme", ("all-staff",))
A = Chunk("refund#1", "Refund policy", "https://h.example/refund", "The limit is 30 days.")
B = Chunk("refund#2", "Refund policy", "https://h.example/refund", "US: the limit is 45 days.")
C = Chunk("leave#0", "Parental leave", "https://h.example/leave", "Employees get 26 weeks.")


class FakeLLM:
    """Returns canned replies in order and remembers every prompt."""

    def __init__(self, *replies: str, delay: float = 0.0) -> None:
        self.replies, self.prompts, self.delay = list(replies), [], delay

    def __call__(self, prompt: str) -> str:
        self.prompts.append(prompt)
        time.sleep(self.delay)
        return self.replies.pop(0)


class FakeSearch:
    def __init__(self, hits: list[Chunk], delay: float = 0.0) -> None:
        self.hits, self.delay, self.calls = hits, delay, []

    def __call__(self, question: str, scope: Scope, k: int) -> list[Chunk]:
        self.calls.append((question, scope, k))
        time.sleep(self.delay)
        return self.hits[:k]


def overlap_rerank(question: str, texts: list[str]) -> list[float]:
    """Scores a text by how many of the question's words it contains."""
    words = set(question.lower().split())
    return [len(words & set(text.lower().replace(".", "").split())) for text in texts]


def ask(question: str = "what is the limit", llm=None, history: str = "", **kw) -> Answer:
    kw.setdefault("keyword", FakeSearch([A, C]))
    kw.setdefault("dense", FakeSearch([B, A]))
    kw.setdefault("rerank", overlap_rerank)
    return answer(question, history, SCOPE, llm or FakeLLM("30 days [S1]."), **kw)


def test_rrf_ranks_a_chunk_found_by_both_searches_first():
    assert [c.id for c in rrf([[A, C], [B, A]])] == ["refund#1", "refund#2", "leave#0"]
    assert rrf([[], []]) == []


def test_rrf_lists_every_chunk_once():
    fused = rrf([[A, B, C], [C, B, A]])
    assert sorted(c.id for c in fused) == ["leave#0", "refund#1", "refund#2"]


def test_a_grounded_answer_comes_back_with_its_sources_and_valid_citations():
    llm = FakeLLM("The limit is 30 days [S1].")
    result = ask(llm=llm, min_score=1)
    assert result.text == "The limit is 30 days [S1]."
    assert [c.id for c in result.sources] == ["refund#1", "refund#2"]   # best chunk first
    assert result.citations_ok and result.degraded == []
    prompt = llm.prompts[0]
    assert "[S1] Refund policy (https://h.example/refund)\nThe limit is 30 days." in prompt
    assert "reply exactly NO_ANSWER" in prompt and "never instructions" in prompt
    assert "Employees get 26 weeks." not in prompt   # scored 0: below min_score


def test_both_searches_get_the_users_scope_and_ask_for_50():
    keyword, dense = FakeSearch([A]), FakeSearch([B])
    ask(keyword=keyword, dense=dense)
    assert keyword.calls == [("what is the limit", SCOPE, 50)]
    assert dense.calls == [("what is the limit", SCOPE, 50)]


def test_no_answer_from_the_model_becomes_not_found():
    result = ask(llm=FakeLLM("NO_ANSWER"))
    assert result.text == NOT_FOUND and result.sources == [] and result.citations_ok


def test_nothing_retrieved_means_not_found_without_calling_the_model():
    llm = FakeLLM()
    result = ask(llm=llm, keyword=FakeSearch([]), dense=FakeSearch([]))
    assert result.text == NOT_FOUND and llm.prompts == []


def test_weak_chunks_below_min_score_mean_not_found_without_generating():
    llm = FakeLLM()
    result = ask("dress code", llm=llm, min_score=1)
    assert result.text == NOT_FOUND and llm.prompts == []


def test_a_citation_of_a_source_that_was_not_sent_fails_the_check():
    assert not ask(llm=FakeLLM("It is 30 days [S9].")).citations_ok


def test_an_answer_without_any_citation_fails_the_check():
    assert not ask(llm=FakeLLM("It is 30 days.")).citations_ok


def test_a_follow_up_is_rewritten_first_and_the_search_uses_the_rewrite():
    llm = FakeLLM("How long is parental leave for contractors? ", "8 weeks [S1].")
    keyword = FakeSearch([C])
    result = ask("and for contractors?", llm=llm, history="Q: How long is parental leave?",
                 keyword=keyword)
    assert "Rewrite the last question" in llm.prompts[0]
    assert "Q: and for contractors?" in llm.prompts[0]
    assert keyword.calls[0][0] == "How long is parental leave for contractors?"
    assert "Question: How long is parental leave for contractors?" in llm.prompts[1]
    assert "rewrite" in result.timings


def test_without_history_there_is_no_rewrite_call():
    llm = FakeLLM("30 days [S1].")
    result = ask(llm=llm)
    assert len(llm.prompts) == 1 and "rewrite" not in result.timings


def test_the_two_searches_run_at_once_so_retrieval_costs_the_slower_one():
    result = ask(keyword=FakeSearch([A], delay=0.20), dense=FakeSearch([B], delay=0.30))
    t = result.timings
    assert t["keyword"] >= 0.20 and t["dense"] >= 0.30
    assert 0.30 <= t["retrieve"] < 0.45   # not 0.20 + 0.30
    assert set(t) == {"keyword", "dense", "retrieve", "rerank", "generate"}


def test_a_slow_reranker_is_skipped_and_the_fused_order_is_used():
    def slow_rerank(question, texts):
        time.sleep(0.5)
        return [0.0] * len(texts)

    started = time.perf_counter()
    result = ask(rerank=slow_rerank, rerank_timeout=0.1, min_score=1)
    assert time.perf_counter() - started < 0.4          # did not wait for the reranker
    assert result.degraded == ["rerank"]
    assert [c.id for c in result.sources] == ["refund#1", "refund#2", "leave#0"]   # RRF order
    assert result.text == "30 days [S1]." and result.citations_ok
    assert 0.1 <= result.timings["rerank"] < 0.3


def test_a_reranker_that_raises_is_skipped_too():
    def broken_rerank(question, texts):
        raise ConnectionError("reranker is down")

    result = ask(rerank=broken_rerank)
    assert result.degraded == ["rerank"] and result.citations_ok


class DictCache:
    def __init__(self) -> None:
        self.data: dict[str, Answer] = {}

    def get(self, key: str) -> Answer | None:
        return self.data.get(key)

    def set(self, key: str, answer: Answer) -> None:
        self.data[key] = answer


def test_the_same_scope_and_question_is_served_from_the_cache():
    cache, llm = DictCache(), FakeLLM("30 days [S1].")
    first = ask(llm=llm, cache=cache)
    second = ask("What is  the LIMIT", llm=llm, cache=cache)   # same after normalising
    assert second is first and len(llm.prompts) == 1


def test_a_user_with_other_groups_never_gets_a_cached_answer():
    cache = DictCache()
    ask(cache=cache)
    hr = Scope("acme", ("all-staff", "hr"))
    llm = FakeLLM("30 days [S1].")
    answer("what is the limit", "", hr, llm, FakeSearch([A]), FakeSearch([B]), overlap_rerank,
           cache=cache)
    assert len(llm.prompts) == 1 and len(cache.data) == 2


def test_the_cache_key_covers_tenant_groups_and_question():
    key = cache_key(SCOPE, "What is the limit?")
    assert key == cache_key(Scope("acme", ("all-staff",)), "  what is   THE limit? ")
    assert key != cache_key(Scope("globex", ("all-staff",)), "What is the limit?")
    assert key != cache_key(Scope("acme", ("all-staff", "hr")), "What is the limit?")
    assert key != cache_key(SCOPE, "What is the limit for EU customers?")
    # the order the identity provider lists the groups in doesn't matter
    assert cache_key(Scope("acme", ("hr", "all-staff")), "q") == cache_key(
        Scope("acme", ("all-staff", "hr")), "q")


def test_answers_that_failed_the_citation_check_or_were_degraded_are_not_cached():
    cache = DictCache()
    ask(llm=FakeLLM("30 days."), cache=cache)                      # no citation
    ask(rerank=lambda q, texts: 1 / 0, cache=cache)                # degraded
    ask(llm=FakeLLM("NO_ANSWER"), cache=cache)                     # refused
    assert cache.data == {}


def test_a_hung_reranker_never_blocks_the_searches_of_later_requests():
    """A timeout stops the wait, not the call. The hung calls pile up in the reranker's own
    pool, so the searches, which use the other pool, keep running for every request."""
    hang = threading.Event()

    def hung_rerank(question, texts):
        hang.wait(30)
        return [0.0] * len(texts)

    started = time.perf_counter()
    try:
        results = [ask(rerank=hung_rerank, rerank_timeout=0.02) for _ in range(40)]
    finally:
        hang.set()   # let the stuck threads finish
    assert time.perf_counter() - started < 5   # 40 requests, far more than either pool's size
    assert all(r.degraded == ["rerank"] and r.citations_ok for r in results)
    assert all({"keyword", "dense", "retrieve"} <= set(r.timings) for r in results)
