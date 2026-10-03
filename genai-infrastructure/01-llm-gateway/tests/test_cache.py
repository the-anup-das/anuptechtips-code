"""Exact-match caching: what goes into the key, and what a hit costs (nothing)."""
from cache import cache_key
from helpers import ACME, GLOBEX, ROUTES, ask, ask_stream, body, calls

CLASSIFY = ROUTES["classify"]


def test_the_second_identical_request_is_served_from_the_cache(gateway, mock_url):
    gw = gateway()
    first = ask(gw.url, body("classify"))
    second = ask(gw.url, body("classify"))
    assert first.json() == second.json()
    assert second.headers["x-llmgw-cache"] == "hit"
    assert calls(mock_url) == {"alpha": 0, "beta": 1}  # one model call for two answers
    assert [(r["cache"], r["cost_micro_usd"]) for r in gw.records] == [("miss", 12), ("hit", 0)]


def test_another_tenant_never_gets_this_tenants_answer(gateway, mock_url):
    gw = gateway()
    ask(gw.url, body("classify"), headers=ACME)
    other = ask(gw.url, body("classify"), headers=GLOBEX)
    assert "x-llmgw-cache" not in other.headers
    assert calls(mock_url)["beta"] == 2


def test_a_changed_system_prompt_is_a_different_request(gateway, mock_url):
    gw = gateway()
    v1 = body("classify", messages=[{"role": "system", "content": "Label as spam or ham"},
                                    {"role": "user", "content": "win a prize"}])
    v2 = body("classify", messages=[{"role": "system", "content": "Label as spam, ham or promo"},
                                    {"role": "user", "content": "win a prize"}])
    ask(gw.url, v1)
    assert "x-llmgw-cache" not in ask(gw.url, v2).headers  # the fix isn't served the old answer
    assert ask(gw.url, v1).headers["x-llmgw-cache"] == "hit"


def test_routes_cache_only_when_they_say_so(gateway, mock_url):
    gw = gateway()
    ask(gw.url, body("support-reply"))
    assert "x-llmgw-cache" not in ask(gw.url, body("support-reply")).headers
    assert calls(mock_url)["alpha"] == 2


def test_streams_are_not_cached(gateway, mock_url):
    gw = gateway()
    ask_stream(gw.url, body("classify"))
    ask_stream(gw.url, body("classify"))
    assert calls(mock_url)["beta"] == 2


def test_the_key_covers_everything_that_changes_the_answer():
    base = body("classify", temperature=0)
    key = cache_key("acme", CLASSIFY, base)
    assert key == cache_key("acme", CLASSIFY, {**base, "stream": False})  # transport only
    assert key != cache_key("globex", CLASSIFY, base)                     # the tenant
    assert key != cache_key("acme", CLASSIFY, {**base, "temperature": 1})  # a parameter
    assert key != cache_key("acme", CLASSIFY, {**base, "tools": [{"type": "function"}]})
    assert key != cache_key("acme", ROUTES["support-reply"], base)        # the models behind it
    assert key.startswith("cache:acme:")
