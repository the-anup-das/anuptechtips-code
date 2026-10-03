"""The route table: every alias maps to an ordered chain of priced, configured targets."""
from config import demo_keys, demo_providers
from routes import PRICES, ROUTES, Target, cost_micro_usd


def test_every_target_has_a_price_and_a_provider():
    providers = demo_providers()
    for alias, route in ROUTES.items():
        assert route.chain, alias
        for target in route.chain:
            assert target.name in PRICES
            assert target.provider in providers
            assert 0 < target.first_token_s <= target.timeout_s


def test_the_chain_is_ordered_primary_first():
    chain = ROUTES["support-reply"].chain
    assert [t.name for t in chain] == ["alpha/alpha-large", "beta/beta-large"]
    # A fallback is a different provider: the same outage can't take both.
    assert len({t.provider for t in chain}) == 2


def test_every_virtual_key_only_names_routes_that_exist():
    for caller in demo_keys().values():
        assert caller.routes <= set(ROUTES)
        assert caller.levels() == ["org", f"team:{caller.team}", f"tenant:{caller.tenant}",
                                   f"key:{caller.key_id}"]


def test_cost_is_dollars_per_million_tokens_in_micro_dollars():
    large = ROUTES["support-reply"].chain[0]  # alpha-large: $3.00 in, $15.00 out per million
    assert cost_micro_usd(large, 1000, 500) == 3_000 + 7_500
    assert cost_micro_usd(large, 1_000_000, 0) == 3_000_000  # a million input tokens: $3


def test_the_worst_case_is_input_plus_every_token_allowed():
    large = ROUTES["support-reply"].chain[0]
    assert cost_micro_usd(large, 7, large.max_tokens) == 7 * 3 + 1024 * 15


def test_fractions_of_a_micro_dollar_round_up():
    small = Target("beta", "beta-small", 5, 2, 64)  # $0.25 in, $1.25 out per million
    assert cost_micro_usd(small, 1, 0) == 1         # 0.25 rounds up
    assert cost_micro_usd(small, 4, 0) == 1         # exactly 1.0 stays 1
    assert cost_micro_usd(small, 5, 8) == 12        # 1.25 + 10 = 11.25
