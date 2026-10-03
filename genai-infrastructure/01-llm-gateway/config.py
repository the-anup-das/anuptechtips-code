"""Control-plane data for the demo: provider credentials, virtual keys and settings.
A real gateway keeps these in a database behind an admin API. Here it's one module."""
import hashlib
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Provider:
    base_url: str
    api_key: str  # the real credential: it never leaves the gateway


@dataclass(frozen=True)
class Caller:
    """Who a virtual key belongs to. Every usage record and budget hangs off this."""
    key_id: str
    org: str
    team: str
    tenant: str
    feature: str
    routes: frozenset[str]  # the route aliases this key may call

    def levels(self) -> list[str]:
        """The budget hierarchy, widest first: org, team, tenant, key."""
        return ["org", f"team:{self.team}", f"tenant:{self.tenant}", f"key:{self.key_id}"]


def key_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def demo_keys() -> dict[str, Caller]:
    """Virtual keys, stored by hash like any other password. Demo values only."""
    callers = {
        "vk-demo-refunds-acme": Caller("vk_refunds", "demo", "support", "acme", "refund-summary",
                                       frozenset({"support-reply", "classify", "summarize"})),
        "vk-demo-search-globex": Caller("vk_search", "demo", "search", "globex", "query-tagging",
                                        frozenset({"classify"})),
    }
    return {key_hash(token): caller for token, caller in callers.items()}


def demo_providers() -> dict[str, Provider]:
    mock = os.environ.get("LLMGW_MOCK_URL", "http://127.0.0.1:8001")
    return {
        "alpha": Provider(os.environ.get("LLMGW_ALPHA_URL", f"{mock}/alpha"),
                          os.environ.get("ALPHA_API_KEY", "sk-mock-alpha")),
        "beta": Provider(os.environ.get("LLMGW_BETA_URL", f"{mock}/beta"),
                         os.environ.get("BETA_API_KEY", "sk-mock-beta")),
    }


def _flag(name: str, default: str) -> bool:
    return os.environ.get(name, default).lower() in ("1", "on", "true", "yes")


@dataclass
class Settings:
    redis_url: str = "redis://localhost:56379/11"
    redis_pool: int = 64              # connections to Redis; a burst beyond it waits
    providers: dict[str, Provider] = field(default_factory=demo_providers)
    keys: dict[str, Caller] = field(default_factory=demo_keys)
    budget: str = "lua"               # "lua" (reserve, then settle), "naive" or "off"
    breaker: bool = True
    first_token_timeout: bool = True  # off: a stream waits out the route's whole timeout_s
    idle_gap_s: float = 10.0          # the longest silence allowed between two stream chunks
    deadline_s: float = 60.0          # one deadline across every attempt and backoff
    attempts: int = 3                 # tries per target before moving down the chain
    backoff_base_s: float = 0.5
    backoff_cap_s: float = 8.0
    max_wait_s: float = 2.0           # the longest Retry-After worth waiting out in-request
    breaker_open_s: float = 10.0
    per_tenant: int = 32              # in-flight requests per tenant, per gateway process
    per_provider: int = 64            # in-flight calls per provider, per gateway process
    queue_wait_s: float = 0.2         # how long a call may wait for a provider slot
    retry_hint: bool = True           # answer final errors with "x-should-retry: false"

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ
        return cls(
            redis_url=env.get("LLMGW_REDIS_URL", cls.redis_url),
            budget=env.get("LLMGW_BUDGET", cls.budget),
            breaker=_flag("LLMGW_BREAKER", "on"),
            first_token_timeout=_flag("LLMGW_FIRST_TOKEN_TIMEOUT", "on"),
            retry_hint=_flag("LLMGW_RETRY_HINT", "on"),
            attempts=int(env.get("LLMGW_ATTEMPTS", cls.attempts)),
            backoff_base_s=float(env.get("LLMGW_BACKOFF_BASE_S", cls.backoff_base_s)),
            breaker_open_s=float(env.get("LLMGW_BREAKER_OPEN_S", cls.breaker_open_s)),
            per_tenant=int(env.get("LLMGW_PER_TENANT", cls.per_tenant)),
            per_provider=int(env.get("LLMGW_PER_PROVIDER", cls.per_provider)),
        )
