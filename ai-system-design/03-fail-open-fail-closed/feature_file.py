"""The consumer's side of the feature file: a preallocated buffer with a hard limit, the
scoring code that crashes past it, and the check that should run before any file is used."""

MAX_FEATURES = 100   # slots a proxy preallocates per request (Cloudflare's limit was 200)
MAX_GROWTH = 1.5     # a new file may hold at most 50% more features than the one it replaces


# Instead of this: the request path is where the proxy finds out the file is too big
def bot_score(names: list[str], request: dict[str, float]) -> int:
    """1 = surely a bot, 99 = surely human. A stand-in for the ML model: it copies the
    request's feature values into the preallocated slots and averages them."""
    slots = [0.0] * MAX_FEATURES
    for i, name in enumerate(names):
        slots[i] = request[name]  # feature 101 raises IndexError: Python's unwrap() panic
    return 1 + round(98 * sum(slots) / len(names))


# Use this: check the file once, when it arrives, like user input and against your own limits
def validate_feature_file(names: list[str], previous: list[str] | None = None,
                          limit: int = MAX_FEATURES, max_growth: float = MAX_GROWTH) -> list[str]:
    """Return the reasons to reject the file. An empty list means it is safe to load."""
    problems = []
    if not names:
        problems.append("the file is empty")
    duplicates = len(names) - len(set(names))
    if duplicates:
        problems.append(f"{duplicates} duplicate feature names")
    if len(names) > limit:
        problems.append(f"{len(names)} features, but room for {limit}")
    if previous and len(names) > len(previous) * max_growth:
        problems.append(f"grew from {len(previous)} to {len(names)} features in one version")
    return problems
