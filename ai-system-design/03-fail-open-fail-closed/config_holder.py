"""What a proxy does with each feature file that arrives. ConfigHolder is the hardened
version: a bad file never replaces a good one, and "no usable config" is None, never 0."""
import logging
import time

from feature_file import MAX_FEATURES, bot_score, validate_feature_file

log = logging.getLogger("config")


class ConfigHolder:
    def __init__(self, limit: int = MAX_FEATURES):
        self.limit = limit
        self.version = 0                       # the version being served; 0 = none yet
        self.names: list[str] | None = None
        self.seen = 0                          # the newest version looked at, good or bad
        self.rejected = 0                      # alert on this...
        self.applied_at = time.monotonic()     # ...and on how old the served file is

    def apply(self, version: int, names: list[str]) -> bool:
        if version <= self.seen:               # a replay or an old message: nothing to do
            return False
        self.seen = version
        problems = validate_feature_file(names, self.names, self.limit)
        if problems:                           # fail stale: keep serving what we have
            self.rejected += 1
            log.warning("feature file v%s rejected, still serving v%s: %s",
                        version, self.version, "; ".join(problems))
            return False
        self.version, self.names, self.applied_at = version, names, time.monotonic()
        return True

    def score(self, request: dict[str, float]) -> int | None:
        if self.names is None:
            return None                        # unknown. Never 0: a 0 reads as "surely a bot"
        return bot_score(self.names, request)


class NaiveHolder:
    """The consumer that crashes: it swaps in whatever arrives and scores with it."""

    def __init__(self):
        self.version, self.names, self.seen, self.rejected = 0, None, 0, 0

    def apply(self, version: int, names: list[str]) -> bool:
        self.version = self.seen = version
        self.names = names
        return True

    def score(self, request: dict[str, float]) -> int | None:
        return bot_score(self.names, request)  # IndexError on a file over the limit


class ScoreZeroHolder(NaiveHolder):
    """The consumer that fails wrong: no crash, but every request gets a score of 0."""

    def score(self, request: dict[str, float]) -> int | None:
        try:
            return bot_score(self.names, request)
        except IndexError:
            return 0
