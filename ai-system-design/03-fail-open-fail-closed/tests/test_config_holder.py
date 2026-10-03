"""The five outcomes, one holder at a time: crash, fail wrong, fail stale, fail open and
fail closed."""
import logging

import pytest

from config_holder import ConfigHolder, NaiveHolder, ScoreZeroHolder
from proxy import BOT, HUMAN, handle


def test_a_bad_second_file_keeps_the_first(files, caplog):
    good, bad = files
    holder = ConfigHolder()
    assert holder.apply(1, good) is True
    with caplog.at_level(logging.WARNING, logger="config"):
        assert holder.apply(2, bad) is False
    assert (holder.version, holder.seen, holder.rejected) == (1, 2, 1)
    assert holder.score(HUMAN) == 89                      # fail stale: still scoring with v1
    assert "feature file v2 rejected, still serving v1" in caplog.text


def test_a_bad_first_file_gives_none_not_zero(files):
    _, bad = files
    holder = ConfigHolder()
    assert holder.apply(1, bad) is False
    assert holder.names is None
    assert holder.score(HUMAN) is None and holder.score(BOT) is None


def test_replays_and_old_versions_are_ignored(files):
    good, bad = files
    holder = ConfigHolder()
    holder.apply(1, good)
    holder.apply(2, bad)
    assert holder.apply(2, bad) is False                  # the bad file again: not looked at
    assert holder.rejected == 1                           # so it isn't counted or alerted twice
    assert holder.apply(1, good) is False                 # an old message can't roll us back
    assert holder.version == 1


def test_a_good_new_version_replaces_the_old_one(files):
    good, _ = files
    holder = ConfigHolder()
    holder.apply(1, good)
    first_applied = holder.applied_at
    assert holder.apply(2, good[:50]) is True
    assert (holder.version, len(holder.names)) == (2, 50)
    assert holder.applied_at >= first_applied


def test_a_rollback_is_just_the_next_version(files):
    good, bad = files
    holder = ConfigHolder()
    holder.apply(1, good)
    holder.apply(2, bad)
    assert holder.apply(3, good) is True                  # the last good file, published again
    assert (holder.version, holder.seen) == (3, 3)


def test_the_naive_holder_crashes_on_every_request(files):
    good, bad = files
    holder = NaiveHolder()
    holder.apply(1, good)
    assert holder.score(HUMAN) == 89
    holder.apply(2, bad)
    with pytest.raises(IndexError):
        holder.score(HUMAN)
    assert handle(holder, HUMAN) == 500                   # crash
    holder.apply(3, good)                                 # and recovers when a good file arrives
    assert handle(holder, HUMAN) == 200


def test_the_score_zero_holder_blocks_humans_without_a_single_error(files):
    good, bad = files
    holder = ScoreZeroHolder()
    holder.apply(1, good)
    holder.apply(2, bad)
    assert holder.score(HUMAN) == 0
    assert handle(holder, HUMAN) == 403                   # fail wrong: a real user, blocked


def test_statuses_with_a_good_file(files):
    good, _ = files
    holder = ConfigHolder()
    holder.apply(1, good)
    assert handle(holder, HUMAN) == 200
    assert handle(holder, BOT) == 403


def test_unknown_is_open_or_closed_by_the_choice_made_in_advance(files):
    _, bad = files
    holder = ConfigHolder()
    holder.apply(1, bad)                                  # no good file has ever arrived
    assert handle(holder, HUMAN, on_unknown="open") == 200
    assert handle(holder, BOT, on_unknown="open") == 200  # the price: bots get through unscored
    assert handle(holder, HUMAN, on_unknown="closed") == 503
