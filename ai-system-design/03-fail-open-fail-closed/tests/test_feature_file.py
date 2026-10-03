"""validate_feature_file: the three checks, and what the unchecked path does instead."""
import pytest

from feature_file import MAX_FEATURES, bot_score, validate_feature_file
from proxy import BOT, HUMAN


def names(count: int) -> list[str]:
    return [f"feature_{i:02d}" for i in range(1, count + 1)]


def test_a_good_file_has_no_problems():
    assert validate_feature_file(names(60)) == []
    assert validate_feature_file(names(60), previous=names(60)) == []


def test_duplicates_are_rejected():
    assert validate_feature_file(names(30) * 2) == ["30 duplicate feature names"]


def test_120_features_do_not_fit_in_100_slots():
    assert MAX_FEATURES == 100
    assert validate_feature_file([f"f{i}" for i in range(120)]) == [
        "120 features, but room for 100"]


def test_doubling_is_rejected_even_when_it_still_fits():
    problems = validate_feature_file([f"f{i}" for i in range(80)], previous=names(40))
    assert problems == ["grew from 40 to 80 features in one version"]


def test_growth_within_50_percent_is_fine():
    assert validate_feature_file(names(90), previous=names(60)) == []


def test_an_empty_file_is_rejected():
    assert validate_feature_file([]) == ["the file is empty"]


def test_the_doubled_file_fails_all_three_checks(files):
    good, bad = files
    assert validate_feature_file(bad, previous=good) == [
        "60 duplicate feature names",
        "120 features, but room for 100",
        "grew from 60 to 120 features in one version",
    ]


def test_scores_with_a_good_file(files):
    good, _ = files
    assert bot_score(good, HUMAN) == 89
    assert bot_score(good, BOT) == 11


def test_the_unchecked_path_crashes_on_the_doubled_file(files):
    _, bad = files
    with pytest.raises(IndexError, match="list assignment index out of range"):
        bot_score(bad, HUMAN)
