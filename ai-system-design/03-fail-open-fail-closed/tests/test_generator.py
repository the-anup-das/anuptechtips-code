"""The duplicate-metadata bug, rebuilt on PostgreSQL: which grants change what the
generator's query returns, and what the schema filter and the producer's check do about it."""
import psycopg
import pytest

from generator import (FILTERED, UNFILTERED, BadFeatureFile, checked_feature_names,
                       feature_names, grant_r0)


def test_before_the_grant_the_query_returns_the_60_features(pg):
    names = feature_names(pg, "feature_gen")
    assert len(names) == 60 and len(set(names)) == 60


def test_a_grant_on_the_underlying_table_doubles_the_rows(pg):
    grant_r0(pg, "feature_gen")
    names = feature_names(pg, "feature_gen")
    assert len(names) == 120
    assert len(set(names)) == 60                  # every feature name now appears twice
    assert names[:2] == ["feature_01", "feature_01"]


def test_the_schema_filter_keeps_60_rows_after_the_grant(pg):
    grant_r0(pg, "feature_gen")
    assert len(feature_names(pg, "feature_gen", FILTERED)) == 60


def test_usage_on_the_schema_alone_changes_nothing(pg):
    pg.execute("GRANT USAGE ON SCHEMA r0 TO feature_gen")
    assert len(feature_names(pg, "feature_gen")) == 60


def test_the_role_sees_the_metadata_without_being_able_to_read_the_table(pg):
    grant_r0(pg, "feature_gen")                   # SELECT on the table, no USAGE on schema r0
    assert len(feature_names(pg, "feature_gen")) == 120
    with pytest.raises(psycopg.errors.InsufficientPrivilege), pg.transaction():
        pg.execute("SET LOCAL ROLE feature_gen")
        pg.execute("SELECT count(*) FROM r0.http_requests_features")


@pytest.mark.parametrize("privilege, rows", [
    ("SELECT", 120), ("INSERT", 120), ("UPDATE", 120), ("REFERENCES", 120),
    ("DELETE", 60), ("TRUNCATE", 60), ("TRIGGER", 60),
])
def test_which_privileges_make_the_columns_visible(pg, privilege, rows):
    grant_r0(pg, "feature_gen", privilege)
    assert len(feature_names(pg, "feature_gen")) == rows


def test_a_grant_on_two_columns_adds_two_rows(pg):
    pg.execute("GRANT SELECT (feature_01, feature_02) ON r0.http_requests_features "
               "TO feature_gen")
    assert len(feature_names(pg, "feature_gen")) == 62
    pg.execute("REVOKE SELECT (feature_01, feature_02) ON r0.http_requests_features "
               "FROM feature_gen")


def test_each_node_role_flips_only_when_it_gets_the_grant(pg):
    grant_r0(pg, "feature_node_2")
    rows = [len(feature_names(pg, f"feature_node_{n}")) for n in (1, 2, 3, 4)]
    assert rows == [60, 120, 60, 60]


def test_the_producers_check_refuses_to_publish_the_doubled_file(pg):
    good = checked_feature_names(pg, "feature_gen", previous=None)
    assert len(good) == 60
    grant_r0(pg, "feature_gen")
    with pytest.raises(BadFeatureFile, match="60 duplicate feature names"):
        checked_feature_names(pg, "feature_gen", previous=good)
    assert checked_feature_names(pg, "feature_gen", good, FILTERED) == good
    assert UNFILTERED != FILTERED
