"""Soft delete: a dropped table waits in the trash for 48 hours, restores in one statement,
and only the owner's purge job can remove it for good."""
from datetime import timedelta

import pytest
from psycopg.errors import InsufficientPrivilege, RaiseException, UndefinedTable

import lab

pytestmark = pytest.mark.usefixtures("db")


def test_soft_drop_moves_the_table_with_its_rows_and_records_who_and_when(connect):
    purge_at = connect(lab.OPERATOR).execute("SELECT ops.soft_drop('executives')").fetchone()[0]
    owner = connect()
    assert owner.execute("SELECT to_regclass('prod.executives')").fetchone() == (None,)
    assert lab.original_rows(owner, "executives", schema="trash") == 1206
    name, who, kept = owner.execute(
        "SELECT table_name, dropped_by, purge_after - dropped_at FROM trash.manifest").fetchone()
    assert (name, who, kept) == ("executives", lab.OPERATOR, timedelta(hours=48))
    assert purge_at == owner.execute("SELECT purge_after FROM trash.manifest").fetchone()[0]


def test_restore_brings_back_rows_primary_key_and_grants(connect):
    operator = connect(lab.OPERATOR)
    operator.execute("SELECT ops.soft_drop('executives')")
    operator.execute("SELECT ops.restore('executives')")
    owner = connect()
    assert lab.original_rows(owner, "executives") == 1206
    assert owner.execute("SELECT count(*) FROM trash.manifest").fetchone() == (0,)
    assert owner.execute("SELECT count(*) FROM pg_indexes WHERE schemaname = 'prod' "
                         "AND indexname = 'executives_pkey'").fetchone() == (1,)
    assert connect(lab.AGENT).execute("SELECT count(*) FROM prod.executives").fetchone() == (1206,)


def test_restoring_something_that_is_not_in_the_trash_fails(connect):
    with pytest.raises(RaiseException, match="nothing called executives is in the trash"):
        connect(lab.OPERATOR).execute("SELECT ops.restore('executives')")


def test_soft_dropping_a_table_that_does_not_exist_fails_and_records_nothing(connect):
    with pytest.raises(UndefinedTable):
        connect(lab.OPERATOR).execute("SELECT ops.soft_drop('no_such_table')")
    assert connect().execute("SELECT count(*) FROM trash.manifest").fetchone() == (0,)


def test_a_table_name_cannot_smuggle_sql_into_soft_drop(connect):
    with pytest.raises(UndefinedTable):
        connect(lab.OPERATOR).execute(
            "SELECT ops.soft_drop(%s)", ("executives; DROP TABLE prod.companies",))
    assert lab.original_rows(connect(), "companies") == 1196


def test_purge_leaves_a_table_alone_until_its_48_hours_are_over(connect):
    connect(lab.OPERATOR).execute("SELECT ops.soft_drop('executives')")
    owner = connect()
    assert owner.execute("SELECT ops.purge()").fetchone() == (0,)
    assert lab.original_rows(owner, "executives", schema="trash") == 1206
    owner.execute("UPDATE trash.manifest SET purge_after = now()")  # 48 hours later
    assert owner.execute("SELECT ops.purge()").fetchone() == (1,)
    assert owner.execute("SELECT to_regclass('trash.executives')").fetchone() == (None,)
    assert owner.execute("SELECT count(*) FROM trash.manifest").fetchone() == (0,)


def test_the_operator_cannot_reach_into_the_trash_or_purge_it(connect):
    operator = connect(lab.OPERATOR)
    operator.execute("SELECT ops.soft_drop('executives')")
    for statement, message in (
            ("SELECT ops.purge()", "permission denied for function purge"),
            ("DROP TABLE trash.executives", "permission denied for schema trash"),
            ("DELETE FROM trash.executives", "permission denied for schema trash"),
            ("UPDATE trash.manifest SET purge_after = now()",
             "permission denied for schema trash")):
        with pytest.raises(InsufficientPrivilege, match=message):
            operator.execute(statement)
    assert lab.original_rows(connect(), "executives", schema="trash") == 1206


def test_public_cannot_execute_the_functions(connect):
    """Postgres grants EXECUTE on new functions to PUBLIC unless you revoke it."""
    rows = connect().execute(
        "SELECT p.proname, has_function_privilege('public', p.oid, 'EXECUTE') "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'ops' ORDER BY 1").fetchall()
    assert rows == [("purge", False), ("restore", False), ("soft_drop", False)]
