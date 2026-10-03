"""Least privilege: what each credential can and can't do. Postgres does the refusing."""
import psycopg
import pytest
from psycopg.errors import InsufficientPrivilege

import lab

pytestmark = pytest.mark.usefixtures("db")


def test_no_lab_role_is_a_superuser_or_can_create_roles_or_databases(connect):
    rows = connect().execute(
        "SELECT rolname, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles "
        "WHERE rolname = ANY(%s)", (list(lab.ROLES),)).fetchall()
    assert sorted(name for name, *_ in rows) == sorted(lab.ROLES)
    assert not any(flag for _, *flags in rows for flag in flags)


def test_the_lab_refuses_to_connect_to_another_database():
    with pytest.raises(RuntimeError, match="refusing to work in database 'patterns'"):
        lab.connect(lab.ADMIN_DSN)


def test_agent_reads_production(connect):
    assert connect(lab.AGENT).execute("SELECT count(*) FROM prod.executives").fetchone() == (1206,)


@pytest.mark.parametrize("statement, message", [
    ("DROP TABLE prod.executives", "must be owner of table executives"),
    ("DROP TABLE prod.executives, prod.companies", "must be owner of table executives"),
    ("ALTER TABLE prod.executives ADD COLUMN note text", "must be owner of table executives"),
    ("TRUNCATE prod.executives", "permission denied for table executives"),
    ("DELETE FROM prod.executives", "permission denied for table executives"),
    ("UPDATE prod.executives SET name = 'x'", "permission denied for table executives"),
    ("INSERT INTO prod.companies VALUES (9999, 'x')", "permission denied for table companies"),
    ("CREATE TABLE prod.scratch (id int)", "permission denied for schema prod"),
    ("DROP SCHEMA prod CASCADE", "must be owner of schema prod"),
])
def test_agent_cannot_change_production(connect, statement, message):
    with pytest.raises(InsufficientPrivilege, match=message):
        connect(lab.AGENT).execute(statement)
    assert lab.original_rows(connect(), "executives") == 1206
    assert lab.original_rows(connect(), "companies") == 1196


def test_a_drop_hidden_behind_a_select_is_still_refused(connect):
    """The gate would classify this as a read. The read-only credential stops it anyway."""
    with pytest.raises(InsufficientPrivilege, match="must be owner of table executives"):
        connect(lab.AGENT).execute("SELECT 1; DROP TABLE prod.executives")
    assert lab.original_rows(connect(), "executives") == 1206


def test_agent_works_on_rows_in_staging(connect):
    agent = connect(lab.AGENT)
    assert agent.execute("DELETE FROM staging.inbox WHERE id <= 3").rowcount == 3
    assert agent.execute("UPDATE staging.inbox SET subject = 'x' WHERE id = 4").rowcount == 1
    agent.execute("INSERT INTO staging.inbox VALUES (1, 'a@example.com', 'again')")
    assert agent.execute("SELECT count(*) FROM staging.inbox").fetchone() == (198,)


@pytest.mark.parametrize("statement, message", [
    ("DROP TABLE staging.inbox", "must be owner of table inbox"),
    ("TRUNCATE staging.inbox", "permission denied for table inbox"),
    ("CREATE TABLE staging.scratch (id int)", "permission denied for schema staging"),
])
def test_agent_owns_nothing_even_in_staging(connect, statement, message):
    with pytest.raises(InsufficientPrivilege, match=message):
        connect(lab.AGENT).execute(statement)


def test_tables_created_later_get_the_same_grants(connect):
    """ALTER DEFAULT PRIVILEGES covers what GRANT ... ON ALL TABLES can't: new tables."""
    lab.create_table(connect(), "prod", "reservations")
    agent = connect(lab.AGENT)
    assert agent.execute("SELECT count(*) FROM prod.reservations").fetchone() == (0,)
    with pytest.raises(InsufficientPrivilege):
        agent.execute("INSERT INTO prod.reservations VALUES (1, 1, 'car-1', 2)")


def test_agent_cannot_use_the_soft_delete_functions_or_see_the_trash(connect):
    agent = connect(lab.AGENT)
    for statement in ("SELECT ops.soft_drop('executives')", "SELECT ops.purge()",
                      "SELECT * FROM trash.manifest"):
        with pytest.raises(InsufficientPrivilege, match="permission denied for schema"):
            agent.execute(statement)


def test_operator_changes_rows_but_cannot_run_ddl(connect):
    operator = connect(lab.OPERATOR)
    assert operator.execute("DELETE FROM prod.executives WHERE id = 1").rowcount == 1
    for statement, message in (("DROP TABLE prod.executives", "must be owner of table"),
                               ("TRUNCATE prod.executives", "permission denied for table"),
                               ("SELECT ops.purge()", "permission denied for function purge")):
        with pytest.raises(InsufficientPrivilege, match=message):
            operator.execute(statement)


def test_vault_role_reads_production_and_writes_nothing(connect):
    backup_job = connect(lab.VAULT)
    assert backup_job.execute("SELECT count(*) FROM prod.companies").fetchone() == (1196,)
    for statement in ("DELETE FROM prod.companies", "DROP TABLE prod.companies",
                      "SELECT count(*) FROM staging.inbox"):
        with pytest.raises(InsufficientPrivilege):
            backup_job.execute(statement)


def test_a_token_for_custom_domains_can_only_manage_domains(connect):
    token = connect(lab.DOMAINS)
    token.execute("INSERT INTO prod.domains VALUES (99, 'new.example.com')")
    assert token.execute("DELETE FROM prod.domains WHERE id = 99").rowcount == 1
    for statement, message in (
            ("SELECT count(*) FROM prod.executives", "permission denied for table executives"),
            ("DROP TABLE prod.executives, prod.companies", "must be owner of table executives"),
            ("DROP TABLE prod.domains", "must be owner of table domains")):
        with pytest.raises(InsufficientPrivilege, match=message):
            token.execute(statement)


def test_a_role_past_its_valid_until_can_no_longer_log_in():
    """Scoped should also mean short-lived: Postgres can put an end date on a credential."""
    with lab._admin() as admin:
        admin.execute("ALTER ROLE agentdel_domains VALID UNTIL '2020-01-01'")
    try:
        with pytest.raises(psycopg.OperationalError, match="password authentication failed"):
            lab.connect(lab.DOMAINS)
    finally:
        with lab._admin() as admin:
            admin.execute("ALTER ROLE agentdel_domains VALID UNTIL 'infinity'")
    lab.connect(lab.DOMAINS).close()


def test_the_owner_credential_can_drop_everything(connect):
    """Setup 1 in one line: this is the credential the first setup hands to the agent."""
    connect(lab.OWNER).execute("DROP TABLE prod.executives, prod.companies")
    assert lab.original_rows(connect(), "executives") == 0


def test_every_refusal_is_sqlstate_42501(connect):
    with pytest.raises(psycopg.Error) as refused:
        connect(lab.AGENT).execute("DROP TABLE prod.executives")
    assert refused.value.sqlstate == "42501"
