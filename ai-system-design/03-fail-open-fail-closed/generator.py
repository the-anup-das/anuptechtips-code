"""The feature-file generator: one feature per column of the features table, read from the
database's own metadata. UNFILTERED is the shape of the query that doubled the file."""
import psycopg
from psycopg import sql

from feature_file import validate_feature_file

# Instead of this: every table with that name, in every schema the role can see
UNFILTERED = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'http_requests_features'
ORDER BY column_name
"""

# Use this: name the schema too
FILTERED = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'http_requests_features'
  AND table_schema = 'public'
ORDER BY column_name
"""


class BadFeatureFile(ValueError):
    """The producer's own check failed, so the file is never published."""


def feature_names(conn: psycopg.Connection, role: str, query: str = UNFILTERED) -> list[str]:
    """Run the generator's query as `role` and return one feature name per row."""
    with conn.transaction():
        conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
        return [name for name, _type in conn.execute(query)]


def checked_feature_names(conn: psycopg.Connection, role: str, previous: list[str] | None,
                          query: str = UNFILTERED) -> list[str]:
    """The producer's half of "validate it twice": the same check the consumers run,
    against the consumers' limit, before anything is published."""
    names = feature_names(conn, role, query)
    problems = validate_feature_file(names, previous)
    if problems:
        raise BadFeatureFile("; ".join(problems))
    return names


def grant_r0(conn: psycopg.Connection, role: str, privilege: str = "SELECT") -> None:
    """The permission change: make the role's access to the underlying table explicit."""
    conn.execute(sql.SQL("GRANT {} ON r0.http_requests_features TO {}").format(
        sql.SQL(privilege), sql.Identifier(role)))


def revoke_r0(conn: psycopg.Connection, role: str) -> None:
    conn.execute(sql.SQL("REVOKE ALL ON r0.http_requests_features FROM {}").format(
        sql.Identifier(role)))
