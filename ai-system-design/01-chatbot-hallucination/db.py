"""Connection settings, schema setup and the one write the policy owners make.

python db.py   creates the `chatbot` database, the tables and the 40 clauses.
"""
import os
import pathlib

import psycopg

from corpus import CLAUSES

HERE = pathlib.Path(__file__).resolve().parent
ADMIN_DSN = os.environ.get("CHATBOT_ADMIN_DSN",
                           "postgresql://patterns:patterns@localhost:55432/patterns")
DSN = os.environ.get("CHATBOT_DSN", "postgresql://patterns:patterns@localhost:55432/chatbot")
REDIS_URL = os.environ.get("CHATBOT_REDIS_URL", "redis://localhost:56379/6")
KAFKA = os.environ.get("CHATBOT_KAFKA", "localhost:59092")


def create_database() -> None:
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = 'chatbot'").fetchone():
            conn.execute("CREATE DATABASE chatbot")


def reset(conn: psycopg.Connection) -> None:
    """Drop and recreate the tables, then load version 1 of every clause."""
    conn.execute("DROP TABLE IF EXISTS policy_clause, answer_audit, outbox")
    conn.execute((HERE / "schema.sql").read_text())
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO policy_clause (clause_id, version, policy_type, owner, body) "
            "VALUES (%s, 1, %s, %s, %s)",
            [(c.clause_id, c.policy_type, c.owner, c.body) for c in CLAUSES])


def publish_clause(conn: psycopg.Connection, clause_id: str, body: str) -> int:
    """Put a new version of a clause in force. The old version stops applying at the same
    instant, and stays in the table for every audit row that cites it."""
    with conn.transaction():
        version, policy_type, owner = conn.execute(
            "UPDATE policy_clause SET effective = tstzrange(lower(effective), now()) "
            "WHERE clause_id = %s AND upper_inf(effective) "
            "RETURNING version, policy_type, owner", (clause_id,)).fetchone()
        conn.execute(
            "INSERT INTO policy_clause (clause_id, version, policy_type, owner, body, effective) "
            "VALUES (%s, %s, %s, %s, %s, tstzrange(now(), NULL))",
            (clause_id, version + 1, policy_type, owner, body))
    return version + 1


if __name__ == "__main__":
    create_database()
    with psycopg.connect(DSN, autocommit=True) as conn:
        reset(conn)
        print(conn.execute("SELECT count(*) FROM policy_clause").fetchone()[0], "clauses loaded")
