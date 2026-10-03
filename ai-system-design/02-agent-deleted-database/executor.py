"""Runs what the gate let through, each call with the narrowest credential that fits."""
import re

import psycopg

DROP_TABLE = re.compile(r"\s*drop\s+table\s+(?P<tables>[\w.,\s]+?)\s*;?\s*", re.IGNORECASE)


class PlanExceeded(Exception):
    """The statement touched more rows than the approved plan said it would."""


class Executor:
    def __init__(self, agent: psycopg.Connection, prod_writer: psycopg.Connection,
                 soft_delete: bool):
        self.agent = agent              # prod: SELECT only. staging: rows, no DDL
        self.prod_writer = prod_writer  # the owner, or the operator once deletes are soft
        self.soft_delete = soft_delete

    def __call__(self, env: str, kind: str, sql: str, max_rows: int | None = None):
        if env != "prod" or kind == "read":
            return self.agent.execute(sql)  # the label can lie; this credential can't
        dropped = DROP_TABLE.fullmatch(sql)
        with self.prod_writer.transaction():
            if dropped and self.soft_delete:  # DROP TABLE becomes a move to the trash
                for name in dropped["tables"].split(","):
                    table = name.strip().removeprefix("prod.")
                    self.prod_writer.execute("SELECT ops.soft_drop(%s)", (table,))
                return None
            cur = self.prod_writer.execute(sql)
            if max_rows is not None and cur.rowcount > max_rows:
                # Leaving the block with an exception rolls the statement back.
                raise PlanExceeded(f"{cur.rowcount} rows, the approved plan said {max_rows}")
        return cur
