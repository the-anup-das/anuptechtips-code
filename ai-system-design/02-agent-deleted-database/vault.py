"""Backups outside the blast radius. A role that can only read production copies each table
into a directory that no database role can touch, and restore() proves the copy comes back.
pg_dump writes the same COPY stream; this version needs no Postgres client tools."""
import json
import pathlib

from psycopg import sql

import lab


def backup(table: str, directory: pathlib.Path) -> int:
    """Copy prod.<table> to <directory>/<table>.copy as the vault role. Returns the rows."""
    directory.mkdir(parents=True, exist_ok=True)
    source = sql.Identifier("prod", table)
    with lab.connect(lab.VAULT) as conn, open(directory / f"{table}.copy", "wb") as out:
        cur = conn.cursor()
        with cur.copy(sql.SQL("COPY {} TO STDOUT (FORMAT binary)").format(source)) as copy:
            for block in copy:
                out.write(block)
        rows = cur.rowcount
    (directory / f"{table}.json").write_text(json.dumps({"table": table, "rows": rows}))
    return rows


def restore(table: str, directory: pathlib.Path, schema: str = "prod") -> int:
    """Rebuild <schema>.<table> from the vault copy. Raises if the row count is off."""
    expected = json.loads((directory / f"{table}.json").read_text())["rows"]
    target = sql.Identifier(schema, table)
    with lab.connect(lab.OWNER) as conn, conn.transaction():
        lab.create_table(conn, schema, table, primary_key=False)
        cur = conn.cursor()
        with open(directory / f"{table}.copy", "rb") as dump, \
                cur.copy(sql.SQL("COPY {} FROM STDIN (FORMAT binary)").format(target)) as copy:
            while block := dump.read(1 << 20):
                copy.write(block)
        conn.execute(sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (id)").format(target))
        if cur.rowcount != expected:
            raise RuntimeError(f"{table}: restored {cur.rowcount} rows, backed up {expected}")
    return expected


def restore_test(table: str, directory: pathlib.Path) -> int:
    """The nightly check: restore into a scratch schema, count, clean up. Production
    isn't touched, and a backup that can't be restored fails here, not during an outage."""
    with lab.connect(lab.OWNER) as conn:
        conn.execute("DROP SCHEMA IF EXISTS restore_check CASCADE")
        conn.execute("CREATE SCHEMA restore_check")
    try:
        return restore(table, directory, schema="restore_check")
    finally:
        with lab.connect(lab.OWNER) as conn:
            conn.execute("DROP SCHEMA restore_check CASCADE")
