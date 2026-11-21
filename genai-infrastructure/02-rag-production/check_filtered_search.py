"""Does a filtered vector search on YOUR pgvector table come back short?

With an HNSW index and the default settings, pgvector picks hnsw.ef_search (40) candidates
first and applies your WHERE filter after, so a search for 10 rows under a narrow filter can
return fewer, or none, with no error. This script measures that on your own table, read only.

It takes a sample of vectors from the table itself as queries, runs the same filtered search
three ways and prints how many rows came back:

  default        your current settings (iterative scan off, unless you changed it)
  relaxed_order  SET hnsw.iterative_scan = relaxed_order (pgvector 0.8.0 or later)
  exact          no vector index: filter first, then sort every allowed row

Example, for a user who may read documents of group g01 only:

    python check_filtered_search.py \\
        --dsn postgresql://user:pass@localhost:5432/mydb \\
        --table chunks --column embedding \\
        --where "acl && '{g01}'::text[]" --k 10 --queries 50

--where is plain SQL and goes into the query as written, so only run it on your own database.
The connection is set to read only, and nothing is written.
"""
import argparse
import statistics
import sys

import psycopg
from psycopg import sql

OPERATORS = {"cosine": "<=>", "l2": "<->", "ip": "<#>"}


def search(conn: psycopg.Connection, args: argparse.Namespace, query: str, exact: bool) -> int:
    op = sql.SQL(OPERATORS[args.metric])
    column = sql.Identifier(*args.column.split("."))
    distance = sql.SQL("({col} {op} %(q)s::vector)").format(col=column, op=op)
    # "+ 0" stops the planner from using the vector index for the ORDER BY.
    order = sql.SQL("{d} + 0").format(d=distance) if exact else distance
    statement = sql.SQL("SELECT 1 FROM {table} WHERE {where} ORDER BY {order} LIMIT %(k)s").format(
        table=sql.Identifier(*args.table.split(".")), where=sql.SQL(args.where), order=order)
    return len(conn.execute(statement, {"q": query, "k": args.k}).fetchall())


def run(conn: psycopg.Connection, args: argparse.Namespace, queries: list[str],
        iterative: str | None, exact: bool) -> list[int]:
    with conn.transaction():
        if iterative:
            conn.execute(sql.SQL("SET LOCAL hnsw.iterative_scan = {}").format(
                sql.Literal(iterative)))
        return [search(conn, args, q, exact) for q in queries]


def report(name: str, rows: list[int], k: int) -> str:
    short = sum(r < k for r in rows)
    empty = sum(r == 0 for r in rows)
    return (f"{name:<14} {statistics.mean(rows):>8.2f} {short:>8} {empty:>7}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--table", required=True, help="table, or schema.table")
    parser.add_argument("--column", default="embedding", help="the vector column")
    parser.add_argument("--where", required=True, help="the filter one user's search uses")
    parser.add_argument("--metric", choices=OPERATORS, default="cosine",
                        help="must match the operator class of your HNSW index")
    parser.add_argument("--k", type=int, default=10, help="rows each search asks for")
    parser.add_argument("--queries", type=int, default=50, help="how many sample queries")
    args = parser.parse_args()

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        conn.execute("SET default_transaction_read_only = on")
        table = sql.Identifier(*args.table.split("."))
        column = sql.Identifier(*args.column.split("."))
        queries = [row[0] for row in conn.execute(
            sql.SQL("SELECT {col}::text FROM {table} ORDER BY random() LIMIT %s").format(
                col=column, table=table), (args.queries,))]
        if not queries:
            print("The table is empty.")
            return 1

        allowed = conn.execute(sql.SQL("SELECT count(*) FROM {table} WHERE {where}").format(
            table=table, where=sql.SQL(args.where))).fetchone()[0]
        total = conn.execute(sql.SQL("SELECT count(*) FROM {table}").format(
            table=table)).fetchone()[0]
        settings = {name: conn.execute(f"SHOW {name}").fetchone()[0]
                    for name in ("hnsw.ef_search", "hnsw.iterative_scan")}

        results = {
            "default": run(conn, args, queries, None, exact=False),
            "relaxed_order": run(conn, args, queries, "relaxed_order", exact=False),
            "exact": run(conn, args, queries, None, exact=True),
        }

    print(f"Rows the filter allows: {allowed:,} of {total:,} "
          f"({allowed / total:.1%}). Current settings: {settings}")
    print(f"{len(queries)} searches, each asking for {args.k} rows\n")
    print(f"{'setting':<14} {'avg rows':>8} {'short':>8} {'empty':>7}")
    for name, rows in results.items():
        print(report(name, rows, args.k))

    expected = min(args.k, allowed)
    if statistics.mean(results["default"]) < expected:
        print("\nYour default search comes back short. Set hnsw.iterative_scan = relaxed_order "
              "on the connection, or use the exact search for users who can read very little.")
    else:
        print("\nNo shortfall with your current settings for this filter.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
