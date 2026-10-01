"""retention.sql deletes inbox rows past the window and keeps the rest."""
from conftest import ROOT


def test_cleanup_deletes_only_rows_past_the_window(conn):
    conn.execute(
        "INSERT INTO processed_messages (consumer, message_id, processed_at) "
        "SELECT 'wallet-credits', 'old-' || g, now() - interval '15 days' FROM generate_series(1, 25000) g")
    conn.execute(
        "INSERT INTO processed_messages (consumer, message_id, processed_at) "
        "SELECT 'wallet-credits', 'new-' || g, now() - interval '13 days' FROM generate_series(1, 100) g")
    index_sql, delete_sql = (ROOT / "retention.sql").read_text().split(";")[:2]
    conn.execute(index_sql)
    deleted = []
    while True:  # what the cron job does
        n = conn.execute(delete_sql).rowcount
        deleted.append(n)
        if n == 0:
            break
    assert deleted == [10000, 10000, 5000, 0]
    left = conn.execute("SELECT count(*), bool_and(message_id LIKE 'new-%') FROM processed_messages").fetchone()
    assert left == (100, True)
