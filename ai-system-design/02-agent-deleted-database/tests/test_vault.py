"""The vault: a backup made by a read-only role, kept where no database role can reach,
and a restore that is tested before the day it is needed."""
import json

import pytest

import lab
import vault

pytestmark = pytest.mark.usefixtures("db")


def checksum(conn, table: str, schema: str = "prod") -> tuple:
    return conn.execute(
        f"SELECT count(*), sum(id), md5(string_agg(name, ',' ORDER BY id)) "
        f"FROM {schema}.{table}").fetchone()


def test_backup_writes_every_row_and_a_manifest(tmp_path):
    assert vault.backup("executives", tmp_path) == 1206
    assert json.loads((tmp_path / "executives.json").read_text()) == {
        "table": "executives", "rows": 1206}
    assert (tmp_path / "executives.copy").stat().st_size > 1206 * 40


def test_restore_after_a_hard_drop_brings_back_identical_rows(tmp_path, connect):
    owner = connect()
    before = checksum(owner, "executives")
    vault.backup("executives", tmp_path)
    owner.execute("DROP TABLE prod.executives")
    assert vault.restore("executives", tmp_path) == 1206
    assert checksum(owner, "executives") == before
    assert lab.original_rows(owner, "executives") == 1206
    # the restored table has its primary key and the default grants again
    owner.execute("INSERT INTO prod.executives VALUES (5000, 1, 'New', 'new@example.com')")
    assert connect(lab.AGENT).execute("SELECT count(*) FROM prod.executives").fetchone() == (1207,)


def test_restore_test_checks_the_backup_without_touching_production(tmp_path, connect):
    vault.backup("companies", tmp_path)
    assert vault.restore_test("companies", tmp_path) == 1196
    owner = connect()
    assert lab.original_rows(owner, "companies") == 1196
    assert owner.execute("SELECT to_regnamespace('restore_check')").fetchone() == (None,)


def test_a_truncated_backup_fails_the_restore_test(tmp_path):
    vault.backup("companies", tmp_path)
    (tmp_path / "companies.json").write_text(json.dumps({"table": "companies", "rows": 1197}))
    with pytest.raises(RuntimeError, match="restored 1196 rows, backed up 1197"):
        vault.restore_test("companies", tmp_path)


def test_a_failed_restore_leaves_nothing_half_built(tmp_path, connect):
    vault.backup("executives", tmp_path)
    connect().execute("DROP TABLE prod.executives")
    (tmp_path / "executives.json").write_text(json.dumps({"table": "executives", "rows": 1}))
    with pytest.raises(RuntimeError):
        vault.restore("executives", tmp_path)
    assert connect().execute("SELECT to_regclass('prod.executives')").fetchone() == (None,)
