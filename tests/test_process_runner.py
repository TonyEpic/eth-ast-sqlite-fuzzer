import shutil
import tempfile
from pathlib import Path

import pytest

from test_db.executor.process_runner import _group_statements, run_statements


# ---------- batching logic ------------------------------------------------

def test_group_statements_no_transaction():
    stmts = ["CREATE TABLE t(c0 INT);", "INSERT INTO t VALUES(1);", "SELECT * FROM t;"]
    groups = _group_statements(stmts)
    assert groups == [(0, [stmts[0]]), (1, [stmts[1]]), (2, [stmts[2]])]


def test_group_statements_simple_transaction():
    stmts = [
        "CREATE TABLE t(c0 INT);",
        "BEGIN;",
        "INSERT INTO t VALUES(1);",
        "INSERT INTO t VALUES(2);",
        "COMMIT;",
        "SELECT * FROM t;",
    ]
    groups = _group_statements(stmts)
    assert groups == [
        (0, [stmts[0]]),
        (1, stmts[1:5]),
        (5, [stmts[5]]),
    ]


def test_group_statements_unmatched_begin_groups_to_end():
    stmts = ["BEGIN;", "INSERT INTO t VALUES(1);", "INSERT INTO t VALUES(2);"]
    groups = _group_statements(stmts)
    assert groups == [(0, stmts)]


def test_group_statements_begin_transaction_and_rollback():
    stmts = [
        "BEGIN TRANSACTION;",
        "DELETE FROM t;",
        "ROLLBACK;",
        "SELECT * FROM t;",
    ]
    groups = _group_statements(stmts)
    assert groups == [(0, stmts[:3]), (3, [stmts[3]])]


def test_group_statements_case_insensitive():
    stmts = ["begin;", "insert into t values(1);", "End;"]
    groups = _group_statements(stmts)
    assert groups == [(0, stmts)]


# ---------- end-to-end: requires sqlite3 in PATH --------------------------

_SQLITE = shutil.which("sqlite3")


@pytest.mark.skipif(_SQLITE is None, reason="sqlite3 binary not available on host")
def test_transaction_commit_does_not_error_end_to_end():
    """Regression test: BEGIN/INSERT/COMMIT used to break because each
    statement ran in its own subprocess. Now the block is batched."""
    tmp = Path(tempfile.mkdtemp(prefix="testdb_test_"))
    db = tmp / "t.db"
    try:
        result = run_statements(
            _SQLITE,
            [
                "CREATE TABLE t(c0 INT);",
                "BEGIN;",
                "INSERT INTO t VALUES(1);",
                "INSERT INTO t VALUES(2);",
                "COMMIT;",
                "SELECT count(*) FROM t;",
            ],
            timeout_sec=5,
            db_path=db,
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    # No statement should have failed.
    for s in result.statements:
        assert s.returncode == 0, f"stmt {s.stmt_idx} failed: {s.stderr!r}"
    # The final SELECT (last result) must report 2 rows.
    assert "2" in result.statements[-1].stdout
