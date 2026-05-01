from test_db.interfaces import ExecutionResult, StatementResult
from test_db.oracle.differential import compare_results
from test_db.oracle.normalizer import (
    has_order_by,
    is_version_sensitive,
    normalize_error,
    normalize_rows,
)


# ---------- normalizer unit tests ----------------------------------------

def test_normalize_rows_unordered_sorts():
    a = "3\n1\n2"
    b = "2\n3\n1"
    assert normalize_rows(a, ordered=False) == normalize_rows(b, ordered=False)


def test_normalize_rows_ordered_preserves_order():
    a = "3\n1\n2"
    b = "1\n2\n3"
    assert normalize_rows(a, ordered=True) != normalize_rows(b, ordered=True)


def test_has_order_by():
    assert has_order_by("SELECT * FROM t ORDER BY c0;")
    assert has_order_by("select x order  by  y limit 5")
    assert not has_order_by("SELECT * FROM t WHERE c0 > 1;")


def test_normalize_error_collapses_variable_detail():
    a = "Parse error: no such table: t0"
    b = "Error: no such table: foo"
    assert normalize_error(a) == normalize_error(b) == "no such table"


def test_normalize_error_strips_near_and_line():
    a = 'Runtime error near line 3: near "FOO": syntax error'
    b = 'Error: near "BAR": syntax error'
    assert normalize_error(a) == normalize_error(b) == "syntax error"


def test_normalize_error_empty():
    assert normalize_error("") == ""
    assert normalize_error("   \n  ") == ""


def test_is_version_sensitive():
    assert is_version_sensitive("INSERT INTO t VALUES (1) RETURNING *;")
    assert is_version_sensitive("PRAGMA journal_mode;")
    assert is_version_sensitive("SELECT json_extract(c0, '$.a') ->> 'b' FROM t;")
    assert not is_version_sensitive("SELECT * FROM t WHERE c0 > 1;")


# ---------- differential oracle integration tests ------------------------

def _stmt(idx: int, sql: str, stdout: str = "", stderr: str = "", rc: int = 0,
          timed_out: bool = False) -> StatementResult:
    return StatementResult(
        stmt_idx=idx, sql=sql, returncode=rc, stdout=stdout, stderr=stderr,
        timed_out=timed_out, duration_ms=1.0,
    )


def _run(*statements: StatementResult, crashed: bool = False, timed_out: bool = False) -> ExecutionResult:
    return ExecutionResult(
        engine="x",
        statements=list(statements),
        total_duration_ms=sum(s.duration_ms for s in statements),
        timed_out=timed_out,
        crashed=crashed,
    )


def test_compare_unordered_rows_in_different_order_is_equivalent():
    sql = "SELECT c0 FROM t;"
    p = _run(_stmt(0, sql, stdout="1\n2\n3"))
    v = _run(_stmt(0, sql, stdout="3\n1\n2"))
    same, _ = compare_results(p, v)
    assert same


def test_compare_ordered_rows_in_different_order_is_mismatch():
    sql = "SELECT c0 FROM t ORDER BY c0;"
    p = _run(_stmt(0, sql, stdout="1\n2\n3"))
    v = _run(_stmt(0, sql, stdout="3\n1\n2"))
    same, reason = compare_results(p, v)
    assert not same
    assert "row set" in reason.lower()


def test_compare_same_error_kind_is_equivalent():
    sql = "SELECT * FROM missing;"
    p = _run(_stmt(0, sql, stderr="Parse error: no such table: missing", rc=1))
    v = _run(_stmt(0, sql, stderr="Error: no such table: missing", rc=1))
    same, _ = compare_results(p, v)
    assert same


def test_compare_different_error_kind_is_mismatch():
    sql = "SELECT * FROM t;"
    p = _run(_stmt(0, sql, stderr="Parse error: no such table: t", rc=1))
    v = _run(_stmt(0, sql, stderr='Error: near "FROM": syntax error', rc=1))
    same, _ = compare_results(p, v)
    assert not same


def test_compare_skips_version_sensitive_statements():
    sql = "PRAGMA journal_mode;"
    p = _run(_stmt(0, sql, stdout="delete"))
    v = _run(_stmt(0, sql, stdout="memory"))
    same, _ = compare_results(p, v)
    assert same  # diff suppressed because PRAGMA is version-sensitive


def test_compare_one_succeeded_one_errored_is_mismatch():
    sql = "SELECT 1;"
    p = _run(_stmt(0, sql, stdout="1"))
    v = _run(_stmt(0, sql, stderr="Error: ...", rc=1))
    same, _ = compare_results(p, v)
    assert not same
