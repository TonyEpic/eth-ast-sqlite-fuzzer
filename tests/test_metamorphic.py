from test_db.interfaces import ExecutionResult, StatementResult
from test_db.oracle.metamorphic import (
    check_metamorphic,
    commute_predicate,
    norec_rewrite,
)


# ---------- commute_predicate ---------------------------------------------

def test_commute_equality():
    assert commute_predicate("SELECT * FROM t WHERE c = 5;") == "SELECT * FROM t WHERE 5 = c;"


def test_commute_lt_to_gt():
    assert commute_predicate("SELECT * FROM t WHERE c < 5;") == "SELECT * FROM t WHERE 5 > c;"


def test_commute_gte_to_lte():
    assert commute_predicate("SELECT * FROM t WHERE c >= 5;") == "SELECT * FROM t WHERE 5 <= c;"


def test_commute_returns_none_when_no_match():
    assert commute_predicate("SELECT * FROM t;") is None
    assert commute_predicate("SELECT * FROM t WHERE c BETWEEN 1 AND 5;") is None


# ---------- norec_rewrite -------------------------------------------------

def test_norec_basic_returns_pair():
    pair = norec_rewrite("SELECT c FROM t WHERE c > 5;")
    assert pair is not None
    count_sql, norec_sql = pair
    assert count_sql == "SELECT count(*) FROM (SELECT c FROM t WHERE c > 5);"
    assert norec_sql == "SELECT SUM(CASE WHEN (c > 5) THEN 1 ELSE 0 END) FROM t;"


def test_norec_bails_on_group_by():
    assert norec_rewrite("SELECT c, COUNT(*) FROM t WHERE c > 5 GROUP BY c;") is None


def test_norec_bails_on_distinct():
    assert norec_rewrite("SELECT DISTINCT c FROM t WHERE c > 5;") is None


def test_norec_bails_on_limit():
    assert norec_rewrite("SELECT c FROM t WHERE c > 5 LIMIT 10;") is None


def test_norec_bails_on_offset():
    assert norec_rewrite("SELECT c FROM t WHERE c > 5 OFFSET 10;") is None


def test_norec_bails_on_aggregate_in_select_list():
    assert norec_rewrite("SELECT MAX(c) FROM t WHERE c > 5;") is None


def test_norec_bails_on_set_operations():
    assert norec_rewrite(
        "SELECT c FROM t WHERE c > 5 UNION SELECT c FROM u WHERE c > 5;"
    ) is None


def test_norec_returns_none_without_where():
    assert norec_rewrite("SELECT * FROM t;") is None


# ---------- check_metamorphic end-to-end ----------------------------------

def _stmt(idx, sql, rc=0, stdout="", stderr=""):
    return StatementResult(stmt_idx=idx, sql=sql, returncode=rc, stdout=stdout,
                           stderr=stderr, timed_out=False, duration_ms=1.0)


def _exec(*stmts):
    return ExecutionResult(engine="x", statements=list(stmts),
                           total_duration_ms=sum(s.duration_ms for s in stmts),
                           timed_out=False, crashed=False)


def test_check_metamorphic_norec_match():
    """The two count forms agree; no bug detected."""
    workload = ["SELECT c FROM t WHERE c > 5;"]
    patched = _exec(_stmt(0, workload[0], stdout="6\n7\n8"))

    def fake_run(stmts):
        # NoREC rerun passes [count_original_sql, norec_sql]; both return 3.
        if len(stmts) == 2 and "count(*)" in stmts[0] and "SUM(CASE" in stmts[1]:
            return _exec(_stmt(0, stmts[0], stdout="3"),
                         _stmt(1, stmts[1], stdout="3"))
        # Commute rerun passes [commuted_sql]; same rows as original.
        return _exec(_stmt(0, stmts[-1], stdout="6\n7\n8"))

    ok, _ = check_metamorphic(workload, patched, fake_run)
    assert ok


def test_check_metamorphic_norec_detects_mismatch():
    """count(*) of original disagrees with sum(case when p then 1 else 0)."""
    workload = ["SELECT c FROM t WHERE c > 5;"]
    patched = _exec(_stmt(0, workload[0], stdout="6\n7\n8"))

    def fake_run(stmts):
        if len(stmts) == 2 and "count(*)" in stmts[0] and "SUM(CASE" in stmts[1]:
            return _exec(_stmt(0, stmts[0], stdout="3"),
                         _stmt(1, stmts[1], stdout="1"))   # disagrees!
        return _exec(_stmt(0, stmts[-1], stdout="6\n7\n8"))

    ok, reason = check_metamorphic(workload, patched, fake_run)
    assert not ok
    assert "NoREC" in reason
    assert "= 3" in reason and "= 1" in reason


def test_check_metamorphic_null_scalar_is_not_a_mismatch():
    """If either count returns NULL (empty stdout), the rule is skipped."""
    workload = ["SELECT c FROM t WHERE c > 5;"]
    patched = _exec(_stmt(0, workload[0], stdout=""))

    def fake_run(stmts):
        if len(stmts) == 2 and "SUM(CASE" in stmts[1]:
            return _exec(_stmt(0, stmts[0], stdout=""),
                         _stmt(1, stmts[1], stdout="3"))
        return _exec(_stmt(0, stmts[-1], stdout=""))

    ok, _ = check_metamorphic(workload, patched, fake_run)
    assert ok


def test_check_metamorphic_commute_match():
    workload = ["SELECT c FROM t WHERE c = 5;"]
    patched = _exec(_stmt(0, workload[0], stdout="5\n5"))

    def fake_run(stmts):
        if len(stmts) == 2 and "SUM(CASE" in stmts[1]:
            return _exec(_stmt(0, stmts[0], stdout="2"),
                         _stmt(1, stmts[1], stdout="2"))
        return _exec(_stmt(0, stmts[-1], stdout="5\n5"))

    ok, _ = check_metamorphic(workload, patched, fake_run)
    assert ok


def test_check_metamorphic_skips_non_select():
    workload = ["INSERT INTO t VALUES (1);"]
    patched = _exec(_stmt(0, workload[0]))
    ok, _ = check_metamorphic(workload, patched, lambda _: _exec())
    assert ok


def test_check_metamorphic_skips_errored_statements():
    workload = ["SELECT c FROM t WHERE c > 5;"]
    patched = _exec(_stmt(0, workload[0], rc=1, stderr="oops"))
    ok, _ = check_metamorphic(workload, patched, lambda _: _exec())
    assert ok
