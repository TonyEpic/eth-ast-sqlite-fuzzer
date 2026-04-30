from test_db.interfaces import ExecutionResult, StatementResult
from test_db.oracle.classifier import classify_single


def _make_result(*statements: StatementResult, crashed: bool = False, timed_out: bool = False) -> ExecutionResult:
    return ExecutionResult(
        engine="sqlite",
        statements=list(statements),
        total_duration_ms=sum(s.duration_ms for s in statements),
        timed_out=timed_out,
        crashed=crashed,
    )


def _ok_stmt(idx: int = 0, sql: str = "SELECT 1;") -> StatementResult:
    return StatementResult(
        stmt_idx=idx, sql=sql, returncode=0, stdout="1\n", stderr="",
        timed_out=False, duration_ms=1.0,
    )


def test_classify_ok():
    result = _make_result(_ok_stmt())
    classification, _ = classify_single(result)
    assert classification == "ok"


def test_classify_error_reports_stmt_index():
    bad = StatementResult(
        stmt_idx=2, sql="SELECT * FROM missing;", returncode=1,
        stdout="", stderr="Parse error: no such table: missing\n",
        timed_out=False, duration_ms=1.0,
    )
    result = _make_result(_ok_stmt(0), _ok_stmt(1), bad)
    classification, reason = classify_single(result)
    assert classification == "error"
    assert "stmt 2" in reason


def test_classify_crash_overrides_error():
    bad = StatementResult(
        stmt_idx=1, sql="SELECT crash();", returncode=-11,
        stdout="", stderr="", timed_out=False, duration_ms=2.0,
    )
    result = _make_result(_ok_stmt(0), bad, crashed=True)
    classification, reason = classify_single(result)
    assert classification == "crash"
    assert "stmt 1" in reason


def test_classify_timeout():
    slow = StatementResult(
        stmt_idx=0, sql="SELECT very_slow();", returncode=-1,
        stdout="", stderr="", timed_out=True, duration_ms=2000.0,
    )
    result = _make_result(slow, timed_out=True)
    classification, _ = classify_single(result)
    assert classification == "timeout"
