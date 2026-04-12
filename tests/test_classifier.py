from test_db.interfaces import ExecutionResult
from test_db.oracle.classifier import classify_single


def test_classify_ok():
    result = ExecutionResult(
        engine="sqlite",
        returncode=0,
        stdout="",
        stderr="",
        timed_out=False,
        duration_ms=1.0,
    )
    classification, _ = classify_single(result)
    assert classification == "ok"