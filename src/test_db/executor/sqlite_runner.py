import shutil
import tempfile
from pathlib import Path
from typing import List

from test_db.config import (
    COVERAGE_SQLITE,
    DEFAULT_TIMEOUT_SEC,
    PATCHED_SQLITE,
    VANILLA_SQLITE,
)
from test_db.executor.process_runner import run_statements
from test_db.interfaces import ExecutionResult


def _run_with_fresh_db(engine_path: str, statements: List[str], timeout_sec: int) -> ExecutionResult:
    """Run a workload against a freshly-created on-disk SQLite database.

    A temp directory holds the DB file so state persists across the per-
    statement subprocess invocations, then is removed when we are done.
    """
    tmp_dir = Path(tempfile.mkdtemp(prefix="testdb_"))
    db_path = tmp_dir / "test.db"
    try:
        return run_statements(engine_path, statements, timeout_sec, db_path)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def run_on_patched(statements: List[str], timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> ExecutionResult:
    return _run_with_fresh_db(PATCHED_SQLITE, statements, timeout_sec)


def run_on_vanilla(statements: List[str], timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> ExecutionResult:
    return _run_with_fresh_db(VANILLA_SQLITE, statements, timeout_sec)


def run_on_coverage(statements: List[str], timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> ExecutionResult:
    """Run against the gcov-instrumented binary.

    .gcda files are written next to the .gcno files inside the build dir at
    process exit; the harness clears and aggregates them around the run.
    """
    return _run_with_fresh_db(COVERAGE_SQLITE, statements, timeout_sec)
