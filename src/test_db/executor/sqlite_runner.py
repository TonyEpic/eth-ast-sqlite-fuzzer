from pathlib import Path
from test_db.config import PATCHED_SQLITE, VANILLA_SQLITE, DEFAULT_TIMEOUT_SEC
from test_db.executor.process_runner import run_sql_file
from test_db.interfaces import ExecutionResult


def run_on_patched(sql_file: Path, timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> ExecutionResult:
    return run_sql_file(PATCHED_SQLITE, sql_file, timeout_sec)


def run_on_vanilla(sql_file: Path, timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> ExecutionResult:
    return run_sql_file(VANILLA_SQLITE, sql_file, timeout_sec)