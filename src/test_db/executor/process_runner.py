import subprocess
import time
from pathlib import Path
from typing import List

from test_db.interfaces import ExecutionResult, StatementResult


def _decode(buf) -> str:
    if buf is None:
        return ""
    if isinstance(buf, bytes):
        return buf.decode("utf-8", errors="replace")
    return buf


def run_statements(
    engine_path: str,
    statements: List[str],
    timeout_sec: int,
    db_path: Path,
) -> ExecutionResult:
    """Execute SQL statements one at a time against a shared on-disk DB.

    Each statement is fed via stdin to its own sqlite3 subprocess that opens
    `db_path`, so DDL/INSERTs in earlier statements are visible to later ones.
    Per-statement stdout/stderr/returncode/duration are captured. Execution
    stops at the first crash (rc < 0) or timeout.
    """
    results: List[StatementResult] = []
    crashed = False
    timed_out_overall = False

    total_start = time.perf_counter()
    for idx, sql in enumerate(statements):
        stmt_start = time.perf_counter()
        try:
            proc = subprocess.run(
                [engine_path, str(db_path)],
                input=sql,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
            duration_ms = (time.perf_counter() - stmt_start) * 1000
            results.append(StatementResult(
                stmt_idx=idx,
                sql=sql,
                returncode=proc.returncode,
                stdout=proc.stdout,
                stderr=proc.stderr,
                timed_out=False,
                duration_ms=duration_ms,
            ))
            if proc.returncode < 0:
                # Negative rc on POSIX = killed by signal => crash. Stop here.
                crashed = True
                break
        except subprocess.TimeoutExpired as e:
            duration_ms = (time.perf_counter() - stmt_start) * 1000
            results.append(StatementResult(
                stmt_idx=idx,
                sql=sql,
                returncode=-1,
                stdout=_decode(e.stdout),
                stderr=_decode(e.stderr),
                timed_out=True,
                duration_ms=duration_ms,
            ))
            timed_out_overall = True
            break

    total_ms = (time.perf_counter() - total_start) * 1000
    return ExecutionResult(
        engine=engine_path,
        statements=results,
        total_duration_ms=total_ms,
        timed_out=timed_out_overall,
        crashed=crashed,
    )
