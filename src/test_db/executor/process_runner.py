import subprocess
import time
from pathlib import Path
from typing import List, Tuple

from test_db.interfaces import ExecutionResult, StatementResult


def _decode(buf) -> str:
    if buf is None:
        return ""
    if isinstance(buf, bytes):
        return buf.decode("utf-8", errors="replace")
    return buf


# Tokens that open / close a transaction block. We batch any statements
# between a BEGIN and the next COMMIT/ROLLBACK/END into a single subprocess
# so the transaction actually transacts (sqlite3 transaction state lives in
# the connection, which dies when the subprocess exits).
_TXN_OPEN_PREFIXES = ("BEGIN",)
_TXN_CLOSE_PREFIXES = ("COMMIT", "ROLLBACK", "END")


def _stmt_kind(sql: str) -> str:
    head = sql.lstrip().upper()
    if any(head.startswith(p) for p in _TXN_OPEN_PREFIXES):
        return "open"
    if any(head.startswith(p) for p in _TXN_CLOSE_PREFIXES):
        return "close"
    return "other"


def _group_statements(statements: List[str]) -> List[Tuple[int, List[str]]]:
    """Group statements into execution batches.

    A batch is either a single statement, or a run of consecutive statements
    delimited by BEGIN ... COMMIT/ROLLBACK/END. An unmatched BEGIN groups
    everything until the end of the workload. Each returned tuple is
    (start_idx, [statements_in_batch]).
    """
    groups: List[Tuple[int, List[str]]] = []
    i = 0
    n = len(statements)
    while i < n:
        kind = _stmt_kind(statements[i])
        if kind != "open":
            groups.append((i, [statements[i]]))
            i += 1
            continue

        # Start of a transaction block. Collect through the matching close.
        start = i
        block = [statements[i]]
        i += 1
        while i < n:
            block.append(statements[i])
            if _stmt_kind(statements[i]) == "close":
                i += 1
                break
            i += 1
        groups.append((start, block))
    return groups


def run_statements(
    engine_path: str,
    statements: List[str],
    timeout_sec: int,
    db_path: Path,
) -> ExecutionResult:
    """Execute SQL statements against a shared on-disk DB.

    Non-transaction statements run one at a time, each in its own sqlite3
    subprocess, so per-statement stdout/stderr/returncode/duration are
    isolated. Transaction blocks (BEGIN ... COMMIT/ROLLBACK/END) are batched
    into a single subprocess so the connection lives long enough for the
    transaction semantics to apply. For batched statements every input
    statement still emits a StatementResult, but the rows share the same
    combined output/returncode of the batch.
    """
    results: List[StatementResult] = []
    crashed = False
    timed_out_overall = False

    total_start = time.perf_counter()
    for start_idx, batch in _group_statements(statements):
        batch_sql = "\n".join(batch)
        is_block = len(batch) > 1
        stmt_start = time.perf_counter()
        try:
            proc = subprocess.run(
                [engine_path, str(db_path)],
                input=batch_sql,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
            duration_ms = (time.perf_counter() - stmt_start) * 1000
            per_stmt_ms = duration_ms / len(batch) if is_block else duration_ms
            for j, sql in enumerate(batch):
                results.append(StatementResult(
                    stmt_idx=start_idx + j,
                    sql=sql,
                    returncode=proc.returncode,
                    # Only attach combined output to the first row of a block
                    # to avoid duplicating identical text len(batch) times.
                    stdout=proc.stdout if (not is_block or j == 0) else "",
                    stderr=proc.stderr if (not is_block or j == 0) else "",
                    timed_out=False,
                    duration_ms=per_stmt_ms,
                ))
            if proc.returncode < 0:
                # Negative rc on POSIX = killed by signal => crash. Stop here.
                crashed = True
                break
        except subprocess.TimeoutExpired as e:
            duration_ms = (time.perf_counter() - stmt_start) * 1000
            per_stmt_ms = duration_ms / len(batch) if is_block else duration_ms
            for j, sql in enumerate(batch):
                results.append(StatementResult(
                    stmt_idx=start_idx + j,
                    sql=sql,
                    returncode=-1,
                    stdout=_decode(e.stdout) if (not is_block or j == 0) else "",
                    stderr=_decode(e.stderr) if (not is_block or j == 0) else "",
                    timed_out=True,
                    duration_ms=per_stmt_ms,
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

