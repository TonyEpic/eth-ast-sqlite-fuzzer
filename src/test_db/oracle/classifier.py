from test_db.interfaces import ExecutionResult


def _summary(sql: str, limit: int = 60) -> str:
    s = " ".join(sql.split())
    return s if len(s) <= limit else s[:limit] + "..."


def classify_single(result: ExecutionResult) -> tuple[str, str]:
    """Classify a workload run as crash / timeout / error / ok.

    Inspects per-statement results and reports the first failing statement,
    so reasons include the index and a snippet of the offending SQL.
    """
    # Crash: a subprocess was killed by a signal (rc < 0).
    for s in result.statements:
        if s.returncode < 0 and not s.timed_out:
            return "crash", f"Crash at stmt {s.stmt_idx} (signal {-s.returncode}): {_summary(s.sql)}"

    # Timeout: any statement hit the per-statement timeout.
    for s in result.statements:
        if s.timed_out:
            return "timeout", f"Timeout at stmt {s.stmt_idx}: {_summary(s.sql)}"

    # SQL error: sqlite returns non-zero on syntax/runtime errors.
    for s in result.statements:
        if s.returncode != 0:
            err = s.stderr.strip().splitlines()
            first_err = err[0] if err else "non-zero returncode"
            return "error", f"SQL error at stmt {s.stmt_idx}: {first_err}"

    return "ok", "Executed successfully"
