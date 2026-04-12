from test_db.interfaces import ExecutionResult


def classify_single(result: ExecutionResult) -> tuple[str, str]:
    if result.timed_out:
        return "timeout", "Execution exceeded timeout"
    if result.returncode != 0:
        stderr_lower = result.stderr.lower()
        if "segmentation fault" in stderr_lower:
            return "crash", "Segmentation fault"
        return "error", "Non-zero return code or SQLite error"
    return "ok", "Executed successfully"