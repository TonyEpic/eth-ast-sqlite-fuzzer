from test_db.interfaces import ExecutionResult
from test_db.oracle.normalizer import normalize_output


def compare_results(patched: ExecutionResult, vanilla: ExecutionResult) -> tuple[bool, str]:
    """Per-statement diff between patched and vanilla runs.

    Step 4 will add row-set normalization and a version-difference allowlist;
    for now this is a stricter version of the previous whole-output diff that
    pinpoints the first divergent statement. Stderr is intentionally ignored
    since error-message wording differs between SQLite versions.
    """
    if patched.crashed != vanilla.crashed:
        return False, "Crash behavior differs"

    if patched.timed_out != vanilla.timed_out:
        return False, "Timeout behavior differs"

    if len(patched.statements) != len(vanilla.statements):
        return False, (
            f"Different number of executed statements: "
            f"patched={len(patched.statements)} vanilla={len(vanilla.statements)}"
        )

    for p, v in zip(patched.statements, vanilla.statements):
        p_failed = p.returncode != 0 or p.timed_out
        v_failed = v.returncode != 0 or v.timed_out
        if p_failed != v_failed:
            return False, (
                f"Stmt {p.stmt_idx} error status differs "
                f"(patched_rc={p.returncode}, vanilla_rc={v.returncode})"
            )
        if normalize_output(p.stdout) != normalize_output(v.stdout):
            return False, f"Stmt {p.stmt_idx} stdout differs"

    return True, "Equivalent"
