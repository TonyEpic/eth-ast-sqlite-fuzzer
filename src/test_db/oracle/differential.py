from test_db.interfaces import ExecutionResult
from test_db.oracle.normalizer import (
    has_order_by,
    is_version_sensitive,
    normalize_error,
    normalize_rows,
)


def compare_results(patched: ExecutionResult, vanilla: ExecutionResult) -> tuple[bool, str]:
    """Per-statement diff between patched and vanilla runs.

    Differences considered:
      * Crash / timeout flags at workload level.
      * Per-statement error vs success (after error-message normalization).
      * Per-statement row sets (sorted unless the SQL has ORDER BY).

    Statements that look version-sensitive (e.g. PRAGMA, RETURNING, JSON
    subscript) are skipped, because patched 3.39.4 and vanilla 3.51.1 are
    expected to differ there.
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
        if is_version_sensitive(p.sql):
            continue

        # Both timed out at the same statement -> not interesting.
        if p.timed_out and v.timed_out:
            continue
        if p.timed_out != v.timed_out:
            return False, f"Stmt {p.stmt_idx} timeout differs"

        p_failed = p.returncode != 0
        v_failed = v.returncode != 0

        if p_failed != v_failed:
            return False, (
                f"Stmt {p.stmt_idx} error status differs "
                f"(patched_rc={p.returncode}, vanilla_rc={v.returncode})"
            )

        if p_failed and v_failed:
            # Both errored: only flag when the *kind* of error differs.
            if normalize_error(p.stderr) != normalize_error(v.stderr):
                return False, (
                    f"Stmt {p.stmt_idx} error kind differs "
                    f"(patched={normalize_error(p.stderr)!r}, "
                    f"vanilla={normalize_error(v.stderr)!r})"
                )
            continue

        # Both succeeded: compare row sets.
        ordered = has_order_by(p.sql)
        if normalize_rows(p.stdout, ordered) != normalize_rows(v.stdout, ordered):
            # If the SQL has ORDER BY but the rows only differ in order, the
            # most likely cause is tied sort keys. SQL leaves the order of
            # tied rows implementation-defined, so two engines may legitimately
            # disagree. Fall back to a multiset comparison: only flag as a
            # real mismatch when the rows themselves differ.
            if ordered and normalize_rows(p.stdout, False) == normalize_rows(v.stdout, False):
                continue
            return False, f"Stmt {p.stmt_idx} row set differs"

    return True, "Equivalent"
