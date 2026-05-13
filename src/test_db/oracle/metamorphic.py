"""Metamorphic oracle for single-engine bug finding.

Where the differential oracle compares the patched engine against a reference
engine, the metamorphic oracle compares the patched engine against *itself*
using semantics-preserving query rewrites. This catches bugs that show up
identically on both engines (so the diff oracle sees nothing) but reveal
themselves when the same logical query is asked a different way -- typically
optimizer bugs.

Two rules are implemented:

1. Predicate commutation. For every WHERE comparison `lhs OP rhs`, swap the
   sides to produce `rhs OP_swap lhs` (e.g. `a < 5` -> `5 > a`). The result
   set must be identical modulo row order. Catches optimizer bugs in
   comparison normalization.

2. NoREC-style count check. Given a SELECT of the form
   `SELECT cols FROM r WHERE p`, build two semantically-equivalent scalars:
       count_original :  SELECT count(*) FROM (<original>)
       norec          :  SELECT SUM(CASE WHEN (p) THEN 1 ELSE 0 END) FROM r
   The first uses the optimizer's chosen plan; the second forces a full scan
   with the predicate evaluated per row. If the two integers disagree, the
   optimizer mis-planned the predicate.

   NoREC is only applied when the original SELECT has no row-collapsing or
   row-limiting clauses (DISTINCT, GROUP BY, HAVING, LIMIT, OFFSET, set
   operations) and no aggregates in the select list -- otherwise the
   equivalence doesn't hold.
"""
from __future__ import annotations

import re
from typing import Callable, List, Optional, Tuple

from test_db.interfaces import ExecutionResult
from test_db.oracle.normalizer import normalize_rows


# ---------------------------------------------------------------------------
# Rule 1: predicate commutation
# ---------------------------------------------------------------------------

# Match a simple `<col> OP <literal-or-col>` predicate inside a WHERE clause.
# Conservative on purpose: skip anything with subqueries, function calls,
# NULL handling, BETWEEN/IN/LIKE, or chained boolean operators.
_PRED_RE = re.compile(
    r"\bWHERE\s+([\w.]+)\s*(=|!=|<>|<=|>=|<|>)\s*"
    r"([\-+]?\d+(?:\.\d+)?|'[^']*'|\w+\.\w+|\w+)\s*(;|$)",
    re.IGNORECASE,
)

_OP_SWAP = {
    "=": "=", "!=": "!=", "<>": "<>",
    "<": ">", ">": "<", "<=": ">=", ">=": "<=",
}


def commute_predicate(sql: str) -> Optional[str]:
    """Return a semantics-equivalent SQL with `WHERE lhs OP rhs` swapped.

    Returns None if no commutable predicate is found.
    """
    m = _PRED_RE.search(sql)
    if not m:
        return None
    lhs, op, rhs, tail = m.group(1), m.group(2), m.group(3), m.group(4)
    swapped_op = _OP_SWAP[op]
    new_where = f"WHERE {rhs} {swapped_op} {lhs}{tail}"
    return sql[: m.start()] + new_where + sql[m.end():]


# ---------------------------------------------------------------------------
# Rule 2: NoREC-style un-optimizable count
# ---------------------------------------------------------------------------

# Match a SELECT with FROM and WHERE. Capture the select list, relation
# (after FROM, up to WHERE) and the predicate (after WHERE, up to clauses
# that terminate it). The regex is intentionally simple; we filter further
# with _NOREC_BAILOUT_RE below.
_SELECT_RE = re.compile(
    r"\bSELECT\s+(.+?)\s+FROM\s+(.+?)\s+WHERE\s+(.+?)\s*"
    r"(?:GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|;|$)",
    re.IGNORECASE | re.DOTALL,
)

# Tokens that make the original SELECT non-equivalent to a per-row count of
# its predicate. Anything that collapses, deduplicates, paginates or windows
# the result breaks the rewrite's invariant.
_NOREC_BAILOUT_RE = re.compile(
    r"\b(?:DISTINCT|GROUP\s+BY|HAVING|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|"
    r"COUNT\s*\(|SUM\s*\(|AVG\s*\(|MIN\s*\(|MAX\s*\(|TOTAL\s*\(|"
    r"GROUP_CONCAT\s*\()",
    re.IGNORECASE,
)


def norec_rewrite(sql: str) -> Optional[Tuple[str, str]]:
    """Build two equivalent count expressions from a `WHERE`-bearing SELECT.

    Returns ``(count_original_sql, norec_sql)`` or None if the SQL doesn't
    match the supported shape or has any row-collapsing clauses.

      * ``count_original_sql`` wraps the original SELECT in a ``count(*)`` so
        the optimizer's chosen plan still applies but we get a single
        integer to compare.
      * ``norec_sql`` evaluates the predicate per row of the underlying
        relation without any optimization, also returning a single integer.

    A real bug shows up as the two integers disagreeing.
    """
    if _NOREC_BAILOUT_RE.search(sql):
        return None
    m = _SELECT_RE.search(sql)
    if not m:
        return None
    select_list = m.group(1).strip()
    relation = m.group(2).strip()
    predicate = m.group(3).strip()
    # Bail on aggregates in the select list (covered by bailout regex but
    # the select list may contain aliases that pass the broad check).
    if _NOREC_BAILOUT_RE.search(select_list):
        return None
    # Bail on relations with subqueries -- safer.
    if "(" in relation and "SELECT" in relation.upper():
        return None
    inner = sql.rstrip().rstrip(";")
    count_original_sql = f"SELECT count(*) FROM ({inner});"
    norec_sql = (
        f"SELECT SUM(CASE WHEN ({predicate}) THEN 1 ELSE 0 END) FROM {relation};"
    )
    return count_original_sql, norec_sql


# ---------------------------------------------------------------------------
# Oracle entry point
# ---------------------------------------------------------------------------

RunFn = Callable[[List[str]], ExecutionResult]


def _scalar_int(stmt) -> Optional[int]:
    """Parse a single-row, single-column integer from a statement's stdout.

    Returns None for empty output (the engine returned NULL) or non-integer
    output, so callers can distinguish that case from a real zero.
    """
    out = (stmt.stdout or "").strip()
    if not out:
        return None
    try:
        return int(out.splitlines()[0].strip())
    except (ValueError, IndexError):
        return None


def check_metamorphic(
    workload_stmts: List[str],
    patched: ExecutionResult,
    run_fn: RunFn,
) -> Tuple[bool, str]:
    """Apply metamorphic checks to the patched run of a workload.

    Returns ``(ok, reason)``. ``ok=True`` means no inconsistency was detected
    (or the workload didn't match any rule). ``ok=False`` means a
    semantics-preserving rewrite produced a different answer than the
    original on the patched engine alone -- a real single-engine bug.
    """
    # Walk the executed statements. We only metamorph-check SELECTs that
    # ran without error, since the rewrites need the same DB state.
    for idx, s in enumerate(patched.statements):
        if s.returncode != 0 or s.timed_out:
            continue
        head = s.sql.lstrip().upper()
        if not head.startswith("SELECT"):
            continue

        # Rule 2: NoREC. Most informative; try first.
        pair = norec_rewrite(s.sql)
        if pair is not None:
            count_original_sql, rewrite_sql = pair
            prefix = workload_stmts[:idx]
            ok, reason = _check_norec(
                prefix, count_original_sql, rewrite_sql, run_fn, idx,
            )
            if not ok:
                return False, reason

        # Rule 1: predicate commutation.
        commuted = commute_predicate(s.sql)
        if commuted is not None and commuted != s.sql:
            prefix = workload_stmts[:idx]
            ok, reason = _check_commute(prefix, s, commuted, run_fn, idx)
            if not ok:
                return False, reason

    return True, "metamorphic ok"


def _check_norec(prefix, count_original_sql, rewrite_sql, run_fn, idx):
    """Run two equivalent scalar-count queries; complain if they disagree."""
    rerun = run_fn(prefix + [count_original_sql, rewrite_sql])
    if len(rerun.statements) < 2:
        return True, "no rerun output"
    count_stmt = rerun.statements[-2]
    rewrite_stmt = rerun.statements[-1]
    if count_stmt.returncode != 0 or rewrite_stmt.returncode != 0:
        return True, "rerun errored, skip"
    original_count = _scalar_int(count_stmt)
    rewrite_count = _scalar_int(rewrite_stmt)
    if original_count is None or rewrite_count is None:
        # Engine returned NULL for one of the scalars (e.g. all-NULL table
        # with SUM over no rows). Not informative; skip.
        return True, "rerun output not scalar int"
    if original_count != rewrite_count:
        return False, (
            f"metamorphic NoREC mismatch at stmt {idx}: "
            f"count(*) of original = {original_count}, "
            f"sum(case when p then 1 end) = {rewrite_count}"
        )
    return True, "norec ok"


def _check_commute(prefix, original_stmt, commuted_sql, run_fn, idx):
    """Re-run the workload with `WHERE x OP y` swapped to `WHERE y OP_swap x`.

    Both rewrites must return the same row set (unordered, to avoid
    tie-break noise from ORDER BY on duplicate keys).
    """
    rerun = run_fn(prefix + [commuted_sql])
    if not rerun.statements:
        return True, "no rerun output"
    commuted_result = rerun.statements[-1]
    if commuted_result.returncode != 0:
        return True, "commuted errored, skip"
    if normalize_rows(original_stmt.stdout, False) != normalize_rows(
        commuted_result.stdout, False
    ):
        return False, (
            f"metamorphic commute mismatch at stmt {idx}: "
            f"x OP y and y OP_swap x produced different rows"
        )
    return True, "commute ok"


__all__ = [
    "commute_predicate",
    "norec_rewrite",
    "check_metamorphic",
]
