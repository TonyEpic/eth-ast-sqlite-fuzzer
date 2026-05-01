"""Normalization helpers for the differential oracle.

The patched (3.39.4) and vanilla (3.51.1) SQLite binaries differ in many
benign ways: error message wording, supported SQL features, and (for some
queries) the order in which rows are returned. This module collapses those
benign differences so the diff can focus on real divergences.
"""
import re
from typing import List


# ---------------------------------------------------------------------------
# Stdout / row-set normalization
# ---------------------------------------------------------------------------

def normalize_output(text: str) -> str:
    """Whitespace-only normalization. Kept for backwards compatibility."""
    lines = [line.rstrip() for line in text.strip().splitlines()]
    return "\n".join(lines)


def parse_rows(text: str) -> List[str]:
    """Split sqlite3 CLI output into a list of row strings.

    The default sqlite3 output mode is one row per line, columns separated
    by '|'. Trailing whitespace on each line is stripped; empty trailing
    lines are dropped. Rows containing literal newlines in column values
    will be miscounted, but that is rare in our generated workloads.
    """
    return [line.rstrip() for line in text.strip().splitlines() if line != ""]


def normalize_rows(text: str, ordered: bool) -> str:
    """Return a canonical string representation of a row set.

    If `ordered` is False the rows are sorted, so that two engines that
    return the same rows in different orders compare equal.
    """
    rows = parse_rows(text)
    if not ordered:
        rows = sorted(rows)
    return "\n".join(rows)


_ORDER_BY_RE = re.compile(r"\border\s+by\b", re.IGNORECASE)


def has_order_by(sql: str) -> bool:
    """True iff the SQL statement contains an ORDER BY clause."""
    # Naive, but good enough for our generated workloads.
    return bool(_ORDER_BY_RE.search(sql))


# ---------------------------------------------------------------------------
# Error-message normalization
# ---------------------------------------------------------------------------

# Capture the first SQLite error phrase (the part before the colon that
# carries the variable detail). Examples we want to canonicalize:
#   "Parse error: no such table: t0"       -> "no such table"
#   "Runtime error near line 1: ..."       -> "runtime error"
#   "Error: near \"FOO\": syntax error"    -> "syntax error"
_ERROR_PREFIX_RE = re.compile(r"^(parse error|runtime error|error)\s*[:]?", re.IGNORECASE)
_NEAR_RE = re.compile(r'near\s+"[^"]*"\s*:\s*', re.IGNORECASE)
_LINE_REF_RE = re.compile(r'\bnear\s+line\s+\d+\s*:\s*', re.IGNORECASE)


def normalize_error(stderr: str) -> str:
    """Reduce a sqlite3 stderr message to a stable category string.

    Strips the leading "Parse error:" / "Runtime error:" / "Error:" prefix,
    removes location hints ("near \"FOO\":", "near line 3:"), drops the
    trailing variable detail (anything after the next colon), lowercases,
    and trims. Returns "" if stderr is empty.
    """
    if not stderr.strip():
        return ""
    # Take only the first non-empty line; SQLite usually prints one error.
    first = next((ln for ln in stderr.splitlines() if ln.strip()), "")
    s = first.strip()
    s = _ERROR_PREFIX_RE.sub("", s).strip()
    s = _LINE_REF_RE.sub("", s)
    s = _NEAR_RE.sub("", s)
    # Keep only the first colon-delimited phrase ("no such table: t0" -> "no such table").
    s = s.split(":", 1)[0]
    return s.lower().strip()


# ---------------------------------------------------------------------------
# Version-difference allowlist
# ---------------------------------------------------------------------------

# Keywords / tokens for SQL features that exist in vanilla 3.51.1 but not in
# the patched 3.39.4 target (or vice-versa). When a statement uses any of
# these, the diff oracle skips it instead of reporting a false-positive bug.
#
# This list is intentionally conservative; grow it as you observe new false
# positives. Match is a case-insensitive substring on the SQL text.
_VERSION_DIFFERENCE_TOKENS = (
    " returning ",       # RETURNING clause: 3.35+, error wording differs
    " strict",           # STRICT tables: 3.37+, may behave differently
    "->>",               # JSON subscript: 3.38+
    "json_each",         # JSON1 helpers may differ
    "json_tree",
    "unixepoch(",        # 3.38+
    "iif(",              # 3.32+, but error paths differ
    "generate_series",   # eponymous virtual table availability differs
    "pragma",            # PRAGMAs are the most version-sensitive surface
    "explain",           # plan output differs between versions
)


def is_version_sensitive(sql: str) -> bool:
    """True iff the SQL likely produces different output across SQLite versions
    purely because of version differences, not because of a bug."""
    s = " " + sql.lower() + " "
    return any(tok in s for tok in _VERSION_DIFFERENCE_TOKENS)
