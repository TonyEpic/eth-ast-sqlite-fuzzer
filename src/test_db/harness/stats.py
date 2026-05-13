"""SQL keyword coverage / frequency + query-validity analyzer.

Reads an experiment run's `workloads.jsonl` and produces the
"Characteristics of generated SQL queries" report demanded by the spec.

*Per the project forum's clarification*, the spec's "query" is **one
generated test case** (a multi-statement workload), not an individual SQL
statement. So coverage / frequency / validity are all aggregated per
workload:

  1. SQL keyword coverage: for each keyword K, in how many of the N
     generated test cases (workloads) does K appear at least once.
  2. SQL keyword frequency: mean number of K's occurrences per test case,
     summed across all statements of the workload.
  3. Query validity: a workload is `ok` only if every statement executed
     cleanly; otherwise it is bucketed by the first failing statement's
     error category (syntax_error / runtime_error / timeout / crash).

For extra depth we also keep a per-statement-type breakdown of validity
(e.g. "how many SELECTs failed"), but the headline numbers follow the
spec/forum definition.

Outputs:
  - <run_dir>/characteristics.json   machine-readable
  - <run_dir>/characteristics.txt    human-readable (top-30 table + summary)
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Iterable, Iterator


# ---------------------------------------------------------------------------
# SQLite reserved keywords
# ---------------------------------------------------------------------------
# Source: https://www.sqlite.org/lang_keywords.html
# Hardcoded so the stats module needs no network and no parser dependency.
SQLITE_KEYWORDS: tuple[str, ...] = (
    "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE",
    "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN",
    "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN",
    "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT",
    "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE",
    "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH",
    "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT",
    "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST",
    "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB",
    "GROUP", "GROUPS", "HAVING", "IF", "IGNORE", "IMMEDIATE", "IN", "INDEX",
    "INDEXED", "INITIALLY", "INNER", "INSERT", "INSTEAD", "INTERSECT",
    "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT", "LIKE", "LIMIT",
    "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING", "NOTNULL",
    "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER",
    "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY",
    "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX",
    "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RETURNING", "RIGHT",
    "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE",
    "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER",
    "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES",
    "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT",
)

# Pre-compile one big regex that tokenizes an upper-cased SQL statement
# into keyword tokens. Anything not in the keyword set is discarded.
_TOKEN_RE = re.compile(r"\b[A-Z_][A-Z0-9_]*\b")
_KEYWORD_SET = frozenset(SQLITE_KEYWORDS)


def count_keywords(sql: str) -> Counter:
    """Return a Counter of {keyword: occurrences} for one SQL statement."""
    upper = sql.upper()
    return Counter(t for t in _TOKEN_RE.findall(upper) if t in _KEYWORD_SET)


# ---------------------------------------------------------------------------
# Per-statement validity
# ---------------------------------------------------------------------------

def classify_validity(rc: int, stderr_kind: str, timed_out: bool) -> str:
    """Bucket a single executed statement into one of:
       ok / syntax_error / runtime_error / timeout / crash.

    `stderr_kind` is the output of oracle.normalizer.normalize_error and is
    one of "syntax error", "no such table", "constraint failed", "", etc.
    """
    if timed_out:
        return "timeout"
    if rc < 0:
        return "crash"
    if rc == 0:
        return "ok"
    # rc > 0: sqlite raised some error.
    kind = (stderr_kind or "").lower()
    if "syntax error" in kind or "unrecognized" in kind or "incomplete" in kind:
        return "syntax_error"
    return "runtime_error"


# Statement-type buckets are based on the leading keyword. Used for the
# per-type validity breakdown the report cites (e.g. "99% of CREATEs are
# valid").
_STMT_TYPES = (
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
    "WITH", "BEGIN", "COMMIT", "ROLLBACK", "PRAGMA", "ANALYZE", "VACUUM",
    "REINDEX", "ATTACH", "DETACH", "EXPLAIN", "SAVEPOINT", "RELEASE",
)


def statement_type(sql: str) -> str:
    """Return the leading SQL keyword (uppercased) or 'OTHER'."""
    head = sql.lstrip().split(None, 1)
    if not head:
        return "OTHER"
    tok = head[0].upper().rstrip(";")
    return tok if tok in _STMT_TYPES else "OTHER"


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def _iter_workloads(jsonl_path: Path) -> Iterator[dict]:
    """Yield workload-level records (one per generated test case).

    Each yielded record carries at least a `statements` list (the per-
    statement results) and the workload's classification metadata. Records
    that pre-date the per-statement persistence change are still yielded so
    callers can decide what to do with them.
    """
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _workload_validity(stmts: list[dict]) -> str:
    """Bucket a whole workload by its first failing statement.

    A workload is `ok` iff every statement returned rc=0 and did not time
    out. Otherwise it inherits the category (timeout / crash / syntax_error
    / runtime_error) of the first statement that broke.
    """
    for s in stmts:
        rc = int(s.get("rc", 0))
        kind = str(s.get("stderr_kind", ""))
        timed_out = bool(s.get("timed_out", False))
        bucket = classify_validity(rc, kind, timed_out)
        if bucket != "ok":
            return bucket
    return "ok"


def collect_stats(run_dir: Path) -> dict:
    """Analyze a run directory and return a stats summary dict.

    Aggregates per workload (test case), not per statement. Also writes
    characteristics.json and characteristics.txt next to workloads.jsonl.
    """
    run_dir = Path(run_dir)
    jsonl_path = run_dir / "workloads.jsonl"
    if not jsonl_path.is_file():
        raise FileNotFoundError(f"workloads.jsonl not found in {run_dir}")

    n_workloads = 0
    n_workloads_with_stmts = 0
    # Per-workload keyword coverage / total-occurrence counts.
    coverage_count: Counter = Counter()
    total_count: Counter = Counter()
    validity = Counter()
    # Supplementary: per-statement-type validity buckets, kept for the
    # report's interpretation paragraph ("99% of CREATEs valid", ...).
    by_type: dict[str, Counter] = {}

    for rec in _iter_workloads(jsonl_path):
        n_workloads += 1
        stmts = rec.get("statements") or []
        if not stmts:
            # Pre-stats-collector record format. Cannot extract keywords;
            # skip so the percentages we report stay honest.
            continue
        n_workloads_with_stmts += 1

        # Per-workload keyword aggregation: presence (coverage) and total
        # occurrence count across all the workload's statements.
        workload_total: Counter = Counter()
        for s in stmts:
            workload_total.update(count_keywords(s.get("sql", "")))
            by_type.setdefault(statement_type(s.get("sql", "")), Counter())[
                classify_validity(
                    int(s.get("rc", 0)),
                    str(s.get("stderr_kind", "")),
                    bool(s.get("timed_out", False)),
                )
            ] += 1
        for kw, n in workload_total.items():
            coverage_count[kw] += 1
            total_count[kw] += n

        validity[_workload_validity(stmts)] += 1

    # Build per-keyword table sorted by coverage descending.
    n_for_ratios = n_workloads_with_stmts
    keyword_rows = []
    for kw in SQLITE_KEYWORDS:
        cov = coverage_count.get(kw, 0)
        tot = total_count.get(kw, 0)
        keyword_rows.append({
            "keyword": kw,
            "coverage_count": cov,
            "coverage_ratio": cov / n_for_ratios if n_for_ratios else 0.0,
            "total_occurrences": tot,
            "avg_per_query": tot / n_for_ratios if n_for_ratios else 0.0,
        })
    keyword_rows.sort(key=lambda r: (-r["coverage_count"], r["keyword"]))

    validity_total = sum(validity.values())
    validity_ratios = {
        k: (v / validity_total if validity_total else 0.0) for k, v in validity.items()
    }

    unique_keywords_used = sum(1 for r in keyword_rows if r["coverage_count"] > 0)

    # Per-statement-type rows -- supplementary breakdown, useful for the
    # report but not the headline number.
    type_rows = []
    for stmt_type, ctr in sorted(by_type.items(), key=lambda kv: -sum(kv[1].values())):
        total = sum(ctr.values())
        type_rows.append({
            "type": stmt_type,
            "total": total,
            "counts": dict(ctr),
            "ok_ratio": (ctr.get("ok", 0) / total) if total else 0.0,
        })

    summary = {
        "run_dir": str(run_dir),
        "total_test_cases": n_for_ratios,
        "total_workloads_seen": n_workloads,
        "unique_keywords_used": unique_keywords_used,
        "total_keywords_known": len(SQLITE_KEYWORDS),
        "validity_counts": dict(validity),
        "validity_ratios": validity_ratios,
        "validity_by_statement_type": type_rows,
        "keywords": keyword_rows,
        "top30": keyword_rows[:30],
    }

    (run_dir / "characteristics.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8",
    )
    (run_dir / "characteristics.txt").write_text(_format_text(summary), encoding="utf-8")
    return summary


# ---------------------------------------------------------------------------
# Pretty-printing
# ---------------------------------------------------------------------------

def _format_text(summary: dict) -> str:
    n = summary.get("total_test_cases", summary.get("total_queries", 0))
    unique = summary.get("unique_keywords_used", 0)
    total_kw = summary.get("total_keywords_known", len(SQLITE_KEYWORDS))
    counts = summary["validity_counts"]
    ratios = summary["validity_ratios"]
    valid_pct = ratios.get("ok", 0.0) * 100

    lines: list[str] = []
    lines.append("Characteristics of generated SQL queries")
    lines.append("=" * 60)
    lines.append(f"Total test cases analyzed : {n}")
    lines.append(f"Unique keywords exercised : {unique} / {total_kw}")
    lines.append(f"Overall validity (ok)     : {valid_pct:.1f}% of test cases")
    lines.append("")
    lines.append("Validity by outcome:")
    lines.append(f"  {'BUCKET':<14} {'COUNT':>8} {'RATIO':>8}")
    order = ("ok", "syntax_error", "runtime_error", "timeout", "crash")
    for k in order:
        if k in counts:
            lines.append(f"  {k:<14} {counts[k]:>8d} {ratios[k]*100:>7.1f}%")
    for k, v in counts.items():
        if k not in order:
            lines.append(f"  {k:<14} {v:>8d} {ratios[k]*100:>7.1f}%")
    lines.append("")
    type_rows = summary.get("validity_by_statement_type", [])
    if type_rows:
        lines.append("Validity by statement type:")
        lines.append(f"  {'TYPE':<10} {'TOTAL':>8} {'OK%':>7} {'ERR':>7} {'SYN':>7} {'TO':>5} {'CR':>4}")
        for r in type_rows:
            c = r["counts"]
            lines.append(
                f"  {r['type']:<10} {r['total']:>8d} {r['ok_ratio']*100:>6.1f}% "
                f"{c.get('runtime_error', 0):>7d} {c.get('syntax_error', 0):>7d} "
                f"{c.get('timeout', 0):>5d} {c.get('crash', 0):>4d}"
            )
        lines.append("")
    lines.append("Top 30 keywords (by coverage):")
    lines.append(f"  {'KEYWORD':<14} {'COVERAGE':>10} {'COV%':>8} {'AVG/Q':>8}")
    for row in summary["top30"]:
        lines.append(
            f"  {row['keyword']:<14} {row['coverage_count']:>10d} "
            f"{row['coverage_ratio']*100:>7.1f}% {row['avg_per_query']:>8.3f}"
        )
    lines.append("")
    lines.append(
        f"SUMMARY: {n} test cases, {unique}/{total_kw} keywords used, {valid_pct:.1f}% valid"
    )
    return "\n".join(lines)


def print_stats(summary: dict) -> None:
    print(_format_text(summary))


__all__ = [
    "SQLITE_KEYWORDS",
    "count_keywords",
    "classify_validity",
    "statement_type",
    "collect_stats",
    "print_stats",
]
