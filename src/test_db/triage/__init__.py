"""Bug-reproducer scaffolding.

Turns a flagged workload outcome (as written by the experiment harness under
`flagged/<id>.json` + `flagged/<id>.sql`) into a `bug-reproducers/<id>/`
directory matching the spec format:

  bug-reproducers/<id>/
    original_test.sql   exact SQL the fuzzer produced
    reduced_test.sql    starts as a copy; reviewer minimizes by hand
    test.db             empty file (workloads create their own schema)
    README.md           Summary / Minimized query / Actual output / Expectation
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional


def _format_actual(record: dict) -> str:
    """Pull the most relevant 'what happened' text out of a flagged record."""
    classification = record.get("classification", "?")
    reason = record.get("reason", "")

    lines = [
        f"classification: {classification}",
        f"reason: {reason}",
    ]
    return "\n".join(lines)


def _expectation_for(classification: str) -> str:
    if classification == "crash":
        return "The query should execute without crashing the SQLite process."
    if classification == "timeout":
        return "The query should terminate within the per-statement timeout."
    if classification == "error":
        return "The query is well-formed and should execute without raising an error."
    if classification == "mismatch":
        return (
            "The patched SQLite (3.39.4) and the vanilla SQLite (3.51.1) should "
            "produce the same result for this query."
        )
    return "The query should execute and produce the documented result."


def _readme_for(record: dict, sql: str) -> str:
    classification = record.get("classification", "?")
    reason = record.get("reason", "")
    return f"""## Summary

<!--
Auto-generated stub. A human reviewer must edit this section to confirm the
behavior is a real bug (not intended SQLite behavior or a known difference
between SQLite 3.39.4 and 3.51.1) before submission.
-->

The fuzzer flagged this workload as **{classification}**: {reason}

## Minimized query

```sql
{sql.strip()}
```

## Actual output

```
{_format_actual(record)}
```

## Expectation

{_expectation_for(classification)}
"""


def scaffold_one(flagged_json: Path, out_root: Path) -> Path:
    """Create one bug-reproducers/<id>/ folder from a flagged JSON record."""
    record = json.loads(flagged_json.read_text(encoding="utf-8"))
    workload_id = record.get("workload_id") or flagged_json.stem
    sql = record.get("sql_text")
    if not sql:
        # Older flagged record format: read sibling .sql.
        sql_path = flagged_json.with_suffix(".sql")
        sql = sql_path.read_text(encoding="utf-8") if sql_path.is_file() else ""

    target = out_root / workload_id
    target.mkdir(parents=True, exist_ok=True)

    (target / "original_test.sql").write_text(sql, encoding="utf-8")
    # reviewer will minimize this by hand; start from the original.
    (target / "reduced_test.sql").write_text(sql, encoding="utf-8")
    # spec allows an empty test.db when the SQL sets up its own state.
    (target / "test.db").write_bytes(b"")
    (target / "README.md").write_text(_readme_for(record, sql), encoding="utf-8")

    return target


def scaffold_run(run_dir: Path, out_root: Path) -> list[Path]:
    """Scaffold reproducers for every flagged outcome in a run directory."""
    flagged_dir = run_dir / "flagged"
    if not flagged_dir.is_dir():
        return []
    created: list[Path] = []
    for js in sorted(flagged_dir.glob("*.json")):
        created.append(scaffold_one(js, out_root))
    return created


__all__ = ["scaffold_one", "scaffold_run"]
