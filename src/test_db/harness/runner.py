"""Controlled-experiment harness.

Generates a target number of queries (counted as SQL statements, excluding
DROP TABLE setup), executes each workload, and writes:

  output/runs/<run_id>/workloads.jsonl   (one line per workload outcome)
  output/runs/<run_id>/summary.json      (aggregate metrics)

Designed for the spec's 10k-query controlled experiment. Workers are
multiprocess so wall-clock throughput scales with cores.
"""
from __future__ import annotations

import json
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from test_db.config import DEFAULT_TIMEOUT_SEC
from test_db.executor.sqlite_runner import run_on_patched, run_on_vanilla
from test_db.generator.workload_generator import generate_workload
from test_db.oracle.classifier import classify_single
from test_db.oracle.differential import compare_results


# ---------------------------------------------------------------------------
# What counts as a "query"
# ---------------------------------------------------------------------------
# The spec talks about "10,000 queries" but our generator emits multi-statement
# workloads (DROP / CREATE / INSERT / SELECT). We count any statement that is
# not pure setup as a query. This matches what the keyword-coverage and
# validity reports will measure.
_SETUP_PREFIXES = ("DROP TABLE", "CREATE TABLE", "CREATE INDEX", "CREATE VIEW", "INSERT INTO")


def _is_query(sql: str) -> bool:
    head = sql.lstrip().upper()
    return not any(head.startswith(p) for p in _SETUP_PREFIXES)


# ---------------------------------------------------------------------------
# Per-worker job
# ---------------------------------------------------------------------------

@dataclass
class WorkloadOutcome:
    workload_id: str
    classification: str
    reason: str
    num_statements: int
    num_queries: int
    num_executed_statements: int
    num_executed_queries: int
    gen_ms: float
    exec_patched_ms: float
    exec_vanilla_ms: float
    sql_text: str


def _run_one(args: tuple[bool, int]) -> WorkloadOutcome:
    """Generate one workload, run it, return a compact outcome record."""
    diff, timeout_sec = args

    workload_id = uuid.uuid4().hex[:8]

    t = time.perf_counter()
    workload = generate_workload()
    gen_ms = (time.perf_counter() - t) * 1000

    t = time.perf_counter()
    patched = run_on_patched(workload.statements, timeout_sec=timeout_sec)
    exec_patched_ms = (time.perf_counter() - t) * 1000

    classification, reason = classify_single(patched)

    exec_vanilla_ms = 0.0
    if diff:
        t = time.perf_counter()
        vanilla = run_on_vanilla(workload.statements, timeout_sec=timeout_sec)
        exec_vanilla_ms = (time.perf_counter() - t) * 1000
        same, diff_reason = compare_results(patched, vanilla)
        if not same:
            classification = "mismatch"
            reason = diff_reason

    queries = [s for s in workload.statements if _is_query(s)]
    n_exec_stmts = len(patched.statements)
    n_exec_queries = sum(1 for s in patched.statements if _is_query(s.sql))

    return WorkloadOutcome(
        workload_id=workload_id,
        classification=classification,
        reason=reason,
        num_statements=len(workload.statements),
        num_queries=len(queries),
        num_executed_statements=n_exec_stmts,
        num_executed_queries=n_exec_queries,
        gen_ms=gen_ms,
        exec_patched_ms=exec_patched_ms,
        exec_vanilla_ms=exec_vanilla_ms,
        sql_text=workload.sql_text,
    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run_experiment(
    output_dir: Path,
    target_queries: int,
    workers: int,
    diff: bool,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    keep_sql: bool = False,
    progress_every: int = 100,
) -> dict:
    """Run workloads until at least `target_queries` queries have been generated.

    Returns the summary dict and writes `workloads.jsonl` + `summary.json` under
    `output/runs/<run_id>/`. If `keep_sql=True`, every workload's SQL is also
    written to `workloads/<id>.sql` (off by default to keep the output small).
    """
    run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
    run_dir = output_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    flagged_dir = run_dir / "flagged"
    sql_dir = run_dir / "workloads"
    if keep_sql:
        sql_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = run_dir / "workloads.jsonl"
    summary_path = run_dir / "summary.json"

    counts: dict[str, int] = {}
    total_queries = 0
    total_executed_queries = 0
    total_statements = 0
    total_executed_statements = 0
    total_gen_ms = 0.0
    total_exec_ms = 0.0
    n_workloads = 0

    wall_start = time.perf_counter()

    job = (diff, timeout_sec)
    with jsonl_path.open("w", encoding="utf-8") as jf, \
         ProcessPoolExecutor(max_workers=workers) as pool:

        # Submit work in waves so we never queue more than ~2x workers ahead.
        # This lets us stop promptly when we hit the query budget.
        in_flight: set = set()
        wave = max(workers * 2, 4)

        def submit_more():
            for _ in range(wave - len(in_flight)):
                in_flight.add(pool.submit(_run_one, job))

        submit_more()

        while in_flight and total_queries < target_queries:
            done = next(as_completed(in_flight))
            in_flight.remove(done)
            outcome: WorkloadOutcome = done.result()
            n_workloads += 1
            total_queries += outcome.num_queries
            total_executed_queries += outcome.num_executed_queries
            total_statements += outcome.num_statements
            total_executed_statements += outcome.num_executed_statements
            total_gen_ms += outcome.gen_ms
            total_exec_ms += outcome.exec_patched_ms + outcome.exec_vanilla_ms
            counts[outcome.classification] = counts.get(outcome.classification, 0) + 1

            record = asdict(outcome)
            if not keep_sql:
                # Keep SQL only for non-ok outcomes so logs stay small.
                if outcome.classification == "ok":
                    record.pop("sql_text", None)
            jf.write(json.dumps(record) + "\n")

            if outcome.classification != "ok":
                flagged_dir.mkdir(parents=True, exist_ok=True)
                (flagged_dir / f"{outcome.workload_id}.sql").write_text(
                    outcome.sql_text, encoding="utf-8"
                )
                (flagged_dir / f"{outcome.workload_id}.json").write_text(
                    json.dumps(asdict(outcome), indent=2), encoding="utf-8"
                )

            if keep_sql:
                (sql_dir / f"{outcome.workload_id}.sql").write_text(
                    outcome.sql_text, encoding="utf-8"
                )

            if n_workloads % progress_every == 0:
                elapsed = time.perf_counter() - wall_start
                qpm = total_queries / elapsed * 60 if elapsed > 0 else 0
                print(
                    f"[{n_workloads:>5} workloads | {total_queries:>6} queries | "
                    f"{qpm:>7.0f} q/min] " + ", ".join(
                        f"{k}={v}" for k, v in sorted(counts.items())
                    ),
                    flush=True,
                )

            if total_queries < target_queries:
                submit_more()

        # Cancel anything still queued past the budget.
        for fut in in_flight:
            fut.cancel()

    wall_elapsed = time.perf_counter() - wall_start

    summary = {
        "run_id": run_id,
        "diff_enabled": diff,
        "workers": workers,
        "timeout_sec": timeout_sec,
        "target_queries": target_queries,
        "wall_seconds": wall_elapsed,
        "workloads": n_workloads,
        "statements": {
            "generated": total_statements,
            "executed": total_executed_statements,
        },
        "queries": {
            "generated": total_queries,
            "executed": total_executed_queries,
        },
        "throughput_per_minute": {
            "queries_generated": total_queries / wall_elapsed * 60 if wall_elapsed else 0,
            "queries_generated_and_executed": total_executed_queries / wall_elapsed * 60 if wall_elapsed else 0,
            "workloads": n_workloads / wall_elapsed * 60 if wall_elapsed else 0,
        },
        "time_breakdown_ms_per_workload": {
            "generation": total_gen_ms / n_workloads if n_workloads else 0,
            "execution": total_exec_ms / n_workloads if n_workloads else 0,
        },
        "classification_counts": counts,
        "validity": {
            "ok_workloads": counts.get("ok", 0),
            "error_workloads": counts.get("error", 0),
            "crash_workloads": counts.get("crash", 0),
            "timeout_workloads": counts.get("timeout", 0),
            "mismatch_workloads": counts.get("mismatch", 0),
            "ok_workload_ratio": (counts.get("ok", 0) / n_workloads) if n_workloads else 0,
        },
        "paths": {
            "workloads_jsonl": str(jsonl_path),
            "flagged_dir": str(flagged_dir),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def print_summary(summary: dict) -> None:
    tp = summary["throughput_per_minute"]
    print()
    print("=" * 60)
    print(f"Run {summary['run_id']} finished in {summary['wall_seconds']:.1f}s")
    print(f"  workloads:           {summary['workloads']}")
    print(f"  queries generated:   {summary['queries']['generated']}")
    print(f"  queries executed:    {summary['queries']['executed']}")
    print(f"  q/min generated:     {tp['queries_generated']:.0f}")
    print(f"  q/min gen + exec:    {tp['queries_generated_and_executed']:.0f}")
    print(f"  classifications:     {summary['classification_counts']}")
    print("=" * 60)


__all__ = ["run_experiment", "print_summary"]
