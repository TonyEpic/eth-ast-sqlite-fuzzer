import argparse
import os
from pathlib import Path

from test_db.config import DEFAULT_OUTPUT_DIR, DEFAULT_TIMEOUT_SEC
from test_db.executor.sqlite_runner import run_on_patched, run_on_vanilla
from test_db.generator.workload_generator import generate_workload
from test_db.harness.runner import print_summary, run_experiment
from test_db.harness.stats import collect_stats, print_stats
from test_db.interfaces import RunOutcome
from test_db.oracle.classifier import classify_single
from test_db.oracle.differential import compare_results
from test_db.storage.artifacts import new_workload_id, save_outcome, save_sql
from test_db.triage import scaffold_one, scaffold_run


def _cmd_run(args: argparse.Namespace) -> None:
    """Ad-hoc mode: generate N workloads sequentially with full per-workload JSON."""
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for _ in range(args.num_queries):
        workload_id = new_workload_id()
        workload = generate_workload()
        save_sql(output_dir, workload_id, workload)

        patched = run_on_patched(workload.statements)
        classification, reason = classify_single(patched)

        vanilla = None
        if args.diff:
            vanilla = run_on_vanilla(workload.statements)
            same, diff_reason = compare_results(patched, vanilla)
            if not same:
                classification = "mismatch"
                reason = diff_reason

        outcome = RunOutcome(
            workload_id=workload_id,
            workload=workload,
            patched=patched,
            vanilla=vanilla,
            classification=classification,
            reason=reason,
        )
        save_outcome(output_dir, outcome)
        print(f"{workload_id}: {classification} - {reason}")


def _cmd_experiment(args: argparse.Namespace) -> None:
    """Controlled experiment: generate until --queries statements have been produced."""
    output_dir = Path(args.output_dir)
    summary = run_experiment(
        output_dir=output_dir,
        target_queries=args.queries,
        workers=args.workers,
        diff=args.diff,
        timeout_sec=args.timeout,
        keep_sql=args.keep_sql,
        progress_every=args.progress_every,
        coverage=args.coverage,
        metamorphic=args.metamorphic,
    )
    print_summary(summary)

    # Auto-generate the characteristics report so the run dir is self-contained.
    run_dir = Path(summary["paths"]["workloads_jsonl"]).parent
    try:
        stats_summary = collect_stats(run_dir)
        print(
            f"[stats] wrote {run_dir / 'characteristics.json'} "
            f"({stats_summary['total_queries']} statements analyzed)",
            flush=True,
        )
    except FileNotFoundError as e:
        print(f"[stats] skipped: {e}", flush=True)


def _cmd_stats(args: argparse.Namespace) -> None:
    if args.run_dir:
        run_dir = Path(args.run_dir)
    else:
        runs = Path(args.output_dir) / "runs"
        if not runs.is_dir():
            raise SystemExit(f"No runs found under {runs}")
        candidates = [p for p in runs.iterdir() if p.is_dir() and (p / "workloads.jsonl").is_file()]
        if not candidates:
            raise SystemExit(f"No completed runs under {runs}")
        run_dir = max(candidates, key=lambda p: p.stat().st_mtime)
    summary = collect_stats(run_dir)
    print_stats(summary)
    print(f"[stats] wrote {run_dir / 'characteristics.json'} and characteristics.txt")


def _cmd_triage(args: argparse.Namespace) -> None:
    out_root = Path(args.out)
    if args.flagged:
        target = scaffold_one(Path(args.flagged), out_root)
        print(f"wrote {target}")
        return

    if args.run_dir:
        run_dir = Path(args.run_dir)
    else:
        runs = Path(args.output_dir) / "runs"
        if not runs.is_dir():
            raise SystemExit(f"No runs found under {runs}")
        candidates = [p for p in runs.iterdir() if p.is_dir() and (p / "summary.json").is_file()]
        if not candidates:
            raise SystemExit(f"No completed runs under {runs}")
        run_dir = max(candidates, key=lambda p: p.stat().st_mtime)

    created = scaffold_run(run_dir, out_root)
    if not created:
        print(f"no flagged outcomes in {run_dir}")
        return
    for p in created:
        print(f"wrote {p}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="test-db", description="SQLite fuzzer")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)

    # Legacy flat flags so `test-db --num-queries 5 --diff` still works.
    parser.add_argument("--num-queries", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--diff", action="store_true", help=argparse.SUPPRESS)

    sub = parser.add_subparsers(dest="cmd")

    run_p = sub.add_parser("run", help="Generate N workloads sequentially (ad-hoc).")
    run_p.add_argument("--num-queries", type=int, default=1)
    run_p.add_argument("--diff", action="store_true")
    run_p.set_defaults(func=_cmd_run)

    exp_p = sub.add_parser("experiment", help="Controlled experiment up to a query budget.")
    exp_p.add_argument("--queries", type=int, default=10000,
                       help="Target number of generated queries (default: 10000).")
    exp_p.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1),
                       help="Number of parallel worker processes.")
    exp_p.add_argument("--diff", action="store_true",
                       help="Also run on vanilla SQLite and compare.")
    exp_p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SEC,
                       help="Per-statement timeout in seconds.")
    exp_p.add_argument("--keep-sql", action="store_true",
                       help="Persist every workload's SQL (default: only flagged).")
    exp_p.add_argument("--progress-every", type=int, default=100,
                       help="Print a progress line every N workloads.")
    exp_p.add_argument("--coverage", action="store_true",
                       help="Also run on the gcov-instrumented binary and emit a coverage report.")
    exp_p.add_argument("--metamorphic", action="store_true",
                       help="Apply metamorphic single-engine oracle (predicate commutation + NoREC).")
    exp_p.set_defaults(func=_cmd_experiment)

    tri_p = sub.add_parser(
        "triage",
        help="Scaffold spec-format bug-reproducers/<id>/ folders from flagged outcomes.",
    )
    tri_p.add_argument("--run-dir", default=None,
                       help="Run dir to scaffold from (default: latest under output/runs).")
    tri_p.add_argument("--flagged", default=None,
                       help="Path to a single flagged <id>.json file (overrides --run-dir).")
    tri_p.add_argument("--out", default="bug-reproducers",
                       help="Output directory (default: bug-reproducers/).")
    tri_p.set_defaults(func=_cmd_triage)

    sta_p = sub.add_parser(
        "stats",
        help="Compute SQL keyword coverage and query-validity report for a run.",
    )
    sta_p.add_argument("--run-dir", default=None,
                       help="Run dir to analyze (default: latest under output/runs).")
    sta_p.set_defaults(func=_cmd_stats)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd is None:
        # Backwards-compatible default: behave like `run`.
        args.func = _cmd_run
        if args.num_queries is None:
            args.num_queries = 1

    args.func(args)


if __name__ == "__main__":
    main()
