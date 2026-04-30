import argparse
from pathlib import Path

from test_db.config import DEFAULT_OUTPUT_DIR
from test_db.generator.workload_generator import generate_workload
from test_db.executor.sqlite_runner import run_on_patched, run_on_vanilla
from test_db.oracle.classifier import classify_single
from test_db.oracle.differential import compare_results
from test_db.interfaces import RunOutcome
from test_db.storage.artifacts import new_workload_id, save_sql, save_outcome


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-queries", type=int, default=1)
    parser.add_argument("--diff", action="store_true")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

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


if __name__ == "__main__":
    main()