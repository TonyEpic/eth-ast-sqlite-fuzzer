import json
import uuid
from pathlib import Path
from test_db.interfaces import GeneratedWorkload, RunOutcome


def new_workload_id() -> str:
    return str(uuid.uuid4())[:8]


def save_sql(output_dir: Path, workload_id: str, workload: GeneratedWorkload) -> Path:
    sql_dir = output_dir / "generated"
    sql_dir.mkdir(parents=True, exist_ok=True)
    path = sql_dir / f"{workload_id}.sql"
    path.write_text(workload.sql_text, encoding="utf-8")
    return path


def save_outcome(output_dir: Path, outcome: RunOutcome) -> Path:
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    path = results_dir / f"{outcome.workload_id}.json"
    path.write_text(json.dumps(outcome, default=lambda o: o.__dict__, indent=2), encoding="utf-8")
    return path