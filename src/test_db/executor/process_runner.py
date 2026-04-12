import subprocess
import time
from pathlib import Path
from test_db.interfaces import ExecutionResult


def run_sql_file(engine_path: str, sql_file: Path, timeout_sec: int) -> ExecutionResult:
    start = time.perf_counter()
    try:
        with sql_file.open("r", encoding="utf-8") as f:
            proc = subprocess.run(
                [engine_path],
                stdin=f,
                capture_output=True,
                text=True,
                timeout=timeout_sec,
            )
        duration_ms = (time.perf_counter() - start) * 1000
        return ExecutionResult(
            engine=engine_path,
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
            timed_out=False,
            duration_ms=duration_ms,
        )
    except subprocess.TimeoutExpired as e:
        duration_ms = (time.perf_counter() - start) * 1000
        return ExecutionResult(
            engine=engine_path,
            returncode=-1,
            stdout=e.stdout or "",
            stderr=e.stderr or "",
            timed_out=True,
            duration_ms=duration_ms,
        )