"""gcov coverage helpers.

Wraps the `gcovr` CLI: clears any stale `.gcda` files in the instrumented
build dir before a run, and after the run produces a JSON summary plus a
plain-text line-coverage report under the run directory.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from test_db.config import COVERAGE_BUILD_DIR, COVERAGE_SRC_DIR


def reset_coverage_data(build_dir: str = COVERAGE_BUILD_DIR) -> int:
    """Delete every .gcda file in the build dir. Returns the count removed."""
    p = Path(build_dir)
    if not p.is_dir():
        return 0
    n = 0
    for gcda in p.rglob("*.gcda"):
        try:
            gcda.unlink()
            n += 1
        except OSError:
            pass
    return n


def _have_gcovr() -> bool:
    return shutil.which("gcovr") is not None


def collect_coverage(
    out_dir: Path,
    src_dir: str = COVERAGE_SRC_DIR,
    build_dir: str = COVERAGE_BUILD_DIR,
) -> Optional[dict]:
    """Run gcovr against the build dir and write reports under `out_dir`.

    Produces:
      out_dir/coverage-summary.json   (gcovr --json-summary)
      out_dir/coverage.txt            (gcovr default text report)

    Returns a small dict with line/branch/function percentages, or None if
    gcovr is unavailable / no .gcda files were found.
    """
    if not _have_gcovr():
        return None
    if not any(Path(build_dir).rglob("*.gcda")):
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / "coverage-summary.json"
    text_path = out_dir / "coverage.txt"

    # JSON summary for the harness to embed.
    subprocess.run(
        [
            "gcovr",
            "--root", src_dir,
            "--object-directory", build_dir,
            "--gcov-ignore-errors", "source_not_found",
            "--gcov-ignore-errors", "no_working_dir_found",
            "--exclude", ".*conftest.*",
            "--json-summary-pretty",
            "-o", str(summary_path),
        ],
        check=False,
        capture_output=True,
    )

    # Human-readable text report for the operator / report.pdf.
    subprocess.run(
        [
            "gcovr",
            "--root", src_dir,
            "--object-directory", build_dir,
            "--gcov-ignore-errors", "source_not_found",
            "--gcov-ignore-errors", "no_working_dir_found",
            "--exclude", ".*conftest.*",
            "-o", str(text_path),
        ],
        check=False,
        capture_output=True,
    )

    if not summary_path.is_file():
        return None
    try:
        data = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None

    return {
        "line_percent": data.get("line_percent"),
        "branch_percent": data.get("branch_percent"),
        "function_percent": data.get("function_percent"),
        "lines_covered": data.get("line_covered"),
        "lines_total": data.get("line_total"),
        "branches_covered": data.get("branch_covered"),
        "branches_total": data.get("branch_total"),
        "functions_covered": data.get("function_covered"),
        "functions_total": data.get("function_total"),
        "summary_path": str(summary_path),
        "text_path": str(text_path),
    }


__all__ = ["reset_coverage_data", "collect_coverage"]
