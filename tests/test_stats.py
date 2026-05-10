import json
from pathlib import Path

from test_db.harness.stats import (
    classify_validity,
    collect_stats,
    count_keywords,
    statement_type,
)


# ---------- count_keywords ------------------------------------------------

def test_count_keywords_basic():
    c = count_keywords("SELECT * FROM t WHERE c0 > 1;")
    assert c["SELECT"] == 1
    assert c["FROM"] == 1
    assert c["WHERE"] == 1
    # `t`, `c0` are identifiers, not keywords.
    assert "t" not in c and "c0" not in c


def test_count_keywords_case_insensitive():
    c = count_keywords("select * from t where c0 > 1")
    assert c["SELECT"] == 1 and c["FROM"] == 1 and c["WHERE"] == 1


def test_count_keywords_multiple_occurrences():
    c = count_keywords("SELECT * FROM t WHERE c0 IN (SELECT c0 FROM u);")
    assert c["SELECT"] == 2
    assert c["FROM"] == 2


def test_count_keywords_join_clauses():
    c = count_keywords("SELECT * FROM t LEFT OUTER JOIN u ON t.x = u.y;")
    assert c["JOIN"] == 1 and c["LEFT"] == 1 and c["OUTER"] == 1 and c["ON"] == 1


def test_count_keywords_ignores_string_literals_loosely():
    # Keywords inside strings still count (we don't run a real parser);
    # this is a known limitation, but generators rarely emit such strings.
    c = count_keywords("INSERT INTO t VALUES ('SELECT');")
    assert c["INSERT"] == 1 and c["INTO"] == 1 and c["VALUES"] == 1


# ---------- classify_validity --------------------------------------------

def test_classify_validity_ok():
    assert classify_validity(0, "", False) == "ok"


def test_classify_validity_syntax_error():
    assert classify_validity(1, "syntax error", False) == "syntax_error"


def test_classify_validity_runtime_error():
    assert classify_validity(1, "no such table", False) == "runtime_error"


def test_classify_validity_timeout_wins_over_error():
    assert classify_validity(1, "something", True) == "timeout"


def test_classify_validity_crash():
    assert classify_validity(-11, "", False) == "crash"


# ---------- collect_stats end-to-end -------------------------------------

def _make_run_dir(tmp_path: Path, workloads: list[dict]) -> Path:
    run = tmp_path / "run1"
    run.mkdir()
    with (run / "workloads.jsonl").open("w", encoding="utf-8") as f:
        for w in workloads:
            f.write(json.dumps(w) + "\n")
    return run


def test_collect_stats_writes_files_and_counts_correctly(tmp_path: Path):
    run = _make_run_dir(tmp_path, [
        {
            "workload_id": "a",
            "statements": [
                {"sql": "CREATE TABLE t(c0 INT);", "rc": 0, "stderr_kind": "", "timed_out": False},
                {"sql": "INSERT INTO t VALUES (1);", "rc": 0, "stderr_kind": "", "timed_out": False},
                {"sql": "SELECT * FROM t WHERE c0 = 1;", "rc": 0, "stderr_kind": "", "timed_out": False},
            ],
        },
        {
            "workload_id": "b",
            "statements": [
                {"sql": "SELECT * FROM missing;", "rc": 1, "stderr_kind": "no such table", "timed_out": False},
                {"sql": "garbage;", "rc": 1, "stderr_kind": "syntax error", "timed_out": False},
            ],
        },
    ])

    summary = collect_stats(run)

    assert (run / "characteristics.json").is_file()
    assert (run / "characteristics.txt").is_file()
    assert summary["total_queries"] == 5
    # Validity buckets.
    assert summary["validity_counts"]["ok"] == 3
    assert summary["validity_counts"]["syntax_error"] == 1
    assert summary["validity_counts"]["runtime_error"] == 1
    # Validity ratios sum to ~1.0
    assert abs(sum(summary["validity_ratios"].values()) - 1.0) < 1e-9

    # Keyword presence checks.
    kw = {r["keyword"]: r for r in summary["keywords"]}
    assert kw["SELECT"]["coverage_count"] == 2
    assert kw["FROM"]["coverage_count"] == 2
    assert kw["CREATE"]["coverage_count"] == 1
    assert kw["INSERT"]["coverage_count"] == 1
    assert kw["WHERE"]["coverage_count"] == 1
    # Top-30 returned.
    assert len(summary["top30"]) == 30


def test_collect_stats_handles_missing_statements_gracefully(tmp_path: Path):
    # Old-format records (no `statements` list) must be tolerated.
    run = _make_run_dir(tmp_path, [
        {"workload_id": "old1", "classification": "ok"},
    ])
    summary = collect_stats(run)
    assert summary["total_queries"] == 0


# ---------- new polish: unique_keywords_used + per-type validity ---------

def test_statement_type_basics():
    assert statement_type("SELECT * FROM t;") == "SELECT"
    assert statement_type("  insert into t values(1);") == "INSERT"
    assert statement_type("CREATE TABLE t(c INT);") == "CREATE"
    assert statement_type("xyz random;") == "OTHER"
    assert statement_type("") == "OTHER"


def test_collect_stats_reports_unique_keywords_used(tmp_path: Path):
    run = _make_run_dir(tmp_path, [{
        "workload_id": "a",
        "statements": [
            {"sql": "CREATE TABLE t(c0 INT);", "rc": 0, "stderr_kind": "", "timed_out": False},
            {"sql": "SELECT * FROM t WHERE c0 = 1;", "rc": 0, "stderr_kind": "", "timed_out": False},
        ],
    }])
    summary = collect_stats(run)
    # CREATE, TABLE, INT, SELECT, FROM, WHERE -> at least 5 unique keywords seen.
    assert summary["unique_keywords_used"] >= 5
    assert summary["total_keywords_known"] >= 140


def test_collect_stats_per_statement_type_validity(tmp_path: Path):
    run = _make_run_dir(tmp_path, [{
        "workload_id": "a",
        "statements": [
            # 2 valid SELECTs, 1 broken SELECT
            {"sql": "SELECT 1;", "rc": 0, "stderr_kind": "", "timed_out": False},
            {"sql": "SELECT 2;", "rc": 0, "stderr_kind": "", "timed_out": False},
            {"sql": "SELECT FROM;", "rc": 1, "stderr_kind": "syntax error", "timed_out": False},
            # 1 valid INSERT
            {"sql": "INSERT INTO t VALUES(1);", "rc": 0, "stderr_kind": "", "timed_out": False},
        ],
    }])
    summary = collect_stats(run)
    by_type = {r["type"]: r for r in summary["validity_by_statement_type"]}
    assert "SELECT" in by_type and "INSERT" in by_type
    assert by_type["SELECT"]["total"] == 3
    assert by_type["SELECT"]["counts"]["ok"] == 2
    assert by_type["SELECT"]["counts"]["syntax_error"] == 1
    assert abs(by_type["SELECT"]["ok_ratio"] - 2/3) < 1e-9
    assert by_type["INSERT"]["ok_ratio"] == 1.0
