# ETH AST 2026 — SQLite Fuzzer (`test-db`)

Team project for the Automated Software Testing course (Spring 2026), Part 1.

**Team:** Toni Krstic, Pirmin Ballmer

A generation-based SQL fuzzer targeting the patched SQLite 3.39.4 binary
provided by the course. The tool generates random multi-statement SQL
workloads with a stateful schema model, executes each workload against the
patched binary (and optionally against the vanilla SQLite 3.51.1 for
differential testing), classifies the outcome, and emits machine-readable
artifacts for code-coverage, keyword-coverage, validity and bug-reproducer
reports.

---

## Quickstart

The tool is designed to run inside the course's
[`theosotr/sqlite3-test`](https://hub.docker.com/r/theosotr/sqlite3-test) base
image, which provides the patched SQLite (`/usr/bin/sqlite3-3.39.4`), vanilla
SQLite 3.51.1 (`/usr/bin/sqlite3`), and the SQLite source tree at
`/home/test/sqlite3-src`. The provided `Dockerfile` builds an additional
`gcov`-instrumented binary (`/usr/bin/sqlite3-coverage`) and installs the
`test-db` entrypoint at `/usr/bin/test-db`.

### Build the image

```bash
docker build -t test-db .
```

### Show the CLI

```bash
docker run --rm test-db test-db --help
```

### Smoke test (a few workloads)

```bash
docker run --rm -v "$PWD/output:/opt/test-db/output" test-db \
    test-db run --num-queries 5 --diff
```

### Run the 10 000-query campaign (bug-finding + characteristics + perf)

```bash
docker run --rm -v "$PWD/output:/opt/test-db/output" test-db \
    test-db experiment --queries 10000 --workers 7 --diff --keep-sql
```

### Run the coverage campaign

```bash
docker run --rm -v "$PWD/output:/opt/test-db/output" test-db \
    test-db experiment --queries 10000 --coverage
```

> Note: the coverage campaign forces `--workers 1` because the `gcov`-
> instrumented binary writes to shared `.gcda` files; concurrent writers
> would corrupt them.

### Scaffold bug reproducers

```bash
docker run --rm -v "$PWD:/work" -w /work test-db \
    test-db triage --out bug-reproducers
```

### Re-run keyword / validity stats on an existing run

```bash
docker run --rm -v "$PWD/output:/opt/test-db/output" test-db \
    test-db stats
```

The `experiment` subcommand already invokes `stats` automatically; this is
mainly useful if you re-import an older run directory.

---

## CLI reference

```
test-db [--output-dir DIR] <subcommand> [options]
```

`--output-dir` (default: `output`) is where run artifacts are written.

### `run`

Generate N workloads sequentially with full per-workload JSON saved next to
the SQL. Intended for ad-hoc development; not used for graded numbers.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--num-queries N` | `1` | Number of workloads to generate. |
| `--diff` | off | Also execute on vanilla SQLite; flag mismatches. |

### `experiment`

The controlled experiment that produces all numbers in the report.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--queries N` | `10000` | Stop after at least N generated statements. |
| `--workers W` | `cpu_count() - 1` | Parallel worker processes. |
| `--diff` | off | Run on vanilla too, enabling the differential oracle. |
| `--coverage` | off | Run on the `gcov`-instrumented binary and run `gcovr`. |
| `--timeout S` | `2` | Per-statement timeout (seconds). |
| `--keep-sql` | off | Persist every workload's SQL (default: only flagged). |
| `--progress-every K` | `100` | Progress line every K workloads. |

Writes per-run artifacts under `output/runs/<run_id>/`:

| File | Description |
| --- | --- |
| `workloads.jsonl` | One line per workload: id, classification, timings, per-statement records. |
| `summary.json` | Aggregate numbers (throughput, classification counts, coverage). |
| `flagged/<id>.{sql,json}` | Full SQL + outcome for every non-`ok` workload. |
| `workloads/<id>.sql` | Per-workload SQL (only when `--keep-sql`). |
| `characteristics.{json,txt}` | Keyword coverage / frequency + validity report. |
| `coverage-summary.json`, `coverage.txt` | `gcovr` output (only when `--coverage`). |

### `triage`

Scaffolds spec-format bug reproducers from flagged outcomes.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--run-dir DIR` | latest run | Source run directory. |
| `--flagged FILE` | — | Scaffold one specific flagged `<id>.json`. |
| `--out DIR` | `bug-reproducers` | Destination root. |

Each scaffolded folder follows the layout required by the spec:

```
bug-reproducers/<id>/
├── original_test.sql   exact SQL the fuzzer produced
├── reduced_test.sql    auto-copied from original; minimize by hand
├── test.db             empty (workloads create their own state)
└── README.md           Summary / Minimized query / Actual output / Expectation
```

Every folder must be human-reviewed before submission: fix the README
summary, run the reduced query, confirm it still triggers.

### `stats`

Re-compute `characteristics.{json,txt}` from a run's `workloads.jsonl`.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--run-dir DIR` | latest run | Source run directory. |

---

## Environment variables

All paths in [`src/test_db/config.py`](src/test_db/config.py) are overridable
via env vars, so the tool also runs outside Docker if you point it at local
binaries.

| Variable | Default | Meaning |
| --- | --- | --- |
| `PATCHED_SQLITE` | `/usr/bin/sqlite3-3.39.4` | Target binary (broken 3.39.4). |
| `VANILLA_SQLITE` | `/usr/bin/sqlite3` | Reference binary (3.51.1). |
| `COVERAGE_SQLITE` | `/usr/bin/sqlite3-coverage` | `gcov`-instrumented binary. |
| `COVERAGE_BUILD_DIR` | `/opt/sqlite3-coverage/build` | Build dir holding `.gcno` + `.gcda`. |
| `COVERAGE_SRC_DIR` | `/opt/sqlite3-coverage` | SQLite source root for `gcovr --root`. |
| `TEST_DB_TIMEOUT` | `2` | Per-statement timeout (seconds). |
| `TEST_DB_OUTPUT` | `output` | Default output directory. |

---

## Repository layout

```
src/test_db/
├── config.py              env-overridable paths / timeouts
├── interfaces.py          shared dataclasses (frozen contract with generator)
├── main.py                CLI entrypoint
├── generator/             SQL workload generation (grammar + schema state)
├── executor/              subprocess wrappers around sqlite3
├── oracle/                classifier + differential comparison + normalizer
├── harness/               experiment runner, coverage, stats
├── storage/               per-workload JSON artifact writers (used by `run`)
└── triage/                bug-reproducer scaffolder
tests/                     pytest smoke + unit tests
Dockerfile                 base image + gcov binary + /usr/bin/test-db
```

---

## Local development (without Docker)

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src
python3 -m pytest tests -q
```

For ad-hoc runs without the patched binaries available, point the env vars
at any `sqlite3` on `$PATH`:

```bash
PATCHED_SQLITE=$(command -v sqlite3) \
VANILLA_SQLITE=$(command -v sqlite3) \
PYTHONPATH=src python3 -m test_db.main run --num-queries 3
```

---

## Notes for graders

- Tool entrypoint is `/usr/bin/test-db` inside the image.
- The 10 000-query controlled experiment is `test-db experiment --queries 10000 --workers 7 --diff` plus a separate `--coverage` run.
- Bug reproducers live under `bug-reproducers/` and follow the spec format.
- The technical report (`report.pdf`) cites the numbers from `summary.json`, `characteristics.{json,txt}`, and `coverage-summary.json` of the campaign runs.
