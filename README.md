# ETH AST 2026 – SQLite Fuzzer

Team project for Automated Software Testing (Spring 2026).

## Team
- Toni Krstic
- Pirmin Ballmer

## Goal
Build a Python-based SQL testing tool for the patched SQLite target.

## Current status
Minimal end-to-end prototype:
- generates one SQL workload
- executes it on patched SQLite
- optionally compares against vanilla SQLite
- stores SQL and JSON results

## Repo structure
- `src/test_db/generator/`: SQL workload generation
- `src/test_db/executor/`: running SQLite
- `src/test_db/oracle/`: classification and differential comparison
- `src/test_db/storage/`: saving generated SQL and outcomes
- `tests/`: small smoke tests

## Run locally
```bash
PYTHONPATH=src python -m test_db.main --num-queries 1 --diff