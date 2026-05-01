import os

PATCHED_SQLITE = os.environ.get("PATCHED_SQLITE", "/usr/bin/sqlite3-3.39.4")
VANILLA_SQLITE = os.environ.get("VANILLA_SQLITE", "/usr/bin/sqlite3")
COVERAGE_SQLITE = os.environ.get("COVERAGE_SQLITE", "/usr/bin/sqlite3-coverage")
COVERAGE_BUILD_DIR = os.environ.get("COVERAGE_BUILD_DIR", "/opt/sqlite3-coverage/build")
COVERAGE_SRC_DIR = os.environ.get("COVERAGE_SRC_DIR", "/opt/sqlite3-coverage")
DEFAULT_TIMEOUT_SEC = int(os.environ.get("TEST_DB_TIMEOUT", "2"))
DEFAULT_OUTPUT_DIR = os.environ.get("TEST_DB_OUTPUT", "output")
