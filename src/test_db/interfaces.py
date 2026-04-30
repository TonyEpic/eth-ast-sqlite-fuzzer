from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class GeneratedWorkload:
    sql_text: str
    statements: List[str]
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class StatementResult:
    """Result of executing a single SQL statement in its own subprocess."""
    stmt_idx: int
    sql: str
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool
    duration_ms: float


@dataclass
class ExecutionResult:
    """Result of executing a workload (sequence of statements) on one engine."""
    engine: str
    statements: List[StatementResult]
    total_duration_ms: float
    timed_out: bool       # True if any statement timed out
    crashed: bool         # True if any statement was killed by a signal (rc < 0)


@dataclass
class RunOutcome:
    workload_id: str
    workload: GeneratedWorkload
    patched: ExecutionResult
    vanilla: Optional[ExecutionResult]
    classification: str
    reason: str
