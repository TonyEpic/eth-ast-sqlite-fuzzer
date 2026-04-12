from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class GeneratedWorkload:
    sql_text: str
    statements: List[str]
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    engine: str
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool
    duration_ms: float


@dataclass
class RunOutcome:
    workload_id: str
    workload: GeneratedWorkload
    patched: ExecutionResult
    vanilla: Optional[ExecutionResult]
    classification: str
    reason: str