from test_db.interfaces import ExecutionResult
from test_db.oracle.normalizer import normalize_output


def compare_results(patched: ExecutionResult, vanilla: ExecutionResult) -> tuple[bool, str]:
    if patched.timed_out != vanilla.timed_out:
        return False, "Timeout behavior differs"

    if patched.returncode != vanilla.returncode:
        return False, "Return code differs"

    if normalize_output(patched.stdout) != normalize_output(vanilla.stdout):
        return False, "Stdout differs"

    if normalize_output(patched.stderr) != normalize_output(vanilla.stderr):
        return False, "Stderr differs"

    return True, "Equivalent"