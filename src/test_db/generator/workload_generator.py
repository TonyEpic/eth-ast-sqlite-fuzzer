from test_db.interfaces import GeneratedWorkload


def generate_workload() -> GeneratedWorkload:
    statements = [
        "DROP TABLE IF EXISTS t0;",
        "CREATE TABLE t0(c0 INT);",
        "INSERT INTO t0(c0) VALUES (1), (2), (NULL);",
        "SELECT * FROM t0 WHERE c0 IS NOT 1;"
    ]
    sql_text = "\n".join(statements) + "\n"
    return GeneratedWorkload(
        sql_text=sql_text,
        statements=statements,
        metadata={"kind": "placeholder"}
    )