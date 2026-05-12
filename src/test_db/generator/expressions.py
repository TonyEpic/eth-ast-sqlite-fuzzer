import random
import string
from typing import List
from test_db.generator.schema import TableSchema

COMPARISON_OPS = ["=", "!=", "<", ">", "<=", ">=", "LIKE", "IS", "IS NOT", "IN", "BETWEEN"]
LOGICAL_OPS = ["AND", "OR"]
AGGREGATE_FUNCTIONS = ["COUNT", "SUM", "AVG", "MIN", "MAX"]


def _generate_literal(column_type: str) -> str:
    if column_type == "INT":
        return str(random.randint(-100, 100))
    if column_type == "REAL":
        return str(round(random.uniform(-1000.0, 1000.0), 2))
    if column_type == "TEXT":
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(1, 8)))
        return f"'{value}'"
    return "NULL"


def generate_comparison_expression(schema: TableSchema) -> str:
    if not schema.get_column_names():
        return "1=1"

    col_name = random.choice(schema.get_column_names())
    col_type = schema.get_column_types()[schema.get_column_names().index(col_name)]
    operator = random.choice(COMPARISON_OPS)

    if operator in ("IS", "IS NOT"):
        return f"{col_name} {operator} NULL"
    if operator == "LIKE":
        length = random.randint(1, 4)
        s = ''.join(random.choice('%_') for _ in range(length))
        pos = random.randint(0, len(s))
        result = s[:pos] + str(_generate_literal(col_type)).replace("'", "") + s[pos:]
        return f"{col_name} LIKE '{result}'"
    if operator == "IN":
        values = ", ".join(_generate_literal(col_type) for _ in range(random.randint(1, 3)))
        return f"{col_name} IN ({values})"
    if operator == "BETWEEN":
        low = random.randint(-100, 0)
        high = random.randint(1, 100)
        if low > high:
            low, high = high, low
        return f"{col_name} BETWEEN {low} AND {high}"

    literal = _generate_literal(col_type)
    return f"{col_name} {operator} {literal}"


def generate_aggregate_expression(schema: TableSchema) -> str:
    if not schema.get_column_names():
        return "COUNT(*)"

    function = random.choice(AGGREGATE_FUNCTIONS)
    column = random.choice(schema.get_column_names())
    distinct = "DISTINCT " if random.random() < 0.2 else ""
    return f"{function}({distinct}{column})"


def generate_boolean_expression(schema: TableSchema, depth: int = 2) -> str:
    if depth <= 1:
        return generate_comparison_expression(schema)

    left = generate_comparison_expression(schema)
    right = generate_comparison_expression(schema)
    operator = random.choice(LOGICAL_OPS)
    return f"({left} {operator} {right})"


def generate_filter_expression(schema: TableSchema, num_conditions: int = 1, use_logical_ops: bool = True) -> str:
    if num_conditions <= 1 or not use_logical_ops:
        return generate_comparison_expression(schema)

    conditions: List[str] = [generate_comparison_expression(schema) for _ in range(num_conditions)]
    operator = random.choice(LOGICAL_OPS)
    return f" {operator} ".join(conditions)
