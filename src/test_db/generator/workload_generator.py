"""
Workload generator for SQLite fuzzing.

This module provides functions to generate SQL statements for testing SQLite.
Supports:
- Table creation with various column types
- INSERT statements with test data
- SELECT statements with WHERE, AND, OR conditions
"""

import random
import string
from typing import List, Dict, Optional, Any
from test_db.interfaces import GeneratedWorkload


# SQLite data types
SQLITE_TYPES = ["INT", "TEXT", "REAL", "NULL"]

# Comparison operators for WHERE clauses
COMPARISON_OPS = ["=", "!=", "<", ">", "<=", ">=", "LIKE", "IS", "IS NOT"]

# Logical operators
LOGICAL_OPS = ["AND", "OR"]


class TableSchema:
    """Represents a table schema for generation purposes."""

    def __init__(self, table_name: str, columns: Dict[str, str]):
        """
        Args:
            table_name: Name of the table (e.g., 't0')
            columns: Dict mapping column names to their types (e.g., {'c0': 'INT', 'c1': 'TEXT'})
        """
        self.table_name = table_name
        self.columns = columns

    def get_column_names(self) -> List[str]:
        """Returns list of column names."""
        return list(self.columns.keys())

    def get_column_types(self) -> List[str]:
        """Returns list of column types."""
        return list(self.columns.values())


def generate_table_name(index: int = 0) -> str:
    """
    Generate a simple table name for now.

    Args:
        index: Index to append to table name

    Returns:
        Table name like 't0', 't1', etc.
    """
    return f"t{index}"


def generate_column_name(index: int = 0) -> str:
    """
    Generate a simple column name.

    Args:
        index: Index to append to column name

    Returns:
        Column name like 'c0', 'c1', etc.
    """
    return f"c{index}"


def generate_random_value(data_type: str) -> Any:
    """
    Generate a random value for a given SQLite data type.

    Args:
        data_type: SQLite data type (INT, TEXT, REAL, NULL)

    Returns:
        A random value appropriate for the type
    """
    if data_type == "INT":
        return random.randint(-1000, 1000)
    elif data_type == "TEXT":
        length = random.randint(1, 10)
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))
    elif data_type == "REAL":
        return round(random.uniform(-1000.0, 1000.0), 2)
    elif data_type == "NULL":
        return None
    return None


def create_table_statement(
    table_name: str,
    num_columns: int = 3,
    column_types: Optional[List[str]] = None,
) -> tuple[str, TableSchema]:
    """
    Generate a CREATE TABLE statement.

    Args:
        table_name: Name of the table to create
        num_columns: Number of columns to create (default: 3)
        column_types: Specific column types to use. If None, randomly selects from SQLITE_TYPES

    Returns:
        Tuple of (SQL statement, TableSchema object)

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=3)
        >>> print(stmt)
        CREATE TABLE t0 (c0 INT, c1 TEXT, c2 REAL);
    """
    if column_types is None:
        column_types = [random.choice(SQLITE_TYPES) for _ in range(num_columns)]

    columns = {}
    column_defs = []

    for i, col_type in enumerate(column_types):
        col_name = generate_column_name(i)
        columns[col_name] = col_type
        column_defs.append(f"{col_name} {col_type}")

    column_part = ", ".join(column_defs)
    statement = f"CREATE TABLE {table_name} ({column_part});"

    schema = TableSchema(table_name, columns)
    return statement, schema


def create_insert_statement(
    schema: TableSchema, num_rows: int = 3, include_nulls: bool = True
) -> str:
    """
    Generate an INSERT statement for a table.

    Args:
        schema: TableSchema object defining the table structure
        num_rows: Number of rows to insert (default: 3)
        include_nulls: Whether to include NULL values (default: True)

    Returns:
        SQL INSERT statement

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=2)
        >>> insert_stmt = create_insert_statement(schema, num_rows=3)
        >>> print(insert_stmt)
        INSERT INTO t0 (c0, c1) VALUES (42, 'abc'), (100, 'xyz'), (NULL, 'def');
    """
    column_names = schema.get_column_names()
    column_part = ", ".join(column_names)

    rows = []
    for _ in range(num_rows):
        row_values = []
        for col_type in schema.get_column_types():
            if include_nulls and random.random() < 0.1:  # 10% chance of NULL
                row_values.append("NULL")
            else:
                value = generate_random_value(col_type)
                if isinstance(value, str):
                    # Escape single quotes in strings
                    value = value.replace("'", "''")
                    row_values.append(f"'{value}'")
                elif value is None:
                    row_values.append("NULL")
                else:
                    row_values.append(str(value))
        rows.append(f"({', '.join(row_values)})")

    values_part = ", ".join(rows)
    statement = f"INSERT INTO {schema.table_name} ({column_part}) VALUES {values_part};"
    return statement


def create_where_condition(
    schema: TableSchema, num_conditions: int = 1, use_logical_ops: bool = True
) -> str:
    """
    Generate a WHERE condition with optional AND/OR operators.

    Args:
        schema: TableSchema object defining the table structure
        num_conditions: Number of conditions to generate (default: 1)
        use_logical_ops: Whether to use AND/OR between conditions (default: True)

    Returns:
        SQL WHERE condition (without the WHERE keyword)

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=2)
        >>> condition = create_where_condition(schema, num_conditions=2)
        >>> print(f"SELECT * FROM {schema.table_name} WHERE {condition}")
        SELECT * FROM t0 WHERE c0 > 5 AND c1 LIKE '%abc%';
    """
    conditions = []
    column_names = schema.get_column_names()

    for _ in range(num_conditions):
        col_name = random.choice(column_names)
        operator = random.choice(COMPARISON_OPS)

        # Generate value based on operator
        if operator == "IS" or operator == "IS NOT":
            value = "NULL"
        elif operator == "LIKE":
            value = f"'%{random.choice(string.ascii_letters)}%'"
        else:
            # Use a simple numeric value
            value = str(random.randint(-100, 100))

        condition = f"{col_name} {operator} {value}"
        conditions.append(condition)

    # Join conditions with AND/OR
    if use_logical_ops and len(conditions) > 1:
        logical_op = random.choice(LOGICAL_OPS)
        where_clause = f" {logical_op} ".join(conditions)
    else:
        where_clause = conditions[0] if conditions else "1=1"

    return where_clause


def create_select_statement(
    schema: TableSchema,
    num_conditions: int = 1,
    select_all: bool = True,
    use_where: bool = True,
) -> str:
    """
    Generate a SELECT statement for a table.

    Args:
        schema: TableSchema object defining the table structure
        num_conditions: Number of WHERE conditions (default: 1)
        select_all: Whether to select all columns or specific columns (default: True)
        use_where: Whether to include a WHERE clause (default: True)

    Returns:
        SQL SELECT statement

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=2)
        >>> select_stmt = create_select_statement(schema, num_conditions=2)
        >>> print(select_stmt)
        SELECT * FROM t0 WHERE c0 > 5 AND c1 LIKE '%a%';
    """
    if select_all:
        select_part = "*"
    else:
        # Randomly select some columns
        columns = random.sample(schema.get_column_names(), k=random.randint(1, len(schema.get_column_names())))
        select_part = ", ".join(columns)

    statement = f"SELECT {select_part} FROM {schema.table_name}"

    if use_where:
        where_clause = create_where_condition(schema, num_conditions=num_conditions)
        statement += f" WHERE {where_clause}"

    statement += ";"
    return statement


def generate_simple_workload(
    num_tables: int = 1,
    num_inserts_per_table: int = 3,
    num_selects_per_table: int = 2,
    seed: Optional[int] = None,
) -> GeneratedWorkload:
    """
    Generate a simple workload with table creation, inserts, and selects.

    Args:
        num_tables: Number of tables to create (default: 1)
        num_inserts_per_table: Number of INSERT statements per table (default: 3)
        num_selects_per_table: Number of SELECT statements per table (default: 2)
        seed: Random seed for reproducibility (default: None)

    Returns:
        GeneratedWorkload object containing all generated SQL statements

    Example:
        >>> workload = generate_simple_workload(num_tables=2, seed=42)
        >>> print(workload.sql_text)
    """
    if seed is not None:
        random.seed(seed)

    statements = []
    schemas = {}
    metadata = {
        "num_tables": num_tables,
        "num_inserts_per_table": num_inserts_per_table,
        "num_selects_per_table": num_selects_per_table,
        "seed": seed,
    }

    # Create tables
    for i in range(num_tables):
        table_name = generate_table_name(i)
        statements.append(f"DROP TABLE IF EXISTS {table_name};")

        create_stmt, schema = create_table_statement(table_name, num_columns=random.randint(2, 4))
        statements.append(create_stmt)
        schemas[table_name] = schema

        # Insert data
        for _ in range(num_inserts_per_table):
            insert_stmt = create_insert_statement(schema, num_rows=random.randint(1, 5))
            statements.append(insert_stmt)

        # Create SELECT queries
        for _ in range(num_selects_per_table):
            num_conditions = random.randint(1, 3)
            select_stmt = create_select_statement(schema, num_conditions=num_conditions)
            statements.append(select_stmt)

    sql_text = "\n".join(statements) + "\n"

    return GeneratedWorkload(
        sql_text=sql_text,
        statements=statements,
        metadata=metadata,
    )


def generate_workload(
    num_tables: int = 1,
    num_inserts_per_table: int = 3,
    num_selects_per_table: int = 3,
    seed: Optional[int] = None,
) -> GeneratedWorkload:
    """
    Generate a default workload for testing.

    This is the main entry point that produces a workload for fuzzing.

    Args:
        num_tables: Number of tables to create (default: 1)
        num_inserts_per_table: Number of INSERT statements per table (default: 3)
        num_selects_per_table: Number of SELECT statements per table (default: 3)
        seed: Random seed for reproducibility, if None, generates random workload.

    Returns:
        GeneratedWorkload object
    """
    return generate_simple_workload(
        num_tables=num_tables,
        num_inserts_per_table=num_inserts_per_table,
        num_selects_per_table=num_selects_per_table,
        seed=seed,
    )