"""
Workload generator for SQLite fuzzing.

This module provides functions to generate SQL statements for testing SQLite.
Supports:
- Table creation with various column types and constraints
- INSERT, UPDATE, DELETE statements with test data
- SELECT statements with WHERE, AND, OR conditions
- ORDER BY, GROUP BY, DISTINCT, LIMIT, OFFSET
- JOIN operations (INNER, LEFT, CROSS)
- CREATE INDEX, UNIQUE INDEX statements
- ALTER TABLE (ADD COLUMN)
- BEGIN/COMMIT transactions
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Advanced operators (BETWEEN, IN, LIKE, COLLATE)
"""

import random
import string
from typing import List, Dict, Optional, Any, Tuple, Set
from test_db.interfaces import GeneratedWorkload


# SQLite data types
SQLITE_TYPES = ["INT", "TEXT", "REAL"]

# Comparison operators for WHERE clauses
COMPARISON_OPS = ["=", "!=", "<", ">", "<=", ">=", "LIKE", "IS", "IS NOT", "IN", "BETWEEN"]

# Logical operators
LOGICAL_OPS = ["AND", "OR"]

# Aggregate functions
AGGREGATE_FUNCTIONS = ["COUNT", "SUM", "AVG", "MIN", "MAX"]

# Collation types
COLLATION_TYPES = ["BINARY", "NOCASE", "RTRIM"]

# Join types
JOIN_TYPES = ["INNER JOIN", "LEFT JOIN", "CROSS JOIN"]

# Constraint types
CONSTRAINT_TYPES = ["PRIMARY KEY", "UNIQUE", "NOT NULL", "DEFAULT", "CHECK", "COLLATE"]


class ColumnConstraint:
    """Represents a column constraint."""

    def __init__(self, constraint_type: str, value: Optional[str] = None):
        """
        Args:
            constraint_type: Type of constraint (PRIMARY KEY, NOT NULL, UNIQUE, etc.)
            value: Optional value for the constraint (e.g., default value)
        """
        self.constraint_type = constraint_type
        self.value = value

    def to_sql(self) -> str:
        """Convert constraint to SQL fragment."""
        if self.constraint_type == "DEFAULT":
            return f"DEFAULT {self.value}"
        elif self.constraint_type == "COLLATE":
            return f"COLLATE {self.value}"
        elif self.constraint_type == "CHECK":
            return f"CHECK ({self.value})"
        else:
            return self.constraint_type


class TableSchema:
    """Represents a table schema for generation purposes."""

    def __init__(self, table_name: str, columns: Dict[str, str], 
                 constraints: Optional[Dict[str, List[ColumnConstraint]]] = None,
                 primary_key: Optional[str] = None):
        """
        Args:
            table_name: Name of the table (e.g., 't0')
            columns: Dict mapping column names to their types (e.g., {'c0': 'INT', 'c1': 'TEXT'})
            constraints: Dict mapping column names to lists of ColumnConstraint objects
            primary_key: Name of the primary key column, if any
        """
        self.table_name = table_name
        self.columns = columns
        self.constraints = constraints or {}
        self.primary_key = primary_key
        self.indexes: List[str] = []  # Track created indexes

    def get_column_names(self) -> List[str]:
        """Returns list of column names."""
        return list(self.columns.keys())

    def get_column_types(self) -> List[str]:
        """Returns list of column types."""
        return list(self.columns.values())
    
    def add_constraint(self, column_name: str, constraint: ColumnConstraint) -> None:
        """Add a constraint to a column."""
        if column_name not in self.constraints:
            self.constraints[column_name] = []
        self.constraints[column_name].append(constraint)
    
    def add_index(self, index_name: str) -> None:
        """Track a created index."""
        self.indexes.append(index_name)


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
        data_type: SQLite data type (INT, TEXT, REAL)

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
    return None


def generate_constraint_for_column(column_type: str) -> Optional[ColumnConstraint]:
    """
    Generate a random constraint for a column.

    Args:
        column_type: The data type of the column

    Returns:
        A ColumnConstraint object or None
    """
    constraint_choice = random.random()
    
    if constraint_choice < 0.15:  # 15% NOT NULL
        return ColumnConstraint("NOT NULL")
    elif constraint_choice < 0.25:  # 10% UNIQUE
        return ColumnConstraint("UNIQUE")
    elif constraint_choice < 0.35:  # 10% DEFAULT value
        if column_type == "INT":
            return ColumnConstraint("DEFAULT", str(random.randint(0, 100)))
        elif column_type == "TEXT":
            return ColumnConstraint("DEFAULT", f"'{random.choice(['default', 'test', 'value'])}'")
        elif column_type == "REAL":
            return ColumnConstraint("DEFAULT", str(round(random.uniform(0.0, 100.0), 2)))
    elif constraint_choice < 0.42:  # 7% COLLATE
        return ColumnConstraint("COLLATE", random.choice(COLLATION_TYPES))
    
    return None


def create_table_statement(
    table_name: str,
    num_columns: int = 3,
    column_types: Optional[List[str]] = None,
    with_primary_key: bool = True,
) -> Tuple[str, TableSchema]:
    """
    Generate a CREATE TABLE statement.

    Args:
        table_name: Name of the table to create
        num_columns: Number of columns to create (default: 3)
        column_types: Specific column types to use. If None, randomly selects from SQLITE_TYPES
        with_primary_key: Whether to create a primary key column (default: True)

    Returns:
        Tuple of (SQL statement, TableSchema object)

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=3)
        >>> print(stmt)
        CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 TEXT NOT NULL, c2 REAL);
    """
    if column_types is None:
        column_types = [random.choice(SQLITE_TYPES) for _ in range(num_columns)]

    columns = {}
    column_defs = []
    constraints = {}
    primary_key = None

    for i, col_type in enumerate(column_types):
        col_name = generate_column_name(i)
        columns[col_name] = col_type
        col_def = col_name + " " + col_type
        
        # Optionally add constraints
        col_constraints = []
        
        # Primary key on first column (optional)
        if i == 0 and with_primary_key and random.random() < 0.5:
            col_def += " PRIMARY KEY"
            primary_key = col_name
        else:
            # Add other constraints
            constraint = generate_constraint_for_column(col_type)
            if constraint:
                col_def += " " + constraint.to_sql()
                col_constraints.append(constraint)
        
        if col_constraints:
            constraints[col_name] = col_constraints
        
        column_defs.append(col_def)

    column_part = ", ".join(column_defs)
    statement = f"CREATE TABLE {table_name} ({column_part});"

    schema = TableSchema(table_name, columns, constraints, primary_key)
    return statement, schema


def create_insert_statement(
    schema: TableSchema, num_rows: int = 3, include_nulls: bool = True
) -> Optional[str]:
    """
    Generate an INSERT statement for a table.

    Args:
        schema: TableSchema object defining the table structure
        num_rows: Number of rows to insert (default: 3)
        include_nulls: Whether to include NULL values (default: True)

    Returns:
        SQL INSERT statement or None if insertion would be invalid

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
        for i, col_type in enumerate(schema.get_column_types()):
            col_name = column_names[i]
            # Don't add NULL to NOT NULL columns or primary keys
            has_not_null = False
            if col_name in schema.constraints:
                for constraint in schema.constraints[col_name]:
                    if constraint.constraint_type == "NOT NULL":
                        has_not_null = True
                        break
            
            if col_name == schema.primary_key:
                # Auto-increment or use a value
                row_values.append(str(random.randint(1, 10000)))
            elif include_nulls and not has_not_null and random.random() < 0.05:
                row_values.append("NULL")
            else:
                value = generate_random_value(col_type)
                if isinstance(value, str) and not value.startswith("x'"):
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


def create_delete_statement(schema: TableSchema, use_where: bool = True) -> str:
    """
    Generate a DELETE statement for a table.

    Args:
        schema: TableSchema object defining the table structure
        use_where: Whether to include a WHERE clause (default: True)

    Returns:
        SQL DELETE statement

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=2)
        >>> delete_stmt = create_delete_statement(schema)
        >>> print(delete_stmt)
        DELETE FROM t0 WHERE c0 > 5;
    """
    statement = f"DELETE FROM {schema.table_name}"
    
    if use_where and schema.get_column_names():
        where_clause = create_where_condition(schema, num_conditions=random.randint(1, 2))
        statement += f" WHERE {where_clause}"
    
    statement += ";"
    return statement


def create_update_statement(schema: TableSchema, use_where: bool = True) -> str:
    """
    Generate an UPDATE statement for a table.

    Args:
        schema: TableSchema object defining the table structure
        use_where: Whether to include a WHERE clause (default: True)

    Returns:
        SQL UPDATE statement

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=2)
        >>> update_stmt = create_update_statement(schema)
        >>> print(update_stmt)
        UPDATE t0 SET c0 = 10 WHERE c1 = 'value';
    """
    if not schema.get_column_names():
        return ""
    
    # Select columns to update (at least one)
    columns = random.sample(schema.get_column_names(), k=random.randint(1, len(schema.get_column_names())))
    
    set_clauses = []
    for col_name in columns:
        col_idx = schema.get_column_names().index(col_name)
        col_type = schema.get_column_types()[col_idx]
        value = generate_random_value(col_type)
        
        if isinstance(value, str) and not value.startswith("x'"):
            value = value.replace("'", "''")
            set_clauses.append(f"{col_name} = '{value}'")
        else:
            set_clauses.append(f"{col_name} = {value if value is not None else 'NULL'}")
    
    statement = f"UPDATE {schema.table_name} SET {', '.join(set_clauses)}"
    
    if use_where and schema.get_column_names():
        where_clause = create_where_condition(schema, num_conditions=1)
        statement += f" WHERE {where_clause}"
    
    statement += ";"
    return statement


def create_index_statement(
    schema: TableSchema,
    index_num: int = 0,
    unique: bool = False,
    if_not_exists: bool = True,
) -> Optional[str]:
    """
    Generate a CREATE INDEX statement.

    Args:
        schema: TableSchema object defining the table structure
        index_num: Index number for naming (default: 0)
        unique: Whether to create a UNIQUE index (default: False)
        if_not_exists: Whether to use IF NOT EXISTS clause (default: True)

    Returns:
        SQL CREATE INDEX statement or None if no columns to index

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=2)
        >>> index_stmt = create_index_statement(schema, unique=True)
        >>> print(index_stmt)
        CREATE UNIQUE INDEX IF NOT EXISTS i0 ON t0(c0);
    """
    if not schema.get_column_names():
        return None
    
    # Select one or more columns for the index
    columns = random.sample(
        schema.get_column_names(),
        k=random.randint(1, min(3, len(schema.get_column_names())))
    )
    column_part = ", ".join(columns)
    
    index_name = f"i{len(schema.indexes)}{index_num}"
    unique_part = "UNIQUE " if unique else ""
    exists_part = "IF NOT EXISTS " if if_not_exists else ""
    
    statement = f"CREATE {unique_part}INDEX {exists_part}{index_name} ON {schema.table_name}({column_part});"
    schema.add_index(index_name)
    
    return statement


def create_alter_table_statement(schema: TableSchema) -> Optional[str]:
    """
    Generate an ALTER TABLE ADD COLUMN statement.

    Args:
        schema: TableSchema object defining the table structure

    Returns:
        SQL ALTER TABLE statement or None

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=2)
        >>> alter_stmt = create_alter_table_statement(schema)
        >>> print(alter_stmt)
        ALTER TABLE t0 ADD COLUMN c2 TEXT;
    """
    new_col_name = generate_column_name(len(schema.get_column_names()))
    new_col_type = random.choice(SQLITE_TYPES)
    
    col_def = f"{new_col_name} {new_col_type}"
    
    # Optionally add a constraint
    constraint = generate_constraint_for_column(new_col_type)
    if constraint:
        col_def += " " + constraint.to_sql()
    
    statement = f"ALTER TABLE {schema.table_name} ADD COLUMN {col_def};"
    
    # Update schema
    schema.columns[new_col_name] = new_col_type
    
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
    if not schema.get_column_names():
        return "1=1"
    
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
        elif operator == "IN":
            # Generate a list of values
            in_list = ", ".join(str(random.randint(-100, 100)) for _ in range(random.randint(1, 3)))
            value = f"({in_list})"
        elif operator == "BETWEEN":
            # BETWEEN requires two values
            val1 = random.randint(-100, 50)
            val2 = random.randint(50, 100)
            if val1 > val2:
                val1, val2 = val2, val1
            condition = f"{col_name} BETWEEN {val1} AND {val2}"
            conditions.append(condition)
            continue
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
    use_order_by: bool = False,
    use_group_by: bool = False,
    use_distinct: bool = False,
    use_limit: bool = False,
    use_join: bool = False,
    join_schema: Optional[TableSchema] = None,
) -> str:
    """
    Generate a SELECT statement for a table.

    Args:
        schema: TableSchema object defining the table structure
        num_conditions: Number of WHERE conditions (default: 1)
        select_all: Whether to select all columns or specific columns (default: True)
        use_where: Whether to include a WHERE clause (default: True)
        use_order_by: Whether to include ORDER BY clause (default: False)
        use_group_by: Whether to include GROUP BY clause (default: False)
        use_distinct: Whether to include DISTINCT keyword (default: False)
        use_limit: Whether to include LIMIT clause (default: False)
        use_join: Whether to include JOIN clause (default: False)
        join_schema: TableSchema for JOIN operations (default: None)

    Returns:
        SQL SELECT statement

    Example:
        >>> stmt, schema = create_table_statement('t0', num_columns=2)
        >>> select_stmt = create_select_statement(schema, use_order_by=True, use_limit=True)
        >>> print(select_stmt)
        SELECT * FROM t0 WHERE c0 > 5 ORDER BY c1 LIMIT 10;
    """
    distinct_part = "DISTINCT " if use_distinct else ""
    
    if select_all:
        select_part = "*"
    else:
        # Randomly select some columns
        if schema.get_column_names():
            columns = random.sample(schema.get_column_names(), k=random.randint(1, min(2, len(schema.get_column_names()))))
            select_part = ", ".join(columns)
        else:
            select_part = "*"

    statement = f"SELECT {distinct_part}{select_part} FROM {schema.table_name}"
    
    # Add JOIN if requested
    if use_join and join_schema:
        join_type = random.choice(JOIN_TYPES)
        statement += f" {join_type} {join_schema.table_name}"
    
    # Add WHERE clause
    if use_where and schema.get_column_names():
        where_clause = create_where_condition(schema, num_conditions=num_conditions)
        statement += f" WHERE {where_clause}"
    
    # Add GROUP BY clause
    if use_group_by and schema.get_column_names():
        group_cols = random.sample(schema.get_column_names(), k=random.randint(1, min(2, len(schema.get_column_names()))))
        statement += f" GROUP BY {', '.join(group_cols)}"
    
    # Add ORDER BY clause
    if use_order_by and schema.get_column_names():
        order_cols = random.sample(schema.get_column_names(), k=random.randint(1, min(2, len(schema.get_column_names()))))
        order_directions = [random.choice(["ASC", "DESC"]) for _ in order_cols]
        order_part = ", ".join(f"{col} {direction}" for col, direction in zip(order_cols, order_directions))
        statement += f" ORDER BY {order_part}"
    
    # Add LIMIT clause
    if use_limit:
        limit_value = random.randint(1, 100)
        statement += f" LIMIT {limit_value}"
        
        # Optionally add OFFSET
        if random.random() < 0.3:
            offset_value = random.randint(0, 50)
            statement += f" OFFSET {offset_value}"

    statement += ";"
    return statement


def generate_simple_workload(
    num_tables: int = 1,
    num_inserts_per_table: int = 3,
    num_selects_per_table: int = 2,
    seed: Optional[int] = None,
    use_transactions: bool = True,
    use_indexes: bool = True,
    use_deletes: bool = True,
    use_updates: bool = True,
    use_alters: bool = True,
    use_advanced_selects: bool = True,
) -> GeneratedWorkload:
    """
    Generate a comprehensive workload with table creation, inserts, updates, deletes, indexes, and advanced selects.

    Args:
        num_tables: Number of tables to create (default: 1)
        num_inserts_per_table: Number of INSERT statements per table (default: 3)
        num_selects_per_table: Number of SELECT statements per table (default: 2)
        seed: Random seed for reproducibility (default: None)
        use_transactions: Whether to use BEGIN/COMMIT transactions (default: True)
        use_indexes: Whether to create indexes (default: True)
        use_deletes: Whether to generate DELETE statements (default: True)
        use_updates: Whether to generate UPDATE statements (default: True)
        use_alters: Whether to generate ALTER TABLE statements (default: True)
        use_advanced_selects: Whether to use advanced SELECT features like ORDER BY, GROUP BY, DISTINCT, LIMIT (default: True)

    Returns:
        GeneratedWorkload object containing all generated SQL statements

    Example:
        >>> workload = generate_simple_workload(num_tables=2, use_transactions=True, seed=42)
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
        "use_transactions": use_transactions,
        "use_indexes": use_indexes,
        "use_deletes": use_deletes,
        "use_updates": use_updates,
        "use_alters": use_alters,
        "use_advanced_selects": use_advanced_selects,
    }

    # Start transaction if enabled
    # TODO: Re-enable transactions when needed
    # if use_transactions and random.random() < 0.7:
    #     statements.append("BEGIN;")
    #     transaction_active = True
    # else:
    transaction_active = False

    # Create tables
    for i in range(num_tables):
        table_name = generate_table_name(i)
        statements.append(f"DROP TABLE IF EXISTS {table_name};")

        create_stmt, schema = create_table_statement(table_name, num_columns=random.randint(2, 4))
        statements.append(create_stmt)
        schemas[table_name] = schema

        # Create indexes if enabled
        if use_indexes:
            num_indexes = random.randint(1, 3)
            for j in range(num_indexes):
                index_stmt = create_index_statement(schema, index_num=j, unique=random.random() < 0.3)
                if index_stmt:
                    statements.append(index_stmt)

        # Insert data
        for _ in range(num_inserts_per_table):
            insert_stmt = create_insert_statement(schema, num_rows=random.randint(1, 5))
            if insert_stmt:
                statements.append(insert_stmt)

        # Alter table if enabled (add a column)
        if use_alters and random.random() < 0.5:
            alter_stmt = create_alter_table_statement(schema)
            if alter_stmt:
                statements.append(alter_stmt)

        # Update some rows if enabled
        if use_updates:
            num_updates = random.randint(1, 2)
            for _ in range(num_updates):
                update_stmt = create_update_statement(schema, use_where=random.random() < 0.8)
                if update_stmt:
                    statements.append(update_stmt)

        # Delete some rows if enabled
        if use_deletes:
            num_deletes = random.randint(0, 2)
            for _ in range(num_deletes):
                delete_stmt = create_delete_statement(schema, use_where=random.random() < 0.9)
                if delete_stmt:
                    statements.append(delete_stmt)

        # Create SELECT queries with various features
        for _ in range(num_selects_per_table):
            num_conditions = random.randint(1, 3) if random.random() < 0.8 else 0
            
            select_features = {}
            if use_advanced_selects:
                select_features = {
                    "use_order_by": random.random() < 0.5,
                    "use_group_by": random.random() < 0.3,
                    "use_distinct": random.random() < 0.3,
                    "use_limit": random.random() < 0.5,
                    "use_join": False,  # Skip JOIN for single table for now
                    "join_schema": None,
                }
            
            select_stmt = create_select_statement(
                schema,
                num_conditions=num_conditions,
                select_all=random.random() < 0.6,
                use_where=num_conditions > 0,
                **select_features
            )
            statements.append(select_stmt)

    # Add some cross-table operations if we have multiple tables
    if num_tables > 1 and use_advanced_selects:
        tables_list = [schemas[generate_table_name(i)] for i in range(num_tables)]
        # Randomly add a few more complex selects
        for _ in range(random.randint(1, 3)):
            base_table = random.choice(tables_list)
            select_stmt = create_select_statement(
                base_table,
                num_conditions=random.randint(0, 2),
                select_all=True,
                use_where=random.random() < 0.6,
                use_order_by=random.random() < 0.5,
                use_limit=random.random() < 0.6,
            )
            statements.append(select_stmt)

    # End transaction if started
    # TODO: Re-enable transactions when needed
    # if transaction_active:
    #     end_stmt = random.choice(["COMMIT;", "ROLLBACK;"])
    #     statements.append(end_stmt)

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
    use_advanced_features: bool = True,
) -> GeneratedWorkload:
    """
    Generate a default workload for testing.

    This is the main entry point that produces a workload for fuzzing.

    Args:
        num_tables: Number of tables to create (default: 1)
        num_inserts_per_table: Number of INSERT statements per table (default: 3)
        num_selects_per_table: Number of SELECT statements per table (default: 3)
        seed: Random seed for reproducibility, if None, generates random workload.
        use_advanced_features: Whether to use advanced SQL features (default: True)

    Returns:
        GeneratedWorkload object
    """
    return generate_simple_workload(
        num_tables=num_tables,
        num_inserts_per_table=num_inserts_per_table,
        num_selects_per_table=num_selects_per_table,
        seed=seed,
        use_transactions=use_advanced_features,
        use_indexes=use_advanced_features,
        use_deletes=use_advanced_features,
        use_updates=use_advanced_features,
        use_alters=use_advanced_features,
        use_advanced_selects=use_advanced_features,
    )