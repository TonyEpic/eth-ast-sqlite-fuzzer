"""
Workload generator for SQLite fuzzing.

This module provides functions to generate SQL statements for testing SQLite.
Supports:
- Table creation with various column types and constraints
- INSERT, UPDATE, DELETE statements with test data
- INSERT INTO ... SELECT statements for data copying
- SELECT statements with WHERE, AND, OR conditions
- ORDER BY, GROUP BY, DISTINCT, LIMIT, OFFSET
- Subqueries in SELECT statements
- JOIN operations (INNER, LEFT, CROSS)
- CREATE INDEX, UNIQUE INDEX statements
- ALTER TABLE (ADD COLUMN)
- BEGIN/COMMIT transactions
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Advanced operators (BETWEEN, IN, LIKE, COLLATE)
"""

# TODO -> PRAGMA, ANALYZE, WITHOUT

import random
import string
from typing import List, Dict, Optional, Any, Tuple, Set
from test_db.interfaces import GeneratedWorkload
from test_db.generator.schema import DatabaseSchema, TableSchema, ColumnConstraint
from test_db.generator.clauses import get_relation, create_subquery, create_where_condition
from test_db.generator.expressions import generate_filter_expression


# SQLite data types
SQLITE_TYPES = ["INT", "TEXT", "REAL", "BLOB"]

# Aggregate functions
AGGREGATE_FUNCTIONS = ["COUNT", "SUM", "AVG", "MIN", "MAX"]

# Collation types
COLLATION_TYPES = ["BINARY", "NOCASE", "RTRIM"]

# Join types
JOIN_TYPES = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN"]

# Constraint types
CONSTRAINT_TYPES = ["PRIMARY KEY", "UNIQUE", "NOT NULL", "DEFAULT", "CHECK", "COLLATE", "NOTNULL"]

# ALTER types
ALTER_TYPES = ["RENAME TO", "RENAME COLUMN", "ADD COLUMN", "DROP COLUMN"]

# Transactions
TRANSACTION_START = ["BEGIN", "BEGIN TRANSACTION", "BEGIN DEFERRED", "BEGIN IMMEDIATE", "BEGIN EXCLUSIVE"]
TRANSACTION_END = ["COMMIT", "END", "END TRANSACTION", "ROLLBACK"]

def generate_table_name(index: int = 0) -> str:
    """
    Generate a simple table name for now.

    Args:
        index: Index to append to table name

    Returns:
        Table name like 't0', 't1', etc.
    """
    # index = random.randint(1,20)
    return f"t{index}"


def generate_column_name(index: int = 0) -> str:
    """
    Generate a simple column name.

    Args:
        index: Index to append to column name

    Returns:
        Column name like 'c0', 'c1', etc.
    """
    index = random.randint(1,100)
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
        threshold = random.random()
        if threshold < 0.2:
            return 0
        elif threshold < 0.45:
            return 2**63 - 1  # Max Integer
        elif threshold < 0.7:
            return -(2**63)  # Min Integer
        else:
            return random.randint(-20, 20)
    elif data_type == "TEXT":
        threshold = random.random()
        if threshold < 0.05:
          return "some_fixed_text"
        elif threshold < 0.5:
          # Cover some edgecases
          edgecases = [
            "", " ", "\x00", "\n", "\t", "'", '"', "A" * 10000, "🚀", "NULL", "\');Drop table t0;", "(", ")", "()", "(a)", "((((((a))))))", "x'"
          ]
          choice = random.choice(edgecases)
          return choice
        else:
          length = random.randint(1, 10)
          return "".join(random.choices(string.ascii_letters + string.digits, k=length))
    elif data_type == "REAL":
        threshold = random.random()
        if threshold < 0.2:
            return 0
        elif threshold < 0.7:
            # Returns either a negative or positive number
            power = random.randint(-100,100)
            return (-1 - random.random())**power
        else:
          return round(random.uniform(-100000.0, 100000.0), 2)
    elif data_type == "BLOB":
        return f"zeroblob({random.randint(1,100)})"
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
    
    if constraint_choice < 0.05:  # 5% NOT NULL
        return ColumnConstraint("NOT NULL")
    elif constraint_choice < 0.15:  # 10% UNIQUE
        return ColumnConstraint("UNIQUE")
    elif constraint_choice < 0.25:  # 10% DEFAULT value
        if column_type == "INT":
            return ColumnConstraint("DEFAULT", str(random.randint(0, 100)))
        elif column_type == "TEXT":
            return ColumnConstraint("DEFAULT", f"'{random.choice(['default', 'test', 'value'])}'")
        elif column_type == "REAL":
            return ColumnConstraint("DEFAULT", str(round(random.uniform(0.0, 100.0), 2)))
    elif constraint_choice < 0.32:  # 7% COLLATE
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
            is_unique = False
            if col_name in schema.constraints:
                for constraint in schema.constraints[col_name]:
                    if constraint.constraint_type == "NOT NULL":
                        has_not_null = True
                    if constraint.constraint_type == "UNIQUE":
                        is_unique = True
            
            if col_name == schema.primary_key:
                # Auto-increment or use a value
                row_values.append(str(random.randint(1, 10000)))
            elif include_nulls and not has_not_null and not is_unique and random.random() < 0.1:
                row_values.append("NULL")
            else:
                value = generate_random_value(col_type)
                if isinstance(value, str) and not col_type == "BLOB":
                    # Escape single quotes in strings
                    if value == "x'":
                        row_values.append(f"hex(zeroblob(10000000))")
                    else:
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
    skip_col = False
    for col_name in columns:
        if col_name == schema.primary_key:
            continue
        if col_name in schema.constraints:
            for constraint in schema.constraints[col_name]:
                if constraint.constraint_type == "UNIQUE":
                    skip_col = True
        if skip_col:
            continue

        col_idx = schema.get_column_names().index(col_name)
        col_type = schema.get_column_types()[col_idx]
        value = generate_random_value(col_type)
        
        if isinstance(value, str) and not col_type == "BLOB":
            value = value.replace("'", "''")
            set_clauses.append(f"{col_name} = '{value}'")
        else:
            set_clauses.append(f"{col_name} = {value if value is not None else 'NULL'}")
    
    if set_clauses == []:
        return ""

    statement = f"UPDATE {schema.table_name} SET {', '.join(set_clauses)}"
    
    if use_where and schema.get_column_names():
        where_clause = create_where_condition(schema, num_conditions=random.randint(1,3))
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


def create_alter_table_statement(schema: TableSchema, next_table_id) -> Tuple[Optional[str], int]:
    """
    Generate an ALTER TABLE statement.
    
    Note: Does NOT include UNIQUE or NOT NULL constraint as SQLite does not support adding
    these columns to existing tables.

    Args:
        database_schema: TableSchema object defining the table structure

    Returns:
        SQL ALTER TABLE statement or None

    Example:
        ALTER TABLE t0 ADD COLUMN c2 TEXT;
    """

    alter_type = random.choice(ALTER_TYPES)
    
    if alter_type == "RENAME TO":
        old_table = schema.table_name
        new_table = generate_table_name(next_table_id)
        schema.table_name = new_table
        statement = f"ALTER TABLE {old_table} {alter_type} {new_table};"
        next_table_id = next_table_id + 1

    elif alter_type == "RENAME COLUMN":
        if not schema.columns:
            return None, next_table_id
        old_col = random.choice(list(schema.columns.keys()))
        new_col = generate_column_name(0)

        statement = f"ALTER TABLE {schema.table_name} {alter_type} {old_col} TO {new_col};"

        schema.columns[new_col] = schema.columns.pop(old_col)
        if old_col in schema.constraints:
            schema.constraints[new_col] = schema.constraints.pop(old_col)
        if schema.primary_key == old_col:
            schema.primary_key = new_col

    elif alter_type == "ADD COLUMN":
        new_col_name = generate_column_name(len(schema.get_column_names()))
        new_col_type = random.choice(SQLITE_TYPES)
        
        col_def = f"{new_col_name} {new_col_type}"
        
        # Optionally add a constraint, but exclude UNIQUE and NOT NULL (SQLite limitation)
        constraint = generate_constraint_for_column(new_col_type)
        while constraint and (constraint.constraint_type == "UNIQUE" or constraint.constraint_type == "NOT NULL"):
            # Regenerate if UNIQUE was selected
            constraint = generate_constraint_for_column(new_col_type)
        
        if constraint:
            col_def += " " + constraint.to_sql()
        
        statement = f"ALTER TABLE {schema.table_name} ADD COLUMN {col_def};"
        
        # Update schema
        schema.columns[new_col_name] = new_col_type

    elif alter_type == "DROP COLUMN":
        if not schema.columns:
            return None, next_table_id
        old_col = random.choice(list(schema.columns.keys()))
        schema.columns.pop(old_col)
        if old_col in schema.constraints:
            schema.constraints.pop(old_col)
        if schema.primary_key == old_col:
            schema.primary_key = ""

        statement = f"ALTER TABLE {schema.table_name} {alter_type} {old_col};"

    return statement, next_table_id


def create_insert_select_statement(
    target_schema: TableSchema,
    database_schema: Optional['DatabaseSchema'] = None,
    num_conditions: int = 1,
) -> Optional[str]:
    """
    Generate an INSERT INTO ... SELECT statement.

    Args:
        target_schema: TableSchema object for the target table
        source_schema: TableSchema object for the source table
        database_schema: DatabaseSchema instance for relation building
        num_conditions: Number of WHERE conditions in SELECT (default: 1)

    Returns:
        SQL INSERT INTO ... SELECT statement or None
    """
    if not target_schema.get_column_names():
        return None

    # Generate SELECT statement for the source
    select_stmt, source_cols_dict = create_select_statement(
        database_schema=database_schema,
        max_depth=0,
        num_conditions=num_conditions,
        select_all=False,  # Use specific columns
        use_where=num_conditions > 0,
        use_order_by=False,
        use_group_by=False,
        use_distinct=False,
        use_limit=False,
        use_aggregates=False,
    )

    # Get matching columns between tables
    target_cols = target_schema.get_column_names()
    source_cols = list(source_cols_dict.keys())

    # Use all columns if they match in count, otherwise use minimum
    if len(target_cols) == len(source_cols):
        insert_cols = target_cols
        select_cols = source_cols
    else:
        min_cols = min(len(target_cols), len(source_cols))
        insert_cols = target_cols[:min_cols]
        select_cols = source_cols[:min_cols]

    insert_part = ", ".join(insert_cols)

    # Extract the SELECT clause (remove FROM and beyond for INSERT ... SELECT)
    select_clause = select_stmt.split(" FROM ")[1].rstrip(";")

    statement = f"INSERT INTO {target_schema.table_name} ({insert_part}) SELECT {', '.join(select_cols)} FROM {select_clause};"
    return statement


def create_select_statement(
    database_schema: Optional['DatabaseSchema'] = None,
    max_depth: int = 2,
    num_conditions: int = 1,
    select_all: bool = True,
    use_where: bool = True,
    use_order_by: bool = False,
    use_group_by: bool = False,
    use_distinct: bool = False,
    use_limit: bool = False,
    use_aggregates: bool = False
) -> Tuple[str, Dict[str, str]]:
    """
    Generate a SELECT statement using relations from clauses.py.

    Args:
        database_schema: DatabaseSchema instance for relation building
        max_depth: maximum depth for subqueries
        num_conditions: Number of WHERE conditions (default: 1)
        select_all: Whether to select all columns or specific columns/aggregates (default: True)
        use_where: Whether to include a WHERE clause (default: True)
        use_order_by: Whether to include ORDER BY clause (default: False)
        use_group_by: Whether to include GROUP BY clause (default: False)
        use_distinct: Whether to include DISTINCT keyword (default: False)
        use_limit: Whether to include LIMIT clause (default: False)
        use_aggregates: Whether to include aggregate functions (default: False)

    Returns:
        SQL SELECT statement
    """
    
    relation, columns = get_relation(database_schema = database_schema, max_depth = max_depth - 1)
    relation = relation.rstrip(";")
    distinct_part = "DISTINCT " if use_distinct else ""
    new_columns = {}
    schema = TableSchema("temp", columns)

    # Build SELECT clause
    if select_all and random.random() < 0.5:
        select_part = "*"
        new_columns = columns
    else:
        # Mix of columns and aggregates
        items = []
        available_cols = list(columns.keys())

        if available_cols:
            # Add some columns
            num_cols = random.randint(1, min(3, len(available_cols)))
            selected_cols = random.sample(available_cols, k=num_cols)            
            items.extend(selected_cols)
            for col in selected_cols:
                new_columns[col] = columns[col]

            # Add aggregates if requested
            if use_aggregates and random.random() < 0.6:
                from test_db.generator.expressions import generate_aggregate_expression
                num_aggregates = random.randint(1, 3)
                for _ in range(num_aggregates):
                    alias_number = random.randint(1,100)
                    items.append(generate_aggregate_expression(schema) + f" AS c{alias_number}")
                    new_columns[f"c{alias_number}"] = "UNKNOWN"

        select_part = ", ".join(items) if items else "*"

    subquery = f"SELECT {distinct_part}{select_part} FROM {relation}"

    # Add WHERE clause
    if use_where and schema.get_column_names():
        where_clause = create_where_condition(schema, num_conditions=num_conditions)
        subquery += f" WHERE {where_clause}"

    # Add GROUP BY clause
    if use_group_by and schema.get_column_names():
        group_cols = random.sample(schema.get_column_names(), k=random.randint(1, min(2, len(schema.get_column_names()))))
        subquery += f" GROUP BY {', '.join(group_cols)}"

        # Add HAVING clause sometimes when GROUP BY is used
        if use_aggregates and random.random() < 0.4:
            from test_db.generator.expressions import generate_aggregate_expression
            having_expr = generate_aggregate_expression(schema)
            having_op = random.choice([">", "<", ">=", "<=", "="])
            having_value = random.randint(1, 100)
            subquery += f" HAVING {having_expr} {having_op} {having_value}"

    # Add ORDER BY clause
    if use_order_by and schema.get_column_names():
        order_cols = random.sample(schema.get_column_names(), k=random.randint(1, min(2, len(schema.get_column_names()))))
        order_directions = [random.choice(["ASC", "DESC"]) for _ in order_cols]
        order_part = ", ".join(f"{col} {direction}" for col, direction in zip(order_cols, order_directions))
        subquery += f" ORDER BY {order_part}"

    # Add LIMIT clause
    if use_limit:
        limit_value = random.randint(1, 100)
        subquery += f" LIMIT {limit_value}"

        # Optionally add OFFSET
        if random.random() < 0.3:
            offset_value = random.randint(0, 50)
            subquery += f" OFFSET {offset_value}"

    subquery += ";"

    return (subquery, columns)


def create_view_statement(
    view_name: str,
    base_schema: TableSchema,
    database_schema: Optional['DatabaseSchema'] = None,
    schemas_list: Optional[List[TableSchema]] = None,
    use_complex_select: bool = True,
) -> Optional[str]:
    """
    Generate a CREATE VIEW statement with complex underlying SELECT.

    Uses get_relation() to build diverse FROM clauses (tables, JOINs, subqueries).
    The underlying SELECT can include WHERE, GROUP BY, ORDER BY, LIMIT, and DISTINCT.

    Args:
        view_name: Name of the view to create
        base_schema: Primary table for the view
        database_schema: DatabaseSchema instance for relation building (optional)
        schemas_list: List of available table schemas for JOIN operations (optional)
        use_complex_select: Whether to use complex SELECT features (default: True)

    Returns:
        SQL CREATE VIEW statement or None
    """
    if not base_schema.get_column_names():
        return None

    # Build the relation (table name, JOIN expression, or subquery)

    # Generate the SELECT statement for the view
    select_stmt, cols = create_select_statement(
        database_schema=database_schema,
        num_conditions=random.randint(0, 2) if random.random() < 0.7 else 0,
        max_depth = 2,
        select_all=random.random() < 0.6,
        use_where=random.random() < 0.5,
        use_order_by=random.random() < 0.4 if use_complex_select else False,
        use_group_by=random.random() < 0.3 if use_complex_select else False,
        use_distinct=random.random() < 0.2 if use_complex_select else False,
        use_limit=random.random() < 0.3 if use_complex_select else False,
        use_aggregates=random.random() < 0.4 if use_complex_select else False,
    )

    # Extract the SELECT clause (remove trailing semicolon if present)
    select_clause = select_stmt.rstrip(";")

    # Create the view statement
    statement = f"CREATE VIEW {view_name} AS {select_clause};"
    new_view = TableSchema(view_name, cols)
    database_schema.add_view(new_view)
    return statement


def generate_simple_workload(
    seed: Optional[int] = None,
) -> GeneratedWorkload:
    """
    Generate a comprehensive workload with diverse statement types for fuzzing.
    
    Creates a single initial table, then loops multiple times generating random
    statement types to maximize SQL diversity for better fuzzing coverage.
    All other operations (indexes, inserts, additional tables) are randomly
    generated within the main loop.

    Args:
        seed: Random seed for reproducibility (default: None)

    Returns:
        GeneratedWorkload object containing all generated SQL statements

    Example:
        >>> workload = generate_simple_workload(seed=42)
        >>> print(workload.sql_text)
    """
    if seed is not None:
        random.seed(seed)

    statements = []
    schemas = {}
    metadata = {
        "seed": seed,
    }

    database_schema = DatabaseSchema()

    # Create exactly one initial table
    table_name = generate_table_name(0)
    statements.append(f"DROP TABLE IF EXISTS {table_name};")
    create_stmt, schema = create_table_statement(table_name, num_columns=random.randint(2, 4))
    statements.append(create_stmt)
    schemas[table_name] = schema
    database_schema.add_table(schema)

    # Main loop: randomly generate diverse statements (7-12 iterations)
    num_iterations = random.randint(9, 16)
    schemas_list = list(schemas.values())
    next_table_id = 1  # For creating new tables
    next_view_id = 0  # For creating new views
    
    for iteration in range(num_iterations):
        # Weight insertions higher early on, then decrease over iterations
        insert_prob = (0.6 - (iteration / num_iterations) * 1.2)
        create_table_prob = insert_prob + (0.3 - (iteration / num_iterations) * 1.2)
        
        
        # Distribute remaining probability among other operations
        remaining_prob = 1.0 - create_table_prob
        select_prob = create_table_prob + (((iteration / num_iterations) * 0.9) * remaining_prob)
        remaining_prob = 1 - select_prob
        update_prob = select_prob + (0.10 * remaining_prob)
        delete_prob = update_prob + (0.08 * remaining_prob)
        insert_select_prob = delete_prob + (0.08 * remaining_prob)
        index_prob = insert_select_prob + (0.15 * remaining_prob)
        create_view_prob = index_prob + (0.12 * remaining_prob)
        alter_prob = 1.0
        
        # Choose a random statement type
        rand_type = random.random()
        
        # Pick a random table for the operation
        selected_schema = random.choice(schemas_list)
        
        if rand_type < insert_prob:
            # INSERT statement (higher probability early on)
            insert_stmt = create_insert_statement(selected_schema, num_rows=random.randint(1, 4))
            if insert_stmt:
                statements.append(insert_stmt)
        
        elif rand_type < create_table_prob:
            # CREATE a new table (adds to available tables for operations)
            new_table_name = generate_table_name(next_table_id)
            statements.append(f"DROP TABLE IF EXISTS {new_table_name};")
            create_stmt, new_schema = create_table_statement(new_table_name, num_columns=random.randint(2, 4))
            statements.append(create_stmt)
            schemas[new_table_name] = new_schema
            database_schema.add_table(new_schema)
            schemas_list = list(schemas.values())
            next_table_id += 1
        
        elif rand_type < select_prob:
            # Simple SELECT with various relations, including joins and views
            num_conditions = random.randint(0, 2) if random.random() < 0.7 else 0

            select_stmt, cols = create_select_statement(
                database_schema=database_schema,
                max_depth=2,
                num_conditions=num_conditions,
                select_all=random.random() < 0.5,
                use_where=num_conditions > 0,
                use_order_by=random.random() < 0.4,
                use_group_by=random.random() < 0.2,
                use_distinct=random.random() < 0.2,
                use_limit=random.random() < 0.4,
                use_aggregates=random.random() < 0.6,  # Increased probability
            )
            statements.append(select_stmt)
        
        elif rand_type < update_prob:
            # UPDATE statement
            update_stmt = create_update_statement(selected_schema, use_where=random.random() < 0.8)
            if update_stmt and update_stmt != "":
                statements.append(update_stmt)
        
        elif rand_type < delete_prob:
            # DELETE statement
            delete_stmt = create_delete_statement(selected_schema, use_where=random.random() < 0.85)
            if delete_stmt:
                statements.append(delete_stmt)
        
        elif rand_type < insert_select_prob and len(schemas_list) > 1:
            # INSERT INTO ... SELECT
            insert_select_stmt = create_insert_select_statement(
                selected_schema,
                database_schema=database_schema,
                num_conditions=random.randint(0, 1)
            )
            if insert_select_stmt:
                statements.append(insert_select_stmt)
        
        elif rand_type < index_prob:
            # CREATE INDEX on a random table
            index_stmt = create_index_statement(selected_schema, index_num=len(selected_schema.indexes), unique=random.random() < 0.3)
            if index_stmt:
                statements.append(index_stmt)
        
        elif rand_type < create_view_prob and len(schemas_list) > 0:
            # CREATE VIEW with complex SELECT
            view_name = f"v{next_view_id}"
            base_table = random.choice(schemas_list)
            
            # Optionally include join tables for the view
            join_tables = []
            if len(schemas_list) > 1 and random.random() < 0.4:
                num_join_tables = random.randint(1, min(2, len(schemas_list) - 1))
                join_tables = random.sample(
                    [s for s in schemas_list if s.table_name != base_table.table_name],
                    k=num_join_tables
                )
            
            view_stmt = create_view_statement(
                view_name,
                base_table,
                database_schema=database_schema,
                schemas_list=schemas_list,
                use_complex_select=True
            )
            if view_stmt:
                statements.append(view_stmt)
                next_view_id += 1
        
        elif random.random() < 0.8:
            # ALTER TABLE (add column)
            alter_stmt, next_table_id = create_alter_table_statement(selected_schema, next_table_id)
            if alter_stmt:
                statements.append(alter_stmt)
        
        else:
            # Complex SELECT with subquery as fallback
            num_conditions = random.randint(0, 2)

            select_stmt, cols = create_select_statement(
                database_schema=database_schema,
                max_depth=2,
                num_conditions=num_conditions,
                select_all=random.random() < 0.5,
                use_where=num_conditions > 0,
                use_order_by=random.random() < 0.4,
                use_group_by=random.random() < 0.2,
                use_distinct=random.random() < 0.2,
                use_limit=random.random() < 0.4,
                use_aggregates=random.random() < 0.6,  # Increased probability
            )
            statements.append(select_stmt)

    if random.random() < 0.2:
        # Add transaction
        start_ins = random.randint(1, len(statements))
        end_ins = random.randint(start_ins+1, len(statements)+1)
        startblock = random.choice(TRANSACTION_START)
        endblock = random.choice(TRANSACTION_END)
        statements.insert(start_ins, startblock)
        statements.insert(end_ins, endblock)

    sql_text = "\n".join(statements) + "\n"

    return GeneratedWorkload(
        sql_text=sql_text,
        statements=statements,
        metadata=metadata,
    )


def generate_workload(
    seed: Optional[int] = None,
) -> GeneratedWorkload:
    """
    Generate a default workload for testing.

    This is the main entry point that produces a workload for fuzzing.

    Args:
        seed: Random seed for reproducibility, if None, generates random workload.

    Returns:
        GeneratedWorkload object
    """
    return generate_simple_workload(seed=seed)