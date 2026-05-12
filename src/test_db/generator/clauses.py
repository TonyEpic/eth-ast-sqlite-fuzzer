import random
from typing import Callable, Optional, Tuple, Dict
from test_db.generator.schema import DatabaseSchema, TableSchema
from test_db.generator.expressions import generate_filter_expression
#from test_db.generator.workload_generator import create_select_statement

JOIN_TYPES = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN"]
SET_OPS = ["UNION ALL", "UNION", "INTERSECT", "EXCEPT"]
JOIN_PARAMETER = 0.5
SUBQUERY_PARAMETER = 0.5

def get_relation(
    database_schema: DatabaseSchema,
    max_depth: int = 2,
    allow_joins: bool = True,
    allow_setops: bool = True,
    allow_views: bool = False,
) -> Tuple[str, Dict[str, str]]:
    """Return a relation string for a FROM clause.

    The relation may be a table, a view, a JOIN expression, or a subquery.
    """
    relations = database_schema.get_table_names()
    if allow_views:
        relations += database_schema.get_view_names()

    if not relations:
        return "(SELECT 1) AS dummy_relation", {}

    if max_depth <= 0 or random.random() < 0.5:
        # Return simple relation
        res = random.choice(relations)
        if res in list(database_schema.tables.keys()):
            return (res, database_schema.tables[res].columns)
        else:
            return (res, database_schema.views[res].columns)
    
    else:
        threshold = random.random()

        def choose_relation_for_complex_clause():
            if random.random() < 0.5:
                return create_subquery(database_schema=database_schema, allow_views=allow_views, max_depth=max_depth - 1)
            return get_relation(database_schema=database_schema, allow_views=allow_views, max_depth=0)

        def relation_alias(relation: str) -> str:
            if " AS " in relation.upper():
                return relation.rsplit(" ", 1)[-1]
            return relation

        if allow_joins and threshold < 0.3 and len(relations) >= 2:
            # Create JOIN Clause
            left, left_dict = choose_relation_for_complex_clause()
            right, right_dict = choose_relation_for_complex_clause()
            join_type = random.choice(JOIN_TYPES)
            left_cols = list(left_dict.keys())
            right_cols = list(right_dict.keys())

            left_ref = relation_alias(left)
            right_ref = relation_alias(right)

            if left_cols and right_cols:
                left_col = random.choice(left_cols)
                right_col = random.choice(right_cols)
                return (f"{left} {join_type} {right} ON {left_ref}.{left_col} = {right_ref}.{right_col}", left_dict | right_dict)
            else:
                # Fallback to c0 if column info not available
                return (f"{left} {join_type} {right} ON {left_ref}.c0 = {right_ref}.c0", left_dict | right_dict)

        if (allow_setops and threshold < 0.6) or (len(relations) < 2 and threshold < 0.2):
            # Create SETOP Clause
            left, left_dict = choose_relation_for_complex_clause()
            right, right_dict = choose_relation_for_complex_clause()
            set_op = random.choice(SET_OPS)
            left_cols = list(left_dict.keys())
            right_cols = list(right_dict.keys())

            if left_cols and right_cols:
                num_cols = random.randint(1, min(len(left_cols), len(right_cols)))
                left_sample = random.sample(left_cols, k=num_cols)
                right_sample = random.sample(right_cols, k=num_cols)
                left_sel = ", ".join(left_sample)
                right_sel = ", ".join(right_sample)
                filtered_cols = {k: v for k, v in (database_schema.tables | database_schema.views).items() if k in left_sample}
                # Construct simpler fixed set_op select statement to be compatible with the rest
                return (f"(SELECT {left_sel} FROM {left} {set_op} SELECT {right_sel} FROM {right}) AS subq{random.randint(1,100)}", filtered_cols)
            else:
                # Fallback to dummy
                return "(SELECT 1) AS dummy_relation", {}

        else:
            return create_subquery(database_schema = database_schema, allow_views=allow_views, max_depth = max_depth)


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
    """
    if not schema.get_column_names():
        return "1=1"

    return generate_filter_expression(
        schema,
        num_conditions=num_conditions,
        use_logical_ops=use_logical_ops,
    )

def create_subquery(
    database_schema: DatabaseSchema, 
    max_depth: int = 0,
    num_conditions: int = 1,
    select_all: bool = True,
    use_where: bool = True,
    use_order_by: bool = False,
    use_group_by: bool = False,
    use_distinct: bool = False,
    use_limit: bool = False,
    use_aggregates: bool = False,
    allow_views: bool = False
) -> Tuple[str, Dict[str, str]]:

    relation, columns = get_relation(database_schema = database_schema, allow_views=allow_views, max_depth = max_depth - 1)
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

    alias = f"subq{random.randint(1,100)}"
    return (f"({subquery}) AS {alias}", new_columns)
