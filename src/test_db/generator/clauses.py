import random
from typing import Callable, Optional
from test_db.generator.schema import DatabaseSchema, TableSchema
#from test_db.generator.workload_generator import create_select_statement

JOIN_TYPES = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN"]


def get_relation(
    database_schema: DatabaseSchema,
    select_builder: Optional[Callable[[int], str]] = None,
    max_depth: int = 2,
    allow_joins: bool = True,
    allow_views: bool = False,
) -> str:
    """Return a relation string for a FROM clause.

    The relation may be a table, a view, a JOIN expression, or a subquery.
    """
    relations = database_schema.get_table_names()
    if allow_views:
        relations += database_schema.get_view_names()

    if not relations:
        return "(SELECT 1) AS dummy_relation"

    if select_builder and max_depth > 0 and random.random() < 0.25:
        subquery = select_builder(max_depth - 1).rstrip(";")
        alias = f"subq{max_depth}"
        return f"({subquery}) AS {alias}"
    
    """ if max_depth > 0 and random.random() < 0.3:
        subquery, columns = create_select_statement(database_schema = DatabaseSchema, depth = max_depth - 1) """

    if allow_joins and len(relations) > 1 and random.random() < 0.6:
        left = random.choice(relations)
        right = random.choice([r for r in relations if r != left])
        join_type = random.choice(JOIN_TYPES)
        leftTable = database_schema.get_table(left)
        rightTable = database_schema.get_table(right)
        
        # Get columns for JOIN condition
        left_cols = _get_columns_for_relation(database_schema, left)
        right_cols = _get_columns_for_relation(database_schema, right)
        
        if left_cols and right_cols:
            # Try to match columns by type, otherwise pick random compatible ones
            left_col = random.choice(left_cols)
            right_col = random.choice(right_cols)
            return f"{left} {join_type} {right} ON {left}.{left_col} = {right}.{right_col}"
        else:
            # Fallback to c0 if column info not available
            return f"{left} {join_type} {right} ON {left}.c0 = {right}.c0"

    return random.choice(relations)


def _get_columns_for_relation(database_schema: DatabaseSchema, relation_name: str) -> list:
    """Get list of column names for a table or view.
    
    Args:
        database_schema: The DatabaseSchema instance
        relation_name: Name of the table or view
        
    Returns:
        List of column names, or empty list if not found
    """
    # Try as a table first
    if relation_name in database_schema.get_table_names():
        table_schema = database_schema.get_table(relation_name)
        if table_schema:
            return table_schema.get_column_names()
    
    # For views, we can't easily determine columns without parsing SQL
    # So we return empty list and let the fallback logic in get_relation handle it
    return []
