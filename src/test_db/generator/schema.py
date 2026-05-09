from typing import Dict, List, Optional


class ColumnConstraint:
    """Represents a column constraint."""

    def __init__(self, constraint_type: str, value: Optional[str] = None):
        self.constraint_type = constraint_type
        self.value = value

    def to_sql(self) -> str:
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

    def __init__(
        self,
        table_name: str,
        columns: Dict[str, str],
        constraints: Optional[Dict[str, List[ColumnConstraint]]] = None,
        primary_key: Optional[str] = None,
    ):
        self.table_name = table_name
        self.columns = columns
        self.constraints = constraints or {}
        self.primary_key = primary_key
        self.indexes: List[str] = []

    def get_column_names(self) -> List[str]:
        return list(self.columns.keys())

    def get_column_types(self) -> List[str]:
        return list(self.columns.values())

    def get_constraints(self) -> Dict[str, List[ColumnConstraint]]:
        return self.constraints

    def add_constraint(self, column_name: str, constraint: ColumnConstraint) -> None:
        if column_name not in self.constraints:
            self.constraints[column_name] = []
        self.constraints[column_name].append(constraint)

    def add_index(self, index_name: str) -> None:
        self.indexes.append(index_name)

    def add_column(self, column_name: str, column_type: str) -> None:
        self.columns[column_name] = column_type


class DatabaseSchema:
    """Holds all tables and views for workload generation."""

    def __init__(self):
        self.tables: Dict[str, TableSchema] = {}
        self.views: Dict[str, TableSchema] = {}

    def add_table(self, schema: TableSchema) -> None:
        self.tables[schema.table_name] = schema

    def get_table(self, table_name: str) -> Optional[TableSchema]:
        return self.tables.get(table_name)

    def add_view(self, schema: TableSchema) -> None:
        self.views[schema.table_name] = schema

    def get_view(self, view_name: str) -> Optional[TableSchema]:
        return self.views.get(view_name)

    def get_table_names(self) -> List[str]:
        return list(self.tables.keys())

    def get_view_names(self) -> List[str]:
        return list(self.views.keys())

    def get_relation_names(self, include_views: bool = False) -> List[str]:
        relations = self.get_table_names()
        if include_views:
            relations += self.get_view_names()
        return relations

    def get_columns(self, relation_name: str) -> List[str]:
        relations = self.tables | self.views
        if relation_name in relations:
            return relations[relation_name].get_column_names()
        return []

    def get_column_types(self, relation_name: str) -> List[str]:
        relations = self.tables | self.views
        if relation_name in relations:
            return relations[relation_name].get_column_types()
        return []

    def get_constraints(self, table_name: str) -> Dict[str, List[ColumnConstraint]]:
        if table_name in self.tables:
            return self.tables[table_name].get_constraints()
        return {}

    def add_column(self, table_name: str, column_name: str, column_type: str) -> None:
        table = self.get_table(table_name)
        if table:
            table.add_column(column_name, column_type)

    def add_constraint(self, table_name: str, column_name: str, constraint: ColumnConstraint) -> None:
        table = self.get_table(table_name)
        if table:
            table.add_constraint(column_name, constraint)
