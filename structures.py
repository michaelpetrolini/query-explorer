class Dependency:
    def __init__(self, name, table=None, dataset=None, project=None, table_alias=None):
        self.name = name
        self.table = table
        self.dataset = dataset
        self.project = project
        self.table_alias = table_alias

    def __str__(self):
        return (
            f"[name: {self.name}, table: {self.table}, dataset: {self.dataset},"
            f" project: {self.project}, table_alias: {self.table_alias}]")

    def __eq__(self, other):
        if not isinstance(other, Dependency):
            return NotImplemented

        return self.name == other.name and \
            self.table == other.table and \
            self.dataset == other.dataset and \
            self.project == other.project and \
            self.table_alias == other.table_alias

    def __hash__(self):
        return hash(self.name) + \
            hash(self.table) + \
            hash(self.dataset) + \
            hash(self.project) + \
            hash(self.table_alias)


class Column:
    def __init__(self, name, cte=None, value=None):
        self.dependencies = set()
        self.name = name
        self.cte = cte
        self.value = value

    def __str__(self):
        col_repr = f"name: {self.name}"
        col_repr += f"\ncte: {self.cte}" if self.cte else ""
        col_repr += f"\nvalue: {self.value}" if self.value else ""
        for dep in self.dependencies:
            col_repr = col_repr + f"\n\t{dep}"
        return col_repr

    def __eq__(self, other):
        if not isinstance(other, Column):
            return NotImplemented

        return self.name == other.name and \
            self.cte == other.cte and \
            self.value == other.value

    def __hash__(self):
        return hash(self.name) + \
            hash(self.cte) + \
            hash(self.value)


class Table:
    def __init__(self, name, alias, dataset=None, project=None):
        self.name = name
        self.dataset = dataset
        self.project = project
        self.alias = alias

    def __str__(self):
        return f"Table[name: {self.name}, dataset: {self.dataset}, project: {self.project}, alias: {self.alias}]"
