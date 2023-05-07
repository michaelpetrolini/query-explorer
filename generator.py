import uuid

import networkx as nx
from sqlparse.sql import Where, Comparison, Identifier, IdentifierList, Function, Case, Parenthesis
from sqlparse.tokens import CTE, DML, Keyword, Wildcard

from exceptions import LogicError, ExceptionType
from structures import Column, Dependency, Table


def _table(token: Identifier) -> Table:
    sources = token.value.replace('`', '').split()[0].split('.')
    return Table(name=sources[0], alias=token.get_alias()) if len(sources) == 1 \
        else Table(name=sources[-1],
                   dataset=sources[-2],
                   project=sources[-3] if len(sources) > 2 else None,
                   alias=token.get_alias())


def _is_constant(field: str):
    return (field.startswith('\"') and field.endswith('\"')) \
        or (field.startswith('\'') and field.endswith('\'')) \
        or field.replace('.', '', 1).isdigit() \
        or field.upper() == "NULL"


def _dependency(parameter) -> Dependency:
    if parameter.ttype == Wildcard:
        elements = parameter.value.split('.')
        return Dependency(name=elements[-1], table_alias=elements[0] if len(elements) > 1 else None)
    return Dependency(name=parameter.get_real_name(),
                      table_alias=parameter.get_parent_name())


def _function(function: Function):
    for elem in function.tokens[-1].tokens:
        yield from _get_dependencies(elem)


def _case(token: Case):
    for condition, value in token.get_cases(skip_ws=True):
        if condition:
            for elem in condition:
                yield from _get_dependencies(elem)
        for elem in value:
            yield from _get_dependencies(elem)


def _get_dependencies(token):
    if isinstance(token, Identifier):
        elements = [e for e in token.tokens if type(e) in (Function, Case, Parenthesis)]
        if elements:
            for element in elements:
                yield from _get_dependencies(element)
        elif not _is_constant(token.value):
            yield _dependency(token)
    elif isinstance(token, IdentifierList) or isinstance(token, Parenthesis):
        for e in token.tokens:
            yield from _get_dependencies(e)
    elif isinstance(token, Function):
        yield from _function(token)
    elif isinstance(token, Case):
        yield from _case(token)
    elif isinstance(token, Comparison):
        yield from _get_dependencies(token.left)
        yield from _get_dependencies(token.right)
    elif token.ttype == Wildcard:
        yield _dependency(token)


def _column(token, cte: str, is_auxiliary: bool = False) -> Column:
    name = (token.get_alias() or token.value) if isinstance(token, Identifier) else token.value
    column = Column(name=name, cte=cte, is_wildcard=token.value.split('.')[-1] == '*', is_auxiliary=is_auxiliary)
    if isinstance(token, Identifier) and _is_constant(token.tokens[0].value):
        column.value = token.tokens[0].value
    else:
        column.dependencies = set(_get_dependencies(token))
    return column


def _where(token: Where, cte: str):
    for t in token.tokens:
        if isinstance(t, Comparison):
            yield from _comparison(t, cte)


def _comparison(token: Comparison, cte: str):
    if not _is_constant(token.left.value):
        yield _column(token.left, cte, is_auxiliary=True)
    if not _is_constant(token.right.value):
        yield _column(token.right, cte, is_auxiliary=True)


def _union(token, columns: set[Column]):
    def add_dependency(tkn):
        name = (tkn.get_alias() or tkn.value) if isinstance(tkn, Identifier) else tkn.value
        twins = [c for c in columns if c.name.split('.')[-1] == name.split('.')[-1]]
        if not twins:
            raise LogicError(ExceptionType.TWIN_NOT_FOUND, tkn.value)
        dependencies = {d for d in twins[0].dependencies}
        dependencies.update(_get_dependencies(tkn))
        twins[0].name = twins[0].name.split('.')[-1]
        twins[0].dependencies = dependencies

    if isinstance(token, IdentifierList):
        for t in token.tokens:
            if isinstance(t, Identifier) or t.ttype == Wildcard:
                add_dependency(t)
    else:
        add_dependency(token)


def _columns(cte: str, token):
    if isinstance(token, IdentifierList):
        for t in token.tokens:
            if isinstance(t, Identifier) or t.ttype == Wildcard:
                yield _column(t, cte)
    else:
        yield _column(token, cte)


def iterate(iterator):
    index = -1

    def next_token():
        nonlocal index
        index, token = iterator.token_next(index)
        return token

    return next_token


class ColumnTree:
    def __init__(self):
        self._graph = nx.DiGraph()

    def get_graph(self):
        self._clean_graph()
        return self._graph

    def get_nodes(self):
        self._clean_graph()
        return self._graph.nodes

    def _clean_graph(self):
        wildcards = [node for node in self._graph.nodes if node.is_wildcard]
        [self._graph.remove_node(wildcard) for wildcard in wildcards]

    def _from(self, token):
        if isinstance(token, Identifier) and isinstance(token.tokens[0], Parenthesis):
            cte = token.tokens[-1].value
            self.generate(token.tokens[0], cte)
            yield Table(name=cte, alias=None)
        elif isinstance(token, Parenthesis):
            table_id = str(uuid.uuid4())
            self.generate(token, table_id)
            yield Table(name=table_id, alias=None)
        elif isinstance(token, IdentifierList):
            for t in token.tokens:
                if isinstance(t, Identifier):
                    yield _table(t)
        else:
            yield _table(token)

    def test_dependencies(self):
        linked_columns = {n for n in self._graph.nodes if self._graph.in_edges(n) or self._graph.out_edges(n)}
        subgraph = self._graph.subgraph(linked_columns)
        return subgraph

    def get_used_columns(self):
        def get_linked_columns(columns):
            linked_columns = set()
            for column in columns:
                linked_columns.add(column)
                incoming_nodes = [n[0] for n in self._graph.in_edges(column)]
                linked_columns.update(get_linked_columns(incoming_nodes))
            return linked_columns

        leaves = [column for column in self._graph.nodes if not column.cte]
        used_columns = get_linked_columns(leaves)
        return used_columns

    def get_unused_columns(self):
        return self._graph.nodes - self.get_used_columns()

    def impact_analysis(self, column: Column):
        impacted_columns = []

        if len(self._graph.out_edges(column)) == 0 and len(self._graph.in_edges(column)) > 0:
            impacted_columns.append(column)
        else:
            children = [n[0] for n in self._graph.out_edges(column)]
            for child in children:
                impacted_columns = impacted_columns + self.impact_analysis(child)

        return impacted_columns

    def dependency_analysis(self, column: Column):
        dependency_columns = []

        if len(self._graph.out_edges(column)) > 0 and len(self._graph.in_edges(column)) == 0:
            dependency_columns.append(column)
        else:
            parents = [n[0] for n in self._graph.in_edges(column)]
            for parent in parents:
                dependency_columns = dependency_columns + self.dependency_analysis(parent)

        return dependency_columns

    def _cte(self, identifier):
        cte = identifier[0].value
        self.generate(identifier[-1], cte)

    def _cte_list(self, statement):
        iterator = iterate(statement)
        token = iterator()
        while token:
            if isinstance(token, Identifier):
                self._cte(token)
            token = iterator()

    def generate(self, statement, cte: str = None):
        columns = set()
        tables = set()
        iterator = iterate(statement)
        token = iterator()
        dml = False
        while token:
            if token.ttype == CTE:
                token = iterator()
                self._cte_list(token) if isinstance(token, IdentifierList) else self._cte(token)
            elif token.ttype == DML:
                dml = True
                token = iterator()
                if token.value == "DISTINCT":
                    token = iterator()
                columns.update(_columns(cte, token))
            elif token.ttype == Keyword and token.value.split(' ')[-1] in ["FROM", "JOIN"]:
                dml = False
                token = iterator()
                tables.update(self._from(token))
            elif dml:
                columns.update(_columns(cte, token))
            elif isinstance(token, Comparison):
                columns.update(_comparison(token, cte))
            elif isinstance(token, Where):
                columns.update(_where(token, cte))
            elif token.ttype == Keyword and token.value.split(' ')[0] == "UNION":
                while type(token) not in (Identifier, IdentifierList) and token.ttype != Wildcard:
                    token = iterator()
                _union(token, columns)
            token = iterator()

        self._add_columns_to_graph(columns, tables)

        return columns

    def _add_columns_to_graph(self, columns: set[Column], tables: set[Table]):
        for column in columns:
            if column.value:
                self._graph.add_node(column)
                continue

            for dependency in column.dependencies:
                if not dependency:
                    print(column)
                dependency_found = False
                if column.is_wildcard:
                    self._prepare_wildcard(column, dependency, tables)

                if dependency.table_alias and not column.is_wildcard:
                    self._manage_table_alias(column, dependency, tables)
                    continue

                for table in [t for t in tables if not t.dataset]:
                    # If the wildcard depends on a temp table, I create a column for every parent already in the graph
                    if column.is_wildcard:
                        for parent_col in [c for c in self._graph.nodes if not c.is_auxiliary and table.name == c.cte]:
                            new_column = Column(name=parent_col.name, cte=column.cte)
                            self._graph.add_edge(parent_col, new_column)
                        dependency_found = True
                        continue

                    # I check every column already in the graph looking for possible parents
                    for parent_col in [c for c in self._graph.nodes if not c.is_auxiliary and table.name == c.cte]:
                        if dependency.name == parent_col.name.split('.')[-1]:
                            self._graph.add_edge(parent_col, column)
                            dependency_found = True

                # If a field doesn't have an alias and doesn't depend on any parent, I have to guess the source table
                if not column.is_wildcard and not dependency_found:
                    source_tables = [t for t in tables if t.dataset]
                    if not source_tables:
                        # I check if there's a wildcard in the graph that can be used as a parent
                        wildcards = [c for c in self._graph.nodes if
                                     c.is_wildcard and c.cte in [t.name for t in tables]]
                        if not wildcards:
                            raise LogicError(ExceptionType.NO_TABLE_FOUND, dependency)
                        self._manage_wildcard(column, wildcards[0])
                    else:
                        dependency.add_attributes(source_tables[0])
                        self._graph.add_node(column)

    def _prepare_wildcard(self, column, dependency, tables):
        if dependency.table_alias:
            # If the wildcard has a table alias, then we must have a relationship with a specific table
            sources = [t for t in tables if t.alias == dependency.table_alias or t.name == dependency.table_alias]
            if len(sources) > 1:
                raise LogicError(ExceptionType.WILDCARD_MULTIPLE_SOURCES, column)
        else:
            # If the wildcard doesn't have any reference to a table, then we use a table for each dependency
            sources = [t for t in tables if t.name not in [d.table for d in column.dependencies]]
            if not sources:
                raise LogicError(ExceptionType.ALIAS_WITH_NO_SOURCES, column)
        dependency.add_attributes(sources[0])
        self._graph.add_node(column)

    def _manage_wildcard(self, column, wildcard):
        # Create a parent column combining the information from the wildcard and the child column
        parent_col = Column(name=column.name, cte=wildcard.cte)
        parent_col.dependencies = {dep.copy() for dep in wildcard.dependencies}
        for dep in parent_col.dependencies:
            dep.name = column.name
        self._graph.add_edge(parent_col, column)

        # I check if the parent wildcard has itself a wildcard dependency and, in that case, I repeat the process
        wildcards = [c for c in self._graph.nodes if c.is_wildcard and c.cte == list(parent_col.dependencies)[0].table]
        if wildcards:
            self._manage_wildcard(parent_col, wildcards[0])

    def _manage_table_alias(self, column, dependency, tables):
        source_tables = [t for t in tables if t.alias == dependency.table_alias or t.name == dependency.table_alias]
        if not source_tables:
            raise LogicError(ExceptionType.NO_ALIAS_TABLE_FOUND, dependency)
        if len(source_tables) > 1:
            raise LogicError(ExceptionType.MULTIPLE_ALIAS_TABLES_FOUND, dependency)

        table = source_tables[0]
        if table.dataset:
            dependency.add_attributes(table)
            self._graph.add_node(column)
        else:
            wildcards = [c for c in self._graph.nodes if not c.is_auxiliary and c.cte == table.name and c.is_wildcard]
            if wildcards:
                parent_col = Column(name=column.name, cte=wildcards[0].cte)
                parent_col.dependencies = {dep.copy() for dep in wildcards[0].dependencies}
                for dep in parent_col.dependencies:
                    dep.name = column.name
            else:
                parent_cols = [c for c in self._graph.nodes if not c.is_auxiliary and c.cte == table.name and
                               dependency.name == c.name.split('.')[-1]]
                if not parent_cols:
                    raise LogicError(ExceptionType.ALIAS_WITH_NO_SOURCES, column)
                if len(parent_cols) > 1:
                    raise LogicError(ExceptionType.MULTIPLE_ALIAS_COLUMNS_FOUND, dependency)
                parent_col = parent_cols[0]
            self._graph.add_edge(parent_col, column)
