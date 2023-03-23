import networkx as nx
from sqlparse.sql import Where, Comparison, Identifier, IdentifierList, Function, Case, Parenthesis
from sqlparse.tokens import CTE, DML, Keyword, Wildcard, Literal
import uuid

from structures import Column, Dependency, Table


def _table(token):
    sources = token.value.replace('`', '').split()[0].split('.')
    return Table(name=sources[0], alias=token.get_alias()) if len(sources) == 1 \
        else Table(name=sources[-1],
                   dataset=sources[-2],
                   project=sources[-3] if len(sources) > 2 else None,
                   alias=token.get_alias())


def _dependency(parameter):
    if parameter.ttype == Wildcard:
        elements = parameter.value.split('.')
        return Dependency(name=elements[-1], table_alias=elements[0] if len(elements) > 1 else None)
    return Dependency(name=parameter.get_real_name(),
                      table_alias=parameter.get_parent_name())


def _function(function):
    for elem in function.get_parameters():
        if isinstance(elem, Identifier):
            elements = [e for e in elem.tokens if type(e) in (Function, Case)]
            if elements:
                for element in elements:
                    yield from _get_dependencies(element)
            else:
                yield _dependency(elem)
        elif isinstance(elem, Function):
            yield from _function(elem)


def _case(token):
    for condition, value in token.get_cases(skip_ws=True):
        if condition:
            for elem in condition:
                if isinstance(elem, Identifier):
                    yield _dependency(elem)
                elif isinstance(elem, Function):
                    yield from _function(elem)
        for elem in value:
            if isinstance(elem, Identifier):
                yield _dependency(elem)
            elif isinstance(elem, Function):
                yield from _function(elem)


def _get_dependencies(token):
    if isinstance(token, Function) and token.get_name() != 'ROW_NUMBER':
        yield from _function(token)
    elif isinstance(token, Case):
        yield from _case(token)
    elif isinstance(token, Identifier):
        elements = [elem for elem in token.tokens if type(elem) in (Function, Case)]
        if elements:
            for element in elements:
                yield from _get_dependencies(element)
        else:
            yield _dependency(token)
    elif token.ttype == Wildcard:
        yield _dependency(token)


def _column(token, cte):
    name = (token.get_alias() or token.value) if isinstance(token, Identifier) else token.value
    column = Column(name=name, cte=cte, is_wildcard=token.ttype == Wildcard)
    if isinstance(token, Identifier) and token.tokens[0].ttype in Literal:
        column.value = token.tokens[0].value
    else:
        column.dependencies = _get_dependencies(token)
    return column


def _where(token, cte):
    columns = set()
    for t in token.tokens:
        if isinstance(t, Comparison):
            columns.add(_column(t.left, cte))
    return columns


def _on(token, cte):
    columns = set()
    columns.add(_column(token.left, cte))
    columns.add(_column(token.right, cte))
    return columns


def _add_attributes(column, table):
    column.name = table.name
    column.dataset = table.dataset
    column.project = table.project


class ColumnTree:
    def __init__(self):
        self.graph = nx.DiGraph()

    def _from(self, token):
        tables = set()
        if isinstance(token, Identifier) and isinstance(token.tokens[0], Parenthesis):
            cte = token.tokens[-1].value
            self.generate(token.tokens[0], cte)
            tables.add(Table(name=cte, alias=None))
        elif isinstance(token, Parenthesis):
            table_id = str(uuid.uuid4())
            self.generate(token, table_id)
            tables.add(Table(name=table_id, alias=None))
        elif isinstance(token, IdentifierList):
            for t in token.tokens:
                if isinstance(t, Identifier):
                    tables.add(_table(t))
        else:
            tables.add(_table(token))
        return tables

    def test_dependencies(self):
        linked_columns = {n for n in self.graph.nodes if self.graph.in_edges(n) or self.graph.out_edges(n)}
        subgraph = self.graph.subgraph(linked_columns)
        return subgraph

    def get_used_columns(self):
        def get_linked_columns(columns):
            linked_columns = set()
            for column in columns:
                linked_columns.add(column)
                incoming_nodes = [n[0] for n in self.graph.in_edges(column)]
                linked_columns.update(get_linked_columns(incoming_nodes))
            return linked_columns

        leaves = [column for column in self.graph.nodes if not column.cte]
        used_columns = get_linked_columns(leaves)
        return used_columns

    def get_unused_columns(self):
        return self.graph.nodes - self.get_used_columns()

    def impact_analysis(self, column):
        impacted_columns = []

        if len(self.graph.out_edges(column)) == 0 and len(self.graph.in_edges(column)) > 0:
            impacted_columns.append(column)
        else:
            children = [n[0] for n in self.graph.out_edges(column)]
            for child in children:
                impacted_columns = impacted_columns + self.impact_analysis(child)

        return impacted_columns

    def dependency_analysis(self, column):
        dependency_columns = []

        if len(self.graph.out_edges(column)) > 0 and len(self.graph.in_edges(column)) == 0:
            dependency_columns.append(column)
        else:
            parents = [n[0] for n in self.graph.in_edges(column)]
            for parent in parents:
                dependency_columns = dependency_columns + self.dependency_analysis(parent)

        return dependency_columns

    def _cte(self, identifier):
        cte = identifier[0].value
        self.generate(identifier[-1], cte)

    def _cte_list(self, iterator):
        index, token = iterator.token_next(-1)
        while token:
            if isinstance(token, Identifier):
                self._cte(token)
            index, token = iterator.token_next(index)

    def generate(self, iterator, cte=None):
        columns = set()
        tables = set()
        wildcards = set()
        index, token = iterator.token_next(-1)
        while token:
            if token.ttype == CTE:
                index, token = iterator.token_next(index)
                self._cte_list(token) if isinstance(token, IdentifierList) else self._cte(token)
            elif token.ttype == DML:
                index, token = iterator.token_next(index)
                if isinstance(token, IdentifierList):
                    for t in token.tokens:
                        if isinstance(t, Identifier) or t.ttype == Wildcard:
                            columns.add(_column(t, cte))
                else:
                    columns.add(_column(token, cte))
            elif token.ttype == Keyword and token.value == 'OVER':
                index, token = iterator.token_next(index)
                columns.add(_column(token, cte))
            elif token.ttype == Keyword and token.value.split(' ')[-1] in ["FROM", "JOIN"]:
                index, token = iterator.token_next(index)
                tables.update(self._from(token))
            elif isinstance(token, Comparison):
                columns.update(_on(token, cte))
            elif isinstance(token, Where):
                columns.update(_where(token, cte))
            index, token = iterator.token_next(index)

        self._add_columns_to_graph(columns, tables, wildcards)

        return columns

    def _add_columns_to_graph(self, columns, tables, wildcards):
        for column in columns:
            if column.value:
                self.graph.add_node(column)
                continue
            for dependency in column.dependencies:
                for table in tables:
                    if dependency.table_alias and (
                            table.alias != dependency.table_alias and table.name != dependency.table_alias):
                        continue

                    if column.is_wildcard:
                        for parent_col in [c for c in self.graph.nodes if table.name == c.cte]:
                            new_column = Column(name=parent_col.name, cte=column.cte)
                            self.graph.add_edge(parent_col, new_column)
                    if not table.dataset:
                        # I check every column already in the graph looking for possible parents
                        for parent_col in [c for c in self.graph.nodes if table.name == c.cte]:
                            if dependency.name == parent_col.name.split('.')[-1]:
                                self.graph.add_edge(parent_col, column)
                    # I found a source table matching with the column alias
                    elif dependency.table_alias and (
                            table.alias == dependency.table_alias or table.name == dependency.table_alias):
                        _add_attributes(dependency, table)
                        self.graph.add_node(column)

                # If a field doesn't have an alias and doesn't depend on any parent, I have to guess the source table
                if not column.is_wildcard and not dependency.dataset and column not in self.graph.nodes:
                    table = [t for t in tables if t.dataset][0]
                    _add_attributes(dependency, table)
                    self.graph.add_node(column)
