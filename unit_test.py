import unittest

from utility import get_tree, get_column, distance_from_ancestor


class TestQuery(unittest.TestCase):
    def test_simple(self):
        tree = get_tree('simple')
        self.assertEqual(tree._graph.number_of_nodes(), 4)
        self.assertEqual(tree._graph.number_of_edges(), 0)

    def test_table_prefix(self):
        tree = get_tree('table_prefix')
        self.assertEqual(tree._graph.number_of_nodes(), 4)
        self.assertEqual(tree._graph.number_of_edges(), 0)

    def test_column_alias(self):
        tree = get_tree('column_alias')
        self.assertEqual(tree._graph.number_of_nodes(), 4)
        self.assertEqual(tree._graph.number_of_edges(), 0)

    def test_prefix_alias(self):
        tree = get_tree('prefix_alias')
        self.assertEqual(tree._graph.number_of_nodes(), 4)
        self.assertEqual(tree._graph.number_of_edges(), 0)

    def test_dependency(self):
        tree = get_tree('dependency')
        self.assertEqual(tree._graph.number_of_nodes(), 12)
        self.assertEqual(tree._graph.number_of_edges(), 5)
        self.assertEqual(distance_from_ancestor(tree, get_column(tree, 'c_surname')), 1)

    def test_long_dependency(self):
        tree = get_tree('long_dependency')
        self.assertEqual(tree._graph.number_of_nodes(), 18)
        self.assertEqual(tree._graph.number_of_edges(), 8)
        self.assertEqual(distance_from_ancestor(tree, get_column(tree, 'cost')), 2)

    def test_double_dependency(self):
        tree = get_tree('double_dependency')
        self.assertEqual(tree._graph.number_of_nodes(), 11)
        self.assertEqual(tree._graph.number_of_edges(), 5)
        self.assertEqual(len(tree._graph.in_edges(get_column(tree, 'name_surname'))), 2)

    def test_unnamed_parenthesis_from(self):
        tree = get_tree('unnamed_parenthesis_from')
        self.assertEqual(tree._graph.number_of_nodes(), 8)
        self.assertEqual(tree._graph.number_of_edges(), 4)

    def test_named_parenthesis_from(self):
        tree = get_tree('named_parenthesis_from')
        self.assertEqual(tree._graph.number_of_nodes(), 10)
        self.assertEqual(tree._graph.number_of_edges(), 4)
        self.assertTrue(get_column(tree, 'id_order', 'orders'))

    def test_constant_values(self):
        tree = get_tree('constant_values')
        self.assertEqual(tree._graph.number_of_nodes(), 3)

    def test_wildcard_of_dependency(self):
        tree = get_tree('wildcard_of_dependency')
        self.assertEqual(len([c for c in tree.get_nodes() if c.cte is None]),
                         len([c for c in tree.get_nodes() if c.cte == 'orders']))

    def test_wildcard_of_source(self):
        tree = get_tree('wildcard_of_source')
        self.assertEqual(len([c for c in tree.get_nodes() if c.cte is None]),
                         len([c for c in tree.get_nodes() if c.cte == 'orders']))

    def test_wildcard_recursion(self):
        tree = get_tree('wildcard_recursion')
        self.assertTrue(get_column(tree, 'id_order', 'orders'))
        self.assertTrue(get_column(tree, 'quantity', 'orders'))
        self.assertTrue(get_column(tree, 'weekday', 'orders'))

    def test_union(self):
        tree = get_tree('union')
        self.assertEqual(len(tree._graph.in_edges(get_column(tree, 'id_product'))), 2)
        self.assertEqual(len(tree._graph.in_edges(get_column(tree, 'quantity'))), 2)
        self.assertEqual(len(tree._graph.in_edges(get_column(tree, 'price'))), 2)

    def test_union_wildcard(self):
        tree = get_tree('union_wildcard')
        self.assertEqual(len(tree._graph.in_edges(get_column(tree, 'id_product'))), 2)
        self.assertEqual(len(tree._graph.in_edges(get_column(tree, 'quantity'))), 2)
        self.assertEqual(len(tree._graph.in_edges(get_column(tree, 'price'))), 2)

    def test_row_number(self):
        tree = get_tree('row_number')
        self.assertEqual(len(get_column(tree, 'rn1').dependencies), 1)
        self.assertEqual(len(get_column(tree, 'rn2').dependencies), 2)
        self.assertEqual(len(get_column(tree, 'rn3').dependencies), 3)
        self.assertEqual(len(get_column(tree, 'rn4').dependencies), 4)
        self.assertEqual(len(get_column(tree, 'rn5').dependencies), 4)


if __name__ == '__main__':
    unittest.main()
