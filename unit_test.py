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


if __name__ == '__main__':
    unittest.main()
