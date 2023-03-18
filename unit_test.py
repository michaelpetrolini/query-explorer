import unittest

from utility import get_tree, get_column, distance_from_ancestor


class TestQuery(unittest.TestCase):
    def test_simple(self):
        tree = get_tree(1)
        self.assertEqual(tree.graph.number_of_nodes(), 4)
        self.assertEqual(tree.graph.number_of_edges(), 0)

    def test_table_prefix(self):
        tree = get_tree(2)
        self.assertEqual(tree.graph.number_of_nodes(), 4)
        self.assertEqual(tree.graph.number_of_edges(), 0)

    def test_column_alias(self):
        tree = get_tree(3)
        self.assertEqual(tree.graph.number_of_nodes(), 4)
        self.assertEqual(tree.graph.number_of_edges(), 0)

    def test_prefix_alias(self):
        tree = get_tree(4)
        self.assertEqual(tree.graph.number_of_nodes(), 4)
        self.assertEqual(tree.graph.number_of_edges(), 0)

    def test_dependency1(self):
        tree = get_tree(5)
        self.assertEqual(tree.graph.number_of_nodes(), 12)
        self.assertEqual(tree.graph.number_of_edges(), 5)
        self.assertEqual(distance_from_ancestor(tree, get_column(tree, 'c_surname')), 1)

    def test_dependency2(self):
        tree = get_tree(6)
        self.assertEqual(tree.graph.number_of_nodes(), 18)
        self.assertEqual(tree.graph.number_of_edges(), 8)
        self.assertEqual(distance_from_ancestor(tree, get_column(tree, 'cost')), 2)


def test_dependency3(self):
    tree = get_tree(7)
    self.assertEqual(tree.graph.number_of_nodes(), 12)
    self.assertEqual(tree.graph.number_of_edges(), 5)
    self.assertEqual(len(tree.graph.in_edges(get_column(tree, 'name_surname'))), 2)


if __name__ == '__main__':
    unittest.main()
