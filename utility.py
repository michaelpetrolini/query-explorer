import matplotlib.colors as pltc
import networkx as nx
import sqlparse
from matplotlib import pyplot as plt

from generator import ColumnTree
from structures import Column


def is_black_font_friendly(hex_str: str):
    """checking if the color luminance is high enough for a black font color"""
    (r, g, b) = (hex_str[1:3], hex_str[3:5], hex_str[5:])
    return (int(r, 16) * 0.299 + int(g, 16) * 0.587 + int(b, 16) * 0.114) / 255 >= 0.4


all_colors = [v for k, v in pltc.cnames.items() if is_black_font_friendly(v)]


def show(graph: nx.DiGraph):
    groups = {n.cte or '' for n in graph.nodes}
    mapping = dict(zip(sorted(groups), all_colors))
    colors = [mapping[n.cte or ''] for n in graph.nodes]
    pos = nx.spring_layout(graph)
    nx.draw(graph, pos=pos, with_labels=True, node_color=colors, node_size=2000, font_size=6)
    plt.show()


def show_clean_graph(tree: ColumnTree):
    used_columns = tree.get_used_columns()
    subgraph = tree.get_graph().subgraph(used_columns)
    show(subgraph)


def save_graph(graph: nx.DiGraph, path: str):
    nx.write_gexf(graph, open(path, 'wb'))


def load_graph(path: str):
    return nx.read_gexf(open(path, 'rb'))


def get_tree(filename: str):
    with open(f'unit_test/{filename}.sql', 'r') as r:
        query = sqlparse.format(r.read(), reindent=True, keyword_case='upper', strip_comments=True)
        statement = sqlparse.parse(query)[0]
        tree = ColumnTree()
        tree.generate(statement)
        return tree


def get_column(tree: ColumnTree, name: str, cte: str = None):
    for c in tree.get_nodes():
        if c.cte == cte and c.name == name:
            return c
    return None


def distance_from_ancestor(tree: ColumnTree, column: Column):
    parents = [n[0] for n in tree.get_graph().in_edges(column)]
    return max([distance_from_ancestor(tree, c) for c in parents]) + 1 if parents else 0
