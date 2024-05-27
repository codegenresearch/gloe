from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any
from uuid import UUID


class NodeType(Enum):
    Transformer = "Transformer"
    Begin = "Begin"
    End = "End"
    Condition = "Condition"
    Convergent = "Convergent"


@dataclass
class GatewaySettings:
    extra_labels: list[str] = field(default_factory=list)


@dataclass
class PlottingSettings:
    node_type: NodeType
    has_children: bool = False
    invisible: bool = False
    is_async: bool = False
    is_gateway: bool = False
    gateway_settings: Optional[GatewaySettings] = None
    parent_id: Optional[str] = None


def export_dot_props(settings: PlottingSettings, instance_id: UUID) -> dict[str, Any]:
    node_props: dict[str, Any] = {"shape": "box"}

    if settings.node_type == NodeType.Condition:
        node_props = {"shape": "diamond", "style": "filled", "port": "n"}
    elif settings.node_type == NodeType.Convergent:
        node_props = {
            "shape": "diamond",
            "width": 0.5,
            "height": 0.5,
        }

    if settings.has_children:
        node_props = node_props | {
            "parent_id": instance_id,
            "bounding_box": True,
            "box_label": "mapping",
        }

    return node_props


class GloeGraph:
    def __init__(self, name: str = ""):
        self.name = name
        self.attrs: dict[str, Any] = {}
        self.nodes: dict[str, Any] = {}
        self.edges: dict[tuple[str, str], Any] = {}
        self.subgraphs: list["GloeGraph"] = []

    def add_node(self, id: str, **attrs):
        self.nodes[id] = attrs

    def add_edge(self, u: str, v: str, **attrs):
        self.edges[(u, v)] = attrs

    def add_subgraph(self, subgraph: "GloeGraph"):
        self.subgraphs.append(subgraph)

    def to_agraph(self):
        try:
            import pygraphviz  # noqa: F401

        except ImportError as err:
            raise ImportError(
                "Please, the module pygraphviz is required for this method,"
                + " install with "
                + """"conda install --channel conda-forge pygraphviz" or """
                + """"pip install pygraphviz". More information is available in """
                + "https://pygraphviz.github.io/documentation/stable/install.html"
            ) from err

        A = pygraphviz.AGraph(
            name=self.name, compound=True, directed=True, style="dotted", **self.attrs
        )

        subgraphs_stack = [(self, self.subgraphs)]
        sub_agraph = A
        while len(subgraphs_stack) > 0:
            graph, subgraphs = subgraphs_stack.pop(0)
            for subgraph in subgraphs:
                sub_agraph = sub_agraph.add_subgraph(name=subgraph.name)

                for node, nodedata in subgraph.nodes.items():
                    sub_agraph.add_node(node, **nodedata)

                for (u, v), edgedata in subgraph.edges.items():
                    sub_agraph.add_edge(u, v, **edgedata)

                if len(subgraph.subgraphs) > 0:
                    subgraphs_stack.append((subgraph, subgraph.subgraphs))

        for node, nodedata in self.nodes.items():
            A.add_node(node, **nodedata)

        for (u, v), edgedata in self.edges.items():
            A.add_edge(u, v, **edgedata)

        return A
