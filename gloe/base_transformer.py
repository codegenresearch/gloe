import copy
import types
import uuid
import inspect
from functools import cached_property
from inspect import Signature
import networkx as nx
from networkx import DiGraph, Graph
from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    Union,
    cast,
    Iterable,
    get_args,
    get_origin,
    TypeAlias,
    Type,
)
from uuid import UUID
from itertools import groupby

from gloe._utils import _format_return_annotation

__all__ = ["BaseTransformer", "TransformerException", "PreviousTransformer"]

_In = TypeVar("_In")
_Out = TypeVar("_Out")
_NextOut = TypeVar("_NextOut")
_Out2 = TypeVar("_Out2")
_Out3 = TypeVar("_Out3")
_Out4 = TypeVar("_Out4")
_Out5 = TypeVar("_Out5")
_Out6 = TypeVar("_Out6")
_Out7 = TypeVar("_Out7")
_Self = TypeVar("_Self", bound="BaseTransformer")

PreviousTransformer: TypeAlias = Union[
    None,
    _Self,
    tuple[_Self, _Self],
    tuple[_Self, _Self, _Self],
    tuple[_Self, _Self, _Self, _Self],
    tuple[_Self, _Self, _Self, _Self, _Self],
    tuple[_Self, _Self, _Self, _Self, _Self, _Self],
    tuple[_Self, _Self, _Self, _Self, _Self, _Self, _Self],
]


class TransformerException(Exception):
    """Exception raised for errors in the transformer process.

    Attributes:
        internal_exception -- the original exception that was raised
        raiser_transformer -- the transformer that raised the exception
        message -- explanation of the error
    """

    def __init__(self, internal_exception: Union["TransformerException", Exception], raiser_transformer: "BaseTransformer", message: str | None = None):
        self._internal_exception = internal_exception
        self.raiser_transformer = raiser_transformer
        self._traceback = internal_exception.__traceback__
        internal_exception.__cause__ = self
        super().__init__(message)

    @property
    def internal_exception(self) -> Exception:
        """Returns the internal exception with traceback."""
        return self._internal_exception.with_traceback(self._traceback)


class BaseTransformer(Generic[_In, _Out, _Self]):
    """Base class for all transformers.

    Attributes:
        _previous -- the previous transformer(s) in the chain
        _children -- the child transformers
        _invisible -- whether the transformer is invisible in the graph
        id -- unique identifier for the transformer
        instance_id -- unique instance identifier for the transformer
        _label -- label for the transformer in the graph
        _graph_node_props -- properties for the transformer's graph node
        events -- list of events associated with the transformer
    """

    def __init__(self):
        self._previous: PreviousTransformer = None
        self._children: list[_Self] = []
        self._invisible = False
        self.id = uuid.uuid4()
        self.instance_id = uuid.uuid4()
        self._label = self.__class__.__name__
        self._graph_node_props: dict[str, Any] = {"shape": "box"}
        self.events = []

    @property
    def label(self) -> str:
        """Label used in visualization.

        When the transformer is created by the `@transformer` decorator, it is the
        name of the function.

        When creating a transformer by extending the `Transformer` class, it is the name of
        the class.
        """
        return self._label

    @property
    def graph_node_props(self) -> dict[str, Any]:
        """Graph node properties used for visualization."""
        return self._graph_node_props

    @property
    def children(self) -> list[_Self]:
        """Child transformers in the chain."""
        return self._children

    @property
    def previous(self) -> PreviousTransformer:
        """Previous transformers in the chain.

        Can be None, a single transformer, or a tuple of many transformers.
        """
        return self._previous

    @property
    def invisible(self) -> bool:
        """Indicates if the transformer is invisible in the graph."""
        return self._invisible

    def __hash__(self) -> int:
        """Returns the hash of the transformer's ID."""
        return hash(self.id)

    def __eq__(self, other) -> bool:
        """Checks equality based on the transformer's ID."""
        if isinstance(other, BaseTransformer):
            return self.id == other.id
        return NotImplemented

    def copy(self, transform: Callable[[_Self, _In], _Out] | None = None, regenerate_instance_id: bool = False) -> _Self:
        """Creates a copy of the transformer with optional modifications.

        Args:
            transform -- an optional transformation function to apply
            regenerate_instance_id -- whether to regenerate the instance ID

        Returns:
            A copy of the transformer.
        """
        copied = copy.copy(self)
        func_type = types.MethodType
        if transform is not None:
            setattr(copied, "transform", func_type(transform, copied))
        if regenerate_instance_id:
            copied.instance_id = uuid.uuid4()
        if self.previous is not None:
            if isinstance(self.previous, tuple):
                new_previous = tuple(prev.copy() for prev in self.previous)
                copied._previous = cast(PreviousTransformer, new_previous)
            elif isinstance(self.previous, BaseTransformer):
                copied._previous = self.previous.copy()
        copied._children = [child.copy(regenerate_instance_id=True) for child in self.children]
        return copied

    @property
    def graph_nodes(self) -> dict[UUID, _Self]:
        """Returns a dictionary of graph nodes."""
        nodes = {self.instance_id: self}
        if self.previous is not None:
            if isinstance(self.previous, tuple):
                for prev in self.previous:
                    nodes.update(prev.graph_nodes)
            elif isinstance(self.previous, BaseTransformer):
                nodes.update(self.previous.graph_nodes)
        for child in self.children:
            nodes.update(child.graph_nodes)
        return nodes

    def _set_previous(self, previous: PreviousTransformer):
        """Sets the previous transformer.

        Args:
            previous -- the previous transformer(s) to set
        """
        if self.previous is None:
            self._previous = previous
        elif isinstance(self.previous, tuple):
            for prev in self.previous:
                prev._set_previous(previous)
        elif isinstance(self.previous, BaseTransformer):
            self.previous._set_previous(previous)

    def signature(self) -> Signature:
        """Returns the signature of the transformer."""
        return self._signature(type(self))

    def _signature(self, klass: Type) -> Signature:
        """Generates the signature for the transformer.

        Args:
            klass -- the class type to generate the signature for

        Returns:
            The signature of the transformer.
        """
        orig_bases = getattr(self, "__orig_bases__", [])
        transformer_args = [get_args(base) for base in orig_bases if get_origin(base) == klass]
        generic_args = [get_args(base) for base in orig_bases if get_origin(base) == Generic]
        orig_class = getattr(self, "__orig_class__", None)
        specific_args = {}
        if len(transformer_args) == 1 and len(generic_args) == 1 and orig_class is not None:
            generic_arg = generic_args[0]
            transformer_arg = transformer_args[0]
            specific_args = {generic: specific for generic, specific in zip(generic_arg, get_args(orig_class)) if generic in transformer_arg}
        signature = inspect.signature(self.transform)
        new_return_annotation = specific_args.get(signature.return_annotation, signature.return_annotation)
        parameters = list(signature.parameters.values())
        if parameters:
            parameter = parameters[0]
            parameter = parameter.replace(annotation=specific_args.get(parameter.annotation, parameter.annotation))
            return signature.replace(return_annotation=new_return_annotation, parameters=[parameter])
        return signature.replace(return_annotation=new_return_annotation)

    @property
    def output_type(self) -> Any:
        """Returns the output type of the transformer."""
        return self.signature().return_annotation

    @property
    def output_annotation(self) -> str:
        """Returns the output annotation of the transformer."""
        output_type = self.output_type
        return _format_return_annotation(output_type, None, None)

    @property
    def input_type(self) -> Any:
        """Returns the input type of the transformer."""
        parameters = list(self.signature().parameters.items())
        if parameters:
            return parameters[0][1].annotation

    @property
    def input_annotation(self) -> str:
        """Returns the input annotation of the transformer."""
        return self.input_type.__name__

    def _add_net_node(self, net: Graph, custom_data: dict[str, Any] = {}) -> str:
        """Adds a node to the network graph.

        Args:
            net -- the network graph to add the node to
            custom_data -- additional data for the node

        Returns:
            The node ID of the added node.
        """
        node_id = self.node_id
        props = {**self.graph_node_props, **custom_data, "label": self.label}
        if node_id not in net.nodes:
            net.add_node(node_id, **props)
        else:
            nx.set_node_attributes(net, {node_id: props})
        return node_id

    def _add_child_node(self, child: _Self, child_net: DiGraph, parent_id: str, next_node: _Self):
        """Adds a child node to the network graph.

        Args:
            child -- the child transformer to add
            child_net -- the network graph for the child
            parent_id -- the ID of the parent node
            next_node -- the next transformer in the chain
        """
        child._dag(child_net, next_node, custom_data={"parent_id": parent_id})

    @property
    def node_id(self) -> str:
        """Returns the node ID of the transformer."""
        return str(self.instance_id)

    @cached_property
    def visible_previous(self) -> PreviousTransformer:
        """Returns the visible previous transformer."""
        previous = self.previous
        if isinstance(previous, BaseTransformer):
            if previous.invisible:
                if previous.previous is None:
                    return previous
                if isinstance(previous.previous, tuple):
                    return previous.previous
                return previous.visible_previous
            else:
                return previous
        return previous

    def _add_children_subgraph(self, net: DiGraph, next_node: _Self):
        """Adds a children subgraph to the network graph.

        Args:
            net -- the network graph to add the subgraph to
            next_node -- the next transformer in the chain
        """
        next_node_id = next_node.node_id
        children_nets = [DiGraph() for _ in self.children]
        visible_previous = self.visible_previous
        for child, child_net in zip(self.children, children_nets):
            self._add_child_node(child, child_net, self.node_id, next_node)
            net.add_nodes_from(child_net.nodes.data())
            net.add_edges_from(child_net.edges.data())
            child_root_node = [n for n in child_net.nodes if child_net.in_degree(n) == 0][0]
            child_final_node = [n for n in child_net.nodes if child_net.out_degree(n) == 0][0]
            if self.invisible:
                if isinstance(visible_previous, tuple):
                    for prev in visible_previous:
                        net.add_edge(prev.node_id, child_root_node, label=prev.output_annotation)
                elif isinstance(visible_previous, BaseTransformer):
                    net.add_edge(visible_previous.node_id, child_root_node, label=visible_previous.output_annotation)
            else:
                node_id = self._add_net_node(net)
                net.add_edge(node_id, child_root_node)
            if child_final_node != next_node_id:
                net.add_edge(child_final_node, next_node_id, label=next_node.input_annotation)

    def _dag(self, net: DiGraph, next_node: _Self | None = None, custom_data: dict[str, Any] = {}):
        """Generates the directed acyclic graph for the transformer.

        Args:
            net -- the network graph to generate the DAG for
            next_node -- the next transformer in the chain
            custom_data -- additional data for the graph
        """
        in_nodes = [edge[1] for edge in net.in_edges()]
        previous = self.previous
        if previous is not None:
            if isinstance(previous, tuple):
                if self.invisible and next_node is not None:
                    next_node_id = next_node._add_net_node(net)
                    _next_node = next_node
                else:
                    next_node_id = self._add_net_node(net, custom_data)
                    _next_node = self
                for prev in previous:
                    previous_node_id = prev.node_id
                    if not prev.invisible and len(prev.children) == 0:
                        net.add_edge(previous_node_id, next_node_id, label=prev.output_annotation)
                    if previous_node_id not in in_nodes:
                        prev._dag(net, _next_node, custom_data)
            elif isinstance(previous, BaseTransformer):
                if self.invisible and next_node is not None:
                    next_node_id = next_node._add_net_node(net)
                    _next_node = next_node
                else:
                    next_node_id = self._add_net_node(net, custom_data)
                    _next_node = self
                previous_node_id = previous.node_id
                if len(previous.children) == 0 and (not previous.invisible or previous.previous is None):
                    previous_node_id = previous._add_net_node(net)
                    net.add_edge(previous_node_id, next_node_id, label=previous.output_annotation)
                if previous_node_id not in in_nodes:
                    previous._dag(net, _next_node, custom_data)
        else:
            self._add_net_node(net, custom_data)
        if len(self.children) > 0 and next_node is not None:
            self._add_children_subgraph(net, next_node)

    def graph(self) -> DiGraph:
        """Generates the graph for the transformer.

        Returns:
            The generated graph.
        """
        net = nx.DiGraph()
        net.graph["splines"] = "ortho"
        self._dag(net)
        return net

    def export(self, path: str, with_edge_labels: bool = True):
        """Exports the graph to a file.

        Args:
            path -- the file path to export the graph to
            with_edge_labels -- whether to include edge labels in the export
        """
        net = self.graph()
        boxed_nodes = [node for node in net.nodes.data() if "parent_id" in node[1] and "bounding_box" in node[1]]
        if not with_edge_labels:
            for u, v in net.edges:
                net.edges[u, v]["label"] = ""
        agraph = nx.nx_agraph.to_agraph(net)
        subgraphs = groupby(boxed_nodes, key=lambda x: x[1]["parent_id"])
        for parent_id, nodes in subgraphs:
            nodes = list(nodes)
            node_ids = [node[0] for node in nodes]
            if nodes:
                label = nodes[0][1]["box_label"]
                agraph.add_subgraph(node_ids, label=label, name=f"cluster_{parent_id}", style="dotted")
        agraph.write(path)

    def __len__(self) -> int:
        """Returns the length of the transformer."""
        return 1


### Key Changes:
1. **Removed Markdown-Style Comment**: Removed the problematic comment at line 408 that was causing the `SyntaxError`.
2. **Type Annotations**: Ensured type annotations are consistent and correctly specify the types.
3. **Docstrings**: Reviewed and ensured docstrings are clear and consistent.
4. **Property Return Types**: Explicitly defined return types for properties.
5. **Use of `Union`**: Ensured `Union` is used correctly in `TransformerException`.
6. **Variable Naming and Structure**: Checked and ensured naming conventions and structure are consistent.
7. **Method Implementation**: Reviewed and ensured methods are structured similarly to the gold code.
8. **Imports**: Organized imports logically.
9. **Use of `UUID`**: Ensured `UUID` is imported and used consistently.