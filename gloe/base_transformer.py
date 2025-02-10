import copy
import types
import uuid
import inspect
from functools import cached_property
from typing import Any, Callable, Generic, TypeVar, Union, cast, Iterable, Type

__all__ = ["BaseTransformer", "TransformerException", "PreviousTransformer"]

_In = TypeVar("_In")
_Out = TypeVar("_Out")
_Self = TypeVar("_Self", bound="BaseTransformer")

PreviousTransformer = Union[
    None,
    _Self,
    tuple[_Self, ...],
]


class TransformerException(Exception):
    def __init__(
        self,
        internal_exception: Exception,
        raiser_transformer: "BaseTransformer",
        message: str = None,
    ):
        self.internal_exception = internal_exception
        self.raiser_transformer = raiser_transformer
        self._traceback = internal_exception.__traceback__
        internal_exception.__cause__ = self
        super().__init__(message)

    @property
    def internal_exception(self):
        return self.internal_exception.with_traceback(self._traceback)


class BaseTransformer(Generic[_In, _Out]):
    def __init__(self):
        self._previous: PreviousTransformer = None
        self._children: list["BaseTransformer"] = []
        self._invisible = False
        self.id = uuid.uuid4()
        self.instance_id = uuid.uuid4()
        self._label = self.__class__.__name__
        self._graph_node_props = {"shape": "box"}
        self.events = []

    @property
    def label(self) -> str:
        return self._label

    @property
    def graph_node_props(self) -> dict[str, Any]:
        return self._graph_node_props

    @property
    def children(self) -> list["BaseTransformer"]:
        return self._children

    @property
    def previous(self) -> PreviousTransformer:
        return self._previous

    @property
    def invisible(self) -> bool:
        return self._invisible

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, BaseTransformer):
            return self.id == other.id
        return NotImplemented

    def copy(
        self,
        transform: Callable[[_In], _Out] = None,
        regenerate_instance_id: bool = False,
    ) -> "BaseTransformer":
        copied = copy.copy(self)

        if transform is not None:
            setattr(copied, "transform", types.MethodType(transform, copied))

        if regenerate_instance_id:
            copied.instance_id = uuid.uuid4()

        if self.previous is not None:
            if isinstance(self.previous, tuple):
                copied._previous = tuple(prev.copy() for prev in self.previous)
            else:
                copied._previous = self.previous.copy()

        copied._children = [child.copy(regenerate_instance_id=True) for child in self.children]

        return copied

    @property
    def graph_nodes(self) -> dict[uuid.UUID, "BaseTransformer"]:
        nodes = {self.instance_id: self}

        if self.previous is not None:
            if isinstance(self.previous, tuple):
                for prev in self.previous:
                    nodes.update(prev.graph_nodes)
            else:
                nodes.update(self.previous.graph_nodes)

        for child in self.children:
            nodes.update(child.graph_nodes)

        return nodes

    def _set_previous(self, previous: PreviousTransformer):
        if self.previous is None:
            self._previous = previous
        elif isinstance(self.previous, tuple):
            for prev in self.previous:
                prev._set_previous(previous)
        else:
            self.previous._set_previous(previous)

    def signature(self) -> inspect.Signature:
        return self._signature(BaseTransformer)

    def _signature(self, klass: Type) -> inspect.Signature:
        orig_bases = getattr(self, "__orig_bases__", [])
        transformer_args = [
            get_args(base) for base in orig_bases if get_origin(base) == klass
        ]
        generic_args = [
            get_args(base) for base in orig_bases if get_origin(base) == Generic
        ]

        orig_class = getattr(self, "__orig_class__", None)

        specific_args = {}
        if (
            len(transformer_args) == 1
            and len(generic_args) == 1
            and orig_class is not None
        ):
            generic_arg = generic_args[0]
            transformer_arg = transformer_args[0]
            specific_args = {
                generic: specific
                for generic, specific in zip(generic_arg, get_args(orig_class))
                if generic in transformer_arg
            }

        signature = inspect.signature(self.transform)
        new_return_annotation = specific_args.get(
            signature.return_annotation, signature.return_annotation
        )
        parameters = list(signature.parameters.values())
        if parameters:
            parameter = parameters[0]
            parameter = parameter.replace(
                annotation=specific_args.get(parameter.annotation, parameter.annotation)
            )
            return signature.replace(
                return_annotation=new_return_annotation,
                parameters=[parameter],
            )

        return signature.replace(return_annotation=new_return_annotation)

    @property
    def output_type(self) -> Any:
        return self.signature().return_annotation

    @property
    def output_annotation(self) -> str:
        return self.output_type.__name__

    @property
    def input_type(self) -> Any:
        parameters = list(self.signature().parameters.items())
        if parameters:
            return parameters[0][1].annotation

    @property
    def input_annotation(self) -> str:
        return self.input_type.__name__

    def _add_net_node(self, net, custom_data={}):
        node_id = self.node_id
        props = {**self.graph_node_props, **custom_data, "label": self.label}
        if node_id not in net.nodes:
            net.add_node(node_id, **props)
        else:
            net.nodes[node_id].update(props)
        return node_id

    def _add_child_node(
        self,
        child: "BaseTransformer",
        child_net,
        parent_id: str,
        next_node: "BaseTransformer",
    ):
        child._dag(child_net, next_node, custom_data={"parent_id": parent_id})

    @property
    def node_id(self) -> str:
        return str(self.instance_id)

    @cached_property
    def visible_previous(self) -> PreviousTransformer:
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

    def _add_children_subgraph(self, net, next_node: "BaseTransformer"):
        next_node_id = next_node.node_id
        children_nets = [DiGraph() for _ in self.children]
        visible_previous = self.visible_previous

        for child, child_net in zip(self.children, children_nets):
            self._add_child_node(child, child_net, self.node_id, next_node)
            net.add_nodes_from(child_net.nodes(data=True))
            net.add_edges_from(child_net.edges(data=True))

            child_root_node = next(n for n, d in child_net.in_degree() if d == 0)
            child_final_node = next(n for n, d in child_net.out_degree() if d == 0)

            if self.invisible:
                if isinstance(visible_previous, tuple):
                    for prev in visible_previous:
                        net.add_edge(
                            prev.node_id, child_root_node, label=prev.output_annotation
                        )
                elif isinstance(visible_previous, BaseTransformer):
                    net.add_edge(
                        visible_previous.node_id,
                        child_root_node,
                        label=visible_previous.output_annotation,
                    )
            else:
                node_id = self._add_net_node(net)
                net.add_edge(node_id, child_root_node)

            if child_final_node != next_node_id:
                net.add_edge(
                    child_final_node, next_node_id, label=next_node.input_annotation
                )

    def _dag(
        self,
        net,
        next_node: "BaseTransformer" = None,
        custom_data={},
    ):
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

                    if not prev.invisible and not prev.children:
                        net.add_edge(
                            previous_node_id, next_node_id, label=prev.output_annotation
                        )

                    if previous_node_id not in net.nodes:
                        prev._dag(net, _next_node, custom_data)
            elif isinstance(previous, BaseTransformer):
                if self.invisible and next_node is not None:
                    next_node_id = next_node._add_net_node(net)
                    _next_node = next_node
                else:
                    next_node_id = self._add_net_node(net, custom_data)
                    _next_node = self

                previous_node_id = previous.node_id

                if not previous.children and (not previous.invisible or previous.previous is None):
                    previous_node_id = previous._add_net_node(net)
                    net.add_edge(
                        previous_node_id, next_node_id, label=previous.output_annotation
                    )

                if previous_node_id not in net.nodes:
                    previous._dag(net, _next_node, custom_data)
        else:
            self._add_net_node(net, custom_data)

        if self.children and next_node is not None:
            self._add_children_subgraph(net, next_node)

    def graph(self) -> DiGraph:
        net = DiGraph()
        net.graph["splines"] = "ortho"
        self._dag(net)
        return net

    def export(self, path: str, with_edge_labels: bool = True):
        net = self.graph()
        boxed_nodes = [
            node
            for node, data in net.nodes(data=True)
            if "parent_id" in data and "bounding_box" in data
        ]
        if not with_edge_labels:
            for u, v in net.edges:
                net.edges[u, v]["label"] = ""

        agraph = nx.nx_agraph.to_agraph(net)
        subgraphs = {}
        for node, data in boxed_nodes:
            subgraphs.setdefault(data["parent_id"], []).append(node)
        for parent_id, nodes in subgraphs.items():
            label = net.nodes[nodes[0]]["box_label"]
            agraph.add_subgraph(
                nodes, label=label, name=f"cluster_{parent_id}", style="dotted"
            )
        agraph.write(path)

    def __len__(self):
        return 1