import copy
import types
import uuid
import inspect
from abc import ABC, abstractmethod
from functools import cache
from inspect import Signature


from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    Union,
    Iterable,
    get_args,
    get_origin,
    Type,
    Optional,
    Awaitable,
)
from uuid import UUID
from itertools import groupby

from typing_extensions import Self, TypeAlias

from gloe._plotting_utils import PlottingSettings, NodeType, export_dot_props, GloeGraph
from gloe._typing_utils import _format_return_annotation

__all__ = ["BaseTransformer", "TransformerException", "PreviousTransformer"]

_NextOut = TypeVar("_NextOut")
_Self = TypeVar("_Self", bound="BaseTransformer")

_Out2 = TypeVar("_Out2")
_Out3 = TypeVar("_Out3")
_Out4 = TypeVar("_Out4")
_Out5 = TypeVar("_Out5")
_Out6 = TypeVar("_Out6")
_Out7 = TypeVar("_Out7")


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

TransformerChildren: TypeAlias = list["BaseTransformer"]


class TransformerException(Exception):
    def __init__(
        self,
        internal_exception: Union["TransformerException", Exception],
        raiser_transformer: "BaseTransformer",
        message: Union[str, None] = None,
    ):
        self._internal_exception = internal_exception
        self.raiser_transformer = raiser_transformer
        self._traceback = internal_exception.__traceback__
        internal_exception.__cause__ = self
        super().__init__(message)

    @property
    def internal_exception(self):
        return self._internal_exception.with_traceback(self._traceback)


_In = TypeVar("_In", contravariant=True)
_Out = TypeVar("_Out", covariant=True)


Flow = list["BaseTransformer"]


class BaseTransformer(Generic[_In, _Out], ABC):
    def __init__(self):
        self._children: TransformerChildren = []
        self.id = uuid.uuid4()
        self.instance_id = uuid.uuid4()
        self.is_atomic = False
        self._label = self.__class__.__name__
        self._already_copied = False
        self._plotting_settings: PlottingSettings = PlottingSettings(
            invisible=False,
            node_type=NodeType.Transformer,
        )
        self._flow: Flow = [self]

    @property
    def label(self) -> str:
        """
        Label used in visualization.

        When the transformer is created by the `@transformer` decorator, it is the
        name of the function.

        When creating a transformer by extending the `Transformer` class, it is the name
        of the class.
        """
        return self._label

    @property
    def children(self) -> TransformerChildren:
        """
        Used when a transformer encapsulates other transformer. The encapsulated
        transformers are called children transformers.
        """
        return self._children

    @property
    def plotting_settings(self) -> PlottingSettings:
        """
        Defines how the transformer will be plotted.
        """
        return self._plotting_settings

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, BaseTransformer):
            return self.id == other.id
        return NotImplemented

    def _copy(
        self: Self,
        transform: Optional[Callable[[Self, _In], _Out]] = None,
        regenerate_instance_id: bool = False,
        transform_method: str = "transform",
        force: bool = False,
    ) -> Self:

        copied: Self = copy.copy(self)
        copied._already_copied = True

        func_type = types.MethodType
        if transform is not None:
            setattr(copied, transform_method, func_type(transform, copied))

        old_instance_id = self.instance_id
        if regenerate_instance_id:
            copied.instance_id = uuid.uuid4()

        if self._already_copied and not force:
            copied._flow = [
                copied if child.instance_id == old_instance_id else child
                for child in self._flow
            ]
        else:
            copied._children = [
                child.copy(regenerate_instance_id=regenerate_instance_id)
                for child in self.children
            ]

            copied._flow = [
                (
                    copied
                    if child.instance_id == old_instance_id
                    else child.copy(regenerate_instance_id=regenerate_instance_id)
                )
                for child in self._flow
            ]
        return copied

    def copy(
        self: Self,
        transform: Optional[Callable[[Self, _In], _Out]] = None,
        regenerate_instance_id: bool = False,
        force: bool = False,
    ) -> Self:
        return self._copy(transform, regenerate_instance_id, "transform", force)

    @property
    def graph_nodes(self) -> dict[UUID, Optional["BaseTransformer"]]:
        graph = self.graph()

        nodes = {}
        for node_id, attrs in graph.nodes.items():
            nodes[node_id] = attrs.get("transformer", attrs.get("_label"))

        return nodes

    @abstractmethod
    def signature(self) -> Signature:
        """Transformer function-like signature"""

    @abstractmethod
    def __call__(self, data: _In) -> Union[_Out, Awaitable[_Out]]:
        """Transformer function-like signature"""

    def _signature(self, klass: Type, transform_method: str = "transform") -> Signature:
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

        signature = inspect.signature(getattr(self, transform_method))
        new_return_annotation = specific_args.get(
            signature.return_annotation, signature.return_annotation
        )
        parameters = list(signature.parameters.values())
        if len(parameters) > 0:
            parameter = parameters[0]
            parameter = parameter.replace(
                annotation=specific_args.get(parameter.annotation, parameter.annotation)
            )
            parameters = [parameter]

        return signature.replace(
            return_annotation=new_return_annotation,
            parameters=parameters,
        )

    @property
    def output_type(self) -> Any:
        signature = self.signature()
        return signature.return_annotation

    @property
    def output_annotation(self) -> str:
        output_type = self.output_type

        return_type = _format_return_annotation(output_type, None, None)
        return return_type

    @property
    def input_type(self) -> Any:
        parameters = list(self.signature().parameters.items())
        if len(parameters) > 0:
            parameter_type = parameters[0][1].annotation
            return parameter_type

    @property
    def input_annotation(self) -> str:
        return self.input_type.__name__

    def _add_net_node(self, net: GloeGraph, custom_data: dict[str, Any] = {}):
        node_id = self.node_id
        graph_node_props = export_dot_props(self.plotting_settings, self.instance_id)
        props = {
            **graph_node_props,
            **custom_data,
            "label": self.label,
            "transformer": self,
        }
        if node_id not in net.nodes:
            net.add_node(node_id, **props)
        else:
            net.nodes[node_id] = props
        return node_id

    @property
    def node_id(self) -> str:
        return str(self.instance_id)

    def _dag(
        self,
        net: GloeGraph,
        root_node: Union[str, "BaseTransformer", GloeGraph],
    ) -> Union[str, "BaseTransformer", GloeGraph]:
        prev_node = root_node
        for node in self._flow:
            # skip if the node is invisible
            if node.plotting_settings.invisible:
                continue

            # if the node is a gateway, we need to go deeper
            if node.plotting_settings.is_gateway:
                prev_node = node._dag(net, prev_node)
            elif node.plotting_settings.has_children and len(node.children) > 0:
                # if the node is not a gateway, but has children, we add its children
                # to a subgraph
                child_node = node.children[0]
                subgraph_name = f"cluster_{node.instance_id}"
                subgraph = child_node.graph(name=subgraph_name)
                net.add_subgraph(subgraph)

                begin_node = f"{subgraph_name}begin"
                end_node = f"{subgraph_name}end"

                if isinstance(prev_node, str):
                    net.add_edge(
                        prev_node,
                        begin_node,
                        label=node.input_annotation,
                        lhead=subgraph_name,
                    )
                else:
                    net.add_edge(
                        prev_node.node_id,
                        begin_node,
                        label=prev_node.output_annotation,
                        lhead=subgraph_name,
                    )
                prev_node = end_node
            else:  # otherwise, we add the node to the graph
                node_id = node._add_net_node(net)
                if isinstance(prev_node, str):
                    net.add_edge(prev_node, node_id, label=node.input_annotation)
                else:
                    net.add_edge(
                        prev_node.node_id,
                        node_id,
                        label=prev_node.output_annotation,
                    )
                prev_node = node
        return prev_node

    @cache
    def graph(self, name: str = "") -> GloeGraph:
        net = GloeGraph(name=name)
        net.attrs["splines"] = "ortho"
        net.add_node(f"{name}begin", label="", _label="begin", shape="circle")

        last_node = self._dag(net, f"{name}begin")

        net.add_node(f"{name}end", label="", _label="end", shape="doublecircle")

        if isinstance(last_node, str):
            net.add_edge(last_node, f"{name}end")
        elif isinstance(last_node, BaseTransformer):
            net.add_edge(
                last_node.node_id,
                f"{name}end",
                label=last_node.output_annotation,
            )
        return net

    def export(self, path: str, with_edge_labels: bool = True):  # pragma: no cover
        """Export Transformer object in dot format"""

        self.graph().to_agraph().write(path)

    def __len__(self):
        return 1
