import copy\nimport types\nimport uuid\nimport inspect\nfrom functools import cached_property\nfrom inspect import Signature\nimport networkx as nx\nfrom networkx import DiGraph, Graph\nfrom typing import Any, Callable, Generic, TypeVar, Union, cast, Iterable, get_args, get_origin, TypeAlias, Type\nfrom uuid import UUID\nfrom itertools import groupby\nfrom gloe._utils import _format_return_annotation\n\n__all__ = ["BaseTransformer", "TransformerException", "PreviousTransformer"]\n\n_In = TypeVar("_In")\n_Out = TypeVar("_Out")\n_NextOut = TypeVar("_NextOut")\n_Self = TypeVar("_Self", bound="BaseTransformer")\n_Out2 = TypeVar("_Out2")\n_Out3 = TypeVar("_Out3")\n_Out4 = TypeVar("_Out4")\n_Out5 = TypeVar("_Out5")\n_Out6 = TypeVar("_Out6")\n_Out7 = TypeVar("_Out7")\n\nPreviousTransformer: TypeAlias = Union[None, _Self, tuple[_Self, ...]]\n\nclass TransformerException(Exception):\n    def __init__(self, internal_exception: Union["TransformerException", Exception], raiser_transformer: "BaseTransformer", message: str = None):\n        self._internal_exception = internal_exception\n        self.raiser_transformer = raiser_transformer\n        self._traceback = internal_exception.__traceback__\n        internal_exception.__cause__ = self\n        super().__init__(message)\n\n    @property\n    def internal_exception(self):\n        """Retrieve the internal exception with traceback."""\n        return self._internal_exception.with_traceback(self._traceback)\n\nclass BaseTransformer(Generic[_In, _Out, _Self]):\n    def __init__(self):\n        self._previous: PreviousTransformer = None\n        self._children: list[_Self] = []\n        self._invisible = False\n        self.id = uuid.uuid4()\n        self.instance_id = uuid.uuid4()\n        self._label = self.__class__.__name__\n        self._graph_node_props: dict[str, Any] = {"shape": "box"}\n        self.events = []\n\n    @property\n    def label(self) -> str:\n        """Label used in visualization."""\n        return self._label\n\n    @property\n    def graph_node_props(self) -> dict[str, Any]:\n        return self._graph_node_props\n\n    @property\n    def children(self) -> list[_Self]:\n        return self._children\n\n    @property\n    def previous(self) -> PreviousTransformer:\n        return self._previous\n\n    @property\n    def invisible(self) -> bool:\n        return self._invisible\n\n    def __hash__(self) -> int:\n        return hash(self.id)\n\n    def __eq__(self, other) -> bool:\n        if isinstance(other, BaseTransformer):\n            return self.id == other.id\n        return NotImplemented\n\n    def copy(self, transform: Callable[[_Self, _In], _Out] = None, regenerate_instance_id: bool = False) -> _Self:\n        copied = copy.copy(self)\n        if transform is not None:\n            setattr(copied, "transform", types.MethodType(transform, copied))\n        if regenerate_instance_id:\n            copied.instance_id = uuid.uuid4()\n        if self.previous is not None:\n            if isinstance(self.previous, tuple):\n                copied._previous = tuple(prev.copy() for prev in self.previous)\n            else:\n                copied._previous = self.previous.copy()\n        copied._children = [child.copy(regenerate_instance_id=True) for child in self.children]\n        return copied\n\n    @property\n    def graph_nodes(self) -> dict[UUID, _Self]:\n        nodes = {self.instance_id: self}\n        if self.previous is not None:\n            if isinstance(self.previous, tuple):\n                nodes.update({k: v for prev in self.previous for k, v in prev.graph_nodes.items()})\n            else:\n                nodes.update(self.previous.graph_nodes)\n        for child in self.children:\n            nodes.update(child.graph_nodes)\n        return nodes\n\n    def _set_previous(self, previous: PreviousTransformer):\n        if self.previous is None:\n            self._previous = previous\n        elif isinstance(self.previous, tuple):\n            for prev in self.previous:\n                prev._set_previous(previous)\n        else:\n            self.previous._set_previous(previous)\n\n    def signature(self) -> Signature:\n        return self._signature(type(self))\n\n    def _signature(self, klass: Type) -> Signature:\n        orig_bases = getattr(self, "__orig_bases__", [])\n        transformer_args = [get_args(base) for base in orig_bases if get_origin(base) == klass]\n        generic_args = [get_args(base) for base in orig_bases if get_origin(base) == Generic]\n        orig_class = getattr(self, "__orig_class__", None)\n        specific_args = {}\n        if len(transformer_args) == 1 and len(generic_args) == 1 and orig_class is not None:\n            generic_arg = generic_args[0]\n            transformer_arg = transformer_args[0]\n            specific_args = {generic: specific for generic, specific in zip(generic_arg, get_args(orig_class)) if generic in transformer_arg}\n        signature = inspect.signature(self.transform)\n        new_return_annotation = specific_args.get(signature.return_annotation, signature.return_annotation)\n        parameters = list(signature.parameters.values())\n        if parameters:\n            parameter = parameters[0]\n            parameter = parameter.replace(annotation=specific_args.get(parameter.annotation, parameter.annotation))\n            return signature.replace(return_annotation=new_return_annotation, parameters=[parameter])\n        return signature.replace(return_annotation=new_return_annotation)\n\n    @property\n    def output_type(self) -> Any:\n        return self.signature().return_annotation\n\n    @property\n    def output_annotation(self) -> str:\n        return _format_return_annotation(self.output_type, None, None)\n\n    @property\n    def input_type(self) -> Any:\n        parameters = list(self.signature().parameters.items())\n        return parameters[0][1].annotation if parameters else None\n\n    @property\n    def input_annotation(self) -> str:\n        return self.input_type.__name__ if self.input_type else ""\n\n    def _add_net_node(self, net: Graph, custom_data: dict[str, Any] = {}) -> str:\n        node_id = self.node_id\n        props = {**self.graph_node_props, **custom_data, "label": self.label}\n        if node_id not in net.nodes:\n            net.add_node(node_id, **props)\n        else:\n            nx.set_node_attributes(net, {node_id: props})\n        return node_id\n\n    def _add_child_node(self, child: _Self, child_net: DiGraph, parent_id: str, next_node: _Self):\n        child._dag(child_net, next_node, custom_data={"parent_id": parent_id})\n\n    @property\n    def node_id(self) -> str:\n        return str(self.instance_id)\n\n    @cached_property\n    def visible_previous(self) -> PreviousTransformer:\n        previous = self.previous\n        if isinstance(previous, BaseTransformer):\n            if previous.invisible:\n                if previous.previous is None:\n                    return previous\n                if isinstance(previous.previous, tuple):\n                    return previous.previous\n                return previous.visible_previous\n            return previous\n        return previous\n\n    def _add_children_subgraph(self, net: DiGraph, next_node: _Self):\n        next_node_id = next_node.node_id\n        children_nets = [DiGraph() for _ in self.children]\n        visible_previous = self.visible_previous\n        for child, child_net in zip(self.children, children_nets):\n            self._add_child_node(child, child_net, self.node_id, next_node)\n            net.add_nodes_from(child_net.nodes.data())\n            net.add_edges_from(child_net.edges.data())\n            child_root_node = next(n for n in child_net.nodes if child_net.in_degree(n) == 0)\n            child_final_node = next(n for n in child_net.nodes if child_net.out_degree(n) == 0)\n            if self.invisible:\n                if isinstance(visible_previous, tuple):\n                    for prev in visible_previous:\n                        net.add_edge(prev.node_id, child_root_node, label=prev.output_annotation)\n                elif isinstance(visible_previous, BaseTransformer):\n                    net.add_edge(visible_previous.node_id, child_root_node, label=visible_previous.output_annotation)\n            else:\n                node_id = self._add_net_node(net)\n                net.add_edge(node_id, child_root_node)\n            if child_final_node != next_node_id:\n                net.add_edge(child_final_node, next_node_id, label=next_node.input_annotation)\n\n    def _dag(self, net: DiGraph, next_node: Union[_Self, None] = None, custom_data: dict[str, Any] = {}):\n        in_nodes = [edge[1] for edge in net.in_edges()]\n        previous = self.previous\n        if previous is not None:\n            if isinstance(previous, tuple):\n                next_node_id = next_node._add_net_node(net) if self.invisible and next_node else self._add_net_node(net, custom_data)\n                _next_node = next_node if self.invisible and next_node else self\n                for prev in previous:\n                    previous_node_id = prev.node_id\n                    if not prev.invisible and not prev.children:\n                        net.add_edge(previous_node_id, next_node_id, label=prev.output_annotation)\n                    if previous_node_id not in in_nodes:\n                        prev._dag(net, _next_node, custom_data)\n            elif isinstance(previous, BaseTransformer):\n                next_node_id = next_node._add_net_node(net) if self.invisible and next_node else self._add_net_node(net, custom_data)\n                _next_node = next_node if self.invisible and next_node else self\n                previous_node_id = previous.node_id\n                if not previous.children and (not previous.invisible or not previous.previous):\n                    previous_node_id = previous._add_net_node(net)\n                    net.add_edge(previous_node_id, next_node_id, label=previous.output_annotation)\n                if previous_node_id not in in_nodes:\n                    previous._dag(net, _next_node, custom_data)\n        else:\n            self._add_net_node(net, custom_data)\n        if self.children and next_node:\n            self._add_children_subgraph(net, next_node)\n\n    def graph(self) -> DiGraph:\n        net = nx.DiGraph()\n        net.graph["splines"] = "ortho"\n        self._dag(net)\n        return net\n\n    def export(self, path: str, with_edge_labels: bool = True):\n        net = self.graph()\n        boxed_nodes = [node for node in net.nodes.data() if "parent_id" in node[1] and "bounding_box" in node[1]]\n        if not with_edge_labels:\n            for u, v in net.edges:\n                net.edges[u, v]["label"] = ""\n        agraph = nx.nx_agraph.to_agraph(net)\n        subgraphs = groupby(boxed_nodes, key=lambda x: x[1]["parent_id"])\n        for parent_id, nodes in subgraphs:\n            nodes = list(nodes)\n            node_ids = [node[0] for node in nodes]\n            if nodes:\n                label = nodes[0][1]["box_label"]\n                agraph.add_subgraph(node_ids, label=label, name=f"cluster_{parent_id}", style="dotted")\n        agraph.write(path)\n\n    def __len__(self) -> int:\n        return 1\n