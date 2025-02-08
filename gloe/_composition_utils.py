import asyncio\nimport types\nfrom inspect import Signature\nfrom types import GenericAlias\nfrom typing import TypeVar, Any, cast, overload, Callable, Awaitable, Tuple, List, Union\nfrom gloe.async_transformer import AsyncTransformer\nfrom gloe.base_transformer import BaseTransformer, PreviousTransformer, TransformerException\nfrom gloe.transformers import Transformer\nfrom gloe._utils import _match_types, _specify_types, awaitify\nfrom gloe.exceptions import UnsupportedTransformerArgException\n\n_In = TypeVar('_In')\n_Out = TypeVar('_Out')\n_NextOut = TypeVar('_NextOut')\n\ndef is_transformer(node: Union[List[Any], Tuple[Any, ...], Any]) -> bool:\n    if isinstance(node, (list, tuple)):\n        return all(is_transformer(n) for n in node)\n    return isinstance(node, Transformer)\n\ndef is_async_transformer(node: Any) -> bool:\n    return isinstance(node, AsyncTransformer)\n\ndef has_any_async_transformer(node: List[Any]) -> bool:\n    return any(is_async_transformer(n) for n in node)\n\ndef _resolve_new_merge_transformers(new_transformer: BaseTransformer, transformer2: BaseTransformer) -> BaseTransformer:\n    new_transformer.__class__.__name__ = transformer2.__class__.__name__\n    new_transformer._label = transformer2.label\n    new_transformer._children = transformer2.children\n    new_transformer._invisible = transformer2.invisible\n    new_transformer._graph_node_props = transformer2.graph_node_props\n    new_transformer._set_previous(transformer2.previous)\n    return new_transformer\n\ndef _resolve_serial_connection_signatures(transformer2: BaseTransformer, generic_vars: dict, signature2: Signature) -> Signature:\n    first_param = list(signature2.parameters.values())[0]\n    new_parameter = first_param.replace(annotation=_specify_types(transformer2.input_type, generic_vars))\n    new_signature = signature2.replace(parameters=[new_parameter], return_annotation=_specify_types(signature2.return_annotation, generic_vars))\n    return new_signature\n\ndef _merge_serial(transformer1: BaseTransformer, transformer2: BaseTransformer) -> BaseTransformer:\n    if transformer1.previous is None:\n        transformer1 = transformer1.copy(regenerate_instance_id=True)\n    transformer2 = transformer2.copy(regenerate_instance_id=True)\n    transformer2._set_previous(transformer1)\n    signature1, signature2 = transformer1.signature(), transformer2.signature()\n    input_generic_vars = _match_types(transformer2.input_type, signature1.return_annotation)\n    output_generic_vars = _match_types(signature1.return_annotation, transformer2.input_type)\n    generic_vars = {**input_generic_vars, **output_generic_vars}\n    def transformer1_signature(_) -> Signature:\n        return signature1.replace(return_annotation=_specify_types(signature1.return_annotation, generic_vars))\n    setattr(transformer1, 'signature', types.MethodType(transformer1_signature, transformer1))\n    class BaseNewTransformer:\n        def signature(self) -> Signature:\n            return _resolve_serial_connection_signatures(transformer2, generic_vars, signature2)\n        def __len__(self) -> int:\n            return len(transformer1) + len(transformer2)\n    new_transformer: BaseTransformer = None\n    if is_transformer(transformer1) and is_transformer(transformer2):\n        class NewTransformer1(BaseNewTransformer, Transformer[_In, _NextOut]):\n            def transform(self, data: _In) -> _NextOut:\n                return transformer2(transformer1(data))\n        new_transformer = NewTransformer1()\n    elif is_async_transformer(transformer1) and is_transformer(transformer2):\n        class NewTransformer2(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):\n            async def transform_async(self, data: _In) -> _NextOut:\n                return transformer2(await transformer1(data))\n        new_transformer = NewTransformer2()\n    elif is_async_transformer(transformer1) and is_async_transformer(transformer2):\n        class NewTransformer3(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):\n            async def transform_async(self, data: _In) -> _NextOut:\n                return await transformer2(await transformer1(data))\n        new_transformer = NewTransformer3()\n    elif is_transformer(transformer1) and is_async_transformer(transformer2):\n        class NewTransformer4(AsyncTransformer[_In, _NextOut]):\n            async def transform_async(self, data: _In) -> _NextOut:\n                return await transformer2(transformer1(data))\n        new_transformer = NewTransformer4()\n    else:\n        raise UnsupportedTransformerArgException(transformer2)\n    return _resolve_new_merge_transformers(new_transformer, transformer2)\n\ndef _merge_diverging(incident_transformer: BaseTransformer, *receiving_transformers: BaseTransformer) -> BaseTransformer:\n    if incident_transformer.previous is None:\n        incident_transformer = incident_transformer.copy(regenerate_instance_id=True)\n    receiving_transformers = tuple(receiving_transformer.copy(regenerate_instance_id=True) for receiving_transformer in receiving_transformers)\n    for receiving_transformer in receiving_transformers:\n        receiving_transformer._set_previous(incident_transformer)\n    incident_signature, receiving_signatures = incident_transformer.signature(), []\n    for receiving_transformer in receiving_transformers:\n        generic_vars = _match_types(receiving_transformer.input_type, incident_signature.return_annotation)\n        receiving_signature = receiving_transformer.signature()\n        new_return_annotation = _specify_types(receiving_signature.return_annotation, generic_vars)\n        new_signature = receiving_signature.replace(return_annotation=new_return_annotation)\n        receiving_signatures.append(new_signature)\n        def _signature(_) -> Signature:\n            return new_signature\n        if receiving_transformer._previous == incident_transformer:\n            setattr(receiving_transformer, 'signature', types.MethodType(_signature, receiving_transformer))\n    class BaseNewTransformer:\n        def signature(self) -> Signature:\n            receiving_signature_returns = [r.return_annotation for r in receiving_signatures]\n            return incident_signature.replace(return_annotation=GenericAlias(tuple, tuple(receiving_signature_returns)))\n        def __len__(self) -> int:\n            return sum(len(t) for t in receiving_transformers) + len(incident_transformer)\n    new_transformer: BaseTransformer = None\n    if is_transformer(incident_transformer) and all(is_transformer(rt) for rt in receiving_transformers):\n        def split_result(data: _In) -> Tuple[Any, ...]:\n            intermediate_result = incident_transformer(data)\n            return tuple(rt(intermediate_result) for rt in receiving_transformers)\n        class NewTransformer1(BaseNewTransformer, Transformer[_In, Tuple[Any, ...]]):\n            def transform(self, data: _In) -> Tuple[Any, ...]:\n                return split_result(data)\n        new_transformer = NewTransformer1()\n    else:\n        async def split_result_async(data: _In) -> Tuple[Any, ...]:\n            intermediate_result = await awaitify(incident_transformer)(data)\n            return tuple(await awaitify(rt)(intermediate_result) for rt in receiving_transformers)\n        class NewTransformer2(BaseNewTransformer, AsyncTransformer[_In, Tuple[Any, ...]]):\n            async def transform_async(self, data: _In) -> Tuple[Any, ...]:\n                return await split_result_async(data)\n        new_transformer = NewTransformer2()\n    new_transformer._previous = cast(PreviousTransformer, receiving_transformers)\n    new_transformer.__class__.__name__ = 'Converge'\n    new_transformer._label = ''\n    new_transformer._graph_node_props = {'shape': 'diamond', 'width': 0.5, 'height': 0.5}\n    return new_transformer\n\n@overload\ndef _compose_nodes(current: BaseTransformer, next_node: BaseTransformer) -> BaseTransformer:\n    pass\n\n@overload\ndef _compose_nodes(current: BaseTransformer, next_node: Tuple[BaseTransformer, ...]) -> BaseTransformer:\n    pass\n\ndef _compose_nodes(current: BaseTransformer, next_node: Union[BaseTransformer, Tuple[BaseTransformer, ...]]) -> BaseTransformer:\n    if isinstance(current, BaseTransformer):\n        if isinstance(next_node, BaseTransformer):\n            return _merge_serial(current, next_node)\n        elif isinstance(next_node, tuple):\n            if all(isinstance(nt, BaseTransformer) for nt in next_node):\n                return _merge_diverging(current, *next_node)\n            unsupported_elem = next((elem for elem in next_node if not isinstance(elem, BaseTransformer)), None)\n            raise UnsupportedTransformerArgException(unsupported_elem)\n        else:\n            raise UnsupportedTransformerArgException(next_node)\n    else:\n        raise UnsupportedTransformerArgException(current)\n