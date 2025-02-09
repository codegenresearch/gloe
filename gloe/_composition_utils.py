import asyncio\nfrom inspect import Signature\nfrom types import GenericAlias\nfrom typing import TypeVar, Any, cast, Callable, Awaitable, Tuple, Union, List\nfrom gloe.async_transformer import AsyncTransformer\nfrom gloe.base_transformer import BaseTransformer, PreviousTransformer\nfrom gloe.transformers import Transformer\nfrom gloe._utils import _match_types, _specify_types, awaitify\nfrom gloe.exceptions import UnsupportedTransformerArgException\n\n_In = TypeVar("_In")\n_Out = TypeVar("_Out")\n_NextOut = TypeVar("_NextOut")\n\ndef is_transformer(node: Union[BaseTransformer, List[BaseTransformer], Tuple[BaseTransformer, ...]]) -> bool:\n    if isinstance(node, (list, tuple)):\n        return all(is_transformer(n) for n in node)\n    return isinstance(node, Transformer)\n\ndef is_async_transformer(node: BaseTransformer) -> bool:\n    return isinstance(node, AsyncTransformer)\n\ndef has_any_async_transformer(nodes: List[BaseTransformer]) -> bool:\n    return any(is_async_transformer(node) for node in nodes)\n\ndef _resolve_new_merged_transformers(\n    new_transformer: BaseTransformer,\n    transformer: BaseTransformer,\n) -> BaseTransformer:\n    new_transformer.__class__.__name__ = transformer.__class__.__name__\n    new_transformer._label = transformer.label\n    new_transformer._children = transformer.children\n    new_transformer._invisible = transformer.invisible\n    new_transformer._graph_node_props = transformer.graph_node_props\n    new_transformer._set_previous(transformer.previous)\n    return new_transformer\n\ndef _resolve_serial_connection_signatures(\n    transformer: BaseTransformer,\n    generic_vars: dict,\n    signature: Signature,\n) -> Signature:\n    first_param = list(signature.parameters.values())[0]\n    new_parameter = first_param.replace(\n        annotation=_specify_types(transformer.input_type, generic_vars)\n    )\n    new_signature = signature.replace(\n        parameters=[new_parameter],\n        return_annotation=_specify_types(signature.return_annotation, generic_vars),\n    )\n    return new_signature\n\ndef _merge_serial(\n    transformer1: BaseTransformer,\n    transformer2: BaseTransformer,\n) -> BaseTransformer:\n    if transformer1.previous is None:\n        transformer1 = transformer1.copy(regenerate_instance_id=True)\n    transformer2 = transformer2.copy(regenerate_instance_id=True)\n    transformer2._set_previous(transformer1)\n    signature1: Signature = transformer1.signature()\n    signature2: Signature = transformer2.signature()\n    input_generic_vars = _match_types(transformer2.input_type, signature1.return_annotation)\n    output_generic_vars = _match_types(signature1.return_annotation, transformer2.input_type)\n    generic_vars = {**input_generic_vars, **output_generic_vars}\n    def transformer1_signature(_) -> Signature:\n        return signature1.replace(\n            return_annotation=_specify_types(signature1.return_annotation, generic_vars),\n        )\n    setattr(transformer1, "signature", types.MethodType(transformer1_signature, transformer1))\n    class BaseNewTransformer:\n        def signature(self) -> Signature:\n            return _resolve_serial_connection_signatures(transformer2, generic_vars, signature2)\n        def __len__(self) -> int:\n            return len(transformer1) + len(transformer2)\n    new_transformer: BaseTransformer = None\n    if is_transformer(transformer1) and is_transformer(transformer2):\n        class NewTransformer1(BaseNewTransformer, Transformer[_In, _NextOut]):\n            def transform(self, input_data: _In) -> _NextOut:\n                transformed = transformer2(transformer1(input_data))\n                return transformed\n        new_transformer = NewTransformer1()\n    elif is_async_transformer(transformer1) and is_transformer(transformer2):\n        class NewTransformer2(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):\n            async def transform_async(self, input_data: _In) -> _NextOut:\n                transformer1_out = await transformer1(input_data)\n                transformed = transformer2(transformer1_out)\n                return transformed\n        new_transformer = NewTransformer2()\n    elif is_async_transformer(transformer1) and is_async_transformer(transformer2):\n        class NewTransformer3(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):\n            async def transform_async(self, input_data: _In) -> _NextOut:\n                transformer1_out = await transformer1(input_data)\n                transformed = await transformer2(transformer1_out)\n                return transformed\n        new_transformer = NewTransformer3()\n    elif is_transformer(transformer1) and is_async_transformer(transformer2):\n        class NewTransformer4(AsyncTransformer[_In, _NextOut]):\n            async def transform_async(self, input_data: _In) -> _NextOut:\n                transformer1_out = transformer1(input_data)\n                transformed = await transformer2(transformer1_out)\n                return transformed\n        new_transformer = NewTransformer4()\n    else:\n        raise UnsupportedTransformerArgException(transformer2)\n    return _resolve_new_merged_transformers(new_transformer, transformer2)\n\ndef _merge_diverging(\n    incident_transformer: BaseTransformer,\n    *receiving_transformers: BaseTransformer,\n) -> BaseTransformer:\n    if incident_transformer.previous is None:\n        incident_transformer = incident_transformer.copy(regenerate_instance_id=True)\n    receiving_transformers = tuple(\n        receiving_transformer.copy(regenerate_instance_id=True)\n        for receiving_transformer in receiving_transformers\n    )\n    for receiving_transformer in receiving_transformers:\n        receiving_transformer._set_previous(incident_transformer)\n    incident_signature: Signature = incident_transformer.signature()\n    receiving_signatures: List[Signature] = []\n    for receiving_transformer in receiving_transformers:\n        generic_vars = _match_types(receiving_transformer.input_type, incident_signature.return_annotation)\n        receiving_signature = receiving_transformer.signature()\n        return_annotation = receiving_signature.return_annotation\n        new_return_annotation = _specify_types(return_annotation, generic_vars)\n        new_signature = receiving_signature.replace(return_annotation=new_return_annotation)\n        receiving_signatures.append(new_signature)\n        def _signature(_) -> Signature:\n            return new_signature\n        if receiving_transformer._previous == incident_transformer:\n            setattr(receiving_transformer, "signature", types.MethodType(_signature, receiving_transformer))\n    class BaseNewTransformer:\n        def signature(self) -> Signature:\n            receiving_signature_returns = [r.return_annotation for r in receiving_signatures]\n            new_signature = incident_signature.replace(\n                return_annotation=GenericAlias(tuple, tuple(receiving_signature_returns)),\n            )\n            return new_signature\n        def __len__(self) -> int:\n            lengths = [len(t) for t in receiving_transformers]\n            return sum(lengths) + len(incident_transformer)\n    new_transformer: BaseTransformer = None\n    if is_transformer(incident_transformer) and all(is_transformer(rt) for rt in receiving_transformers):\n        def split_result(input_data: _In) -> Tuple[Any, ...]:\n            intermediate_result = incident_transformer(input_data)\n            outputs = [rt(intermediate_result) for rt in receiving_transformers]\n            return tuple(outputs)\n        class NewTransformer1(BaseNewTransformer, Transformer[_In, Tuple[Any, ...]]):\n            def transform(self, input_data: _In) -> Tuple[Any, ...]:\n                return split_result(input_data)\n        new_transformer = NewTransformer1()\n    else:\n        async def split_result_async(input_data: _In) -> Tuple[Any, ...]:\n            if asyncio.iscoroutinefunction(incident_transformer.__call__):\n                intermediate_result = await incident_transformer(input_data)\n            else:\n                intermediate_result = incident_transformer(input_data)\n            outputs = []\n            for rt in receiving_transformers:\n                if asyncio.iscoroutinefunction(rt.__call__):\n                    output = await rt(intermediate_result)\n                else:\n                    output = rt(intermediate_result)\n                outputs.append(output)\n            return tuple(outputs)\n        class NewTransformer2(BaseNewTransformer, AsyncTransformer[_In, Tuple[Any, ...]]):\n            async def transform_async(self, input_data: _In) -> Tuple[Any, ...]:\n                return await split_result_async(input_data)\n        new_transformer = NewTransformer2()\n    new_transformer._previous = cast(PreviousTransformer, receiving_transformers)\n    new_transformer.__class__.__name__ = "Converge"\n    new_transformer._label = ""\n    new_transformer._graph_node_props = {\