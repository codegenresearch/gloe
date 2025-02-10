import asyncio
import types
from inspect import Signature, get_origin
from typing import TypeVar, Any, cast, Tuple, GenericAlias

from gloe.async_transformer import AsyncTransformer
from gloe.base_transformer import BaseTransformer
from gloe.transformers import Transformer
from gloe._utils import _specify_types
from gloe.exceptions import UnsupportedTransformerArgException

_In = TypeVar("_In")
_Out = TypeVar("_Out")
_NextOut = TypeVar("_NextOut")

def is_transformer(node) -> bool:
    return isinstance(node, Transformer) or (isinstance(node, (list, tuple)) and all(is_transformer(n) for n in node))

def is_async_transformer(node) -> bool:
    return isinstance(node, AsyncTransformer)

def has_any_async_transformer(node: list) -> bool:
    return any(is_async_transformer(n) for n in node)

def _match_types(generic: Any, specific: Any) -> dict:
    generic_origin = get_origin(generic) or generic
    specific_origin = get_origin(specific) or specific

    if not isinstance(generic_origin, type) or not isinstance(specific_origin, type):
        return {}

    if issubclass(specific_origin, generic_origin):
        if isinstance(generic, GenericAlias) and isinstance(specific, GenericAlias):
            return {generic.__args__[i]: specific.__args__[i] for i in range(len(generic.__args__))}
        elif isinstance(generic, GenericAlias):
            return {generic.__args__[0]: specific}
        elif isinstance(specific, GenericAlias):
            return {generic: specific.__args__[0]}
        else:
            return {generic: specific}
    return {}

def _resolve_new_merge_transformers(new_transformer: BaseTransformer, transformer2: BaseTransformer) -> BaseTransformer:
    new_transformer.__class__.__name__ = transformer2.__class__.__name__
    new_transformer._label = transformer2.label
    new_transformer._children = transformer2.children
    new_transformer._invisible = transformer2.invisible
    new_transformer._graph_node_props = transformer2.graph_node_props
    new_transformer._set_previous(transformer2.previous)
    return new_transformer

def _resolve_serial_connection_signatures(transformer2: BaseTransformer, generic_vars: dict, signature2: Signature) -> Signature:
    first_param = list(signature2.parameters.values())[0]
    new_parameter = first_param.replace(annotation=_specify_types(transformer2.input_type, generic_vars))
    return signature2.replace(parameters=[new_parameter], return_annotation=_specify_types(signature2.return_annotation, generic_vars))

def _merge_serial(transformer1: BaseTransformer, transformer2: BaseTransformer) -> BaseTransformer:
    if transformer1.previous is None:
        transformer1 = transformer1.copy(regenerate_instance_id=True)
    transformer2 = transformer2.copy(regenerate_instance_id=True)
    transformer2._set_previous(transformer1)
    signature1, signature2 = transformer1.signature(), transformer2.signature()
    input_generic_vars = _match_types(transformer2.input_type, signature1.return_annotation)
    output_generic_vars = _match_types(signature1.return_annotation, transformer2.input_type)
    generic_vars = {**input_generic_vars, **output_generic_vars}

    def transformer1_signature(_) -> Signature:
        return signature1.replace(return_annotation=_specify_types(signature1.return_annotation, generic_vars))

    setattr(transformer1, "signature", types.MethodType(transformer1_signature, transformer1))

    class BaseNewTransformer:
        def signature(self) -> Signature:
            return _resolve_serial_connection_signatures(transformer2, generic_vars, signature2)

        def __len__(self):
            return len(transformer1) + len(transformer2)

    new_transformer: BaseTransformer | None = None
    if is_transformer(transformer1) and is_transformer(transformer2):
        class NewTransformer1(BaseNewTransformer, Transformer[_In, _NextOut]):
            def transform(self, data: _In) -> _NextOut:
                return transformer2.__call__(transformer1.__call__(data))
        new_transformer = NewTransformer1()
    elif is_async_transformer(transformer1) and is_transformer(transformer2):
        class NewTransformer2(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                return transformer2.__call__(await transformer1.__call__(data))
        new_transformer = NewTransformer2()
    elif is_async_transformer(transformer1) and is_async_transformer(transformer2):
        class NewTransformer3(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                return await transformer2.__call__(await transformer1.__call__(data))
        new_transformer = NewTransformer3()
    elif is_transformer(transformer1) and is_async_transformer(transformer2):
        class NewTransformer4(AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                return await transformer2.__call__(transformer1.__call__(data))
        new_transformer = NewTransformer4()
    else:
        raise UnsupportedTransformerArgException(transformer2)

    return _resolve_new_merge_transformers(new_transformer, transformer2)

def _merge_diverging(incident_transformer: BaseTransformer, *receiving_transformers: BaseTransformer) -> BaseTransformer:
    if incident_transformer.previous is None:
        incident_transformer = incident_transformer.copy(regenerate_instance_id=True)
    receiving_transformers = tuple(t.copy(regenerate_instance_id=True) for t in receiving_transformers)
    for t in receiving_transformers:
        t._set_previous(incident_transformer)
    incident_signature = incident_transformer.signature()
    receiving_signatures = []

    for t in receiving_transformers:
        generic_vars = _match_types(t.input_type, incident_signature.return_annotation)
        receiving_signature = t.signature()
        new_return_annotation = _specify_types(receiving_signature.return_annotation, generic_vars)
        new_signature = receiving_signature.replace(return_annotation=new_return_annotation)
        receiving_signatures.append(new_signature)

        def _signature(_) -> Signature:
            return new_signature

        if t._previous == incident_transformer:
            setattr(t, "signature", types.MethodType(_signature, t))

    class BaseNewTransformer:
        def signature(self) -> Signature:
            return incident_signature.replace(return_annotation=GenericAlias(Tuple, tuple(r.return_annotation for r in receiving_signatures)))

        def __len__(self):
            return sum(len(t) for t in receiving_transformers) + len(incident_transformer)

    new_transformer: BaseTransformer | None = None
    if is_transformer(incident_transformer) and all(is_transformer(t) for t in receiving_transformers):
        def split_result(data: _In) -> Tuple[Any, ...]:
            intermediate_result = incident_transformer.__call__(data)
            return tuple(t.__call__(intermediate_result) for t in receiving_transformers)

        class NewTransformer1(BaseNewTransformer, Transformer[_In, Tuple[Any, ...]]):
            def transform(self, data: _In) -> Tuple[Any, ...]:
                return split_result(data)
        new_transformer = NewTransformer1()
    else:
        async def split_result_async(data: _In) -> Tuple[Any, ...]:
            intermediate_result = await incident_transformer.__call__(data) if asyncio.iscoroutinefunction(incident_transformer.__call__) else incident_transformer.__call__(data)
            results = []
            for t in receiving_transformers:
                if asyncio.iscoroutinefunction(t.__call__):
                    results.append(await t.__call__(intermediate_result))
                else:
                    results.append(t.__call__(intermediate_result))
            return tuple(results)

        class NewTransformer2(BaseNewTransformer, AsyncTransformer[_In, Tuple[Any, ...]]):
            async def transform_async(self, data: _In) -> Tuple[Any, ...]:
                return await split_result_async(data)
        new_transformer = NewTransformer2()

    new_transformer._previous = cast(Transformer, receiving_transformers)
    new_transformer.__class__.__name__ = "Converge"
    new_transformer._label = ""
    new_transformer._graph_node_props = {"shape": "diamond", "width": 0.5, "height": 0.5}
    return new_transformer

def _compose_nodes(current: BaseTransformer, next_node: Tuple[BaseTransformer, ...] | BaseTransformer) -> BaseTransformer:
    if isinstance(current, BaseTransformer):
        if isinstance(next_node, BaseTransformer):
            return _merge_serial(current, next_node)
        elif isinstance(next_node, tuple) and all(isinstance(t, BaseTransformer) for t in next_node):
            return _merge_diverging(current, *next_node)
        else:
            raise UnsupportedTransformerArgException(next_node)
    else:
        raise UnsupportedTransformerArgException(current)