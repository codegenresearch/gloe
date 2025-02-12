from asyncio import iscoroutinefunction
from inspect import Signature
from typing import TypeVar, Any, cast, Callable, Generic, Tuple, List, Union, overload
from types import GenericAlias, MethodType

from gloe.async_transformer import AsyncTransformer
from gloe.base_transformer import BaseTransformer
from gloe.transformers import Transformer
from gloe._utils import _match_types, _specify_types, awaitify
from gloe.exceptions import UnsupportedTransformerArgException

_In = TypeVar("_In")
_Out = TypeVar("_Out")
_NextOut = TypeVar("_NextOut")

def is_transformer(node) -> bool:
    if isinstance(node, (list, tuple)):
        return all(is_transformer(n) for n in node)
    return isinstance(node, Transformer)

def is_async_transformer(node) -> bool:
    return isinstance(node, AsyncTransformer)

def has_any_async_transformer(node: list) -> bool:
    return any(is_async_transformer(n) for n in node)

def _resolve_new_merge_transformers(new_transformer: BaseTransformer, transformer2: BaseTransformer) -> BaseTransformer:
    new_transformer.__class__.__name__ = transformer2.__class__.__name__
    new_transformer._label = transformer2.label
    new_transformer._children = transformer2.children
    new_transformer._invisible = transformer2.invisible
    new_transformer._graph_node_props = transformer2.graph_node_props
    new_transformer._set_previous(transformer2.previous)
    return new_transformer

def _resolve_serial_connection_signatures(transformer2: BaseTransformer, generic_vars: dict, signature2: Signature) -> Signature:
    first_param = next(iter(signature2.parameters.values()))
    new_parameter = first_param.replace(annotation=_specify_types(transformer2.input_type, generic_vars))
    return signature2.replace(parameters=[new_parameter], return_annotation=_specify_types(signature2.return_annotation, generic_vars))

def _nerge_serial(transformer1: BaseTransformer, transformer2: BaseTransformer) -> BaseTransformer:
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

    setattr(transformer1, "signature", MethodType(transformer1_signature, transformer1))

    class BaseNewTransformer:
        def signature(self) -> Signature:
            return _resolve_serial_connection_signatures(transformer2, generic_vars, signature2)

        def __len__(self):
            return len(transformer1) + len(transformer2)

    if is_transformer(transformer1) and is_transformer(transformer2):
        class NewTransformer(BaseNewTransformer, Transformer[_In, _NextOut]):
            def transform(self, data: _In) -> _NextOut:
                return transformer2(transformer1(data))
        return NewTransformer()

    elif is_async_transformer(transformer1) and is_transformer(transformer2):
        class NewTransformer(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                transformer1_out = await transformer1(data)
                return transformer2(transformer1_out)
        return NewTransformer()

    elif is_async_transformer(transformer1) and is_async_transformer(transformer2):
        class NewTransformer(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                transformer1_out = await transformer1(data)
                return await transformer2(transformer1_out)
        return NewTransformer()

    elif is_transformer(transformer1) and is_async_transformer(transformer2):
        class NewTransformer(AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                transformer1_out = transformer1(data)
                return await transformer2(transformer1_out)
        return NewTransformer()

    else:
        raise UnsupportedTransformerArgException(transformer2)

def _merge_diverging(incident_transformer: BaseTransformer, *receiving_transformers: BaseTransformer) -> BaseTransformer:
    if incident_transformer.previous is None:
        incident_transformer = incident_transformer.copy(regenerate_instance_id=True)
    receiving_transformers = tuple(t.copy(regenerate_instance_id=True) for t in receiving_transformers)

    for transformer in receiving_transformers:
        transformer._set_previous(incident_transformer)

    incident_signature = incident_transformer.signature()
    receiving_signatures = []

    for transformer in receiving_transformers:
        generic_vars = _match_types(transformer.input_type, incident_signature.return_annotation)
        receiving_signature = transformer.signature()
        new_return_annotation = _specify_types(receiving_signature.return_annotation, generic_vars)
        new_signature = receiving_signature.replace(return_annotation=new_return_annotation)
        receiving_signatures.append(new_signature)

        def _signature(_) -> Signature:
            return new_signature

        if transformer._previous == incident_transformer:
            setattr(transformer, "signature", MethodType(_signature, transformer))

    class BaseNewTransformer:
        def signature(self) -> Signature:
            return incident_signature.replace(return_annotation=GenericAlias(Tuple, tuple(r.return_annotation for r in receiving_signatures)))

        def __len__(self):
            return sum(len(t) for t in receiving_transformers) + len(incident_transformer)

    if all(is_transformer(t) for t in [incident_transformer, *receiving_transformers]):
        class NewTransformer(BaseNewTransformer, Transformer[_In, Tuple[Any, ...]]):
            def transform(self, data: _In) -> Tuple[Any, ...]:
                intermediate_result = incident_transformer(data)
                return tuple(transformer(intermediate_result) for transformer in receiving_transformers)
        return NewTransformer()

    else:
        class NewTransformer(BaseNewTransformer, AsyncTransformer[_In, Tuple[Any, ...]]):
            async def transform_async(self, data: _In) -> Tuple[Any, ...]:
                if iscoroutinefunction(incident_transformer.__call__):
                    intermediate_result = await incident_transformer(data)
                else:
                    intermediate_result = incident_transformer(data)
                results = []
                for transformer in receiving_transformers:
                    if iscoroutinefunction(transformer.__call__):
                        results.append(await transformer(intermediate_result))
                    else:
                        results.append(transformer(intermediate_result))
                return tuple(results)
        return NewTransformer()

@overload
def _compose_nodes(current: BaseTransformer, next_node: BaseTransformer) -> BaseTransformer: ...
@overload
def _compose_nodes(current: BaseTransformer, next_node: Tuple[BaseTransformer, ...]) -> BaseTransformer: ...
def _compose_nodes(current: BaseTransformer, next_node: Union[BaseTransformer, Tuple[BaseTransformer, ...]]) -> BaseTransformer:
    if isinstance(current, BaseTransformer):
        if isinstance(next_node, BaseTransformer):
            return _nerge_serial(current, next_node)
        elif isinstance(next_node, tuple) and all(isinstance(t, BaseTransformer) for t in next_node):
            return _merge_diverging(current, *next_node)
        else:
            raise UnsupportedTransformerArgException(next_node)
    else:
        raise UnsupportedTransformerArgException(current)