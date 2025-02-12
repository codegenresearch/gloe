import asyncio
from typing import TypeVar, Any, cast, Tuple

from gloe.async_transformer import AsyncTransformer
from gloe.base_transformer import BaseTransformer
from gloe.transformers import Transformer
from gloe._utils import _match_types, _specify_types
from gloe.exceptions import UnsupportedTransformerArgException

_In = TypeVar("_In")
_Out = TypeVar("_Out")
_NextOut = TypeVar("_NextOut")

def is_transformer(node):
    if isinstance(node, (list, tuple)):
        return all(is_transformer(n) for n in node)
    return isinstance(node, Transformer)

def is_async_transformer(node):
    return isinstance(node, AsyncTransformer)

def has_any_async_transformer(nodes):
    return any(is_async_transformer(n) for n in nodes)

def _resolve_new_merge_transformers(new_transformer, transformer2):
    new_transformer.__class__.__name__ = transformer2.__class__.__name__
    new_transformer._label = transformer2.label
    new_transformer._children = transformer2.children
    new_transformer._invisible = transformer2.invisible
    new_transformer._graph_node_props = transformer2.graph_node_props
    new_transformer._set_previous(transformer2.previous)
    return new_transformer

def _resolve_serial_connection_signatures(transformer2, generic_vars, signature2):
    first_param = list(signature2.parameters.values())[0]
    new_param = first_param.replace(
        annotation=_specify_types(transformer2.input_type, generic_vars)
    )
    new_signature = signature2.replace(
        parameters=[new_param],
        return_annotation=_specify_types(signature2.return_annotation, generic_vars),
    )
    return new_signature

def _merge_serial(transformer1, transformer2):
    if transformer1.previous is None:
        transformer1 = transformer1.copy(regenerate_instance_id=True)

    transformer2 = transformer2.copy(regenerate_instance_id=True)
    transformer2._set_previous(transformer1)

    sig1 = transformer1.signature()
    sig2 = transformer2.signature()

    input_vars = _match_types(transformer2.input_type, sig1.return_annotation)
    output_vars = _match_types(sig1.return_annotation, transformer2.input_type)
    generic_vars = {**input_vars, **output_vars}

    def new_sig(_) -> Signature:
        return sig1.replace(
            return_annotation=_specify_types(sig1.return_annotation, generic_vars)
        )

    transformer1.signature = new_sig.__get__(transformer1)

    class BaseNewTransformer:
        def signature(self) -> Signature:
            return _resolve_serial_connection_signatures(transformer2, generic_vars, sig2)

        def __len__(self):
            return len(transformer1) + len(transformer2)

    new_transformer = None
    if is_transformer(transformer1) and is_transformer(transformer2):
        class NewTransformer(BaseNewTransformer, Transformer[_In, _NextOut]):
            def transform(self, data: _In) -> _NextOut:
                return transformer2(transformer1(data))
        new_transformer = NewTransformer()

    elif is_async_transformer(transformer1) and is_transformer(transformer2):
        class NewTransformer(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                return transformer2(await transformer1(data))
        new_transformer = NewTransformer()

    elif is_async_transformer(transformer1) and is_async_transformer(transformer2):
        class NewTransformer(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                return await transformer2(await transformer1(data))
        new_transformer = NewTransformer()

    elif is_transformer(transformer1) and is_async_transformer(transformer2):
        class NewTransformer(AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, data: _In) -> _NextOut:
                return await transformer2(transformer1(data))
        new_transformer = NewTransformer()

    else:
        raise UnsupportedTransformerArgException(transformer2)

    return _resolve_new_merge_transformers(new_transformer, transformer2)

def _merge_diverging(incident_transformer, *receiving_transformers):
    if incident_transformer.previous is None:
        incident_transformer = incident_transformer.copy(regenerate_instance_id=True)

    receiving_transformers = tuple(
        t.copy(regenerate_instance_id=True) for t in receiving_transformers
    )

    for transformer in receiving_transformers:
        transformer._set_previous(incident_transformer)

    incident_sig = incident_transformer.signature()
    receiving_sigs = []

    for transformer in receiving_transformers:
        generic_vars = _match_types(transformer.input_type, incident_sig.return_annotation)
        sig = transformer.signature()
        new_sig = sig.replace(
            return_annotation=_specify_types(sig.return_annotation, generic_vars)
        )
        receiving_sigs.append(new_sig)

        def _new_sig(_) -> Signature:
            return new_sig

        if transformer._previous == incident_transformer:
            transformer.signature = _new_sig.__get__(transformer)

    class BaseNewTransformer:
        def signature(self) -> Signature:
            returns = [s.return_annotation for s in receiving_sigs]
            return incident_sig.replace(return_annotation=Tuple[tuple(returns)])

        def __len__(self):
            return len(incident_transformer) + sum(len(t) for t in receiving_transformers)

    new_transformer = None
    if all(isinstance(t, Transformer) for t in [incident_transformer] + list(receiving_transformers)):
        def split_result(data: _In) -> Tuple[Any, ...]:
            intermediate = incident_transformer(data)
            outputs = [t(intermediate) for t in receiving_transformers]
            return tuple(outputs)

        class NewTransformer(BaseNewTransformer, Transformer[_In, Tuple[Any, ...]]):
            def transform(self, data: _In) -> Tuple[Any, ...]:
                return split_result(data)

        new_transformer = NewTransformer()

    else:
        async def split_result_async(data: _In) -> Tuple[Any, ...]:
            intermediate = await incident_transformer(data) if asyncio.iscoroutinefunction(incident_transformer.__call__) else incident_transformer(data)
            outputs = [await t(intermediate) if asyncio.iscoroutinefunction(t.__call__) else t(intermediate) for t in receiving_transformers]
            return tuple(outputs)

        class NewTransformer(BaseNewTransformer, AsyncTransformer[_In, Tuple[Any, ...]]):
            async def transform_async(self, data: _In) -> Tuple[Any, ...]:
                return await split_result_async(data)

        new_transformer = NewTransformer()

    new_transformer._previous = cast(Transformer, receiving_transformers)
    new_transformer.__class__.__name__ = "Converge"
    new_transformer._label = ""
    new_transformer._graph_node_props = {
        "shape": "diamond",
        "width": 0.5,
        "height": 0.5,
    }

    return new_transformer

def _compose_nodes(current, next_node):
    if isinstance(current, BaseTransformer):
        if isinstance(next_node, BaseTransformer):
            return _merge_serial(current, next_node)
        elif isinstance(next_node, tuple):
            if all(isinstance(n, BaseTransformer) for n in next_node):
                return _merge_diverging(current, *next_node)
            else:
                unsupported = next((n for n in next_node if not isinstance(n, BaseTransformer)), None)
                raise UnsupportedTransformerArgException(unsupported)
        else:
            raise UnsupportedTransformerArgException(next_node)
    else:
        raise UnsupportedTransformerArgException(current)