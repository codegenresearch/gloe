import asyncio
from inspect import Signature
from typing import TypeVar, Any, cast, Tuple

from gloe.async_transformer import AsyncTransformer
from gloe.base_transformer import BaseTransformer
from gloe.transformers import Transformer
from gloe._utils import _match_types, _specify_types
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

def has_any_async_transformer(nodes: list) -> bool:
    return any(is_async_transformer(node) for node in nodes)

def _resolve_new_merged_transformers(
    new_transformer: BaseTransformer, transformer: BaseTransformer
) -> BaseTransformer:
    new_transformer.__class__.__name__ = transformer.__class__.__name__
    new_transformer._label = transformer.label
    new_transformer._children = transformer.children
    new_transformer._invisible = transformer.invisible
    new_transformer._graph_node_props = transformer.graph_node_props
    new_transformer._set_previous(transformer.previous)
    return new_transformer

def _resolve_serial_connection_signatures(
    transformer: BaseTransformer, generic_vars: dict, signature: Signature
) -> Signature:
    first_param = list(signature.parameters.values())[0]
    new_parameter = first_param.replace(
        annotation=_specify_types(transformer.input_type, generic_vars)
    )
    new_signature = signature.replace(
        parameters=[new_parameter],
        return_annotation=_specify_types(signature.return_annotation, generic_vars),
    )
    return new_signature

def _merge_serial(
    transformer1: BaseTransformer, transformer2: BaseTransformer
) -> BaseTransformer:
    if transformer1.previous is None:
        transformer1 = transformer1.copy(regenerate_instance_id=True)

    transformer2 = transformer2.copy(regenerate_instance_id=True)
    transformer2._set_previous(transformer1)

    signature1: Signature = transformer1.signature()
    signature2: Signature = transformer2.signature()

    input_generic_vars = _match_types(
        transformer2.input_type, signature1.return_annotation
    )
    output_generic_vars = _match_types(
        signature1.return_annotation, transformer2.input_type
    )
    generic_vars = {**input_generic_vars, **output_generic_vars}

    def transformer1_signature(_) -> Signature:
        return signature1.replace(
            return_annotation=_specify_types(signature1.return_annotation, generic_vars)
        )

    setattr(
        transformer1,
        "signature",
        types.MethodType(transformer1_signature, transformer1),
    )

    class BaseNewTransformer:
        def signature(self) -> Signature:
            return _resolve_serial_connection_signatures(
                transformer2, generic_vars, signature2
            )

        def __len__(self) -> int:
            return len(transformer1) + len(transformer2)

    new_transformer: BaseTransformer | None = None
    if is_transformer(transformer1) and is_transformer(transformer2):

        class NewTransformer1(BaseNewTransformer, Transformer[_In, _NextOut]):
            def transform(self, input_data: _In) -> _NextOut:
                return transformer2(transformer1(input_data))

        new_transformer = NewTransformer1()

    elif is_async_transformer(transformer1) and is_transformer(transformer2):

        class NewTransformer2(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, input_data: _In) -> _NextOut:
                intermediate_result = await transformer1(input_data)
                return transformer2(intermediate_result)

        new_transformer = NewTransformer2()

    elif is_async_transformer(transformer1) and is_async_transformer(transformer2):

        class NewTransformer3(BaseNewTransformer, AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, input_data: _In) -> _NextOut:
                intermediate_result = await transformer1(input_data)
                return await transformer2(intermediate_result)

        new_transformer = NewTransformer3()

    elif is_transformer(transformer1) and is_async_transformer(transformer2):

        class NewTransformer4(AsyncTransformer[_In, _NextOut]):
            async def transform_async(self, input_data: _In) -> _NextOut:
                intermediate_result = transformer1(input_data)
                return await transformer2(intermediate_result)

        new_transformer = NewTransformer4()

    else:
        raise UnsupportedTransformerArgException(transformer2)

    return _resolve_new_merged_transformers(new_transformer, transformer2)

def _merge_diverging(
    incident_transformer: BaseTransformer, *receiving_transformers: BaseTransformer
) -> BaseTransformer:
    if incident_transformer.previous is None:
        incident_transformer = incident_transformer.copy(regenerate_instance_id=True)

    receiving_transformers = tuple(
        transformer.copy(regenerate_instance_id=True) for transformer in receiving_transformers
    )

    for transformer in receiving_transformers:
        transformer._set_previous(incident_transformer)

    incident_signature: Signature = incident_transformer.signature()
    receiving_signatures: list[Signature] = []

    for transformer in receiving_transformers:
        generic_vars = _match_types(
            transformer.input_type, incident_signature.return_annotation
        )

        signature = transformer.signature()
        return_annotation = signature.return_annotation

        new_return_annotation = _specify_types(return_annotation, generic_vars)

        new_signature = signature.replace(
            return_annotation=new_return_annotation
        )
        receiving_signatures.append(new_signature)

        def _signature(_) -> Signature:
            return new_signature

        if transformer._previous == incident_transformer:
            setattr(
                transformer,
                "signature",
                types.MethodType(_signature, transformer),
            )

    class BaseNewTransformer:
        def signature(self) -> Signature:
            return_annotations = [
                signature.return_annotation for signature in receiving_signatures
            ]
            new_signature = incident_signature.replace(
                return_annotation=Tuple[tuple(return_annotations)]
            )
            return new_signature

        def __len__(self) -> int:
            return sum(len(transformer) for transformer in receiving_transformers) + len(incident_transformer)

    new_transformer = None
    if is_transformer(incident_transformer) and all(is_transformer(transformer) for transformer in receiving_transformers):

        def split_result(input_data: _In) -> Tuple[Any, ...]:
            intermediate_result = incident_transformer(input_data)

            outputs = []
            for transformer in receiving_transformers:
                output = transformer(intermediate_result)
                outputs.append(output)

            return tuple(outputs)

        class NewTransformer1(BaseNewTransformer, Transformer[_In, Tuple[Any, ...]]):
            def transform(self, input_data: _In) -> Tuple[Any, ...]:
                return split_result(input_data)

        new_transformer = NewTransformer1()

    else:

        async def split_result_async(input_data: _In) -> Tuple[Any, ...]:
            if asyncio.iscoroutinefunction(incident_transformer.__call__):
                intermediate_result = await incident_transformer(input_data)
            else:
                intermediate_result = incident_transformer(input_data)

            outputs = []
            for transformer in receiving_transformers:
                if asyncio.iscoroutinefunction(transformer.__call__):
                    output = await transformer(intermediate_result)
                else:
                    output = transformer(intermediate_result)
                outputs.append(output)

            return tuple(outputs)

        class NewTransformer2(BaseNewTransformer, AsyncTransformer[_In, Tuple[Any, ...]]):
            async def transform_async(self, input_data: _In) -> Tuple[Any, ...]:
                return await split_result_async(input_data)

        new_transformer = NewTransformer2()

    new_transformer._previous = cast(Transformer, receiving_transformers)
    new_transformer.__class__.__name__ = "Converge"
    new_transformer._label = ""
    new_transformer._graph_node_props = {
        "shape": "diamond",
        "width": 0.5,
        "height": 0.5,
    }

    return new_transformer

def _compose_nodes(
    current_transformer: BaseTransformer,
    next_node: Tuple[BaseTransformer, ...] | BaseTransformer,
) -> BaseTransformer:
    if isinstance(current_transformer, BaseTransformer):
        if isinstance(next_node, BaseTransformer):
            return _merge_serial(current_transformer, next_node)
        elif isinstance(next_node, tuple):
            if all(isinstance(transformer, BaseTransformer) for transformer in next_node):
                return _merge_diverging(current_transformer, *next_node)

            unsupported_elem = [
                elem for elem in next_node if not isinstance(elem, BaseTransformer)
            ]
            raise UnsupportedTransformerArgException(unsupported_elem[0])
        else:
            raise UnsupportedTransformerArgException(next_node)
    else:
        raise UnsupportedTransformerArgException(current_transformer)