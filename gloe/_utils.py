from functools import wraps
from types import GenericAlias
from typing import (
    TypeVar,
    get_origin,
    TypeAlias,
    TypedDict,
    Generic,
    Union,
    _GenericAlias,
    ParamSpec,
    Callable,
    Awaitable,
)  # type: ignore


def _format_tuple(
    tuple_annotation: tuple, generic_input_param, input_annotation
) -> str:
    formatted: list[str] = []
    for annotation in tuple_annotation:
        formatted.append(
            _format_return_annotation(annotation, generic_input_param, input_annotation)
        )
    return f"({', '.join(formatted)})"


def _format_union(
    tuple_annotation: tuple, generic_input_param, input_annotation
) -> str:
    formatted: list[str] = []
    for annotation in tuple_annotation:
        formatted.append(
            _format_return_annotation(annotation, generic_input_param, input_annotation)
        )
    return f"({' | '.join(formatted)})"


def _format_generic_alias(
    return_annotation: GenericAlias, generic_input_param, input_annotation
) -> str:
    alias_name = return_annotation.__name__
    formatted: list[str] = []
    for annotation in return_annotation.__args__:
        formatted.append(
            _format_return_annotation(annotation, generic_input_param, input_annotation)
        )
    return f"{alias_name}[{', '.join(formatted)}]"


def _format_return_annotation(
    return_annotation, generic_input_param, input_annotation
) -> str:
    """
    Formats the return annotation based on the generic input parameter and the input annotation.
    
    Args:
        return_annotation: The annotation to format.
        generic_input_param: The generic input parameter.
        input_annotation: The input annotation.
    
    Returns:
        A formatted string representation of the return annotation.
    """
    if isinstance(return_annotation, str):
        return return_annotation
    if isinstance(return_annotation, tuple):
        return _format_tuple(return_annotation, generic_input_param, input_annotation)
    if return_annotation.__name__ in {"tuple", "Tuple"}:
        return _format_tuple(
            return_annotation.__args__, generic_input_param, input_annotation
        )
    if return_annotation.__name__ in {"Union"}:
        return _format_union(
            return_annotation.__args__, generic_input_param, input_annotation
        )
    if isinstance(return_annotation, (GenericAlias, _GenericAlias)):
        return _format_generic_alias(
            return_annotation, generic_input_param, input_annotation
        )

    if return_annotation == generic_input_param:
        return input_annotation.__name__

    return return_annotation.__name__


def _match_types(generic, specific, ignore_mismatches=True):
    """
    Matches generic types with specific types.
    
    Args:
        generic: The generic type.
        specific: The specific type.
        ignore_mismatches: Whether to ignore mismatches.
    
    Returns:
        A dictionary of matched types.
    
    Raises:
        Exception: If types do not match and ignore_mismatches is False.
    """
    if isinstance(generic, TypeVar):
        return {generic: specific}

    specific_origin = get_origin(specific)
    generic_origin = get_origin(generic)

    if specific_origin is None and generic_origin is None:
        return {}

    if (specific_origin is None or generic_origin is None) or not issubclass(
        specific_origin, generic_origin
    ):
        if ignore_mismatches:
            return {}
        raise Exception(f"Type {generic} does not match with {specific}")

    generic_args = getattr(generic, "__args__", None)
    specific_args = getattr(specific, "__args__", None)

    if specific_args is None and specific_args is None:
        return {}

    if generic_args is None:
        if ignore_mismatches:
            return {}
        raise Exception(f"Type {generic} in generic has no arguments")

    if specific_args is None:
        if ignore_mismatches:
            return {}
        raise Exception(f"Type {specific} in specific has no arguments")

    if len(generic_args) != len(specific_args):
        if ignore_mismatches:
            return {}
        raise Exception(
            f"Number of arguments of type {generic} is different in specific type"
        )

    matches = {}
    for generic_arg, specific_arg in zip(generic_args, specific_args):
        matched_types = _match_types(generic_arg, specific_arg)
        matches.update(matched_types)

    return matches


def _specify_types(generic, spec):
    """
    Specifies types based on a given specification.
    
    Args:
        generic: The generic type.
        spec: The specification dictionary.
    
    Returns:
        The specified type.
    """
    if isinstance(generic, TypeVar):
        tp = spec.get(generic)
        if tp is None:
            return generic
        return tp

    generic_args = getattr(generic, "__args__", None)

    if generic_args is None:
        return generic

    origin = get_origin(generic)

    args = tuple(_specify_types(arg, spec) for arg in generic_args)

    return GenericAlias(origin, args)


_Args = ParamSpec("_Args")
_R = TypeVar("_R")


def awaitify(sync_func: Callable[_Args, _R]) -> Callable[_Args, Awaitable[_R]]:
    """
    Converts a synchronous function to an asynchronous function.
    
    Args:
        sync_func: The synchronous function to convert.
    
    Returns:
        An asynchronous function.
    """
    async def async_func(*args, **kwargs):
        return sync_func(*args, **kwargs)

    return async_func