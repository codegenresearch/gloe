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
    Type,
    Mapping,
)  # type: ignore


class TypeMismatchError(Exception):
    """Raised when there is a type mismatch during type validation."""
    pass


class InvalidTypeAnnotationError(Exception):
    """Raised when a type annotation is invalid."""
    pass


def _format_tuple(
    tuple_annotation: tuple, generic_input_param, input_annotation
) -> str:
    """Format a tuple annotation into a string representation."""
    formatted = [_format_return_annotation(a, generic_input_param, input_annotation) for a in tuple_annotation]
    return f"({', '.join(formatted)})"


def _format_union(
    union_annotation: tuple, generic_input_param, input_annotation
) -> str:
    """Format a union annotation into a string representation."""
    formatted = [_format_return_annotation(a, generic_input_param, input_annotation) for a in union_annotation]
    return f"({' | '.join(formatted)})"


def _format_generic_alias(
    return_annotation: GenericAlias, generic_input_param, input_annotation
) -> str:
    """Format a generic alias annotation into a string representation."""
    alias_name = return_annotation.__name__
    formatted = [_format_return_annotation(a, generic_input_param, input_annotation) for a in return_annotation.__args__]
    return f"{alias_name}[{', '.join(formatted)}]"


def _format_return_annotation(
    return_annotation, generic_input_param, input_annotation
) -> str:
    """Format the return annotation into a string representation."""
    if isinstance(return_annotation, str):
        return return_annotation
    if isinstance(return_annotation, tuple):
        if return_annotation.__origin__ is Union:
            return _format_union(return_annotation.__args__, generic_input_param, input_annotation)
        return _format_tuple(return_annotation, generic_input_param, input_annotation)
    if isinstance(return_annotation, _GenericAlias) or isinstance(return_annotation, GenericAlias):
        return _format_generic_alias(return_annotation, generic_input_param, input_annotation)
    if return_annotation == generic_input_param:
        return input_annotation.__name__
    return return_annotation.__name__


def _match_types(generic, specific, ignore_mismatches=True) -> Mapping[TypeVar, Type]:
    """Match generic types with specific types."""
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
        raise TypeMismatchError(f"Type {generic} does not match with {specific}")

    generic_args = getattr(generic, "__args__", None)
    specific_args = getattr(specific, "__args__", None)

    if specific_args is None and generic_args is None:
        return {}

    if generic_args is None:
        if ignore_mismatches:
            return {}
        raise InvalidTypeAnnotationError(f"Type {generic} in generic has no arguments")

    if specific_args is None:
        if ignore_mismatches:
            return {}
        raise InvalidTypeAnnotationError(f"Type {specific} in specific has no arguments")

    if len(generic_args) != len(specific_args):
        if ignore_mismatches:
            return {}
        raise InvalidTypeAnnotationError(
            f"Number of arguments of type {generic} is different in specific type"
        )

    matches = {}
    for generic_arg, specific_arg in zip(generic_args, specific_args):
        matched_types = _match_types(generic_arg, specific_arg)
        matches.update(matched_types)

    return matches


def _specify_types(generic, spec) -> Type:
    """Specify the types in a generic type with the given specification."""
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
    """Convert a synchronous function into an asynchronous function."""
    @wraps(sync_func)
    async def async_func(*args, **kwargs):
        return sync_func(*args, **kwargs)
    return async_func