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
)  # type: ignore


class TypeMismatchError(Exception):
    """Exception raised when there is a type mismatch."""
    def __init__(self, generic, specific):
        self.generic = generic
        self.specific = specific
        super().__init__(f"Type {generic} does not match with {specific}")


class MissingArgumentsError(Exception):
    """Exception raised when a type has no arguments."""
    def __init__(self, type_name):
        self.type_name = type_name
        super().__init__(f"Type {type_name} has no arguments")


class ArgumentCountMismatchError(Exception):
    """Exception raised when the number of arguments in generic and specific types differ."""
    def __init__(self, generic, specific):
        self.generic = generic
        self.specific = specific
        super().__init__(f"Number of arguments of type {generic} is different in specific type {specific}")


def _format_tuple(
    tuple_annotation: tuple, generic_input_param, input_annotation
) -> str:
    formatted = [_format_return_annotation(annotation, generic_input_param, input_annotation) for annotation in tuple_annotation]
    return f"({', '.join(formatted)})"


def _format_union(
    tuple_annotation: tuple, generic_input_param, input_annotation
) -> str:
    formatted = [_format_return_annotation(annotation, generic_input_param, input_annotation) for annotation in tuple_annotation]
    return f"({' | '.join(formatted)})"


def _format_generic_alias(
    return_annotation: GenericAlias, generic_input_param, input_annotation
) -> str:
    alias_name = return_annotation.__name__
    formatted = [_format_return_annotation(annotation, generic_input_param, input_annotation) for annotation in return_annotation.__args__]
    return f"{alias_name}[{', '.join(formatted)}]"


def _format_return_annotation(
    return_annotation, generic_input_param, input_annotation
) -> str:
    if isinstance(return_annotation, str):
        return return_annotation
    if isinstance(return_annotation, tuple):
        return _format_tuple(return_annotation, generic_input_param, input_annotation)
    if return_annotation.__name__ in {"tuple", "Tuple"}:
        return _format_tuple(return_annotation.__args__, generic_input_param, input_annotation)
    if return_annotation.__name__ in {"Union"}:
        return _format_union(return_annotation.__args__, generic_input_param, input_annotation)
    if isinstance(return_annotation, (GenericAlias, _GenericAlias)):
        return _format_generic_alias(return_annotation, generic_input_param, input_annotation)

    if return_annotation == generic_input_param:
        return input_annotation.__name__

    return return_annotation.__name__


def _match_types(generic, specific, ignore_mismatches=True):
    if isinstance(generic, TypeVar):
        return {generic: specific}

    specific_origin = get_origin(specific)
    generic_origin = get_origin(generic)

    if specific_origin is None and generic_origin is None:
        return {}

    if (specific_origin is None or generic_origin is None) or not issubclass(specific_origin, generic_origin):
        if ignore_mismatches:
            return {}
        raise TypeMismatchError(generic, specific)

    generic_args = getattr(generic, "__args__", None)
    specific_args = getattr(specific, "__args__", None)

    if specific_args is None and specific_args is None:
        return {}

    if generic_args is None:
        if ignore_mismatches:
            return {}
        raise MissingArgumentsError(generic.__name__)

    if specific_args is None:
        if ignore_mismatches:
            return {}
        raise MissingArgumentsError(specific.__name__)

    if len(generic_args) != len(specific_args):
        if ignore_mismatches:
            return {}
        raise ArgumentCountMismatchError(generic, specific)

    matches = {}
    for generic_arg, specific_arg in zip(generic_args, specific_args):
        matched_types = _match_types(generic_arg, specific_arg)
        matches.update(matched_types)

    return matches


def _specify_types(generic, spec):
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
    async def async_func(*args, **kwargs):
        return sync_func(*args, **kwargs)

    return async_func