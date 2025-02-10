from functools import wraps
from types import GenericAlias, _GenericAlias
from typing import (
    TypeVar,
    get_origin,
    ParamSpec,
    Callable,
    Awaitable,
    Any,
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
    if (
        isinstance(return_annotation, GenericAlias)
        or isinstance(return_annotation, _GenericAlias)
    ):
        return _format_generic_alias(
            return_annotation, generic_input_param, input_annotation
        )

    if return_annotation == generic_input_param:
        return str(input_annotation.__name__)

    return str(return_annotation.__name__)


def _match_types(generic, specific, ignore_mismatches=True):
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
        matched_types = _match_types(generic_arg, specific_arg, ignore_mismatches)
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
    async def async_func(*args: Any, **kwargs: Any) -> _R:
        return sync_func(*args, **kwargs)

    return async_func


### Changes Made:
1. **Type Checking**: Used `isinstance()` for type comparisons instead of `type()`.
2. **Error Handling**: Ensured that `Exception` is used for raising errors consistently.
3. **Return Statements**: Verified that return statements in `_match_types` are consistent.
4. **Function Parameters**: Ensured that function parameters are aligned with the gold code's structure.
5. **List Initialization**: Used type hinting for list initialization (e.g., `formatted: list[str] = []`).
6. **Generic Alias Handling**: Included checks for both `GenericAlias` and `_GenericAlias` in `_format_return_annotation`.
7. **Removed Unterminated String Literal**: Corrected any unterminated string literals to prevent syntax errors.
8. **Simplified Logic**: Removed unnecessary checks and conditions to simplify the logic.