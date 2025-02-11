import inspect
import warnings
from inspect import Signature
from types import FunctionType
from typing import (
    Callable,
    Concatenate,
    ParamSpec,
    TypeVar,
    cast,
    Awaitable,
    Generic,
)

from gloe.async_transformer import AsyncTransformer
from gloe.transformers import Transformer

__all__ = [
    "partial_transformer",
    "partial_async_transformer",
    "transformer",
    "async_transformer",
]

A = TypeVar("A")
S = TypeVar("S")
P1 = ParamSpec("P1")

class _PartialTransformer(Generic[A, P1, S]):
    def __init__(self, func: Callable[Concatenate[A, P1], S]):
        self.func = func

    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> Transformer[A, S]:
        func = self.func
        func_signature = inspect.signature(func)

        class LambdaTransformer(Transformer[A, S]):
            __doc__ = func.__doc__
            __annotations__ = cast(FunctionType, func).__annotations__

            def signature(self) -> Signature:
                return func_signature

            def transform(self, data: A) -> S:
                return func(data, *args, **kwargs)

        lambda_transformer = LambdaTransformer()
        lambda_transformer.__class__.__name__ = func.__name__
        lambda_transformer._label = func.__name__
        return lambda_transformer


def partial_transformer(
    func: Callable[Concatenate[A, P1], S]
) -> _PartialTransformer[A, P1, S]:
    """
    Decorator to create partial transformers, allowing partial application of arguments.
    Useful for creating configurable transformer instances.

    Example:
        @partial_transformer
        def enrich_data(data: Data, enrichment_type: str) -> Data:
            ...

        enrich_with_metadata = enrich_data(enrichment_type="metadata")
        get_enriched_data = get_data >> enrich_with_metadata

    Args:
        func: Callable with arguments where the first is of type A. Returns a value of type S.

    Returns:
        _PartialTransformer instance for partial transformer instantiation.
    """
    return _PartialTransformer(func)


class _PartialAsyncTransformer(Generic[A, P1, S]):
    def __init__(self, func: Callable[Concatenate[A, P1], Awaitable[S]]):
        self.func = func

    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> AsyncTransformer[A, S]:
        func = self.func
        func_signature = inspect.signature(func)

        class LambdaTransformer(AsyncTransformer[A, S]):
            __doc__ = func.__doc__
            __annotations__ = cast(FunctionType, func).__annotations__

            def signature(self) -> Signature:
                return func_signature

            async def transform_async(self, data: A) -> S:
                return await func(data, *args, **kwargs)

        lambda_transformer = LambdaTransformer()
        lambda_transformer.__class__.__name__ = func.__name__
        lambda_transformer._label = func.__name__
        return lambda_transformer


def partial_async_transformer(
    func: Callable[Concatenate[A, P1], Awaitable[S]]
) -> _PartialAsyncTransformer[A, P1, S]:
    """
    Decorator to create partial asynchronous transformers, allowing partial application of arguments.
    Useful for creating reusable asynchronous transformer instances.

    Example:
        @partial_async_transformer
        async def load_data(user_id: int, data_type: str) -> Data:
            ...

        load_user_data = load_data(data_type="user_profile")
        user_data = await load_user_data(user_id=1234)

    Args:
        func: Callable with arguments where the first is of type A. Returns an Awaitable of type S.

    Returns:
        _PartialAsyncTransformer instance for partial async transformer instantiation.
    """
    return _PartialAsyncTransformer(func)


def transformer(func: Callable[[A], S]) -> Transformer[A, S]:
    """
    Convert a callable to a Transformer instance.

    Example:
        @transformer
        def filter_subscribed_users(users: list[User]) -> list[User]:
           ...

        subscribed_users = filter_subscribed_users(users_list)

    Args:
        func: Callable with a single argument returning a result of type S.

    Returns:
        Transformer instance encapsulating the transformation logic.
    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            "Only one parameter is allowed on Transformers. "
            f"Function '{func.__name__}' has the following signature: {func_signature}. "
            "Use complex types like named tuples, typed dicts, dataclasses, etc.",
            category=RuntimeWarning,
        )

    class LambdaTransformer(Transformer[A, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def signature(self) -> Signature:
            return func_signature

        def transform(self, data):
            return func(data)

    lambda_transformer = LambdaTransformer()
    lambda_transformer.__class__.__name__ = func.__name__
    lambda_transformer._label = func.__name__
    return lambda_transformer


def async_transformer(func: Callable[[A], Awaitable[S]]) -> AsyncTransformer[A, S]:
    """
    Convert a callable to an AsyncTransformer instance.

    Example:
        @async_transformer
        async def get_user_by_role(role: str) -> list[User]:
           ...

        await get_user_by_role("admin")

    Args:
        func: Callable with a single argument returning an Awaitable of type S.

    Returns:
        AsyncTransformer instance representing the async transformer.
    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            "Only one parameter is allowed on Transformers. "
            f"Function '{func.__name__}' has the following signature: {func_signature}. "
            "Use complex types like named tuples, typed dicts, dataclasses, etc.",
            category=RuntimeWarning,
        )

    class LambdaAsyncTransformer(AsyncTransformer[A, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def signature(self) -> Signature:
            return func_signature

        async def transform_async(self, data):
            return await func(data)

    lambda_transformer = LambdaAsyncTransformer()
    lambda_transformer.__class__.__name__ = func.__name__
    lambda_transformer._label = func.__name__
    return lambda_transformer