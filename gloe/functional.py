import inspect
from typing import Callable, Concatenate, ParamSpec, TypeVar, cast, Awaitable, Generic

from gloe.async_transformer import AsyncTransformer
from gloe.transformers import Transformer

__all__ = [
    "transformer",
    "partial_transformer",
    "async_transformer",
    "partial_async_transformer",
]

A = TypeVar("A")
S = TypeVar("S")
P1 = ParamSpec("P1")
O = TypeVar("O")


class _PartialTransformer(Generic[A, P1, S]):
    def __init__(self, func: Callable[Concatenate[A, P1], S]):
        self.func = func

    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> Transformer[A, S]:
        func = self.func
        func_signature = inspect.signature(func)

        class LambdaTransformer(Transformer[A, S]):
            __doc__ = func.__doc__
            __annotations__ = cast(Callable, func).__annotations__

            def signature(self) -> inspect.Signature:
                return func_signature

            def transform(self, data: A) -> S:
                return func(data, *args, **kwargs)

        lambda_transformer = LambdaTransformer()
        lambda_transformer.__class__.__name__ = func.__name__
        lambda_transformer._label = func.__name__
        return lambda_transformer


def partial_transformer(func: Callable[Concatenate[A, P1], S]) -> _PartialTransformer[A, P1, S]:
    """\n    Decorator to create partial transformers with pre-applied arguments.\n\n    Args:\n        func: Callable with one or more arguments where the first is of type A. Returns a value of type S.\n\n    Returns:\n        _PartialTransformer instance for partial instantiation of transformers.\n    """
    return _PartialTransformer(func)


class _PartialAsyncTransformer(Generic[A, P1, S]):
    def __init__(self, func: Callable[Concatenate[A, P1], Awaitable[S]]):
        self.func = func

    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> AsyncTransformer[A, S]:
        func = self.func
        func_signature = inspect.signature(func)

        class LambdaTransformer(AsyncTransformer[A, S]):
            __doc__ = func.__doc__
            __annotations__ = cast(Callable, func).__annotations__

            def signature(self) -> inspect.Signature:
                return func_signature

            async def transform_async(self, data: A) -> S:
                return await func(data, *args, **kwargs)

        lambda_transformer = LambdaTransformer()
        lambda_transformer.__class__.__name__ = func.__name__
        lambda_transformer._label = func.__name__
        return lambda_transformer


def partial_async_transformer(func: Callable[Concatenate[A, P1], Awaitable[S]]) -> _PartialAsyncTransformer[A, P1, S]:
    """\n    Decorator to create partial asynchronous transformers with pre-applied arguments.\n\n    Args:\n        func: Callable with one or more arguments where the first is of type A. Returns an Awaitable of type S.\n\n    Returns:\n        _PartialAsyncTransformer instance for partial instantiation of asynchronous transformers.\n    """
    return _PartialAsyncTransformer(func)


def transformer(func: Callable[[A], S]) -> Transformer[A, S]:
    """\n    Convert a callable to a Transformer instance.\n\n    Args:\n        func: Callable that takes a single argument of type A and returns a value of type S.\n\n    Returns:\n        Transformer instance encapsulating the transformation logic.\n    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            f"Only one parameter allowed for Transformers. Function '{func.__name__}' has signature: {func_signature}. "
            "Use complex types like named tuples, typed dicts, dataclasses, etc., for complex data.",
            category=RuntimeWarning,
        )

    class LambdaTransformer(Transformer[A, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(Callable, func).__annotations__

        def signature(self) -> inspect.Signature:
            return func_signature

        def transform(self, data: A) -> S:
            return func(data)

    lambda_transformer = LambdaTransformer()
    lambda_transformer.__class__.__name__ = func.__name__
    lambda_transformer._label = func.__name__
    return lambda_transformer


def async_transformer(func: Callable[[A], Awaitable[S]]) -> AsyncTransformer[A, S]:
    """\n    Convert a callable to an AsyncTransformer instance.\n\n    Args:\n        func: Callable that takes a single argument of type A and returns an Awaitable of type S.\n\n    Returns:\n        AsyncTransformer instance representing the asynchronous transformer.\n    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            f"Only one parameter allowed for Transformers. Function '{func.__name__}' has signature: {func_signature}. "
            "Use complex types like named tuples, typed dicts, dataclasses, etc., for complex data.",
            category=RuntimeWarning,
        )

    class LambdaAsyncTransformer(AsyncTransformer[A, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(Callable, func).__annotations__

        def signature(self) -> inspect.Signature:
            return func_signature

        async def transform_async(self, data: A) -> S:
            return await func(data)

    lambda_transformer = LambdaAsyncTransformer()
    lambda_transformer.__class__.__name__ = func.__name__
    lambda_transformer._label = func.__name__
    return lambda_transformer