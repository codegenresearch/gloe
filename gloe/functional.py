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
    "transformer",
    "partial_transformer",
    "async_transformer",
    "partial_async_transformer",
]

A = TypeVar("A")
S = TypeVar("S")
S2 = TypeVar("S2")
P1 = ParamSpec("P1")
P2 = ParamSpec("P2")
O = TypeVar("O")


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
    This decorator allows the creation of partial transformers, which enable partial
    application of their arguments. This is useful for creating configurable transformer
    instances where some arguments are preset, enhancing modularity and reusability in
    data processing pipelines.

    See Also:
        For further details on partial transformers and their applications, see
        :ref:`partial-transformers`.

    Example:
        Here's how to apply the `@partial_transformer` decorator to create a transformer
        with a pre-applied argument::

            @partial_transformer
            def enrich_data(data: Data, enrichment_type: str) -> Data:
                # Implementation for data enrichment based on the enrichment_type
                ...

            # Instantiate a transformer with the 'enrichment_type' pre-set
            enrich_with_metadata = enrich_data(enrichment_type="metadata")

            # Use the partially applied transformer
            get_enriched_data = get_data >> enrich_with_metadata

    Args:
        func: A callable that takes a single argument of type :code:`A` and additional
            arguments specified by :code:`P1`. This callable returns a value of type
            :code:`S`.

    Returns:
        An instance of the :code:`_PartialTransformer`, an internal class used within
        Gloe to facilitate partial instantiation of transformers.
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
    This decorator enables the creation of partial asynchronous transformers, which are
    capable of partial argument application. This is useful for crafting reusable
    asynchronous transformer instances where certain arguments are predetermined,
    enhancing modularity and reusability within asynchronous data processing flows.

    See Also:
        For additional insights into partial asynchronous transformers and their practical
        applications, consult :ref:`partial-async-transformers`.

    Example:
        Utilize the `@partial_async_transformer` decorator to build a transformer with
        a pre-set argument::

            @partial_async_transformer
            async def load_data(user_id: int, data_type: str) -> Data:
                # Logic for loading data based on user_id and data_type
                ...

            # Instantiate a transformer with 'data_type' predefined
            load_user_data = load_data(data_type="user_profile")

            # Subsequent usage requires only the user_id
            user_data = await load_user_data(user_id=1234)

    Args:
        func: A callable that takes a single argument of type :code:`A` and additional
            arguments specified by :code:`P1`. This callable returns a coroutine that
            yields a result of type :code:`S`.

    Returns:
        An instance of the :code:`_PartialAsyncTransformer`, an internal class used
        within Gloe to facilitate the partial instantiation of asynchronous transformers.
    """
    return _PartialAsyncTransformer(func)


def transformer(func: Callable[[A], S]) -> Transformer[A, S]:
    """
    Convert a callable to an instance of the Transformer class.

    See Also:
        The most common usage is as a decorator. This example demonstrates how to use the
        `@transformer` decorator to filter a list of users.

    Example:
        The most common use is as a decorator::

            @transformer
            def filter_subscribed_users(users: list[User]) -> list[User]:
               ...

            subscribed_users = filter_subscribed_users(users_list)

    Args:
        func: A callable that takes a single argument of type :code:`A` and returns a
            result of type :code:`S`.

    Returns:
        An instance of the Transformer class, encapsulating the transformation logic
        defined in the provided callable. This instance can be used in data processing
        pipelines to apply the transformation to input data.
    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            "Only one parameter is allowed on Transformers. "
            f"Function '{func.__name__}' has the following signature: {func_signature}. "
            "To pass complex data, use a complex type like named tuples, "
            "typed dicts, dataclasses, or anything else.",
            category=RuntimeWarning,
        )

    class LambdaTransformer(Transformer[A, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def signature(self) -> Signature:
            return func_signature

        def transform(self, data: A) -> S:
            return func(data)

    lambda_transformer = LambdaTransformer()
    lambda_transformer.__class__.__name__ = func.__name__
    lambda_transformer._label = func.__name__
    return lambda_transformer


def async_transformer(func: Callable[[A], Awaitable[S]]) -> AsyncTransformer[A, S]:
    """
    Convert a callable to an instance of the AsyncTransformer class.

    See Also:
        For more information about this feature, refer to the :ref:`async-transformers`.

    Example:
        The most common use is as a decorator::

            @async_transformer
            async def get_user_by_role(role: str) -> list[User]:
               ...

            await get_user_by_role("admin")

    Args:
        func: A callable that takes a single argument of type :code:`A` and returns a
            coroutine that yields a result of type :code:`S`.

    Returns:
        An instance of the AsyncTransformer class, representing the built async
        transformer. This instance can be used in asynchronous data processing pipelines
        to apply the transformation to input data.
    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            "Only one parameter is allowed on Transformers. "
            f"Function '{func.__name__}' has the following signature: {func_signature}. "
            "To pass complex data, use a complex type like named tuples, "
            "typed dicts, dataclasses, or anything else.",
            category=RuntimeWarning,
        )

    class LambdaAsyncTransformer(AsyncTransformer[A, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def signature(self) -> Signature:
            return func_signature

        async def transform_async(self, data: A) -> S:
            return await func(data)

    lambda_transformer = LambdaAsyncTransformer()
    lambda_transformer.__class__.__name__ = func.__name__
    lambda_transformer._label = func.__name__
    return lambda_transformer