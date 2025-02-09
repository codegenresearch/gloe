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
        """Initialize a _PartialTransformer with a function that can be partially applied.

        Args:
            func (Callable[Concatenate[A, P1], S]): A callable that takes a primary argument of type A and additional arguments.
        """
        self.func = func

    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> Transformer[A, S]:
        """Create a Transformer instance with partially applied arguments.

        Args:
            *args (P1.args): Positional arguments to be partially applied.
            **kwargs (P1.kwargs): Keyword arguments to be partially applied.

        Returns:
            Transformer[A, S]: A Transformer instance with the partially applied function.
        """
        func = self.func
        func_signature = inspect.signature(func)

        class LambdaTransformer(Transformer[A, S]):
            __doc__ = func.__doc__
            __annotations__ = cast(FunctionType, func).__annotations__

            def signature(self) -> Signature:
                """Return the signature of the partially applied function.

                Returns:
                    Signature: The signature of the function.
                """
                return func_signature

            def transform(self, data: A) -> S:
                """Apply the partially applied function to the input data.

                Args:
                    data (A): The input data to transform.

                Returns:
                    S: The result of the function application.
                """
                return func(data, *args, **kwargs)

        lambda_transformer = LambdaTransformer()
        lambda_transformer.__class__.__name__ = func.__name__
        lambda_transformer._label = func.__name__
        return lambda_transformer


def partial_transformer(
    func: Callable[Concatenate[A, P1], S]
) -> _PartialTransformer[A, P1, S]:
    """Decorator to create partial transformers, allowing partial application of arguments.
    Useful for creating configurable transformer instances.

    This decorator is particularly useful when you want to fix some arguments of a transformer
    function and create a new transformer with those arguments pre-filled. This can enhance
    modularity and reusability in data processing pipelines.

    See Also:
        For further details on partial transformers and their applications, see
        :ref:`partial-transformers`.

    Example:
        .. code-block:: python

            @partial_transformer
            def enrich_data(data: Data, enrichment_type: str) -> Data:
                # Implementation for data enrichment based on the enrichment_type
                ...

            # Instantiate a transformer with the 'enrichment_type' pre-set
            enrich_with_metadata = enrich_data(enrichment_type="metadata")

            # Use the partially applied transformer
            get_enriched_data = get_data >> enrich_with_metadata

    Args:
        func (Callable[Concatenate[A, P1], S]): Callable with arguments where the first is of type A and others are retained.

    Returns:
        _PartialTransformer[A, P1, S]: An instance of _PartialTransformer.
    """
    return _PartialTransformer(func)


class _PartialAsyncTransformer(Generic[A, P1, S]):
    def __init__(self, func: Callable[Concatenate[A, P1], Awaitable[S]]):
        """Initialize a _PartialAsyncTransformer with an asynchronous function that can be partially applied.

        Args:
            func (Callable[Concatenate[A, P1], Awaitable[S]]): An asynchronous callable that takes a primary argument of type A and additional arguments.
        """
        self.func = func

    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> AsyncTransformer[A, S]:
        """Create an AsyncTransformer instance with partially applied arguments.

        Args:
            *args (P1.args): Positional arguments to be partially applied.
            **kwargs (P1.kwargs): Keyword arguments to be partially applied.

        Returns:
            AsyncTransformer[A, S]: An AsyncTransformer instance with the partially applied function.
        """
        func = self.func
        func_signature = inspect.signature(func)

        class LambdaTransformer(AsyncTransformer[A, S]):
            __doc__ = func.__doc__
            __annotations__ = cast(FunctionType, func).__annotations__

            def signature(self) -> Signature:
                """Return the signature of the partially applied function.

                Returns:
                    Signature: The signature of the function.
                """
                return func_signature

            async def transform_async(self, data: A) -> S:
                """Asynchronously apply the partially applied function to the input data.

                Args:
                    data (A): The input data to transform.

                Returns:
                    S: The result of the function application.
                """
                return await func(data, *args, **kwargs)

        lambda_transformer = LambdaTransformer()
        lambda_transformer.__class__.__name__ = func.__name__
        lambda_transformer._label = func.__name__
        return lambda_transformer


def partial_async_transformer(
    func: Callable[Concatenate[A, P1], Awaitable[S]]
) -> _PartialAsyncTransformer[A, P1, S]:
    """Decorator to create partial asynchronous transformers, allowing partial application of arguments.

    This decorator is useful when you want to fix some arguments of an asynchronous transformer
    function and create a new transformer with those arguments pre-filled. This can enhance
    modularity and reusability in asynchronous data processing flows.

    See Also:
        For additional insights into partial asynchronous transformers and their practical
        applications, consult :ref:`partial-async-transformers`.

    Example:
        .. code-block:: python

            @partial_async_transformer
            async def load_data(user_id: int, data_type: str) -> Data:
                # Logic for loading data based on user_id and data_type
                ...

            # Instantiate a transformer with 'data_type' predefined
            load_user_data = load_data(data_type="user_profile")

            # Subsequent usage requires only the user_id
            user_data = await load_user_data(user_id=1234)

    Args:
        func (Callable[Concatenate[A, P1], Awaitable[S]]): Callable with arguments where the first is of type A and others are retained.

    Returns:
        _PartialAsyncTransformer[A, P1, S]: An instance of _PartialAsyncTransformer.
    """
    return _PartialAsyncTransformer(func)


def transformer(func: Callable[[A], S]) -> Transformer[A, S]:
    """Convert a callable to a Transformer instance.

    This decorator is used to convert a regular function into a Transformer, which can be
    used in data processing pipelines. The function should take a single argument and return
    a result. If the function has more than one parameter, a warning is issued, and it is
    recommended to use complex types like named tuples, typed dicts, or dataclasses to pass
    multiple pieces of data.

    See Also:
        The most common usage is as a decorator. This example demonstrates how to use the
        `@transformer` decorator to filter a list of users.

    Example:
        .. code-block:: python

            @transformer
            def filter_subscribed_users(users: list[User]) -> list[User]:
               ...

            subscribed_users = filter_subscribed_users(users_list)

    Args:
        func (Callable[[A], S]): Callable with a single argument.

    Returns:
        Transformer[A, S]: A Transformer instance encapsulating the transformation logic.
    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            f"Function '{func.__name__}' has more than one parameter. Only one parameter is allowed on Transformers. "
            "Consider using complex types like named tuples, typed dicts, or dataclasses to pass multiple pieces of data.",
            category=RuntimeWarning,
        )

    class LambdaTransformer(Transformer[A, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def signature(self) -> Signature:
            """Return the signature of the function.

            Returns:
                Signature: The signature of the function.
            """
            return func_signature

        def transform(self, data: A) -> S:
            """Apply the function to the input data.

            Args:
                data (A): The input data to transform.

            Returns:
                S: The result of the function application.
            """
            return func(data)

    lambda_transformer = LambdaTransformer()
    lambda_transformer.__class__.__name__ = func.__name__
    lambda_transformer._label = func.__name__
    return lambda_transformer


def async_transformer(func: Callable[[A], Awaitable[S]]) -> AsyncTransformer[A, S]:
    """Convert a callable to an AsyncTransformer instance.

    This decorator is used to convert an asynchronous function into an AsyncTransformer,
    which can be used in asynchronous data processing pipelines. The function should take a
    single argument and return a coroutine. If the function has more than one parameter,
    a warning is issued, and it is recommended to use complex types like named tuples,
    typed dicts, or dataclasses to pass multiple pieces of data.

    See Also:
        For more information about this feature, refer to the :ref:`async-transformers`.

    Example:
        .. code-block:: python

            @async_transformer
            async def get_user_by_role(role: str) -> list[User]:
               ...

            await get_user_by_role("admin")

    Args:
        func (Callable[[A], Awaitable[S]]): Callable with a single argument that returns a coroutine.

    Returns:
        AsyncTransformer[A, S]: An AsyncTransformer instance encapsulating the asynchronous transformation logic.
    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            f"Function '{func.__name__}' has more than one parameter. Only one parameter is allowed on Transformers. "
            "Consider using complex types like named tuples, typed dicts, or dataclasses to pass multiple pieces of data.",
            category=RuntimeWarning,
        )

    class LambdaAsyncTransformer(AsyncTransformer[A, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def signature(self) -> Signature:
            """Return the signature of the function.

            Returns:
                Signature: The signature of the function.
            """
            return func_signature

        async def transform_async(self, data: A) -> S:
            """Asynchronously apply the function to the input data.

            Args:
                data (A): The input data to transform.

            Returns:
                S: The result of the function application.
            """
            return await func(data)

    lambda_transformer = LambdaAsyncTransformer()
    lambda_transformer.__class__.__name__ = func.__name__
    lambda_transformer._label = func.__name__
    return lambda_transformer