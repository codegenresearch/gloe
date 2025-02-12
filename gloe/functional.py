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
    """\n    This decorator let us create partial transformers, which are transformers that\n    allow for partial application of their arguments. This capability is particularly\n    useful for creating configurable transformer instances where some arguments are preset\n    enhancing modularity and reusability in data processing pipelines.\n\n    See Also:\n        For further details on partial transformers and their applications, see\n        :ref:`partial-transformers`.\n\n    Example:\n        Here's how to apply the `@partial_transformer` decorator to create a transformer\n        with a pre-applied argument::\n\n            @partial_transformer\n            def enrich_data(data: Data, enrichment_type: str) -> Data:\n                # Implementation for data enrichment based on the enrichment_type\n                ...\n\n            # Instantiate a transformer with the 'enrichment_type' pre-set\n            enrich_with_metadata = enrich_data(enrichment_type="metadata")\n\n            # Use the partially applied transformer\n            get_enriched_data = get_data >> enrich_with_metadata\n\n    Args:\n        func: A callable with one or more arguments. The first argument is of\n            type :code:`A`. The subsequent arguments are retained for use during\n            transformer instantiation. This callable returns a value of type\n            :code:`S`.\n\n    Returns:\n        An instance of the :code:`_PartialTransformer`, an internal class utilized within\n        Gloe that facilitates partial instantiation of transformers by the user.\n        The underlying mechanics of :code:`_PartialTransformer` are managed internally,\n        the user just needs to understand its usage.\n    """
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
    """\n    This decorator enables the creation of partial asynchronous transformers, which are\n    transformers capable of partial argument application. Such functionality is invaluable\n    for crafting reusable asynchronous transformer instances where certain arguments are\n    predetermined, enhancing both modularity and reusability within asynchronous data\n    processing flows.\n\n    See Also:\n        For additional insights into partial asynchronous transformers and their practical\n        applications, consult :ref:`partial-async-transformers`.\n\n    Example:\n        Utilize the `@partial_async_transformer` decorator to build a transformer with\n        a pre-set argument::\n\n            @partial_async_transformer\n            async def load_data(user_id: int, data_type: str) -> Data:\n                # Logic for loading data based on user_id and data_type\n                ...\n\n            # Instantiate a transformer with 'data_type' predefined\n            load_user_data = load_data(data_type="user_profile")\n\n            # Subsequent usage requires only the user_id\n            user_data = await load_user_data(user_id=1234)\n\n    Args:\n        func: A callable with one or more arguments, the first of which is of type `A`.\n            Remaining arguments are preserved for later use during the instantiation of\n            the transformer. This callable must asynchronously return a result of type\n            `S`, indicating an operation that produces an output of type `S` upon\n            completion.\n\n    Returns:\n        An instance of the :code:`_PartialAsyncTransformer`, an internally managed class\n        within Gloe designed to facilitate the partial instantiation of asynchronous\n        transformers. Users are encouraged to understand its application, as the\n        underlying mechanics of :code:`_PartialAsyncTransformer` are handled internally.\n    """
    return _PartialAsyncTransformer(func)


def transformer(func: Callable[[A], S]) -> Transformer[A, S]:
    """\n    Convert a callable to an instance of the Transformer class.\n\n    See Also:\n        The most common usage is as a decorator. This example demonstrates how to use the\n        `@transformer` decorator to filter a list of users::\n\n    Example:\n        The most common use is as a decorator::\n\n            @transformer\n            def filter_subscribed_users(users: list[User]) -> list[User]:\n               ...\n\n            subscribed_users = filter_subscribed_users(users_list)\n\n    Args:\n        func: A callable that takes a single argument and returns a result. The callable\n            should return an instance of the generic type :code:`S` specified.\n    Returns:\n        An instance of the Transformer class, encapsulating the transformation logic\n        defined in the provided callable.\n    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            "Only one parameter is allowed on Transformers. "
            f"Function '{func.__name__}' has the following signature: {func_signature}. "
            "To pass a complex data, use a complex type like named tuples, "
            "typed dicts, dataclasses or anything else.",
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
    """\n    Convert a callable to an instance of the AsyncTransformer class.\n\n    See Also:\n        For more information about this feature, refer to the :ref:`async-transformers`.\n\n    Example:\n        The most common use is as a decorator::\n\n            @async_transformer\n            async def get_user_by_role(role: str) -> list[User]:\n               ...\n\n            await get_user_by_role("admin")\n\n    Args:\n        func: A callable that takes a single argument and returns a coroutine.\n    Returns:\n        Returns an instance of the AsyncTransformer class, representing the built async\n        transformer.\n    """
    func_signature = inspect.signature(func)

    if len(func_signature.parameters) > 1:
        warnings.warn(
            "Only one parameter is allowed on Transformers. "
            f"Function '{func.__name__}' has the following signature: {func_signature}. "
            "To pass a complex data, use a complex type like named tuples, "
            "typed dicts, dataclasses or anything else.",
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