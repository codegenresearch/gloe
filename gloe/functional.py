import inspect\nfrom inspect import Signature\nfrom types import FunctionType\nfrom typing import (\n    Callable,\n    Concatenate,\n    ParamSpec,\n    TypeVar,\n    cast,\n    Awaitable,\n    Generic,\n)\n\nfrom gloe.async_transformer import AsyncTransformer\nfrom gloe.transformers import Transformer\n\n__all__ = [\n    "transformer",\n    "partial_transformer",\n    "async_transformer",\n    "partial_async_transformer",\n]\n\nA = TypeVar("A")\nS = TypeVar("S")\nP1 = ParamSpec("P1")\n\nclass _PartialTransformer(Generic[A, P1, S]):\n    def __init__(self, func: Callable[Concatenate[A, P1], S]):\n        self.func = func\n\n    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> Transformer[A, S]:\n        func = self.func\n        func_signature = inspect.signature(func)\n\n        class LambdaTransformer(Transformer[A, S]):\n            __doc__ = func.__doc__\n            __annotations__ = cast(FunctionType, func).__annotations__\n\n            def signature(self) -> Signature:\n                return func_signature\n\n            def transform(self, data: A) -> S:\n                return func(data, *args, **kwargs)\n\n        lambda_transformer = LambdaTransformer()\n        lambda_transformer.__class__.__name__ = func.__name__\n        lambda_transformer._label = func.__name__\n        return lambda_transformer\n\ndef partial_transformer(\n    func: Callable[Concatenate[A, P1], S]\n) -> _PartialTransformer[A, P1, S]:\n    """\n    Decorator to create partial transformers with pre-applied arguments.\n    """\n    return _PartialTransformer(func)\n\nclass _PartialAsyncTransformer(Generic[A, P1, S]):\n    def __init__(self, func: Callable[Concatenate[A, P1], Awaitable[S]]):\n        self.func = func\n\n    def __call__(self, *args: P1.args, **kwargs: P1.kwargs) -> AsyncTransformer[A, S]:\n        func = self.func\n        func_signature = inspect.signature(func)\n\n        class LambdaTransformer(AsyncTransformer[A, S]):\n            __doc__ = func.__doc__\n            __annotations__ = cast(FunctionType, func).__annotations__\n\n            def signature(self) -> Signature:\n                return func_signature\n\n            async def transform_async(self, data: A) -> S:\n                return await func(data, *args, **kwargs)\n\n        lambda_transformer = LambdaTransformer()\n        lambda_transformer.__class__.__name__ = func.__name__\n        lambda_transformer._label = func.__name__\n        return lambda_transformer\n\ndef partial_async_transformer(\n    func: Callable[Concatenate[A, P1], Awaitable[S]]\n) -> _PartialAsyncTransformer[A, P1, S]:\n    """\n    Decorator to create partial asynchronous transformers with pre-applied arguments.\n    """\n    return _PartialAsyncTransformer(func)\n\ndef transformer(func: Callable[[A], S]) -> Transformer[A, S]:\n    """\n    Convert a callable to a Transformer instance.\n    """\n    func_signature = inspect.signature(func)\n\n    if len(func_signature.parameters) > 1:\n        warnings.warn(\n            "Only one parameter is allowed on Transformers. "\n            f"Function '{func.__name__}' has the following signature: {func_signature}. "\n            "Use complex types like named tuples, typed dicts, dataclasses, etc.",\n            category=RuntimeWarning,\n        )\n\n    class LambdaTransformer(Transformer[A, S]):\n        __doc__ = func.__doc__\n        __annotations__ = cast(FunctionType, func).__annotations__\n\n        def signature(self) -> Signature:\n            return func_signature\n\n        def transform(self, data: A) -> S:\n            return func(data)\n\n    lambda_transformer = LambdaTransformer()\n    lambda_transformer.__class__.__name__ = func.__name__\n    lambda_transformer._label = func.__name__\n    return lambda_transformer\n\ndef async_transformer(func: Callable[[A], Awaitable[S]]) -> AsyncTransformer[A, S]:\n    """\n    Convert a callable to an AsyncTransformer instance.\n    """\n    func_signature = inspect.signature(func)\n\n    if len(func_signature.parameters) > 1:\n        warnings.warn(\n            "Only one parameter is allowed on Transformers. "\n            f"Function '{func.__name__}' has the following signature: {func_signature}. "\n            "Use complex types like named tuples, typed dicts, dataclasses, etc.",\n            category=RuntimeWarning,\n        )\n\n    class LambdaAsyncTransformer(AsyncTransformer[A, S]):\n        __doc__ = func.__doc__\n        __annotations__ = cast(FunctionType, func).__annotations__\n\n        def signature(self) -> Signature:\n            return func_signature\n\n        async def transform_async(self, data: A) -> S:\n            return await func(data)\n\n    lambda_transformer = LambdaAsyncTransformer()\n    lambda_transformer.__class__.__name__ = func.__name__\n    lambda_transformer._label = func.__name__\n    return lambda_transformer\n