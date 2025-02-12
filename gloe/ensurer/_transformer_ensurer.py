import inspect
from abc import abstractmethod, ABC
from typing import Any, Callable, Generic, Sequence, cast

from gloe.async_transformer import AsyncTransformer
from gloe.functional import _PartialTransformer, _PartialAsyncTransformer
from gloe.transformers import Transformer

T = Any
S = Any
U = Any


class TransformerEnsurer(Generic[T, S], ABC):
    def __call__(self, transformer: Transformer[T, S]) -> Transformer[T, S]:
        def transform(this, data):
            self.validate_input(data)
            output = transformer.transform(data)
            self.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    @abstractmethod
    def validate_input(self, data: T):
        """Validate incoming data"""

    @abstractmethod
    def validate_output(self, data: T, output: S):
        """Validate outcome data"""


def input_ensurer(func: Callable[[T], Any]) -> TransformerEnsurer[T, Any]:
    class LambdaEnsurer(TransformerEnsurer[T, S]):
        __doc__ = func.__doc__

        def validate_input(self, data: T):
            func(data)

        def validate_output(self, data: T, output: S):
            pass

    return LambdaEnsurer()


def output_ensurer(func: Callable) -> TransformerEnsurer:
    class LambdaEnsurer(TransformerEnsurer):
        __doc__ = func.__doc__

        def validate_input(self, data):
            pass

        def validate_output(self, data, output):
            params = inspect.signature(func).parameters
            if len(params) == 1:
                func(output)
            elif len(params) == 2:
                func(data, output)
            else:
                raise ValueError("Output ensurer function must have 1 or 2 parameters")

    return LambdaEnsurer()


class _ensure_base:
    def __call__(self, arg):
        if isinstance(arg, Transformer):
            return self._generate_new_transformer(arg)
        elif isinstance(arg, AsyncTransformer):
            return self._generate_new_async_transformer(arg)
        elif isinstance(arg, _PartialTransformer):
            def ensured_transformer_init(*args, **kwargs):
                transformer = arg(*args, **kwargs)
                return self._generate_new_transformer(transformer)
            return ensured_transformer_init
        elif isinstance(arg, _PartialAsyncTransformer):
            async def ensured_async_transformer_init(*args, **kwargs):
                async_transformer = arg(*args, **kwargs)
                return self._generate_new_async_transformer(async_transformer)
            return ensured_async_transformer_init
        else:
            raise TypeError("Unsupported argument type")

    @abstractmethod
    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        pass

    @abstractmethod
    def _generate_new_async_transformer(self, transformer: AsyncTransformer) -> AsyncTransformer:
        pass


class _ensure_incoming(_ensure_base):
    def __init__(self, incoming: Sequence[Callable[[T], Any]]):
        self.input_ensurers_instances = [input_ensurer(ensurer) for ensurer in incoming]

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data):
            for ensurer in self.input_ensurers_instances:
                ensurer.validate_input(data)
            return transformer.transform(data)

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(self, transformer: AsyncTransformer) -> AsyncTransformer:
        async def transform_async(_, data):
            for ensurer in self.input_ensurers_instances:
                ensurer.validate_input(data)
            return await transformer.transform_async(data)

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


class _ensure_outcome(_ensure_base):
    def __init__(self, outcome: Sequence[Callable[[S], Any]]):
        self.output_ensurers_instances = [output_ensurer(ensurer) for ensurer in outcome]

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data):
            output = transformer.transform(data)
            for ensurer in self.output_ensurers_instances:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(self, transformer: AsyncTransformer) -> AsyncTransformer:
        async def transform_async(_, data):
            output = await transformer.transform_async(data)
            for ensurer in self.output_ensurers_instances:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


class _ensure_changes(_ensure_base):
    def __init__(self, changes: Sequence[Callable[[T, S], Any]]):
        self.changes_ensurers_instances = [output_ensurer(ensurer) for ensurer in changes]

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data):
            output = transformer.transform(data)
            for ensurer in self.changes_ensurers_instances:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(self, transformer: AsyncTransformer) -> AsyncTransformer:
        async def transform_async(_, data):
            output = await transformer.transform_async(data)
            for ensurer in self.changes_ensurers_instances:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


class _ensure_both(_ensure_base):
    def __init__(self, incoming: Sequence[Callable[[T], Any]], outcome: Sequence[Callable[[S], Any]], changes: Sequence[Callable[[T, S], Any]]):
        self.input_ensurers_instances = [input_ensurer(ensurer) for ensurer in incoming]
        self.output_ensurers_instances = [output_ensurer(ensurer) for ensurer in outcome + changes]

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data):
            for ensurer in self.input_ensurers_instances:
                ensurer.validate_input(data)
            output = transformer.transform(data)
            for ensurer in self.output_ensurers_instances:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(self, transformer: AsyncTransformer) -> AsyncTransformer:
        async def transform_async(_, data):
            for ensurer in self.input_ensurers_instances:
                ensurer.validate_input(data)
            output = await transformer.transform_async(data)
            for ensurer in self.output_ensurers_instances:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


def ensure(*, incoming: Sequence[Callable[[T], Any]] = [], outcome: Sequence[Callable[[S], Any]] = [], changes: Sequence[Callable[[T, S], Any]] = []) -> _ensure_base:
    return _ensure_both(incoming, outcome, changes)