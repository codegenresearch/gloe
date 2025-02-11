import inspect
from abc import abstractmethod, ABC
from typing import Any, Callable, Generic, Sequence, TypeVar, cast

from gloe.async_transformer import AsyncTransformer
from gloe.functional import _PartialTransformer, _PartialAsyncTransformer
from gloe.transformers import Transformer

T = TypeVar("T")
S = TypeVar("S")
U = TypeVar("U")


class TransformerEnsurer(Generic[T, S], ABC):
    @abstractmethod
    def validate_input(self, data: T):
        """Validate incoming data before executing the transformer."""

    @abstractmethod
    def validate_output(self, data: T, output: S):
        """Validate output data after executing the transformer."""

    def __call__(self, transformer: Transformer[T, S]) -> Transformer[T, S]:
        def transform(this: Transformer, data: T) -> S:
            self.validate_input(data)
            try:
                output = transformer.transform(data)
            except Exception as e:
                raise RuntimeError(f"Error during transformation: {e}")
            self.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp


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
            sig = inspect.signature(func)
            if len(sig.parameters) == 1:
                func(output)
            else:
                func(data, output)

    return LambdaEnsurer()


class _ensure_base:
    def __call__(self, arg):
        if isinstance(arg, Transformer):
            return self._generate_new_transformer(arg)
        if isinstance(arg, AsyncTransformer):
            return self._generate_new_async_transformer(arg)
        if isinstance(arg, _PartialTransformer):
            transformer_init = arg

            def ensured_transformer_init(*args, **kwargs):
                transformer = transformer_init(*args, **kwargs)
                return self._generate_new_transformer(transformer)

            return ensured_transformer_init
        if isinstance(arg, _PartialAsyncTransformer):
            async_transformer_init = arg

            def ensured_async_transformer_init(*args, **kwargs):
                async_transformer = async_transformer_init(*args, **kwargs)
                return self._generate_new_async_transformer(async_transformer)

            return ensured_async_transformer_init

    @abstractmethod
    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        pass

    @abstractmethod
    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        pass


class _ensure_incoming(Generic[T], _ensure_base):
    def __init__(self, incoming: Sequence[Callable[[T], Any]]):
        self.input_ensurers = [input_ensurer(ensurer) for ensurer in incoming]

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data: T) -> S:
            for ensurer in self.input_ensurers:
                ensurer.validate_input(data)
            try:
                output = transformer.transform(data)
            except Exception as e:
                raise RuntimeError(f"Error during transformation: {e}")
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        async def transform_async(_, data: T) -> S:
            for ensurer in self.input_ensurers:
                ensurer.validate_input(data)
            try:
                output = await transformer.transform_async(data)
            except Exception as e:
                raise RuntimeError(f"Error during asynchronous transformation: {e}")
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


class _ensure_outcome(Generic[S], _ensure_base):
    def __init__(self, outcome: Sequence[Callable[[S], Any]]):
        self.output_ensurers = [output_ensurer(ensurer) for ensurer in outcome]

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data: T) -> S:
            try:
                output = transformer.transform(data)
            except Exception as e:
                raise RuntimeError(f"Error during transformation: {e}")
            for ensurer in self.output_ensurers:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        async def transform_async(_, data: T) -> S:
            try:
                output = await transformer.transform_async(data)
            except Exception as e:
                raise RuntimeError(f"Error during asynchronous transformation: {e}")
            for ensurer in self.output_ensurers:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


class _ensure_changes(Generic[T, S], _ensure_base):
    def __init__(self, changes: Sequence[Callable[[T, S], Any]]):
        self.changes_ensurers = [output_ensurer(ensurer) for ensurer in changes]

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data: T) -> S:
            try:
                output = transformer.transform(data)
            except Exception as e:
                raise RuntimeError(f"Error during transformation: {e}")
            for ensurer in self.changes_ensurers:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        async def transform_async(_, data: T) -> S:
            try:
                output = await transformer.transform_async(data)
            except Exception as e:
                raise RuntimeError(f"Error during asynchronous transformation: {e}")
            for ensurer in self.changes_ensurers:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


class _ensure_both(Generic[T, S], _ensure_base):
    def __init__(
        self,
        incoming: Sequence[Callable[[T], Any]],
        outcome: Sequence[Callable[[S], Any]],
        changes: Sequence[Callable[[T, S], Any]],
    ):
        self.input_ensurers = [input_ensurer(ensurer) for ensurer in incoming]
        self.output_ensurers = [output_ensurer(ensurer) for ensurer in outcome]
        self.changes_ensurers = [output_ensurer(ensurer) for ensurer in changes]
        self.output_ensurers.extend(self.changes_ensurers)

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data: T) -> S:
            for ensurer in self.input_ensurers:
                ensurer.validate_input(data)
            try:
                output = transformer.transform(data)
            except Exception as e:
                raise RuntimeError(f"Error during transformation: {e}")
            for ensurer in self.output_ensurers:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        async def transform_async(_, data: T) -> S:
            for ensurer in self.input_ensurers:
                ensurer.validate_input(data)
            try:
                output = await transformer.transform_async(data)
            except Exception as e:
                raise RuntimeError(f"Error during asynchronous transformation: {e}")
            for ensurer in self.output_ensurers:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


def ensure(*args, **kwargs):
    """
    Decorator to add validation layers to transformers based on incoming, outcome, or both data.

    Args:
        incoming (Sequence[Callable[[T], Any]]): Validators for incoming data.
        outcome (Sequence[Callable[[S], Any]]): Validators for outcome data.
        changes (Sequence[Callable[[T, S], Any]]): Validators for both incoming and outcome data.
    """
    if "incoming" in kwargs:
        return _ensure_incoming(kwargs["incoming"])

    if "outcome" in kwargs:
        return _ensure_outcome(kwargs["outcome"])

    if "changes" in kwargs:
        return _ensure_changes(kwargs["changes"])

    if len(kwargs) > 1:
        incoming = kwargs.get("incoming", [])
        outcome = kwargs.get("outcome", [])
        changes = kwargs.get("changes", [])
        return _ensure_both(incoming, outcome, changes)