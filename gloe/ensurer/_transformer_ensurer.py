import inspect
from abc import abstractmethod, ABC
from types import FunctionType
from typing import Any, Callable, Generic, ParamSpec, Sequence, TypeVar, cast, overload

from gloe.async_transformer import AsyncTransformer
from gloe.functional import _PartialTransformer, _PartialAsyncTransformer
from gloe.transformers import Transformer

T = TypeVar("T")
S = TypeVar("S")
U = TypeVar("U")
P = ParamSpec("P")


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
                raise RuntimeError(f"Error during transformation: {e}") from e
            self.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp


def input_ensurer(func: Callable[[T], Any]) -> TransformerEnsurer[T, Any]:
    class LambdaEnsurer(TransformerEnsurer[T, S]):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def validate_input(self, data: T):
            try:
                func(data)
            except Exception as e:
                raise ValueError(f"Input validation failed: {e}") from e

        def validate_output(self, data: T, output: S):
            pass

    return LambdaEnsurer()


@overload
def output_ensurer(func: Callable[[T, S], Any]) -> TransformerEnsurer[T, S]:
    ...


@overload
def output_ensurer(func: Callable[[S], Any]) -> TransformerEnsurer[Any, S]:
    ...


def output_ensurer(func: Callable) -> TransformerEnsurer:
    class LambdaEnsurer(TransformerEnsurer):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def validate_input(self, data):
            pass

        def validate_output(self, data, output):
            try:
                if len(inspect.signature(func).parameters) == 1:
                    func(output)
                else:
                    func(data, output)
            except Exception as e:
                raise ValueError(f"Output validation failed: {e}") from e

    return LambdaEnsurer()


class _ensure_base:
    @overload
    def __call__(self, transformer: Transformer[U, S]) -> Transformer[U, S]:
        ...

    @overload
    def __call__(
        self, transformer_init: _PartialTransformer[T, P, U]
    ) -> _PartialTransformer[T, P, U]:
        ...

    @overload
    def __call__(self, transformer: AsyncTransformer[U, S]) -> AsyncTransformer[U, S]:
        ...

    @overload
    def __call__(
        self, transformer_init: _PartialAsyncTransformer[T, P, U]
    ) -> _PartialAsyncTransformer[T, P, U]:
        ...

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
        self.input_ensurers_instances = [input_ensurer(ensurer) for ensurer in incoming]

    def _generate_new_transformer(self, transformer: Transformer) -> Transformer:
        def transform(_, data):
            for ensurer in self.input_ensurers_instances:
                ensurer.validate_input(data)
            output = transformer.transform(data)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp

    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        async def transform_async(_, data):
            for ensurer in self.input_ensurers_instances:
                ensurer.validate_input(data)
            output = await transformer.transform_async(data)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


class _ensure_outcome(Generic[S], _ensure_base):
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

    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        async def transform_async(_, data):
            output = await transformer.transform_async(data)
            for ensurer in self.output_ensurers_instances:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


class _ensure_changes(Generic[T, S], _ensure_base):
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

    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        async def transform_async(_, data):
            output = await transformer.transform_async(data)
            for ensurer in self.changes_ensurers_instances:
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
        self.input_ensurers_instances = [input_ensurer(ensurer) for ensurer in incoming]
        self.output_ensurers_instances = [output_ensurer(ensurer) for ensurer in outcome]
        self.output_ensurers_instances.extend(
            output_ensurer(ensurer) for ensurer in changes
        )

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

    def _generate_new_async_transformer(
        self, transformer: AsyncTransformer
    ) -> AsyncTransformer:
        async def transform_async(_, data):
            for ensurer in self.input_ensurers_instances:
                ensurer.validate_input(data)
            output = await transformer.transform_async(data)
            for ensurer in self.output_ensurers_instances:
                ensurer.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform_async)
        return transformer_cp


@overload
def ensure(incoming: Sequence[Callable[[T], Any]]) -> _ensure_incoming[T]:
    ...


@overload
def ensure(outcome: Sequence[Callable[[S], Any]]) -> _ensure_outcome[S]:
    ...


@overload
def ensure(changes: Sequence[Callable[[T, S], Any]]) -> _ensure_changes[T, S]:
    ...


@overload
def ensure(
    incoming: Sequence[Callable[[T], Any]], outcome: Sequence[Callable[[S], Any]]
) -> _ensure_both[T, S]:
    ...


@overload
def ensure(
    incoming: Sequence[Callable[[T], Any]], changes: Sequence[Callable[[T, S], Any]]
) -> _ensure_both[T, S]:
    ...


@overload
def ensure(
    outcome: Sequence[Callable[[T], Any]], changes: Sequence[Callable[[T, S], Any]]
) -> _ensure_both[T, S]:
    ...


@overload
def ensure(
    incoming: Sequence[Callable[[T], Any]],
    outcome: Sequence[Callable[[S], Any]],
    changes: Sequence[Callable[[T, S], Any]],
) -> _ensure_both[T, S]:
    ...


def ensure(*args, **kwargs):
    """
    Decorator to add validation layers to transformers.

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