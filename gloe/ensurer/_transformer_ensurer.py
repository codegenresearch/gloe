import inspect
from abc import abstractmethod, ABC
from types import FunctionType
from typing import Any, Callable, Generic, ParamSpec, Sequence, TypeVar, cast, overload

from gloe.async_transformer import AsyncTransformer
from gloe.functional import _PartialTransformer, _PartialAsyncTransformer
from gloe.transformers import Transformer

_T = TypeVar("_T")
_S = TypeVar("_S")
_U = TypeVar("_U")
_P1 = ParamSpec("_P1")


class TransformerEnsurer(Generic[_T, _S], ABC):
    @abstractmethod
    def validate_input(self, data: _T):
        """Validate incoming data before executing the transformer."""

    @abstractmethod
    def validate_output(self, data: _T, output: _S):
        """Validate output data after executing the transformer."""

    def __call__(self, transformer: Transformer[_T, _S]) -> Transformer[_T, _S]:
        def transform(this: Transformer, data: _T) -> _S:
            self.validate_input(data)
            output = transformer.transform(data)
            self.validate_output(data, output)
            return output

        transformer_cp = transformer.copy(transform)
        return transformer_cp


def input_ensurer(func: Callable[[_T], Any]) -> TransformerEnsurer[_T, Any]:
    class LambdaEnsurer(TransformerEnsurer[_T, _S]):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def validate_input(self, data: _T):
            func(data)

        def validate_output(self, data: _T, output: _S):
            pass

    return LambdaEnsurer()


@overload
def output_ensurer(func: Callable[[_T, _S], Any]) -> TransformerEnsurer[_T, _S]:
    pass


@overload
def output_ensurer(func: Callable[[_S], Any]) -> TransformerEnsurer[Any, _S]:
    pass


def output_ensurer(func: Callable) -> TransformerEnsurer:
    class LambdaEnsurer(TransformerEnsurer):
        __doc__ = func.__doc__
        __annotations__ = cast(FunctionType, func).__annotations__

        def validate_input(self, data):
            pass

        def validate_output(self, data, output):
            if len(inspect.signature(func).parameters) == 1:
                func(output)
            else:
                func(data, output)

    return LambdaEnsurer()


class _ensure_base:
    @overload
    def __call__(self, transformer: Transformer[_U, _S]) -> Transformer[_U, _S]:
        pass

    @overload
    def __call__(
        self, transformer_init: _PartialTransformer[_T, _P1, _U]
    ) -> _PartialTransformer[_T, _P1, _U]:
        pass

    @overload
    def __call__(self, transformer: AsyncTransformer[_U, _S]) -> AsyncTransformer[_U, _S]:
        pass

    @overload
    def __call__(
        self, transformer_init: _PartialAsyncTransformer[_T, _P1, _U]
    ) -> _PartialAsyncTransformer[_T, _P1, _U]:
        pass

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


class _ensure_incoming(Generic[_T], _ensure_base):
    def __init__(self, incoming: Sequence[Callable[[_T], Any]]):
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


class _ensure_outcome(Generic[_S], _ensure_base):
    def __init__(self, outcome: Sequence[Callable[[_S], Any]]):
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


class _ensure_changes(Generic[_T, _S], _ensure_base):
    def __init__(self, changes: Sequence[Callable[[_T, _S], Any]]):
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


class _ensure_both(Generic[_T, _S], _ensure_base):
    def __init__(
        self,
        incoming: Sequence[Callable[[_T], Any]],
        outcome: Sequence[Callable[[_S], Any]],
        changes: Sequence[Callable[[_T, _S], Any]],
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
def ensure(incoming: Sequence[Callable[[_T], Any]]) -> _ensure_incoming[_T]:
    pass


@overload
def ensure(outcome: Sequence[Callable[[_S], Any]]) -> _ensure_outcome[_S]:
    pass


@overload
def ensure(changes: Sequence[Callable[[_T, _S], Any]]) -> _ensure_changes[_T, _S]:
    pass


@overload
def ensure(
    incoming: Sequence[Callable[[_T], Any]], outcome: Sequence[Callable[[_S], Any]]
) -> _ensure_both[_T, _S]:
    pass


@overload
def ensure(
    incoming: Sequence[Callable[[_T], Any]], changes: Sequence[Callable[[_T, _S], Any]]
) -> _ensure_both[_T, _S]:
    pass


@overload
def ensure(
    outcome: Sequence[Callable[[_T], Any]], changes: Sequence[Callable[[_T, _S], Any]]
) -> _ensure_both[_T, _S]:
    pass


@overload
def ensure(
    incoming: Sequence[Callable[[_T], Any]],
    outcome: Sequence[Callable[[_S], Any]],
    changes: Sequence[Callable[[_T, _S], Any]],
) -> _ensure_both[_T, _S]:
    pass


def ensure(*args, **kwargs):
    """
    Decorator to add validation layers to transformers.

    Args:
        incoming (Sequence[Callable[[_T], Any]]): Validators for incoming data.
        outcome (Sequence[Callable[[_S], Any]]): Validators for outcome data.
        changes (Sequence[Callable[[_T, _S], Any]]): Validators for both incoming and outcome data.
    """
    if "incoming" in kwargs and "outcome" in kwargs and "changes" in kwargs:
        return _ensure_both(
            kwargs["incoming"], kwargs["outcome"], kwargs["changes"]
        )
    if "incoming" in kwargs and "outcome" in kwargs:
        return _ensure_both(kwargs["incoming"], kwargs["outcome"], [])
    if "incoming" in kwargs and "changes" in kwargs:
        return _ensure_both(kwargs["incoming"], [], kwargs["changes"])
    if "outcome" in kwargs and "changes" in kwargs:
        return _ensure_both([], kwargs["outcome"], kwargs["changes"])
    if "incoming" in kwargs:
        return _ensure_incoming(kwargs["incoming"])
    if "outcome" in kwargs:
        return _ensure_outcome(kwargs["outcome"])
    if "changes" in kwargs:
        return _ensure_changes(kwargs["changes"])
    return _ensure_both([], [], [])