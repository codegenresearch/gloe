import copy
import traceback
import types
import uuid
from abc import abstractmethod, ABC
from inspect import Signature
from typing import TypeVar, overload, cast, Any, Callable, Awaitable, Tuple

from gloe.base_transformer import (
    TransformerException,
    BaseTransformer,
    PreviousTransformer,
)
from gloe._composition_utils import _compose_nodes, UnsupportedTransformerArgException

__all__ = ["AsyncTransformer"]

_In = TypeVar("_In")
_Out = TypeVar("_Out")
_NextOut = TypeVar("_NextOut")
_Out2 = TypeVar("_Out2")
_Out3 = TypeVar("_Out3")
_Out4 = TypeVar("_Out4")
_Out5 = TypeVar("_Out5")
_Out6 = TypeVar("_Out6")
_Out7 = TypeVar("_Out7")


class AsyncTransformer(BaseTransformer[_In, _Out, "AsyncTransformer"], ABC):
    def __init__(self):
        super().__init__()
        self._graph_node_props: dict[str, Any] = {
            **self._graph_node_props,
            "isAsync": True,
        }
        self.__class__.__annotations__ = self.transform_async.__annotations__

    @abstractmethod
    async def transform_async(self, data: _In) -> _Out:
        """
        Method to perform the transformation asynchronously.

        Args:
            data: the incoming data passed to the transformer during the pipeline execution.

        Return:
            The outcome data, it means, the result of the transformation.
        """
        pass

    def signature(self) -> Signature:
        return self._signature(AsyncTransformer)

    def __repr__(self):
        return f"{self.input_annotation} -> ({type(self).__name__}) -> {self.output_annotation}"

    async def __call__(self, data: _In) -> _Out:
        try:
            transformed = await self.transform_async(data)
            if transformed is None:
                raise ValueError(f"Transformation result is None for {self.__class__.__name__}")
            if not isinstance(transformed, self.output_type):
                raise ValueError(f"Transformation result is not of expected type for {self.__class__.__name__}")
            return cast(_Out, transformed)
        except TransformerException as te:
            raise te.internal_exception from te
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            transformer_frames = [
                frame
                for frame in tb
                if frame.name == self.__class__.__name__ or frame.name == "transform_async"
            ]
            if len(transformer_frames) == 1:
                transformer_frame = transformer_frames[0]
                exception_message = (
                    f"\n  "
                    f'File "{transformer_frame.filename}", line {transformer_frame.lineno}, '
                    f'in transformer "{self.__class__.__name__}"\n  '
                    f"  >> {transformer_frame.line}"
                )
            else:
                exception_message = (
                    f'An error occurred in transformer "{self.__class__.__name__}"'
                )
            raise TransformerException(
                internal_exception=e,
                raiser_transformer=self,
                message=exception_message,
            ) from e

    def copy(
        self,
        transform: Callable[[BaseTransformer, _In], Awaitable[_Out]] | None = None,
        regenerate_instance_id: bool = False,
    ) -> "AsyncTransformer[_In, _Out]":
        if not isinstance(self, AsyncTransformer):
            raise TypeError("Can only copy instances of AsyncTransformer")
        copied = copy.copy(self)

        func_type = types.MethodType
        if transform is not None:
            if not callable(transform):
                raise ValueError("Transform must be a callable")
            setattr(copied, "transform_async", func_type(transform, copied))

        if regenerate_instance_id:
            copied.instance_id = uuid.uuid4()

        if self.previous is not None:
            if isinstance(self.previous, tuple):
                new_previous: list[BaseTransformer] = [
                    previous_transformer.copy() for previous_transformer in self.previous
                ]
                copied._previous = cast(PreviousTransformer, tuple(new_previous))
            elif isinstance(self.previous, BaseTransformer):
                copied._previous = self.previous.copy()
            else:
                raise TypeError("Previous must be either a BaseTransformer or a tuple of BaseTransformers")

        copied._children = [
            child.copy(regenerate_instance_id=True) for child in self.children
        ]

        return copied

    @overload
    def __rshift__(
        self, next_node: BaseTransformer[_Out, _NextOut, Any]
    ) -> "AsyncTransformer[_In, _NextOut]":
        ...

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any]
        ]
    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2]]":
        ...

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any]
        ]
    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3]]":
        ...

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
            BaseTransformer[_Out, _Out4, Any]
        ]
    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3, _Out4]]":
        ...

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
            BaseTransformer[_Out, _Out4, Any],
            BaseTransformer[_Out, _Out5, Any]
        ]
    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3, _Out4, _Out5]]":
        ...

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
            BaseTransformer[_Out, _Out4, Any],
            BaseTransformer[_Out, _Out5, Any],
            BaseTransformer[_Out, _Out6, Any]
        ]
    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3, _Out4, _Out5, _Out6]]":
        ...

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
            BaseTransformer[_Out, _Out4, Any],
            BaseTransformer[_Out, _Out5, Any],
            BaseTransformer[_Out, _Out6, Any],
            BaseTransformer[_Out, _Out7, Any]
        ]
    ) -> "AsyncTransformer[_In, Tuple[_NextOut, _Out2, _Out3, _Out4, _Out5, _Out6, _Out7]]":
        ...

    def __rshift__(self, next_node):
        return _compose_nodes(self, next_node)


This code addresses the feedback by ensuring all string literals and comments are properly terminated. Specifically, it checks for any missing quotation marks that would complete the string and ensures that any comments or documentation strings are also properly formatted to avoid similar issues in the future.