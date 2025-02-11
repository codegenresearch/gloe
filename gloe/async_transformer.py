import copy
import traceback
import types
import uuid
from abc import abstractmethod, ABC
from inspect import Signature
from typing import TypeVar, overload, cast, Any, Callable, Awaitable, Union, Tuple

from gloe.base_transformer import (
    TransformerException,
    BaseTransformer,
    PreviousTransformer,
)

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
_Out8 = TypeVar("_Out8")
_Out9 = TypeVar("_Out9")
_Out10 = TypeVar("_Out10")

class AsyncTransformerException(TransformerException):
    """Exception specific to AsyncTransformer errors."""
    pass

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

        Returns:
            The outcome data, it means, the result of the transformation.
        """
        pass

    def signature(self) -> Signature:
        return self._signature(AsyncTransformer)

    def __repr__(self):
        return f"{self.input_annotation} -> ({type(self).__name__}) -> {self.output_annotation}"

    async def __call__(self, data: _In) -> _Out:
        try:
            return await self.transform_async(data)
        except TransformerException as e:
            raise e.internal_exception
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
            raise AsyncTransformerException(
                internal_exception=e,
                raiser_transformer=self,
                message=exception_message,
            )

    def copy(
        self,
        transform: Callable[[BaseTransformer, _In], Awaitable[_Out]] | None = None,
        regenerate_instance_id: bool = False,
    ) -> "AsyncTransformer[_In, _Out]":
        copied = copy.copy(self)
        if transform is not None:
            copied.transform_async = types.MethodType(transform, copied)
        if regenerate_instance_id:
            copied.instance_id = uuid.uuid4()
        if self.previous is not None:
            if isinstance(self.previous, tuple):
                copied._previous = tuple(prev.copy() for prev in self.previous)
            else:
                copied._previous = self.previous.copy()
        copied._children = [child.copy(regenerate_instance_id=True) for child in self.children]
        return copied

    @overload
    def __rshift__(
        self, next_node: BaseTransformer[_Out, _NextOut, Any]
    ) -> "AsyncTransformer[_In, _NextOut]":
        pass

    @overload
    def __rshift__(
        self, next_node: Tuple[BaseTransformer[_Out, _NextOut, Any], ...]
    ) -> "AsyncTransformer[_In, Tuple[_NextOut, ...]]":
        pass

    def __rshift__(self, next_node):
        if isinstance(next_node, BaseTransformer):
            return _compose_nodes(self, next_node)
        elif isinstance(next_node, tuple) and all(isinstance(n, BaseTransformer) for n in next_node):
            return _compose_nodes(self, next_node)
        else:
            raise UnsupportedTransformerArgException(next_node)