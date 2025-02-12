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

AsyncNext2 = Union[
    Tuple[BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, Any, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out2, Any]],
]

AsyncNext3 = Union[
    Tuple[BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, Any, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out3, Any]],
]

AsyncNext4 = Union[
    Tuple[BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, Any, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out4, Any]],
]

AsyncNext5 = Union[
    Tuple[BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, Any, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out5, Any]],
]

AsyncNext6 = Union[
    Tuple[BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any], BaseTransformer[_Out, Any, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out6, Any]],
]

AsyncNext7 = Union[
    Tuple[BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any], BaseTransformer[_Out, _Out7, Any], BaseTransformer[_Out, Any, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out2, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any], BaseTransformer[_Out, _Out7, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out3, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any], BaseTransformer[_Out, _Out7, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out4, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any], BaseTransformer[_Out, _Out7, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out5, Any], BaseTransformer[_Out, _Out6, Any], BaseTransformer[_Out, _Out7, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out6, Any], BaseTransformer[_Out, _Out7, Any]],
    Tuple[BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, Any, Any], BaseTransformer[_Out, _Out7, Any]],
]


class AsyncTransformer(BaseTransformer[_In, _Out, "AsyncTransformer"], ABC):
    def __init__(self):
        super().__init__()
        self._validate_init()
        self._graph_node_props: dict[str, Any] = {
            **self._graph_node_props,
            "isAsync": True,
        }
        self.__class__.__annotations__ = self.transform_async.__annotations__

    def _validate_init(self):
        if not isinstance(self.instance_id, uuid.UUID):
            raise ValueError(f"instance_id must be a UUID, got {type(self.instance_id)} instead.")
        if self.previous is not None and not isinstance(self.previous, (BaseTransformer, Tuple)):
            raise ValueError(f"previous must be a BaseTransformer or a tuple of BaseTransformers, got {type(self.previous)} instead.")

    @abstractmethod
    async def transform_async(self, data: _In) -> _Out:
        """\n        Method to perform the transformation asynchronously.\n\n        Args:\n            data: the incoming data passed to the transformer during the pipeline execution.\n\n        Return:\n            The outcome data, it means, the result of the transformation.\n        """
        pass

    def signature(self) -> Signature:
        return self._signature(AsyncTransformer)

    def __repr__(self):
        return f"{self.input_annotation} -> ({type(self).__name__}) -> {self.output_annotation}"

    async def __call__(self, data: _In) -> _Out:
        try:
            transformed = await self.transform_async(data)
            if not isinstance(transformed, self.output_type):
                raise TypeError(f"Expected output type {self.output_type}, got {type(transformed)} instead.")
            return cast(_Out, transformed)
        except TransformerException as e:
            raise e.internal_exception from e
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            transformer_frames = [
                frame for frame in tb
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
        copied = copy.copy(self)
        self._validate_copy(copied, transform, regenerate_instance_id)
        func_type = types.MethodType
        if transform is not None:
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

        copied._children = [
            child.copy(regenerate_instance_id=True) for child in self.children
        ]

        return copied

    def _validate_copy(self, copied, transform, regenerate_instance_id):
        if transform is not None and not callable(transform):
            raise ValueError(f"transform must be callable, got {type(transform)} instead.")
        if not isinstance(regenerate_instance_id, bool):
            raise ValueError(f"regenerate_instance_id must be a boolean, got {type(regenerate_instance_id)} instead.")

    @overload
    def __rshift__(
        self, next_node: BaseTransformer[_Out, _NextOut, Any]
    ) -> "AsyncTransformer[_In, _NextOut]":
        pass

    @overload
    def __rshift__(
        self, next_node: AsyncNext2[_Out, _NextOut, _Out2]
    ) -> "AsyncTransformer[_In, tuple[_NextOut, _Out2]]":
        pass

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
        ],
    ) -> "AsyncTransformer[_In, tuple[_NextOut, _Out2, _Out3]]":
        pass

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
            BaseTransformer[_Out, _Out4, Any],
        ],
    ) -> "AsyncTransformer[_In, tuple[_NextOut, _Out2, _Out3, _Out4]]":
        pass

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
            BaseTransformer[_Out, _Out4, Any],
            BaseTransformer[_Out, _Out5, Any],
        ],
    ) -> "AsyncTransformer[_In, tuple[_NextOut, _Out2, _Out3, _Out4, _Out5]]":
        pass

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
            BaseTransformer[_Out, _Out4, Any],
            BaseTransformer[_Out, _Out5, Any],
            BaseTransformer[_Out, _Out6, Any],
        ],
    ) -> "AsyncTransformer[_In, tuple[_NextOut, _Out2, _Out3, _Out4, _Out5, _Out6]]":
        pass

    @overload
    def __rshift__(
        self, next_node: Tuple[
            BaseTransformer[_Out, _NextOut, Any],
            BaseTransformer[_Out, _Out2, Any],
            BaseTransformer[_Out, _Out3, Any],
            BaseTransformer[_Out, _Out4, Any],
            BaseTransformer[_Out, _Out5, Any],
            BaseTransformer[_Out, _Out6, Any],
            BaseTransformer[_Out, _Out7, Any],
        ],
    ) -> "AsyncTransformer[_In, tuple[_NextOut, _Out2, _Out3, _Out4, _Out5, _Out6, _Out7]]":
        pass

    def __rshift__(self, next_node):
        if isinstance(next_node, BaseTransformer):
            self._validate_next_node(next_node)
            return _compose_nodes(self, next_node)
        elif isinstance(next_node, tuple) and all(isinstance(n, BaseTransformer) for n in next_node):
            self._validate_next_node(*next_node)
            return _compose_nodes(self, next_node)
        else:
            raise UnsupportedTransformerArgException(next_node)

    def _validate_next_node(self, *nodes):
        for node in nodes:
            if not isinstance(node, BaseTransformer):
                raise TypeError(f"Expected BaseTransformer, got {type(node)} instead.")