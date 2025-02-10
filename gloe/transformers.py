import traceback
from abc import ABC, abstractmethod
from inspect import Signature
from typing import (
    TypeVar,
    overload,
    cast,
    Any,
    TypeAlias,
    Union,
    get_origin,
    get_args,
)

from gloe.base_transformer import BaseTransformer, TransformerException
from gloe.async_transformer import AsyncTransformer

__all__ = ["Transformer"]

I = TypeVar("I")
O = TypeVar("O")
O1 = TypeVar("O1")
O2 = TypeVar("O2")
O3 = TypeVar("O3")
O4 = TypeVar("O4")
O5 = TypeVar("O5")
O6 = TypeVar("O6")
O7 = TypeVar("O7")

Tr: TypeAlias = "Transformer"
AT: TypeAlias = AsyncTransformer
BT: TypeAlias = BaseTransformer[I, O, Any]

AsyncNext2 = Union[
    tuple[AT[O, O1], BT[O, O2]],
    tuple[BT[O, O1], AT[O, O2]],
]

AsyncNext3 = Union[
    tuple[AT[O, O1], BT[O, O2], BT[O, O3]],
    tuple[BT[O, O1], AT[O, O2], BT[O, O3]],
    tuple[BT[O, O1], BT[O, O2], AT[O, O3]],
]

AsyncNext4 = Union[
    tuple[AT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4]],
    tuple[BT[O, O1], AT[O, O2], BT[O, O3], BT[O, O4]],
    tuple[BT[O, O1], BT[O, O2], AT[O, O3], BT[O, O4]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], AT[O, O4]],
]

AsyncNext5 = Union[
    tuple[AT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5]],
    tuple[BT[O, O1], AT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5]],
    tuple[BT[O, O1], BT[O, O2], AT[O, O3], BT[O, O4], BT[O, O5]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], AT[O, O4], BT[O, O5]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], AT[O, O5]],
]

AsyncNext6 = Union[
    tuple[AT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5], BT[O, O6]],
    tuple[BT[O, O1], AT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5], BT[O, O6]],
    tuple[BT[O, O1], BT[O, O2], AT[O, O3], BT[O, O4], BT[O, O5], BT[O, O6]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], AT[O, O4], BT[O, O5], BT[O, O6]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], AT[O, O5], BT[O, O6]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5], AT[O, O6]],
]

AsyncNext7 = Union[
    tuple[AT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5], BT[O, O6], BT[O, O7]],
    tuple[BT[O, O1], AT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5], BT[O, O6], BT[O, O7]],
    tuple[BT[O, O1], BT[O, O2], AT[O, O3], BT[O, O4], BT[O, O5], BT[O, O6], BT[O, O7]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], AT[O, O4], BT[O, O5], BT[O, O6], BT[O, O7]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], AT[O, O5], BT[O, O6], BT[O, O7]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5], AT[O, O6], BT[O, O7]],
    tuple[BT[O, O1], BT[O, O2], BT[O, O3], BT[O, O4], BT[O, O5], BT[O, O6], AT[O, O7]],
]


class Transformer(BaseTransformer[I, O, "Transformer"], ABC):
    """
    A Transformer is the generic block with the responsibility to take an input of type
    `T` and transform it to an output of type `S`.
    """

    def __init__(self):
        super().__init__()
        self.__class__.__annotations__ = self.transform.__annotations__

    @abstractmethod
    def transform(self, data: I) -> O:
        """Main method to be implemented and responsible to perform the transformer logic"""

    def signature(self) -> Signature:
        return self._signature(Transformer)

    def __repr__(self):
        return f"{self.input_annotation} -> ({type(self).__name__}) -> {self.output_annotation}"

    def __call__(self, data: I) -> O:
        # Validate input type
        if not self._is_instance_of(data, self.input_type):
            raise TypeError(f"Expected input of type {self.input_type}, got {type(data)}")

        transform_exception = None

        transformed: O | None = None
        try:
            transformed = self.transform(data)
        except TransformerException as te:
            transform_exception = te
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)

            transformer_frames = [
                frame
                for frame in tb
                if frame.name == self.__class__.__name__ or frame.name == "transform"
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

            transform_exception = TransformerException(
                internal_exception=e,
                raiser_transformer=self,
                message=exception_message,
            )

        if transform_exception is not None:
            raise transform_exception.internal_exception

        if type(transformed) is not None:
            return cast(O, transformed)

        raise NotImplementedError("Transform method did not return a value")  # pragma: no cover

    def _is_instance_of(self, value: Any, type_: Any) -> bool:
        if type_ is Any:
            return True
        origin = get_origin(type_)
        if origin is None:
            return isinstance(value, type_)
        elif origin is Union:
            return any(self._is_instance_of(value, arg) for arg in get_args(type_))
        elif origin is list or origin is tuple:
            arg_type = get_args(type_)[0]
            return isinstance(value, origin) and all(self._is_instance_of(item, arg_type) for item in value)
        elif origin is dict:
            key_type, value_type = get_args(type_)
            return isinstance(value, origin) and all(
                self._is_instance_of(k, key_type) and self._is_instance_of(v, value_type)
                for k, v in value.items()
            )
        return False

    @overload
    def __rshift__(
        self,
        next_node: tuple["Tr[O, O1]", "Tr[O, O2]"],
    ) -> "Tr[I, tuple[O1, O2]]":
        pass

    @overload
    def __rshift__(
        self,
        next_node: tuple["Tr[O, O1]", "Tr[O, O2]", "Tr[O, O3]"],
    ) -> "Transformer[I, tuple[O1, O2, O3]]":
        pass

    @overload
    def __rshift__(
        self,
        next_node: tuple["Tr[O, O1]", "Tr[O, O2]", "Tr[O, O3]", "Tr[O, O4]"],
    ) -> "Tr[I, tuple[O1, O2, O3, O4]]":
        pass

    @overload
    def __rshift__(
        self,
        next_node: tuple["Tr[O, O1]", "Tr[O, O2]", "Tr[O, O3]", "Tr[O, O4]", "Tr[O, O5]"],
    ) -> "Tr[I, tuple[O1, O2, O3, O4, O5]]":
        pass

    @overload
    def __rshift__(
        self,
        next_node: tuple[
            "Tr[O, O1]", "Tr[O, O2]", "Tr[O, O3]", "Tr[O, O4]", "Tr[O, O5]", "Tr[O, O6]"
        ],
    ) -> "Tr[I, tuple[O1, O2, O3, O4, O5, O6]]":
        pass

    @overload
    def __rshift__(
        self,
        next_node: tuple[
            "Tr[O, O1]",
            "Tr[O, O2]",
            "Tr[O, O3]",
            "Tr[O, O4]",
            "Tr[O, O5]",
            "Tr[O, O6]",
            "Tr[O, O7]",
        ],
    ) -> "Tr[I, tuple[O1, O2, O3, O4, O5, O6, O7]]":
        pass

    @overload
    def __rshift__(self, next_node: "Tr[O, O1]") -> "Tr[I, O1]":
        pass

    @overload
    def __rshift__(self, next_node: AsyncTransformer[O, O1]) -> AsyncTransformer[I, O1]:
        pass

    @overload
    def __rshift__(
        self,
        next_node: AsyncNext2[O, O1, O2],
    ) -> AsyncTransformer[I, tuple[O1, O2]]:
        pass

    @overload
    def __rshift__(
        self,
        next_node: AsyncNext3[O, O1, O2, O3],
    ) -> AsyncTransformer[I, tuple[O1, O2, O3]]:
        pass

    @overload
    def __rshift__(
        self,
        next_node: AsyncNext4[O, O1, O2, O3, O4],
    ) -> AsyncTransformer[I, tuple[O1, O2, O3, O4]]:
        pass

    @overload
    def __rshift__(
        self,
        next_node: AsyncNext5[O, O1, O2, O3, O4, O5],
    ) -> AsyncTransformer[I, tuple[O1, O2, O3, O4, O5]]:
        pass

    @overload
    def __rshift__(
        self,
        next_node: AsyncNext6[O, O1, O2, O3, O4, O5, O6],
    ) -> AsyncTransformer[I, tuple[O1, O2, O3, O4, O5, O6]]:
        pass

    @overload
    def __rshift__(
        self,
        next_node: AsyncNext7[O, O1, O2, O3, O4, O5, O6, O7],
    ) -> AsyncTransformer[I, tuple[O1, O2, O3, O4, O5, O6, O7]]:
        pass

    def __rshift__(self, next_node):
        pass  # Placeholder as per the gold code