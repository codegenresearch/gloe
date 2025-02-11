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
    Tuple,
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

AsyncNext = Union[
    AT[O, O1],
    BT[O, O1],
    Tuple[Union[AT[O, O1], BT[O, O1]], Union[AT[O, O2], BT[O, O2]]],
    Tuple[Union[AT[O, O1], BT[O, O1]], Union[AT[O, O2], BT[O, O2]], Union[AT[O, O3], BT[O, O3]]],
    Tuple[Union[AT[O, O1], BT[O, O1]], Union[AT[O, O2], BT[O, O2]], Union[AT[O, O3], BT[O, O3]], Union[AT[O, O4], BT[O, O4]]],
    Tuple[Union[AT[O, O1], BT[O, O1]], Union[AT[O, O2], BT[O, O2]], Union[AT[O, O3], BT[O, O3]], Union[AT[O, O4], BT[O, O4]], Union[AT[O, O5], BT[O, O5]]],
    Tuple[Union[AT[O, O1], BT[O, O1]], Union[AT[O, O2], BT[O, O2]], Union[AT[O, O3], BT[O, O3]], Union[AT[O, O4], BT[O, O4]], Union[AT[O, O5], BT[O, O5]], Union[AT[O, O6], BT[O, O6]]],
    Tuple[Union[AT[O, O1], BT[O, O1]], Union[AT[O, O2], BT[O, O2]], Union[AT[O, O3], BT[O, O3]], Union[AT[O, O4], BT[O, O4]], Union[AT[O, O5], BT[O, O5]], Union[AT[O, O6], BT[O, O6]], Union[AT[O, O7], BT[O, O7]]],
]


class Transformer(BaseTransformer[I, O, "Transformer"], ABC):
    def __init__(self):
        super().__init__()
        self.__class__.__annotations__ = self.transform.__annotations__

    @abstractmethod
    def transform(self, data: I) -> O:
        """Main method to be implemented and responsible to perform the transformer logic"""

    def signature(self) -> Signature:
        return self._signature(Transformer)

    def __repr__(self) -> str:
        return f"{self.input_annotation} -> ({type(self).__name__}) -> {self.output_annotation}"

    def __call__(self, data: I) -> O:
        try:
            return self.transform(data)
        except TransformerException as e:
            raise e.internal_exception
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
            raise TransformerException(
                internal_exception=e,
                raiser_transformer=self,
                message=exception_message,
            )

    @overload
    def __rshift__(self, next_node: "Tr[O, O1]") -> "Tr[I, O1]":
        ...

    @overload
    def __rshift__(self, next_node: AT[O, O1]) -> AT[I, O1]:
        ...

    @overload
    def __rshift__(self, next_node: Tuple[Union[AT[O, O1], BT[O, O1]], ...]) -> AT[I, Tuple[Any, ...]]:
        ...

    def __rshift__(self, next_node: AsyncNext) -> Union["Tr[I, O1]", AT[I, O1], AT[I, Tuple[Any, ...]]]:
        pass