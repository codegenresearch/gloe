from typing import Any, Tuple, TypeVar, Generic
from gloe.functional import transformer
from gloe.transformers import Transformer

__all__ = ["forget", "debug", "forward", "forward_incoming"]

_In = TypeVar("_In")
_Out = TypeVar("_Out")


@transformer
def forget(data: Any) -> None:
    """
    Transform any input data to `None`.

    Parameters:
    data (Any): The input data to be transformed.

    Returns:
    None: Always returns None.
    """
    return None


@transformer
def debug(incoming: _In) -> _In:
    """
    Insert a breakpoint for debugging purposes and return the incoming data.

    Parameters:
    incoming (_In): The data to be debugged.

    Returns:
    _In: The same data that was passed in.
    """
    breakpoint()
    return incoming


class forward(Generic[_In], Transformer[_In, _In]):
    def __init__(self):
        """
        Initialize the forward transformer.

        This transformer simply passes the input data through without modification.
        """
        super().__init__()
        self._invisible = True

    def __repr__(self) -> str:
        """
        Return a string representation of the transformer.

        If the transformer is part of a chain, it returns the string representation of the previous transformer.
        Otherwise, it returns the default string representation.

        Returns:
        str: The string representation of the transformer.
        """
        if self.previous is not None:
            return str(self.previous)

        return super().__repr__()

    def transform(self, data: _In) -> _In:
        """
        Transform the input data by simply returning it.

        Parameters:
        data (_In): The input data to be transformed.

        Returns:
        _In: The same data that was passed in.
        """
        if data is None:
            raise ValueError("Input data cannot be None")
        return data


def forward_incoming(
    inner_transformer: Transformer[_In, _Out]
) -> Transformer[_In, Tuple[_Out, _In]]:
    """
    Create a transformer that applies an inner transformer to the input data and returns a tuple of the result and the original input.

    Parameters:
    inner_transformer (Transformer[_In, _Out]): The transformer to apply to the input data.

    Returns:
    Transformer[_In, Tuple[_Out, _In]]: A transformer that returns a tuple of the inner transformer's result and the original input.
    """
    if inner_transformer is None:
        raise ValueError("Inner transformer cannot be None")
    return forward[_In]() >> (inner_transformer, forward())