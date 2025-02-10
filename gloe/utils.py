from typing import Any, Tuple, TypeVar, Generic, Type
from gloe.functional import transformer
from gloe.transformers import Transformer

__all__ = ["forget", "debug", "forward", "forward_incoming"]

_In = TypeVar("_In")
_Out = TypeVar("_Out")

class InvalidInputTypeError(Exception):
    """Exception raised for errors in the input type."""
    def __init__(self, message: str):
        super().__init__(message)

@transformer
def forget(data: Any) -> None:
    """Transform any input data to `None`"""
    if not isinstance(data, (int, float, str, list, dict, tuple, set, bool, type(None))):
        raise InvalidInputTypeError(f"Unsupported input type: {type(data)}")
    return None

@transformer
def debug(incoming: _In) -> _In:
    """Breakpoint for debugging purposes"""
    if not isinstance(incoming, (int, float, str, list, dict, tuple, set, bool, type(None))):
        raise InvalidInputTypeError(f"Unsupported input type: {type(incoming)}")
    breakpoint()
    return incoming

class forward(Generic[_In], Transformer[_In, _In]):
    def __init__(self):
        super().__init__()
        self._invisible = True

    def __repr__(self):
        """Return a string representation of the transformer chain."""
        if self.previous is not None:
            return str(self.previous)
        return super().__repr__()

    def transform(self, data: _In) -> _In:
        """Return the input data unchanged."""
        if not isinstance(data, (int, float, str, list, dict, tuple, set, bool, type(None))):
            raise InvalidInputTypeError(f"Unsupported input type: {type(data)}")
        return data

def forward_incoming(
    inner_transformer: Transformer[_In, _Out]
) -> Transformer[_In, Tuple[_Out, _In]]:
    """Create a transformer that applies the inner_transformer and returns the result along with the original input."""
    if not isinstance(inner_transformer, Transformer):
        raise InvalidInputTypeError(f"Expected a Transformer, got {type(inner_transformer)}")
    return forward[_In]() >> (inner_transformer, forward())