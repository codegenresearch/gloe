from typing import Any, Tuple, TypeVar, Generic, Optional
from gloe.functional import transformer
from gloe.transformers import Transformer

__all__ = ["forget", "debug", "forward", "forward_incoming"]

_In = TypeVar("_In")
_Out = TypeVar("_Out")

@transformer
def forget(data: Any) -> None:
    """\n    Transform any input data to `None`.\n    \n    Args:\n        data (Any): The input data to be transformed.\n        \n    Returns:\n        None\n    """
    return None


@transformer
def debug(incoming: _In) -> _In:
    """\n    Insert a breakpoint for debugging purposes and return the input data.\n    \n    Args:\n        incoming (_In): The input data to be debugged.\n        \n    Returns:\n        _In: The input data unchanged.\n    """
    breakpoint()
    return incoming


class forward(Generic[_In], Transformer[_In, _In]):
    def __init__(self):
        """\n        Initialize the forward transformer.\n        \n        This transformer simply passes the input data through without any modification.\n        """
        super().__init__()
        self._invisible = True

    def __repr__(self) -> str:
        """\n        Return a string representation of the transformer.\n        \n        If the transformer has a previous transformer in the chain, its string representation is returned.\n        Otherwise, the default string representation of the transformer is returned.\n        \n        Returns:\n            str: String representation of the transformer.\n        """
        if self.previous is not None:
            return str(self.previous)
        return super().__repr__()

    def transform(self, data: _In) -> _In:
        """\n        Transform the input data by returning it unchanged.\n        \n        Args:\n            data (_In): The input data to be transformed.\n            \n        Returns:\n            _In: The input data unchanged.\n        """
        if data is None:
            raise ValueError("Input data cannot be None")
        return data


def forward_incoming(
    inner_transformer: Transformer[_In, _Out]
) -> Transformer[_In, Tuple[_Out, _In]]:
    """\n    Create a transformer that applies an inner transformer to the input data and then returns a tuple\n    of the inner transformer's output and the original input data.\n    \n    Args:\n        inner_transformer (Transformer[_In, _Out]): The inner transformer to be applied to the input data.\n        \n    Returns:\n        Transformer[_In, Tuple[_Out, _In]]: A transformer that returns a tuple of the inner transformer's\n        output and the original input data.\n    """
    if not isinstance(inner_transformer, Transformer):
        raise TypeError("inner_transformer must be an instance of Transformer")
    return forward[_In]() >> (inner_transformer, forward())