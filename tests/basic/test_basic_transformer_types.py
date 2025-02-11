from typing import TypeVar, Union
from typing_extensions import assert_type

from gloe import (
    Transformer,
    AsyncTransformer,
    async_transformer,
)
from gloe.experimental import bridge
from gloe.utils import forward
from tests.lib.transformers import (
    square,
    square_root,
    plus1,
    minus1,
    to_string,
    tuple_concatenate,
)
from tests.type_utils.mypy_test_suite import MypyTestSuite

In = TypeVar("In")
Out = TypeVar("Out")


class TestBasicTransformerTypes(MypyTestSuite):

    def test_transformer_simple_typing(self):
        """
        Test the most simple transformer typing with a single transformer.
        """

        graph = square
        assert_type(graph, Transformer[float, float])

    def test_simple_flow_typing(self):
        """
        Test the most simple transformer typing with a flow of two transformers.
        """

        graph = square >> square_root

        assert_type(graph, Transformer[float, float])

    def test_flow_with_mixed_types(self):
        """
        Test the most simple transformer typing with a flow that changes the output type.
        """

        graph = square >> square_root >> to_string

        assert_type(graph, Transformer[float, str])

    def test_divergent_flow_types(self):
        """
        Test the most simple transformer typing with divergent outputs.
        """

        graph_diverge_1 = square >> square_root >> (to_string, square)
        assert_type(graph_diverge_1, Transformer[float, tuple[str, float]])

        graph_diverge_2 = square >> square_root >> (to_string, square, to_string)
        assert_type(graph_diverge_2, Transformer[float, tuple[str, float, str]])

        graph_diverge_3 = square >> square_root >> (to_string, square, to_string, square)
        assert_type(graph_diverge_3, Transformer[float, tuple[str, float, str, float]])

        graph_diverge_4 = (
            square >> square_root >> (to_string, square, to_string, square, to_string)
        )
        assert_type(graph_diverge_4, Transformer[float, tuple[str, float, str, float, str]])

        graph_diverge_5 = (
            square
            >> square_root
            >> (to_string, square, to_string, square, to_string, square)
        )
        assert_type(
            graph_diverge_5, Transformer[float, tuple[str, float, str, float, str, float]]
        )

        graph_diverge_6 = (
            square
            >> square_root
            >> (to_string, square, to_string, square, to_string, square, to_string)
        )
        assert_type(
            graph_diverge_6, Transformer[float, tuple[str, float, str, float, str, float, str]]
        )

    def test_bridge(self):
        """
        Test the most simple transformer typing with the bridge function.
        """

        num_bridge = bridge[float]("num")

        graph_bridge = plus1 >> num_bridge.pick() >> minus1 >> num_bridge.drop()

        assert_type(graph_bridge, Transformer[float, tuple[float, float]])

    def test_async_transformer(self):
        """
        Test the most simple transformer typing with async transformers.
        """

        @async_transformer
        async def _square(num: int) -> float:
            return float(num * num)

        async_pipeline_1 = _square >> to_string
        async_pipeline_2 = forward[int]() >> _square >> to_string
        async_pipeline_3 = forward[int]() >> (_square, _square >> to_string)
        async_pipeline_4 = _square >> (to_string, forward[float]())
        async_pipeline_5 = _square >> (to_string, forward[float]()) >> tuple_concatenate

        assert_type(_square, AsyncTransformer[int, float])
        assert_type(async_pipeline_1, AsyncTransformer[int, str])
        assert_type(async_pipeline_2, AsyncTransformer[int, str])
        assert_type(async_pipeline_3, AsyncTransformer[int, tuple[float, str]])
        assert_type(async_pipeline_4, AsyncTransformer[int, tuple[str, float]])
        assert_type(async_pipeline_5, AsyncTransformer[int, str])