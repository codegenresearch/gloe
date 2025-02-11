from typing import TypeVar, Union
from typing_extensions import assert_type

from gloe import Transformer, AsyncTransformer, async_transformer
from gloe.experimental import bridge
from gloe.utils import forward
from tests.lib.transformers import square, square_root, plus1, minus1, to_string, tuple_concatenate
from tests.type_utils.mypy_test_suite import MypyTestSuite

In = TypeVar("In")
Out = TypeVar("Out")


class TestBasicTransformerTypes(MypyTestSuite):

    def test_transformer_simple_typing(self):
        """
        Test the basic transformer typing with a single transformer.
        """

        simple_graph = square
        assert_type(simple_graph, Transformer[float, float])

    def test_simple_flow_typing(self):
        """
        Test the typing of a simple transformer flow with two transformers.
        """

        simple_flow_graph = square >> square_root

        assert_type(simple_flow_graph, Transformer[float, float])

    def test_flow_with_mixed_types(self):
        """
        Test the typing of a transformer flow that changes the output type.
        """

        mixed_types_graph = square >> square_root >> to_string

        assert_type(mixed_types_graph, Transformer[float, str])

    def test_divergent_flow_types(self):
        """
        Test the typing of transformer flows with divergent outputs.
        """

        divergent_graph1 = square >> square_root >> (to_string, square)
        assert_type(divergent_graph1, Transformer[float, tuple[str, float]])

        divergent_graph2 = square >> square_root >> (to_string, square, to_string)
        assert_type(divergent_graph2, Transformer[float, tuple[str, float, str]])

        divergent_graph3 = square >> square_root >> (to_string, square, to_string, square)
        assert_type(divergent_graph3, Transformer[float, tuple[str, float, str, float]])

        divergent_graph4 = (
            square >> square_root >> (to_string, square, to_string, square, to_string)
        )
        assert_type(divergent_graph4, Transformer[float, tuple[str, float, str, float, str]])

        divergent_graph5 = (
            square
            >> square_root
            >> (to_string, square, to_string, square, to_string, square)
        )
        assert_type(
            divergent_graph5, Transformer[float, tuple[str, float, str, float, str, float]]
        )

        divergent_graph6 = (
            square
            >> square_root
            >> (to_string, square, to_string, square, to_string, square, to_string)
        )
        assert_type(
            divergent_graph6, Transformer[float, tuple[str, float, str, float, str, float, str]]
        )

    def test_bridge(self):
        """
        Test the typing of a transformer flow using the bridge function.
        """

        num_bridge = bridge[float]("num")

        bridge_graph = plus1 >> num_bridge.pick() >> minus1 >> num_bridge.drop()

        assert_type(bridge_graph, Transformer[float, tuple[float, float]])

    def test_async_transformer(self):
        """
        Test the typing of async transformers and their combinations.
        """

        @async_transformer
        async def _square(num: int) -> float:
            return float(num * num)

        async_pipeline1 = _square >> to_string
        async_pipeline2 = forward[int]() >> _square >> to_string
        async_pipeline3 = forward[int]() >> (_square, _square >> to_string)
        async_pipeline4 = _square >> (to_string, forward[float]())
        async_pipeline5 = _square >> (to_string, forward[float]()) >> tuple_concatenate

        assert_type(_square, AsyncTransformer[int, float])
        assert_type(async_pipeline1, AsyncTransformer[int, str])
        assert_type(async_pipeline2, AsyncTransformer[int, str])
        assert_type(async_pipeline3, AsyncTransformer[int, tuple[float, str]])
        assert_type(async_pipeline4, AsyncTransformer[int, tuple[str, float]])
        assert_type(async_pipeline5, AsyncTransformer[int, str])