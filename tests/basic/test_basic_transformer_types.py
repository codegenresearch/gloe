from typing import TypeVar, Tuple
from typing_extensions import assert_type
from gloe import Transformer, AsyncTransformer, async_transformer, bridge, forward
from tests.lib.transformers import square, square_root, plus1, minus1, to_string, tuple_concatenate
from tests.type_utils.mypy_test_suite import MypyTestSuite

In = TypeVar("In")
Out = TypeVar("Out")

TUPLE_TYPES = Tuple[str, float]
TUPLE_TYPES_3 = Tuple[str, float, str]
TUPLE_TYPES_4 = Tuple[str, float, str, float]
TUPLE_TYPES_5 = Tuple[str, float, str, float, str]
TUPLE_TYPES_6 = Tuple[str, float, str, float, str, float]
TUPLE_TYPES_7 = Tuple[str, float, str, float, str, float, str]
TUPLE_TYPES_FLOAT = Tuple[float, float]

class TestBasicTransformerTypes(MypyTestSuite):

    def test_transformer_simple_typing(self):
        """
        Test the most simple transformer typing
        """

        graph = square
        assert_type(graph, Transformer[float, float])

    def test_simple_flow_typing(self):
        """
        Test the most simple transformer typing
        """

        graph = square >> square_root

        assert_type(graph, Transformer[float, float])

    def test_flow_with_mixed_types(self):
        """
        Test the most simple transformer typing
        """

        graph = square >> square_root >> to_string

        assert_type(graph, Transformer[float, str])

    def test_divergent_flow_types(self):
        """
        Test the most simple transformer typing
        """

        graph2 = square >> square_root >> (to_string, square)
        assert_type(graph2, Transformer[float, TUPLE_TYPES])

        graph3 = square >> square_root >> (to_string, square, to_string)
        assert_type(graph3, Transformer[float, TUPLE_TYPES_3])

        graph4 = square >> square_root >> (to_string, square, to_string, square)
        assert_type(graph4, Transformer[float, TUPLE_TYPES_4])

        graph5 = (
            square >> square_root >> (to_string, square, to_string, square, to_string)
        )
        assert_type(graph5, Transformer[float, TUPLE_TYPES_5])

        graph6 = (
            square
            >> square_root
            >> (to_string, square, to_string, square, to_string, square)
        )
        assert_type(
            graph6, Transformer[float, TUPLE_TYPES_6]
        )

        graph7 = (
            square
            >> square_root
            >> (to_string, square, to_string, square, to_string, square, to_string)
        )
        assert_type(
            graph7, Transformer[float, TUPLE_TYPES_7]
        )

    def test_bridge(self):
        num_bridge = bridge[float]("num")

        graph = plus1 >> num_bridge.pick() >> minus1 >> num_bridge.drop()

        assert_type(graph, Transformer[float, TUPLE_TYPES_FLOAT])

    def test_async_transformer(self):
        @async_transformer
        async def _square(num: int) -> float:
            return float(num * num)

        async_pipeline = _square >> to_string
        async_pipeline2 = forward[int]() >> _square >> to_string
        async_pipeline3 = forward[int]() >> (_square, _square >> to_string)
        async_pipeline4 = _square >> (to_string, forward[float]())
        async_pipeline5 = _square >> (to_string, forward[float]()) >> tuple_concatenate

        assert_type(_square, AsyncTransformer[int, float])
        assert_type(async_pipeline, AsyncTransformer[int, str])
        assert_type(async_pipeline2, AsyncTransformer[int, str])
        assert_type(async_pipeline3, AsyncTransformer[int, TUPLE_TYPES])
        assert_type(async_pipeline4, AsyncTransformer[int, Tuple[str, float]])
        assert_type(async_pipeline5, AsyncTransformer[int, str])