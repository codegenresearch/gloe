from typing import TypeVar, Tuple
from typing_extensions import assert_type

from gloe import Transformer, AsyncTransformer, async_transformer, forward
from gloe.experimental import bridge
from tests.lib.transformers import square, square_root, plus1, minus1, to_string, tuple_concatenate
from tests.type_utils.mypy_test_suite import MypyTestSuite

In = TypeVar("In")
Out = TypeVar("Out")


class TestBasicTransformerTypes(MypyTestSuite):

    def test_transformer_simple_typing(self):
        # Test the most simple transformer typing
        graph = square
        assert_type(graph, Transformer[float, float])

    def test_simple_flow_typing(self):
        # Test the most simple transformer typing
        graph = square >> square_root
        assert_type(graph, Transformer[float, float])

    def test_flow_with_mixed_types(self):
        # Test the most simple transformer typing
        graph = square >> square_root >> to_string
        assert_type(graph, Transformer[float, str])

    def test_divergent_flow_types(self):
        # Test the most simple transformer typing
        graph2 = square >> square_root >> (to_string, square)
        assert_type(graph2, Transformer[float, Tuple[str, float]])

        graph3 = square >> square_root >> (to_string, square, to_string)
        assert_type(graph3, Transformer[float, Tuple[str, float, str]])

        graph4 = square >> square_root >> (to_string, square, to_string, square)
        assert_type(graph4, Transformer[float, Tuple[str, float, str, float]])

        graph5 = (
            square >> square_root >> (to_string, square, to_string, square, to_string)
        )
        assert_type(graph5, Transformer[float, Tuple[str, float, str, float, str]])

        graph6 = (
            square
            >> square_root
            >> (to_string, square, to_string, square, to_string, square)
        )
        assert_type(
            graph6, Transformer[float, Tuple[str, float, str, float, str, float]]
        )

        graph7 = (
            square
            >> square_root
            >> (to_string, square, to_string, square, to_string, square, to_string)
        )
        assert_type(
            graph7, Transformer[float, Tuple[str, float, str, float, str, float, str]]
        )

    def test_bridge(self):
        # Test the bridge functionality
        num_bridge = bridge[float]("num")

        graph = plus1 >> num_bridge.pick() >> minus1 >> num_bridge.drop()
        assert_type(graph, Transformer[float, Tuple[float, float]])

    def test_async_transformer(self):
        # Test the async transformer functionality
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
        assert_type(async_pipeline3, AsyncTransformer[int, Tuple[float, str]])
        assert_type(async_pipeline4, AsyncTransformer[int, Tuple[str, float]])
        assert_type(async_pipeline5, AsyncTransformer[int, str])


### Changes Made:
1. **Removed Non-Python Syntax in Comments**: Removed the list format in the comments and used standard comment format with `#` for clarity and to avoid syntax errors.
2. **Import Organization**: Grouped imports logically: standard library, third-party, and local application imports.
3. **Consistent Commenting**: Ensured comments are clear and consistent without non-standard comment formats.
4. **Spacing and Line Breaks**: Ensured consistent spacing around operators and effective use of line breaks for readability.
5. **Tuple Type Hints**: Used `Tuple` consistently for type hints.
6. **Code Structure**: Improved the structure of the code, especially in the test methods, to ensure logical flow and clear separation of graph definitions and assertions.