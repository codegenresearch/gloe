import asyncio
from typing import Union
from typing_extensions import assert_type

from gloe import (
    Transformer,
    async_transformer,
    AsyncTransformer,
    ensure,
    partial_async_transformer,
)
from gloe.utils import forward
from gloe.experimental import bridge
from tests.lib.conditioners import if_not_zero, if_is_even
from tests.lib.transformers import (
    square,
    square_root,
    plus1,
    minus1,
    to_string,
    async_plus1,
)
from tests.type_utils.mypy_test_suite import MypyTestSuite


class TestTransformerTypes(MypyTestSuite):
    def test_conditioned_flow_types(self):
        """
        Test transformer typing with conditional flows.
        """

        conditioned_graph = (
            square >> square_root >> if_not_zero.Then(plus1).Else(minus1)
        )

        assert_type(conditioned_graph, Transformer[float, float])

        conditioned_graph2 = (
            square >> square_root >> if_not_zero.Then(to_string).Else(square)
        )

        assert_type(conditioned_graph2, Transformer[float, Union[str, float]])

    def test_chained_condition_flow_types(self):
        """
        Test transformer typing with chained conditional flows.
        """

        chained_conditions_graph = (
            if_is_even.Then(square).ElseIf(lambda x: x < 10).Then(to_string).ElseNone()
        )

        assert_type(
            chained_conditions_graph, Transformer[float, Union[float, str, None]]
        )

    def test_async_chained_condition_flow_types(self):
        """
        Test async transformer typing with chained conditional flows.
        """

        async_chained_conditions_graph = (
            if_is_even.Then(async_plus1)
            .ElseIf(lambda x: x < 10)
            .Then(to_string)
            .ElseNone()
        )

        assert_type(
            async_chained_conditions_graph,
            AsyncTransformer[float, Union[float, str, None]],
        )

        async_chained_conditions_graph = (
            if_is_even.Then(square)
            .ElseIf(lambda x: x < 10)
            .Then(async_plus1)
            .ElseNone()
        )

        assert_type(
            async_chained_conditions_graph,
            AsyncTransformer[float, Union[float, None]],
        )