import asyncio\nimport unittest\nfrom typing import TypeVar, Union\nfrom typing_extensions import assert_type\n\nfrom tests.lib.conditioners import if_not_zero, if_is_even\nfrom tests.lib.transformers import (\n    square,\n    square_root,\n    plus1,\n    minus1,\n    to_string,\n    async_plus1,\n)\nfrom gloe import (\n    Transformer,\n    async_transformer,\n    AsyncTransformer,\n)\nfrom gloe.utils import forward\nfrom tests.type_utils.mypy_test_suite import MypyTestSuite\n\nIn = TypeVar("In")\nOut = TypeVar("Out")\n\nclass TestTransformerTypes(MypyTestSuite):\n    mypy_result: str\n\n    def test_conditioned_flow_types(self):\n        """\n        Test the most simple transformer typing\n        """\n\n        conditioned_graph = (\n            square >> square_root >> if_not_zero.Then(plus1).Else(minus1)\n        )\n\n        assert_type(conditioned_graph, Transformer[float, float])\n\n        conditioned_graph2 = (\n            square >> square_root >> if_not_zero.Then(to_string).Else(square)\n        )\n\n        assert_type(conditioned_graph2, Transformer[float, Union[str, float]])\n\n    def test_chained_condition_flow_types(self):\n        """\n        Test the most simple transformer typing\n        """\n\n        chained_conditions_graph = (\n            if_is_even.Then(square)\n            .ElseIf(lambda x: x < 10)\n            .Then(to_string)\n            .ElseNone()\n        )\n\n        assert_type(\n            chained_conditions_graph, Transformer[float, Union[float, str, None]]\n        )\n\n    def test_async_chained_condition_flow_types(self):\n        """\n        Test the most simple transformer typing\n        """\n\n        async_chained_conditions_graph = (\n            if_is_even.Then(async_plus1)\n            .ElseIf(lambda x: x < 10)\n            .Then(to_string)\n            .ElseNone()\n        )\n\n        assert_type(\n            async_chained_conditions_graph,\n            AsyncTransformer[float, Union[float, str, None]],\n        )\n\n        async_chained_conditions_graph = (\n            if_is_even.Then(square)\n            .ElseIf(lambda x: x < 10)\n            .Then(async_plus1)\n            .ElseNone()\n        )\n\n        assert_type(\n            async_chained_conditions_graph,\n            AsyncTransformer[float, Union[float, None]],\n        )\n