import asyncio\nimport unittest\nfrom typing import TypeVar, Any\nfrom gloe import async_transformer, ensure, UnsupportedTransformerArgException, transformer\nfrom gloe.functional import partial_async_transformer\nfrom gloe.utils import forward\n\n_In = TypeVar("_In")\n\n_DATA = {"foo": "bar"}\n\n@async_transformer\nasync def request_data(url: str) -> dict[str, str]:\n    await asyncio.sleep(0.1)\n    return _DATA\n\nclass HasNotBarKey(Exception):\n    pass\n\ndef has_bar_key(dict: dict[str, str]):\n    if "bar" not in dict.keys():\n        raise HasNotBarKey()\n\ndef is_string(input_data: Any):\n    if not isinstance(input_data, str):\n        raise ValueError(f"Expected str, got {type(input_data).__name__}")\n\n_URL = "http://my-service"\n\nclass TestAsyncTransformer(unittest.IsolatedAsyncioTestCase):\n    async def test_basic_case(self):\n        test_forward = request_data >> forward()\n        result = await test_forward(_URL)\n        self.assertDictEqual(result, _DATA)\n\n    async def test_begin_with_transformer(self):\n        test_forward = forward[str]() >> request_data\n        result = await test_forward(_URL)\n        self.assertDictEqual(result, _DATA)\n\n    async def test_async_on_divergent_connection(self):\n        test_forward = forward[str]() >> (\n            forward[str](),\n            request_data\n        )\n        result = await test_forward(_URL)\n        self.assertEqual(result, (_URL, _DATA))\n\n    async def test_divergent_connection_from_async(self):\n        test_forward = request_data >> (\n            forward[dict[str, str]](),\n            forward[dict[str, str]](),\n        )\n        result = await test_forward(_URL)\n        self.assertEqual(result, (_DATA, _DATA))\n\n    async def test_partial_async_transformer(self):\n        @partial_async_transformer\n        async def sleep_and_forward(data: dict[str, str], delay: float) -> dict[str, str]:\n            await asyncio.sleep(delay)\n            return data\n        pipeline = sleep_and_forward(0.1) >> forward()\n        result = await pipeline(_DATA)\n        self.assertEqual(result, _DATA)\n\n    async def test_ensure_async_transformer(self):\n        @ensure(incoming=[is_string], outcome=[has_bar_key])\n        @async_transformer\n        async def ensured_request(url: str) -> dict[str, str]:\n            await asyncio.sleep(0.1)\n            return _DATA\n        pipeline = ensured_request >> forward()\n        with self.assertRaises(HasNotBarKey):\n            await pipeline(_URL)\n\n    async def test_ensure_partial_async_transformer(self):\n        @ensure(incoming=[is_string], outcome=[has_bar_key])\n        @partial_async_transformer\n        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:\n            await asyncio.sleep(delay)\n            return _DATA\n        pipeline = ensured_delayed_request(0.1) >> forward()\n        with self.assertRaises(HasNotBarKey):\n            await pipeline(_URL)\n\n    async def test_async_transformer_wrong_arg(self):\n        with self.assertWarns(RuntimeWarning):\n            @transformer  # type: ignore\n            def wrong_arg_transformer(arg1: str, arg2: int):\n                return arg1, arg2\n\n    async def test_async_transformer_copy(self):\n        test_forward = request_data >> forward()\n        test_forward_copy = test_forward.copy()\n        self.assertEqual(test_forward, test_forward_copy)\n        self.assertIsNot(test_forward, test_forward_copy)\n