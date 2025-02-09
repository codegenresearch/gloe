import asyncio\nimport unittest\nfrom typing import TypeVar, Any, cast\nfrom gloe import (\n    async_transformer,\n    ensure,\n    UnsupportedTransformerArgException,\n    transformer,\n    AsyncTransformer,\n    TransformerException,\n)\nfrom gloe.functional import partial_async_transformer\nfrom gloe.utils import forward\nfrom tests.lib.ensurers import is_odd\nfrom tests.lib.exceptions import LnOfNegativeNumber, NumbersEqual, NumberIsEven\nfrom tests.lib.transformers import async_plus1, async_natural_logarithm, minus1\n\n_In = TypeVar("_In")\n\n_DATA = {"foo": "bar"}\n\nasync def raise_an_error():\n    await asyncio.sleep(0.1)\n    raise NotImplementedError()\n\n@async_transformer\nasync def request_data(url: str) -> dict[str, str]:\n    await asyncio.sleep(0.01)\n    return _DATA\n\nclass RequestData(AsyncTransformer[str, dict[str, str]]):\n    async def transform_async(self, url: str) -> dict[str, str]:\n        await asyncio.sleep(0.01)\n        return _DATA\n\nclass HasNotBarKey(Exception):\n    pass\n\nclass HasNotFooKey(Exception):\n    pass\n\nclass HasFooKey(Exception):\n    pass\n\nclass IsNotInt(Exception):\n    pass\n\ndef has_bar_key(data: dict[str, str]):\n    if "bar" not in data.keys():\n        raise HasNotBarKey()\n\ndef has_foo_key(data: dict[str, str]):\n    if "foo" not in data.keys():\n        raise HasNotFooKey()\n    if "foo" in data.keys():\n        raise HasFooKey()\n\ndef is_int(data: Any):\n    if type(data) is not int:\n        raise IsNotInt()\n\ndef is_str(data: Any):\n    if type(data) is not str:\n        raise Exception("data is not string")\n\ndef foo_key_removed(incoming: dict[str, str], outcome: dict[str, str]):\n    if "foo" not in incoming.keys():\n        raise HasNotFooKey()\n    if "foo" in outcome.keys():\n        raise HasFooKey()\n\n_URL = "http://my-service"\n\nclass TestAsyncTransformer(unittest.IsolatedAsyncioTestCase):\n    async def test_basic_case(self):\n        # Test basic transformation pipeline\n        test_forward = request_data >> forward()\n        result = await test_forward(_URL)\n        self.assertDictEqual(_DATA, result)\n\n    async def test_begin_with_transformer(self):\n        # Test pipeline starting with a transformer\n        test_forward = forward[str]() >> request_data\n        result = await test_forward(_URL)\n        self.assertDictEqual(_DATA, result)\n\n    async def test_async_on_divergent_connection(self):\n        # Test divergent connection starting with a transformer\n        test_forward = forward[str]() >> (forward[str](), request_data)\n        result = await test_forward(_URL)\n        self.assertEqual((_URL, _DATA), result)\n\n    async def test_divergent_connection_from_async(self):\n        # Test divergent connection from an async transformer\n        test_forward = request_data >> (forward[dict[str, str]](), forward[dict[str, str]]())\n        result = await test_forward(_URL)\n        self.assertEqual((_DATA, _DATA), result)\n\n    async def test_async_transformer_wrong_arg(self):\n        # Test error handling for incorrect transformer argument\n        def next_transformer():\n            pass\n\n        @ensure(outcome=[has_bar_key])\n        @partial_async_transformer\n        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:\n            await asyncio.sleep(delay)\n            return _DATA\n\n        with self.assertRaises(UnsupportedTransformerArgException):\n            ensured_delayed_request(0.01) >> next_transformer  # type: ignore\n\n    async def test_async_transformer_copy(self):\n        # Test copying a pipeline\n        @transformer\n        def add_slash(path: str) -> str:\n            return path + "/"\n\n        @partial_async_transformer\n        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:\n            await asyncio.sleep(delay)\n            return _DATA\n\n        pipeline = add_slash >> ensured_delayed_request(0)\n        pipeline = pipeline.copy()\n        result = await pipeline(_URL)\n        self.assertEqual(_DATA, result)\n\n    def test_async_transformer_wrong_signature(self):\n        # Test warning for incorrect transformer signature\n        with self.assertWarns(RuntimeWarning):\n            @async_transformer  # type: ignore\n            async def many_args(arg1: str, arg2: int):\n                await asyncio.sleep(1)\n                return arg1, arg2\n\n    def test_async_transformer_signature_representation(self):\n        # Test string representation of transformer signature\n        signature = request_data.signature()\n        self.assertEqual(str(signature), "(url: str) -> dict[str, str]")\n\n    def test_async_transformer_representation(self):\n        # Test string representation of transformers\n        self.assertEqual(repr(request_data), "str -> (request_data) -> dict[str, str]")\n        class_request_data = RequestData()\n        self.assertEqual(repr(class_request_data), "str -> (RequestData) -> dict[str, str]")\n\n        @transformer\n        def dict_to_str(_dict: dict) -> str:\n            return str(_dict)\n\n        request_and_serialize = request_data >> dict_to_str\n        self.assertEqual(repr(request_and_serialize), "dict -> (2 transformers omitted) -> str")\n\n    async def test_exhausting_large_flow(self):\n        # Test large graph instantiation\n        graph = async_plus1\n        max_iters = 1500\n        for i in range(max_iters):\n            graph = graph >> async_plus1\n        result = await graph(0)\n        self.assertEqual(result, max_iters + 1)\n\n    async def test_async_transformer_error_handling(self):\n        # Test error handling in async transformer\n        async_graph = async_plus1 >> async_natural_logarithm\n        try:\n            await async_graph(-2)\n        except LnOfNegativeNumber as exception:\n            self.assertEqual(type(exception.__cause__), TransformerException)\n            exception_ctx = cast(TransformerException, exception.__cause__)\n            self.assertEqual(async_natural_logarithm, exception_ctx.raiser_transformer)\n\n    async def test_execute_async_wrong_flow(self):\n        # Test error handling for wrong async flow\n        flow = [2]\n        with self.assertRaises(NotImplementedError):\n            await _execute_async_flow(flow, 1)  # type: ignore\n\n    async def test_composition_transform_method(self):\n        # Test transform_async method\n        test3 = forward[float]() >> async_plus1\n        result = await test3.transform_async(5)\n        self.assertIsNone(result)\n        test2 = forward[float]() >> (async_plus1, async_plus1)\n        result2 = await test2.transform_async(5)\n        self.assertIsNone(result2)\n