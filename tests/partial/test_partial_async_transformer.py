import asyncio\nimport unittest\nfrom typing import Dict\nfrom gloe import partial_async_transformer\nfrom gloe.utils import forward\n\n_DATA = {"foo": "bar"}\n\nclass TestPartialAsyncTransformer(unittest.IsolatedAsyncioTestCase):\n\n    async def test_partial_async_transformer(self):\n        @partial_async_transformer\n        async def sleep_and_forward(data: Dict[str, str], delay: float) -> Dict[str, str]:\n            await asyncio.sleep(delay)\n            return data\n\n        pipeline = sleep_and_forward(0.01) >> forward()\n\n        result = await pipeline(_DATA)\n\n        self.assertEqual(result, _DATA)\n