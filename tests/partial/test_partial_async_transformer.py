import asyncio
import unittest

from gloe import partial_async_transformer
from gloe.utils import forward

_DATA = {"foo": "bar"}


class TestPartialAsyncTransformer(unittest.IsolatedAsyncioTestCase):

    async def test_partial_async_transformer(self):
        @partial_async_transformer
        async def sleep_and_forward(
            data: dict[str, str], delay: float
        ) -> dict[str, str]:
            await asyncio.sleep(delay)
            return data

        pipeline = sleep_and_forward(0.01) >> forward()

        result = await pipeline(_DATA)

        self.assertEqual(result, _DATA)


I have removed any unterminated string literals or comments that might have caused the `SyntaxError`. The code snippet should now be correctly formatted and free of syntax issues, allowing the tests to run as intended.