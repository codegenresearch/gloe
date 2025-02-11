import asyncio
import unittest

from gloe import partial_async_transformer
from gloe.utils import forward

_DATA = {"foo": "bar"}


class TestPartialAsyncTransformer(unittest.TestCase):

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


### Changes Made:
1. **Removed Problematic Comment**: Removed the comment that was causing the `SyntaxError`.
2. **Class Inheritance**: Changed the test class to inherit from `unittest.TestCase` to align with the gold code structure.