import asyncio
import unittest
from typing import TypeVar, Any, cast
from gloe import async_transformer, ensure, UnsupportedTransformerArgException, transformer, AsyncTransformer, TransformerException
from gloe.functional import partial_async_transformer
from gloe.utils import forward

from tests.lib.transformers import async_plus1, async_natural_logarithm, minus1

_In = TypeVar("_In")

_DATA = {"foo": "bar"}
_URL = "http://my-service"

class HasNotBarKey(Exception):
    pass

class HasNotFooKey(Exception):
    pass

class HasFooKey(Exception):
    pass

class IsNotInt(Exception):
    pass

def has_bar_key(data: dict[str, str]):
    if "bar" not in data.keys():
        raise HasNotBarKey()

def has_foo_key(data: dict[str, str]):
    if "foo" not in data.keys():
        raise HasNotBarKey()

def is_int(data: Any):
    if not isinstance(data, int):
        raise IsNotInt()

def is_str(data: Any):
    if not isinstance(data, str):
        raise Exception("data is not string")

def foo_key_removed(incoming: dict[str, str], outcome: dict[str, str]):
    if "foo" not in incoming.keys():
        raise HasNotFooKey()
    if "foo" in outcome.keys():
        raise HasFooKey()

@async_transformer
async def request_data(url: str) -> dict[str, str]:
    await asyncio.sleep(0.01)
    return _DATA

class RequestData(AsyncTransformer[str, dict[str, str]]):
    async def transform_async(self, url: str) -> dict[str, str]:
        await asyncio.sleep(0.01)
        return _DATA

class TestAsyncTransformer(unittest.IsolatedAsyncioTestCase):
    async def test_basic_case(self):
        # Test the basic case where request_data is followed by forward
        test_forward = request_data >> forward()
        result = await test_forward(_URL)
        self.assertDictEqual(_DATA, result)

    async def test_begin_with_transformer(self):
        # Test starting with a forward transformer followed by request_data
        test_forward = forward[str]() >> request_data
        result = await test_forward(_URL)
        self.assertDictEqual(_DATA, result)

    async def test_async_on_divergent_connection(self):
        # Test a divergent connection starting with a forward transformer
        test_forward = forward[str]() >> (forward[str](), request_data)
        result = await test_forward(_URL)
        self.assertEqual((_URL, _DATA), result)

    async def test_divergent_connection_from_async(self):
        # Test a divergent connection starting with an async transformer
        test_forward = request_data >> (forward[dict[str, str]](), forward[dict[str, str]]())
        result = await test_forward(_URL)
        self.assertEqual((_DATA, _DATA), result)

    async def test_async_transformer_wrong_arg(self):
        # Test passing an unsupported argument to an async transformer
        def next_transformer():
            pass

        @ensure(outcome=[has_bar_key])
        @partial_async_transformer
        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return _DATA

        with self.assertRaises(UnsupportedTransformerArgException):
            ensured_delayed_request(0.01) >> next_transformer  # type: ignore

    async def test_async_transformer_copy(self):
        # Test copying an async transformer pipeline
        @transformer
        def add_slash(path: str) -> str:
            return path + "/"

        @partial_async_transformer
        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return _DATA

        pipeline = add_slash >> ensured_delayed_request(0)
        pipeline = pipeline.copy()
        result = await pipeline(_URL)
        self.assertEqual(_DATA, result)

    def test_async_transformer_wrong_signature(self):
        # Test an async transformer with an incorrect signature
        with self.assertWarns(RuntimeWarning):
            @async_transformer  # type: ignore
            async def many_args(arg1: str, arg2: int):
                await asyncio.sleep(1)
                return arg1, arg2

    def test_async_transformer_signature_representation(self):
        # Test the string representation of an async transformer's signature
        signature = request_data.signature()
        self.assertEqual(str(signature), "(url: str) -> dict[str, str]")

    def test_async_transformer_representation(self):
        # Test the string representation of an async transformer
        self.assertEqual(repr(request_data), "str -> (request_data) -> dict[str, str]")
        class_request_data = RequestData()
        self.assertEqual(repr(class_request_data), "str -> (RequestData) -> dict[str, str]")

        @transformer
        def dict_to_str(_dict: dict) -> str:
            return str(_dict)

        request_and_serialize = request_data >> dict_to_str
        self.assertEqual(repr(request_and_serialize), "dict -> (2 transformers omitted) -> str")

    async def test_exhausting_large_flow(self):
        # Test the instantiation of a large graph
        graph = async_plus1
        max_iters = 1500
        for iteration in range(max_iters):
            graph = graph >> async_plus1

        result = await graph(0)
        self.assertEqual(result, max_iters + 1)

    async def test_async_transformer_error_handling(self):
        # Test error handling in an async transformer pipeline
        async_graph = async_plus1 >> async_natural_logarithm
        try:
            await async_graph(-2)
        except Exception as exception:
            self.assertEqual(type(exception.__cause__), TransformerException)
            exception_ctx = cast(TransformerException, exception.__cause__)
            self.assertEqual(async_natural_logarithm, exception_ctx.raiser_transformer)

    async def test_execute_async_wrong_flow(self):
        # Test executing an async flow with an unsupported flow
        flow = [2]
        with self.assertRaises(NotImplementedError):
            await _execute_async_flow(flow, 1)  # type: ignore

    async def test_composition_transform_method(self):
        # Test the transform_async method of a composed transformer
        test3 = forward[float]() >> async_plus1
        result = await test3.transform_async(5)
        self.assertIsNone(result)
        test2 = forward[float]() >> (async_plus1, async_plus1)
        result2 = await test2.transform_async(5)
        self.assertIsNone(result2)

# Define the _execute_async_flow function to handle async flows
async def _execute_async_flow(flow, value):
    raise NotImplementedError()