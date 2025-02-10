import asyncio
import unittest
from typing import TypeVar, Any
from gloe import (
    async_transformer,
    ensure,
    UnsupportedTransformerArgException,
    transformer,
)
from gloe.functional import partial_async_transformer
from gloe.utils import forward

_In = TypeVar("_In")

_DATA = {"foo": "bar"}


@async_transformer
async def request_data(url: str) -> dict[str, str]:
    await asyncio.sleep(0.1)
    return _DATA


class HasNotBarKey(Exception):
    pass


class HasFooKey(Exception):
    pass


class IsNotInt(Exception):
    pass


class IsNotStr(Exception):
    pass


def has_bar_key(data: dict[str, str]):
    if "bar" not in data.keys():
        raise HasNotBarKey()


def has_foo_key(data: dict[str, str]):
    if "foo" not in data.keys():
        raise HasFooKey()


def foo_key_removed(incoming: dict[str, str], outcome: dict[str, str]):
    if "foo" in incoming and "foo" not in outcome:
        raise HasFooKey()


def is_int(data: Any):
    if type(data) is not int:
        raise IsNotInt()


def is_str(data: Any):
    if type(data) is not str:
        raise IsNotStr()


_URL = "http://my-service"


class TestAsyncTransformer(unittest.IsolatedAsyncioTestCase):
    async def test_basic_case(self):
        # Test a simple pipeline with a single async transformer
        test_forward = request_data >> forward()

        result = await test_forward(_URL)

        self.assertDictEqual(result, _DATA)

    async def test_begin_with_transformer(self):
        # Test a pipeline starting with a forward transformer
        test_forward = forward[str]() >> request_data

        result = await test_forward(_URL)

        self.assertDictEqual(result, _DATA)

    async def test_async_on_divergent_connection(self):
        # Test a pipeline with a divergent connection
        test_forward = forward[str]() >> (forward[str](), request_data)

        result = await test_forward(_URL)

        self.assertEqual(result, (_URL, _DATA))

    async def test_divergent_connection_from_async(self):
        # Test a divergent connection starting from an async transformer
        test_forward = request_data >> (
            forward[dict[str, str]](),
            forward[dict[str, str]](),
        )

        result = await test_forward(_URL)

        self.assertEqual(result, (_DATA, _DATA))

    async def test_partial_async_transformer(self):
        # Test a pipeline with a partial async transformer
        @partial_async_transformer
        async def sleep_and_forward(data: dict[str, str], delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return data

        pipeline = sleep_and_forward(0.1) >> forward()

        result = await pipeline(_DATA)

        self.assertEqual(result, _DATA)

    async def test_ensure_async_transformer(self):
        # Test an async transformer with ensure decorator
        @ensure(incoming=[is_str], outcome=[has_bar_key])
        @async_transformer
        async def ensured_request(url: str) -> dict[str, str]:
            await asyncio.sleep(0.1)
            return _DATA

        pipeline = ensured_request >> forward()

        with self.assertRaises(HasNotBarKey):
            await pipeline(_URL)

    async def test_ensure_partial_async_transformer(self):
        # Test a partial async transformer with ensure decorator
        @ensure(incoming=[is_str], outcome=[has_bar_key])
        @partial_async_transformer
        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return _DATA

        pipeline = ensured_delayed_request(0.1) >> forward()

        with self.assertRaises(HasNotBarKey):
            await pipeline(_URL)

    async def test_async_transformer_wrong_arg(self):
        # Test handling of unsupported transformer arguments
        def next_transformer():
            pass

        @ensure(incoming=[is_str], outcome=[has_bar_key])
        @partial_async_transformer
        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return _DATA

        with self.assertRaises(UnsupportedTransformerArgException):
            pipeline = ensured_delayed_request(0.1) >> next_transformer

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
        self.assertEqual(result, _DATA)

    async def test_integer_input(self):
        # Test handling of integer inputs with ensure decorator
        @ensure(incoming=[is_int], outcome=[has_bar_key])
        @async_transformer
        async def int_request(num: int) -> dict[str, str]:
            await asyncio.sleep(0.1)
            return _DATA

        pipeline = int_request >> forward()

        with self.assertRaises(IsNotInt):
            await pipeline(_URL)

    async def test_foo_key_handling(self):
        # Test handling of the "foo" key with ensure decorator
        @ensure(outcome=[foo_key_removed])
        @async_transformer
        async def remove_foo(data: dict[str, str]) -> dict[str, str]:
            await asyncio.sleep(0.1)
            if "foo" in data:
                data.pop("foo", None)
            return data

        pipeline = request_data >> remove_foo >> forward()

        with self.assertRaises(HasFooKey):
            await pipeline(_URL)

    async def test_transformer_wrong_signature(self):
        # Test handling of transformers with incorrect signatures
        with self.assertWarns(RuntimeWarning):

            @transformer  # type: ignore
            def many_args(arg1: str, arg2: int):
                return arg1, arg2