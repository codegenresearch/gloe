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


class HasNotFooKey(Exception):
    pass


class HasFooKey(Exception):
    pass


class IsNotInt(Exception):
    pass


def has_bar_key(data: dict[str, str]):
    if "bar" not in data:
        raise HasNotBarKey()


def has_foo_key(data: dict[str, str]):
    if "foo" not in data:
        raise HasNotFooKey()


def foo_key_removed(data: dict[str, str]):
    if "foo" in data:
        raise HasFooKey()


def is_string(data: Any):
    if not isinstance(data, str):
        raise Exception("Data is not a string")


def is_int(data: Any):
    if not isinstance(data, int):
        raise IsNotInt()


_URL = "http://my-service"


class TestAsyncTransformer(unittest.IsolatedAsyncioTestCase):
    async def test_basic_case(self):
        test_forward = request_data >> forward()
        result = await test_forward(_URL)
        self.assertDictEqual(result, _DATA)

    async def test_begin_with_transformer(self):
        test_forward = forward[str]() >> request_data
        result = await test_forward(_URL)
        self.assertDictEqual(result, _DATA)

    async def test_async_on_divergent_connection(self):
        test_forward = forward[str]() >> (forward[str](), request_data)
        result = await test_forward(_URL)
        self.assertEqual(result, (_URL, _DATA))

    async def test_divergent_connection_from_async(self):
        test_forward = request_data >> (forward[dict[str, str]](), forward[dict[str, str]]())
        result = await test_forward(_URL)
        self.assertEqual(result, (_DATA, _DATA))

    async def test_partial_async_transformer(self):
        @partial_async_transformer
        async def sleep_and_forward(data: dict[str, str], delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return data

        pipeline = sleep_and_forward(0.1) >> forward()
        result = await pipeline(_DATA)
        self.assertEqual(result, _DATA)

    async def test_ensure_async_transformer(self):
        @ensure(incoming=[is_string], outcome=[has_bar_key])
        @async_transformer
        async def ensured_request(url: str) -> dict[str, str]:
            await asyncio.sleep(0.1)
            return _DATA

        pipeline = ensured_request >> forward()
        with self.assertRaises(HasNotBarKey):
            await pipeline(_URL)

    async def test_ensure_partial_async_transformer(self):
        @ensure(incoming=[is_string], outcome=[has_bar_key])
        @partial_async_transformer
        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return _DATA

        pipeline = ensured_delayed_request(0.1) >> forward()
        with self.assertRaises(HasNotBarKey):
            await pipeline(_URL)

    async def test_async_transformer_wrong_arg(self):
        def next_transformer():
            pass

        @ensure(incoming=[is_string], outcome=[has_bar_key])
        @partial_async_transformer
        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return _DATA

        with self.assertRaises(UnsupportedTransformerArgException):
            pipeline = ensured_delayed_request(0.1) >> next_transformer

    async def test_async_transformer_copy(self):
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

    async def test_ensure_with_changes(self):
        @ensure(incoming=[is_int], outcome=[is_int], changes=[foo_key_removed])
        @async_transformer
        async def transform_with_changes(data: int) -> dict[str, str]:
            await asyncio.sleep(0.1)
            return {"bar": str(data)}

        pipeline = transform_with_changes >> forward()
        with self.assertRaises(HasFooKey):
            await pipeline(10)

    async def test_ensure_with_foo_key(self):
        @ensure(incoming=[is_string], outcome=[has_foo_key])
        @async_transformer
        async def transform_with_foo_key(url: str) -> dict[str, str]:
            await asyncio.sleep(0.1)
            return {"foo": "bar"}

        pipeline = transform_with_foo_key >> forward()
        await pipeline(_URL)

    async def test_ensure_with_no_foo_key(self):
        @ensure(incoming=[is_string], outcome=[has_not_foo_key])
        @async_transformer
        async def transform_with_no_foo_key(url: str) -> dict[str, str]:
            await asyncio.sleep(0.1)
            return {"bar": "foo"}

        pipeline = transform_with_no_foo_key >> forward()
        with self.assertRaises(HasNotFooKey):
            await pipeline(_URL)


This code addresses the feedback by adding the requested exception classes, key check functions, and additional test cases. It also ensures that the `ensure` decorators are used with the appropriate configurations.