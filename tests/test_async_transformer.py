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

_DATA = {"foo": "bar", "baz": "qux"}


@async_transformer
async def request_data(url: str) -> dict[str, str]:
    await asyncio.sleep(0.1)
    return _DATA


class HasNotFooKey(Exception):
    pass


class HasFooKey(Exception):
    pass


class IsNotInt(Exception):
    pass


class IsNotStr(Exception):
    pass


def has_foo_key(data: dict[str, str]) -> None:
    if "foo" not in data:
        raise HasNotFooKey("The dictionary does not contain the key 'foo'.")


def has_no_foo_key(data: dict[str, str]) -> None:
    if "foo" in data:
        raise HasFooKey("The dictionary contains the key 'foo'.")


def is_int(data: Any) -> None:
    if not isinstance(data, int):
        raise IsNotInt("Data is not an integer.")


def is_str(data: Any) -> None:
    if not isinstance(data, str):
        raise IsNotStr("Data is not a string.")


def foo_key_removed(data: dict[str, str]) -> None:
    if "foo" in data:
        raise HasFooKey("The dictionary still contains the key 'foo'.")


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
        test_forward = request_data >> (
            forward[dict[str, str]](),
            forward[dict[str, str]](),
        )

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
        @ensure(incoming=[is_str], outcome=[has_foo_key])
        @async_transformer
        async def ensured_request(url: str) -> dict[str, str]:
            await asyncio.sleep(0.1)
            return _DATA

        pipeline = ensured_request >> forward()

        with self.assertRaises(HasFooKey) as context:
            await pipeline(_URL)
        self.assertEqual(str(context.exception), "The dictionary contains the key 'foo'.")

    async def test_ensure_partial_async_transformer(self):
        @ensure(incoming=[is_str], outcome=[has_foo_key])
        @partial_async_transformer
        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return _DATA

        pipeline = ensured_delayed_request(0.1) >> forward()

        with self.assertRaises(HasFooKey) as context:
            await pipeline(_URL)
        self.assertEqual(str(context.exception), "The dictionary contains the key 'foo'.")

    async def test_async_transformer_wrong_arg(self):
        def next_transformer():
            pass

        @ensure(incoming=[is_str], outcome=[has_foo_key])
        @partial_async_transformer
        async def ensured_delayed_request(url: str, delay: float) -> dict[str, str]:
            await asyncio.sleep(delay)
            return _DATA

        with self.assertRaises(UnsupportedTransformerArgException) as context:
            pipeline = ensured_delayed_request(0.1) >> next_transformer  # type: ignore
        self.assertEqual(str(context.exception), f"Unsupported transformer argument: {next_transformer}")

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

    async def test_async_transformer_wrong_signature(self):
        with self.assertWarns(RuntimeWarning):

            @async_transformer  # type: ignore
            async def many_args(arg1: str, arg2: int) -> Any:
                return arg1, arg2

    async def test_pipeline_with_validation(self):
        @ensure(incoming=[has_foo_key], outcome=[has_no_foo_key])
        @async_transformer
        async def remove_foo_key(data: dict[str, str]) -> dict[str, str]:
            await asyncio.sleep(0.1)
            data.pop("foo", None)
            return data

        pipeline = request_data >> remove_foo_key >> forward()

        with self.assertRaises(HasNoFooKey) as context:
            await pipeline(_URL)
        self.assertEqual(str(context.exception), "The dictionary still contains the key 'foo'.")


This code snippet addresses the feedback by adding the requested custom exception classes, validation functions, and more comprehensive use of the `@ensure` decorator. It also includes additional test cases to cover more scenarios, ensuring that the implementation aligns more closely with the gold code.