"""Tests for async client functionality."""

import pytest

from gatun.async_client import AsyncGatunClient, AsyncJavaObject, run_sync


@pytest.fixture
async def async_client(java_gateway):
    """Provides an async client connected to the test gateway."""
    client = AsyncGatunClient(java_gateway.socket_path)
    connected = await client.connect()
    assert connected, "Failed to connect async client"

    yield client

    await client.close()


class TestAsyncClientBasics:
    """Test basic async client operations."""

    @pytest.mark.asyncio
    async def test_create_object(self, async_client):
        """Test creating a Java object asynchronously."""
        arr = await async_client.create_object("java.util.ArrayList")
        assert isinstance(arr, AsyncJavaObject)

    @pytest.mark.asyncio
    async def test_invoke_method(self, async_client):
        """Test invoking a method asynchronously."""
        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("hello")
        await arr.add("world")
        size = await arr.size()
        assert size == 2

    @pytest.mark.asyncio
    async def test_invoke_static_method(self, async_client):
        """Test invoking a static method asynchronously."""
        result = await async_client.invoke_static_method(
            "java.lang.Integer", "parseInt", "42"
        )
        assert result == 42

    @pytest.mark.asyncio
    async def test_get_method_result(self, async_client):
        """Test getting results from method calls."""
        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("first")
        await arr.add("second")

        first = await arr.get(0)
        assert first == "first"

        second = await arr.get(1)
        assert second == "second"


class TestAsyncJVMView:
    """Test async JVM view for package navigation."""

    @pytest.mark.asyncio
    async def test_jvm_view_create_object(self, async_client):
        """Test creating objects via JVM view."""
        ArrayList = async_client.jvm.java.util.ArrayList
        arr = await ArrayList()
        assert isinstance(arr, AsyncJavaObject)

    @pytest.mark.asyncio
    async def test_jvm_view_static_method(self, async_client):
        """Test calling static methods via JVM view."""
        Integer = async_client.jvm.java.lang.Integer
        result = await Integer.parseInt("123")
        assert result == 123

    @pytest.mark.asyncio
    async def test_jvm_view_math_operations(self, async_client):
        """Test Math static methods via JVM view."""
        Math = async_client.jvm.java.lang.Math
        max_val = await Math.max(10, 20)
        assert max_val == 20

        abs_val = await Math.abs(-42)
        assert abs_val == 42


class TestAsyncContextManager:
    """Test async context manager support."""

    @pytest.mark.asyncio
    async def test_client_context_manager(self, java_gateway):
        """Test AsyncGatunClient as async context manager."""
        async with AsyncGatunClient(java_gateway.socket_path) as client:
            arr = await client.create_object("java.util.ArrayList")
            await arr.add("test")
            size = await arr.size()
            assert size == 1


class TestAsyncCallbacks:
    """Test async callback functionality."""

    @pytest.mark.asyncio
    async def test_sync_callback(self, async_client):
        """Test registering a sync callback."""

        def compare(a, b):
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        comparator = await async_client.register_callback(
            compare, "java.util.Comparator"
        )
        assert isinstance(comparator, AsyncJavaObject)

        # Use it to sort
        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("c")
        await arr.add("a")
        await arr.add("b")

        await async_client.invoke_static_method(
            "java.util.Collections", "sort", arr, comparator
        )

        assert await arr.get(0) == "a"
        assert await arr.get(1) == "b"
        assert await arr.get(2) == "c"

    @pytest.mark.asyncio
    async def test_async_callback(self, async_client):
        """Test registering an async callback."""

        async def async_compare(a, b):
            # Simulate async work
            await asyncio.sleep(0)
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        import asyncio

        comparator = await async_client.register_callback(
            async_compare, "java.util.Comparator"
        )

        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("z")
        await arr.add("m")
        await arr.add("a")

        await async_client.invoke_static_method(
            "java.util.Collections", "sort", arr, comparator
        )

        assert await arr.get(0) == "a"
        assert await arr.get(1) == "m"
        assert await arr.get(2) == "z"


class TestRunSync:
    """Test run_sync utility for running sync client in async context."""

    @pytest.mark.asyncio
    async def test_run_sync_create_object(self, client):
        """Test run_sync with create_object."""
        arr = await run_sync(client.create_object, "java.util.ArrayList")
        assert arr is not None

    @pytest.mark.asyncio
    async def test_run_sync_method_call(self, client):
        """Test run_sync with method calls."""
        arr = await run_sync(client.create_object, "java.util.ArrayList")
        await run_sync(arr.add, "hello")
        size = await run_sync(arr.size)
        assert size == 1

    @pytest.mark.asyncio
    async def test_run_sync_static_method(self, client):
        """Test run_sync with static method calls."""
        result = await run_sync(
            client.invoke_static_method,
            "java.lang.Integer",
            "parseInt",
            "99",
        )
        assert result == 99


class TestAsyncHashMap:
    """Test async operations with HashMap."""

    @pytest.mark.asyncio
    async def test_hashmap_operations(self, async_client):
        """Test HashMap put/get operations."""
        hm = await async_client.create_object("java.util.HashMap")

        await hm.put("key1", "value1")
        await hm.put("key2", "value2")

        v1 = await hm.get("key1")
        v2 = await hm.get("key2")

        assert v1 == "value1"
        assert v2 == "value2"

        size = await hm.size()
        assert size == 2


class TestAsyncStringBuilder:
    """Test async operations with StringBuilder."""

    @pytest.mark.asyncio
    async def test_stringbuilder_chaining(self, async_client):
        """Test StringBuilder append operations."""
        sb = await async_client.create_object("java.lang.StringBuilder")

        await sb.append("Hello")
        await sb.append(" ")
        await sb.append("World")

        result = await sb.toString()
        assert result == "Hello World"
