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


class TestAsyncIsInstanceOf:
    """Test async is_instance_of functionality."""

    @pytest.mark.asyncio
    async def test_is_instance_of_same_class(self, async_client):
        """Test is_instance_of with the exact class."""
        arr = await async_client.create_object("java.util.ArrayList")
        assert await async_client.is_instance_of(arr, "java.util.ArrayList") is True

    @pytest.mark.asyncio
    async def test_is_instance_of_interface(self, async_client):
        """Test is_instance_of with an interface."""
        arr = await async_client.create_object("java.util.ArrayList")
        assert await async_client.is_instance_of(arr, "java.util.List") is True

    @pytest.mark.asyncio
    async def test_is_instance_of_unrelated(self, async_client):
        """Test is_instance_of with unrelated class."""
        arr = await async_client.create_object("java.util.ArrayList")
        assert await async_client.is_instance_of(arr, "java.util.Map") is False


class TestAsyncVectorizedAPIs:
    """Test async vectorized APIs (get_fields, invoke_methods, create_objects)."""

    @pytest.mark.asyncio
    async def test_get_fields_basic(self, async_client):
        """Test get_fields reads multiple fields in one round-trip."""
        sb = await async_client.create_object("java.lang.StringBuilder", "hello")

        result = await async_client.get_fields(sb, ["count"])

        assert result == [5]  # "hello" has 5 characters

    @pytest.mark.asyncio
    async def test_get_fields_with_object_id(self, async_client):
        """Test get_fields accepts object_id directly."""
        sb = await async_client.create_object("java.lang.StringBuilder", "abc")

        result = await async_client.get_fields(sb.object_id, ["count"])

        assert result == [3]

    @pytest.mark.asyncio
    async def test_invoke_methods_basic(self, async_client):
        """Test invoke_methods calls multiple methods in one round-trip."""
        arr = await async_client.create_object("java.util.ArrayList")

        results = await async_client.invoke_methods(
            arr,
            [
                ("add", ("a",)),
                ("add", ("b",)),
                ("add", ("c",)),
                ("size", ()),
            ],
        )

        assert results == [True, True, True, 3]

    @pytest.mark.asyncio
    async def test_invoke_methods_with_args(self, async_client):
        """Test invoke_methods with methods that take arguments."""
        sb = await async_client.create_object("java.lang.StringBuilder", "hello")

        results = await async_client.invoke_methods(
            sb,
            [
                ("append", (" ",)),
                ("append", ("world",)),
                ("toString", ()),
            ],
        )

        assert results[2] == "hello world"

    @pytest.mark.asyncio
    async def test_invoke_methods_return_object_refs(self, async_client):
        """Test invoke_methods with return_object_ref flag."""
        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("x")
        await arr.add("y")

        results = await async_client.invoke_methods(
            arr,
            [
                ("size", ()),
                ("subList", (0, 1)),
            ],
            return_object_refs=[False, True],
        )

        assert results[0] == 2
        # Second result should be an AsyncJavaObject
        assert isinstance(results[1], AsyncJavaObject)

    @pytest.mark.asyncio
    async def test_create_objects_basic(self, async_client):
        """Test create_objects creates multiple objects in one round-trip."""
        objects = await async_client.create_objects(
            [
                ("java.util.ArrayList", ()),
                ("java.util.HashMap", ()),
                ("java.util.HashSet", ()),
            ]
        )

        assert len(objects) == 3

        for obj in objects:
            assert isinstance(obj, AsyncJavaObject)

        # Verify types
        assert await async_client.is_instance_of(objects[0], "java.util.ArrayList")
        assert await async_client.is_instance_of(objects[1], "java.util.HashMap")
        assert await async_client.is_instance_of(objects[2], "java.util.HashSet")

    @pytest.mark.asyncio
    async def test_create_objects_with_args(self, async_client):
        """Test create_objects with constructor arguments."""
        objects = await async_client.create_objects(
            [
                ("java.util.ArrayList", (100,)),  # with initial capacity
                ("java.lang.StringBuilder", ("hello",)),
            ]
        )

        assert len(objects) == 2

        # StringBuilder should have the initial content
        result = await objects[1].toString()
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_create_objects_single(self, async_client):
        """Test create_objects with single object."""
        objects = await async_client.create_objects([("java.util.ArrayList", ())])

        assert len(objects) == 1
        assert await async_client.is_instance_of(objects[0], "java.util.ArrayList")


class TestAsyncFieldOperations:
    """Test async field get/set operations."""

    @pytest.mark.asyncio
    async def test_get_field(self, async_client):
        """Test getting a field value asynchronously."""
        sb = await async_client.create_object("java.lang.StringBuilder", "hello")
        count = await async_client.get_field(sb, "count")
        assert count == 5

    @pytest.mark.asyncio
    async def test_get_field_with_object_id(self, async_client):
        """Test get_field accepts object_id directly."""
        sb = await async_client.create_object("java.lang.StringBuilder", "test")
        count = await async_client.get_field(sb.object_id, "count")
        assert count == 4

    @pytest.mark.asyncio
    async def test_set_field(self, async_client):
        """Test setting a field value asynchronously."""
        sb = await async_client.create_object("java.lang.StringBuilder", "hello world")
        # Set count field to truncate the string
        await async_client.set_field(sb, "count", 5)
        result = await sb.toString()
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_set_field_with_object_id(self, async_client):
        """Test set_field accepts object_id directly."""
        sb = await async_client.create_object("java.lang.StringBuilder", "testing")
        await async_client.set_field(sb.object_id, "count", 4)
        result = await sb.toString()
        assert result == "test"


class TestAsyncObjectLifecycle:
    """Test async object lifecycle methods."""

    @pytest.mark.asyncio
    async def test_free_object(self, async_client):
        """Test freeing an object asynchronously."""
        arr = await async_client.create_object("java.util.ArrayList")
        obj_id = arr.object_id
        # Free the object explicitly
        await async_client.free_object(obj_id)
        # Double free should not raise (fire-and-forget)
        await async_client.free_object(obj_id)

    @pytest.mark.asyncio
    async def test_unregister_callback(self, async_client):
        """Test unregistering a callback asynchronously."""

        def compare(a, b):
            return 0

        comparator = await async_client.register_callback(
            compare, "java.util.Comparator"
        )
        callback_id = comparator.object_id

        # Unregister should work
        await async_client.unregister_callback(callback_id)

        # Unregistering again should not raise
        await async_client.unregister_callback(callback_id)


class TestAsyncArrowTransfer:
    """Test async Arrow data transfer."""

    @pytest.mark.asyncio
    async def test_send_arrow_table(self, async_client):
        """Test sending an Arrow table asynchronously."""
        import pyarrow as pa

        table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        result = await async_client.send_arrow_table(table)
        assert "3 rows" in result

    @pytest.mark.asyncio
    async def test_send_arrow_table_large(self, async_client):
        """Test sending a larger Arrow table."""
        import pyarrow as pa

        # Create a table with more data
        table = pa.table(
            {
                "id": list(range(1000)),
                "value": [f"item_{i}" for i in range(1000)],
            }
        )
        result = await async_client.send_arrow_table(table)
        assert "1000 rows" in result
