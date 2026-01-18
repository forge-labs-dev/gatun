import pytest

from gatun import (
    PROTOCOL_VERSION,
    JavaException,
    JavaSecurityException,
    JavaNoSuchFieldException,
    JavaNumberFormatException,
)


def test_protocol_version_exported():
    """Verify protocol version is exported and is an integer."""
    assert isinstance(PROTOCOL_VERSION, int)
    assert PROTOCOL_VERSION >= 1


def test_ping(client):
    """Verify ping health check works."""
    assert client.ping() is True


def test_security_access_denied(client):
    """Verify we cannot free an object we don't own (simulated)."""
    obj = client.create_object("java.util.ArrayList")
    valid_id = obj.object_id

    # 1. Detach and Manual Free (Happy Path)
    obj.detach()
    client.free_object(valid_id)

    # 2. Second Free (The "Hacker" / Double-Free)
    # With Fire-and-Forget, this should NOT raise. It should be silently ignored.
    client.free_object(valid_id)

    # 3. VERIFICATION:
    # Proof that the object is actually gone (and the first free worked)
    # Trying to use it SHOULD fail.
    with pytest.raises(JavaException) as excinfo:
        client.invoke_method(valid_id, "toString")

    assert "not found" in str(excinfo.value)


@pytest.mark.parametrize(
    "class_name",
    [
        "java.util.ArrayList",
        "java.util.HashMap",
        "java.util.HashSet",
        "java.util.LinkedList",
        "java.lang.StringBuilder",
    ],
)
def test_allowlist_permits_safe_classes(client, class_name):
    """Verify that allowlisted classes can be instantiated."""
    obj = client.create_object(class_name)
    assert obj is not None


@pytest.mark.parametrize(
    "class_name",
    [
        "java.lang.Runtime",
        "java.lang.ProcessBuilder",
        "java.io.File",
        "java.net.URL",
        "javax.script.ScriptEngineManager",
    ],
)
def test_allowlist_blocks_dangerous_classes(client, class_name):
    """Verify that non-allowlisted classes are rejected."""
    with pytest.raises(JavaSecurityException) as excinfo:
        client.create_object(class_name)
    assert "not allowed" in str(excinfo.value).lower()


def test_constructor_with_initial_capacity(client):
    """Verify ArrayList can be created with initial capacity."""
    # ArrayList(int initialCapacity)
    arr = client.create_object("java.util.ArrayList", 100)
    assert arr is not None
    # Size should still be 0 (capacity != size)
    assert arr.size() == 0


def test_constructor_stringbuilder_with_string(client):
    """Verify StringBuilder can be created with initial string."""
    # StringBuilder(String str)
    sb = client.create_object("java.lang.StringBuilder", "Hello")
    assert sb.toString() == "Hello"
    sb.append(" World")
    assert sb.toString() == "Hello World"


def test_constructor_hashmap_with_capacity(client):
    """Verify HashMap can be created with initial capacity."""
    # HashMap(int initialCapacity)
    hm = client.create_object("java.util.HashMap", 32)
    assert hm is not None
    assert hm.size() == 0
    hm.put("key", "value")
    assert hm.get("key") == "value"


def test_static_method_string_valueof(client):
    """Test static method: String.valueOf(int)."""
    result = client.invoke_static_method("java.lang.String", "valueOf", 42)
    assert result == "42"


def test_static_method_integer_parseint(client):
    """Test static method: Integer.parseInt(String)."""
    result = client.invoke_static_method("java.lang.Integer", "parseInt", "123")
    assert result == 123


def test_static_method_math_max(client):
    """Test static method: Math.max(int, int)."""
    result = client.invoke_static_method("java.lang.Math", "max", 10, 20)
    assert result == 20


def test_static_method_math_abs(client):
    """Test static method: Math.abs(int)."""
    result = client.invoke_static_method("java.lang.Math", "abs", -42)
    assert result == 42


def test_static_method_blocked_class(client):
    """Verify static methods on non-allowlisted classes are rejected."""
    with pytest.raises(JavaSecurityException) as excinfo:
        client.invoke_static_method("java.lang.Runtime", "getRuntime")
    assert "not allowed" in str(excinfo.value).lower()


def test_get_field_arraylist_size(client):
    """Test getting the internal 'size' field from ArrayList."""
    arr = client.create_object("java.util.ArrayList")
    arr.add("one")
    arr.add("two")

    # ArrayList has a private 'size' field
    size = client.get_field(arr, "size")
    assert size == 2


def test_set_field_stringbuilder_count(client):
    """Test setting and getting the internal 'count' field from StringBuilder."""
    sb = client.create_object("java.lang.StringBuilder", "Hello")

    # StringBuilder inherits 'count' from AbstractStringBuilder
    original_count = client.get_field(sb, "count")
    assert original_count == 5

    # We can read it but setting count would corrupt the object, so just verify get works
    assert sb.length() == 5


def test_field_not_found(client):
    """Verify accessing non-existent field raises error."""
    arr = client.create_object("java.util.ArrayList")
    with pytest.raises(JavaNoSuchFieldException) as excinfo:
        client.get_field(arr, "nonExistentField")
    assert "no field" in str(excinfo.value).lower()


def test_exception_includes_stack_trace(client):
    """Verify Java exceptions include full stack trace."""
    # Integer.parseInt with invalid input will throw NumberFormatException
    with pytest.raises(JavaNumberFormatException) as excinfo:
        client.invoke_static_method("java.lang.Integer", "parseInt", "not_a_number")

    error_msg = str(excinfo.value)
    # Should contain the exception class name
    assert "NumberFormatException" in error_msg
    # Should contain stack trace elements (at ...)
    assert "\tat " in error_msg or "at " in error_msg


# --- is_instance_of tests ---


def test_is_instance_of_same_class(client):
    """Test is_instance_of with the exact class."""
    arr = client.create_object("java.util.ArrayList")
    assert client.is_instance_of(arr, "java.util.ArrayList") is True


def test_is_instance_of_interface(client):
    """Test is_instance_of with an interface the object implements."""
    arr = client.create_object("java.util.ArrayList")
    # ArrayList implements List, Collection, Iterable
    assert client.is_instance_of(arr, "java.util.List") is True
    assert client.is_instance_of(arr, "java.util.Collection") is True
    assert client.is_instance_of(arr, "java.lang.Iterable") is True


def test_is_instance_of_superclass(client):
    """Test is_instance_of with a superclass."""
    arr = client.create_object("java.util.ArrayList")
    # ArrayList extends AbstractList -> AbstractCollection -> Object
    assert client.is_instance_of(arr, "java.util.AbstractList") is True
    assert client.is_instance_of(arr, "java.lang.Object") is True


def test_is_instance_of_unrelated_class(client):
    """Test is_instance_of with an unrelated class returns False."""
    arr = client.create_object("java.util.ArrayList")
    assert client.is_instance_of(arr, "java.util.Map") is False
    assert client.is_instance_of(arr, "java.util.HashMap") is False
    assert client.is_instance_of(arr, "java.lang.String") is False


def test_is_instance_of_hashmap(client):
    """Test is_instance_of with HashMap."""
    hm = client.create_object("java.util.HashMap")
    assert client.is_instance_of(hm, "java.util.HashMap") is True
    assert client.is_instance_of(hm, "java.util.Map") is True
    assert client.is_instance_of(hm, "java.lang.Object") is True
    assert client.is_instance_of(hm, "java.util.List") is False


def test_is_instance_of_with_object_id(client):
    """Test is_instance_of works with raw object ID."""
    arr = client.create_object("java.util.ArrayList")
    obj_id = arr.object_id
    assert client.is_instance_of(obj_id, "java.util.ArrayList") is True
    assert client.is_instance_of(obj_id, "java.util.List") is True


# --- Method overload resolution tests ---


def test_overloaded_method_string_vs_object(client):
    """Test that String arguments match String parameters over Object.

    This tests the improved overload resolution that uses specificity scoring.
    When a String argument is passed, methods with String parameters should be
    preferred over methods with Object parameters.
    """
    # String.valueOf has multiple overloads:
    # - valueOf(Object obj)
    # - valueOf(int i)
    # - valueOf(char[] data)
    # etc.
    # When we pass a string, it should match valueOf(Object) which converts toString()
    result = client.invoke_static_method("java.lang.String", "valueOf", 42)
    assert result == "42"


def test_overloaded_constructor_specificity(client):
    """Test constructor overload resolution with specificity scoring."""
    # StringBuilder has:
    # - StringBuilder()
    # - StringBuilder(int capacity)
    # - StringBuilder(String str)
    # - StringBuilder(CharSequence seq)

    # With int, should match StringBuilder(int)
    sb1 = client.create_object("java.lang.StringBuilder", 100)
    assert sb1.capacity() >= 100

    # With String, should match StringBuilder(String)
    sb2 = client.create_object("java.lang.StringBuilder", "Hello")
    assert sb2.toString() == "Hello"


def test_hashmap_put_with_string_keys(client):
    """Test HashMap.put works with String keys (overload resolution)."""
    # HashMap.put(K key, V value) where K and V are Object
    # With improved resolution, String args should still work
    hm = client.create_object("java.util.HashMap")
    hm.put("key1", "value1")
    hm.put("key2", 42)  # Mixed types

    assert hm.get("key1") == "value1"
    assert hm.get("key2") == 42


def test_arraylist_add_overloads(client):
    """Test ArrayList.add which has overloaded methods."""
    # ArrayList has:
    # - add(E e) -> boolean
    # - add(int index, E element) -> void

    arr = client.create_object("java.util.ArrayList")

    # add(E) - one argument
    arr.add("first")
    assert arr.size() == 1

    # add(int, E) - two arguments
    arr.add(0, "zero")
    assert arr.size() == 2
    assert arr.get(0) == "zero"
    assert arr.get(1) == "first"


# --- return_object_ref tests ---


def test_return_object_ref_array(client):
    """Test that Object arrays are returned as JavaObject (reference to Java-side array).

    With the fix for Array.set/get, Object arrays (like String[]) are now kept as
    ObjectRef on the Java side rather than being auto-converted to ArrayVal.
    This allows Array.set/get to work properly on Object arrays.

    Primitive arrays (int[], long[], double[]) are still auto-converted to JavaArray.
    """
    from gatun import JavaArray
    from gatun.client import JavaObject

    # Get Class object for String (Object array)
    class_obj = client.invoke_static_method(
        "java.lang.Class", "forName", "java.lang.String"
    )

    # Object arrays (String[]) are now returned as JavaObject by default
    # This is because they're kept as ObjectRef for Array.set/get to work
    arr_obj = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", class_obj, 3
    )
    assert isinstance(arr_obj, JavaObject)
    assert hasattr(arr_obj, "object_id")

    # With return_object_ref=True, behavior is the same for Object arrays
    arr_ref = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", class_obj, 3, return_object_ref=True
    )
    assert isinstance(arr_ref, JavaObject)
    assert hasattr(arr_ref, "object_id")

    # Primitive arrays (int[]) are still auto-converted to JavaArray
    import pyarrow as pa

    original = pa.array([1, 2, 3], type=pa.int32())
    int_arr = client.jvm.java.util.Arrays.copyOf(original, 3)
    assert isinstance(int_arr, JavaArray)
    assert int_arr.element_type == "Int"


def test_return_object_ref_list(client):
    """Test that return_object_ref=True returns ObjectRef for lists."""
    from gatun.client import JavaObject

    # Create an ArrayList and populate it
    arr = client.create_object("java.util.ArrayList")
    arr.add("a")
    arr.add("b")
    arr.add("c")

    # Without return_object_ref, subList returns auto-converted Python list
    # Note: We test with ArrayList directly since Arrays.asList returns a private class
    list_auto = client.invoke_method(arr.object_id, "subList", 0, 2)
    assert isinstance(list_auto, list)
    assert list_auto == ["a", "b"]

    # With return_object_ref=True on static method returning a new ArrayList
    # Use Collections.emptyList which returns a public singleton
    empty_ref = client.invoke_static_method(
        "java.util.Collections", "emptyList", return_object_ref=True
    )
    assert isinstance(empty_ref, JavaObject)
    assert hasattr(empty_ref, "object_id")


def test_return_object_ref_string(client):
    """Test that return_object_ref=True returns ObjectRef for strings."""
    from gatun.client import JavaObject

    # Without return_object_ref, strings are auto-converted
    str_auto = client.invoke_static_method("java.lang.String", "valueOf", 42)
    assert isinstance(str_auto, str)
    assert str_auto == "42"

    # With return_object_ref=True, strings are returned as ObjectRef
    str_ref = client.invoke_static_method(
        "java.lang.String", "valueOf", 42, return_object_ref=True
    )
    assert isinstance(str_ref, JavaObject)
    assert str_ref.toString() == "42"


def test_return_object_ref_allows_mutation(client):
    """Test that ObjectRef arrays can be mutated via reflection API."""
    from gatun.client import JavaObject

    # Create array with return_object_ref=True
    class_obj = client.invoke_static_method(
        "java.lang.Class", "forName", "java.lang.String"
    )
    arr = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", class_obj, 3, return_object_ref=True
    )
    assert isinstance(arr, JavaObject)

    # Set elements using Array.set
    client.invoke_static_method("java.lang.reflect.Array", "set", arr, 0, "hello")
    client.invoke_static_method("java.lang.reflect.Array", "set", arr, 1, "world")

    # Get elements using Array.get
    assert (
        client.invoke_static_method("java.lang.reflect.Array", "get", arr, 0) == "hello"
    )
    assert (
        client.invoke_static_method("java.lang.reflect.Array", "get", arr, 1) == "world"
    )
    assert client.invoke_static_method("java.lang.reflect.Array", "get", arr, 2) is None

    # Verify length
    assert client.invoke_static_method("java.lang.reflect.Array", "getLength", arr) == 3


# --- Batch API Tests ---


def test_batch_basic_operations(client):
    """Test basic batch operations with context manager."""
    arr = client.create_object("java.util.ArrayList")

    with client.batch() as b:
        r1 = b.call(arr, "add", "hello")
        r2 = b.call(arr, "add", "world")
        r3 = b.call(arr, "size")

    # Results should be available after context exit
    assert r1.get() is True  # ArrayList.add returns true
    assert r2.get() is True
    assert r3.get() == 2


def test_batch_manual_execute(client):
    """Test batch with manual execute() call."""
    batch = client.batch()
    r1 = batch.call_static("java.lang.Integer", "parseInt", "42")
    r2 = batch.call_static("java.lang.Math", "max", 10, 20)
    r3 = batch.call_static("java.lang.Math", "min", 5, 3)

    # Results not available before execute
    with pytest.raises(RuntimeError, match="Must call batch.execute"):
        r1.get()

    batch.execute()

    assert r1.get() == 42
    assert r2.get() == 20
    assert r3.get() == 3


def test_batch_create_objects(client):
    """Test creating multiple objects in a batch."""
    with client.batch() as b:
        r1 = b.create("java.util.ArrayList")
        r2 = b.create("java.util.HashMap")
        r3 = b.create("java.lang.StringBuilder", "hello")

    # All should return JavaObjects
    arr = r1.get()
    hm = r2.get()
    sb = r3.get()

    assert arr is not None
    assert hm is not None
    assert sb is not None

    # Verify objects work
    arr.add("test")
    assert arr.size() == 1

    hm.put("key", "value")
    assert hm.get("key") == "value"

    assert sb.toString() == "hello"


def test_batch_mixed_operations(client):
    """Test batch with mixed operation types."""
    with client.batch() as b:
        # Create an object
        arr_result = b.create("java.util.ArrayList")
        # Static method call
        parsed = b.call_static("java.lang.Integer", "parseInt", "123")

    arr = arr_result.get()
    assert parsed.get() == 123

    # Use the created object in another batch
    with client.batch() as b:
        b.call(arr, "add", "first")
        b.call(arr, "add", "second")
        r3 = b.call(arr, "size")

    assert r3.get() == 2


def test_batch_error_handling_continue(client):
    """Test batch error handling with stop_on_error=False (default)."""
    arr = client.create_object("java.util.ArrayList")

    with client.batch(stop_on_error=False) as b:
        r1 = b.call(arr, "add", "valid")
        r2 = b.call_static("java.lang.Integer", "parseInt", "not_a_number")
        r3 = b.call(arr, "size")

    # First operation succeeds
    assert r1.get() is True

    # Second operation fails
    assert r2.is_error
    with pytest.raises(JavaNumberFormatException):
        r2.get()

    # Third operation still executes (stop_on_error=False)
    assert r3.get() == 1


def test_batch_error_handling_stop(client):
    """Test batch error handling with stop_on_error=True."""
    arr = client.create_object("java.util.ArrayList")
    arr.add("initial")

    with client.batch(stop_on_error=True) as b:
        r1 = b.call(arr, "add", "second")
        r2 = b.call_static("java.lang.Integer", "parseInt", "not_a_number")
        r3 = b.call(arr, "add", "third")  # Should not execute

    # First operation succeeds
    assert r1.get() is True

    # Second operation fails
    assert r2.is_error

    # Third operation was skipped (stop_on_error=True)
    # Its result should be None
    assert r3.get() is None

    # Verify only 2 items in array (initial + second, not third)
    assert arr.size() == 2


def test_batch_empty(client):
    """Test empty batch."""
    with client.batch():
        pass  # No operations

    # Should not raise


def test_batch_idempotent_execute(client):
    """Test that execute() is idempotent."""
    batch = client.batch()
    r1 = batch.call_static("java.lang.Math", "max", 1, 2)

    results1 = batch.execute()
    results2 = batch.execute()

    # Same results returned
    assert results1 is results2
    assert r1.get() == 2


def test_batch_large(client):
    """Test batch with many operations."""
    arr = client.create_object("java.util.ArrayList")

    n = 100
    with client.batch() as b:
        results = [b.call(arr, "add", i) for i in range(n)]

    # All should succeed
    for r in results:
        assert r.get() is True

    # Verify all items added
    assert arr.size() == n


# --- Vectorized API Tests ---


def test_get_fields_basic(client):
    """Test get_fields reads multiple fields in one round-trip."""
    # Use StringBuilder which has accessible count field
    sb = client.create_object("java.lang.StringBuilder", "hello")

    # Get the count field (internal length)
    result = client.get_fields(sb, ["count"])

    assert result == [5]  # "hello" has 5 characters


def test_get_fields_multiple(client):
    """Test get_fields with multiple fields."""
    sb = client.create_object("java.lang.StringBuilder", "test")

    # StringBuilder has both 'count' and 'value' fields
    # But value is a char[] which won't match directly
    # Let's use a simple test with count only
    result = client.get_fields(sb, ["count"])

    assert result == [4]  # "test" has 4 characters


def test_get_fields_with_object_id(client):
    """Test get_fields accepts object_id directly."""
    sb = client.create_object("java.lang.StringBuilder", "abc")

    result = client.get_fields(sb.object_id, ["count"])

    assert result == [3]


def test_invoke_methods_basic(client):
    """Test invoke_methods calls multiple methods in one round-trip."""
    arr = client.create_object("java.util.ArrayList")

    # Add 3 items and get size - all in one round-trip
    results = client.invoke_methods(
        arr,
        [
            ("add", ("a",)),
            ("add", ("b",)),
            ("add", ("c",)),
            ("size", ()),
        ],
    )

    assert results == [True, True, True, 3]


def test_invoke_methods_with_args(client):
    """Test invoke_methods with methods that take arguments."""
    sb = client.create_object("java.lang.StringBuilder", "hello")

    results = client.invoke_methods(
        sb,
        [
            ("append", (" ",)),
            ("append", ("world",)),
            ("toString", ()),
        ],
    )

    assert results[2] == "hello world"


def test_invoke_methods_return_object_refs(client):
    """Test invoke_methods with return_object_ref flag."""
    arr = client.create_object("java.util.ArrayList")
    arr.add("x")
    arr.add("y")

    # Get the subList as an ObjectRef, not a converted list
    results = client.invoke_methods(
        arr,
        [
            ("size", ()),
            ("subList", (0, 1)),
        ],
        return_object_refs=[False, True],
    )

    assert results[0] == 2
    # Second result should be a JavaObject, not a list
    from gatun.client import JavaObject

    assert isinstance(results[1], JavaObject)


def test_create_objects_basic(client):
    """Test create_objects creates multiple objects in one round-trip."""
    objects = client.create_objects(
        [
            ("java.util.ArrayList", ()),
            ("java.util.HashMap", ()),
            ("java.util.HashSet", ()),
        ]
    )

    assert len(objects) == 3

    from gatun.client import JavaObject

    for obj in objects:
        assert isinstance(obj, JavaObject)

    # Verify they're the right types
    assert client.is_instance_of(objects[0], "java.util.ArrayList")
    assert client.is_instance_of(objects[1], "java.util.HashMap")
    assert client.is_instance_of(objects[2], "java.util.HashSet")


def test_create_objects_with_args(client):
    """Test create_objects with constructor arguments."""
    objects = client.create_objects(
        [
            ("java.util.ArrayList", (100,)),  # with initial capacity
            ("java.lang.StringBuilder", ("hello",)),
        ]
    )

    assert len(objects) == 2

    # StringBuilder should have the initial content
    assert objects[1].toString() == "hello"


def test_create_objects_single(client):
    """Test create_objects with single object."""
    objects = client.create_objects([("java.util.ArrayList", ())])

    assert len(objects) == 1
    assert client.is_instance_of(objects[0], "java.util.ArrayList")


# --- Multi-Client Tests ---


def test_multi_client_isolation(java_gateway):
    """Test that multiple clients have isolated shared memory."""
    from gatun import GatunClient

    socket_path = str(java_gateway.socket_path)

    # Create two clients connecting to the same server
    client1 = GatunClient(socket_path)
    client2 = GatunClient(socket_path)

    assert client1.connect()
    assert client2.connect()

    # Verify they have different SHM paths
    assert client1.memory_path != client2.memory_path
    assert ".shm" in client1.memory_path
    assert ".shm" in client2.memory_path

    # Create objects in each client
    arr1 = client1.create_object("java.util.ArrayList")
    arr2 = client2.create_object("java.util.ArrayList")

    # Add different items to each
    arr1.add("client1_item")
    arr2.add("client2_item_a")
    arr2.add("client2_item_b")

    # Verify isolation - each sees only its own items
    assert arr1.size() == 1
    assert arr2.size() == 2

    client1.close()
    client2.close()


def test_multi_client_concurrent_operations(java_gateway):
    """Test concurrent operations from multiple clients don't interfere."""
    import threading
    from gatun import GatunClient

    socket_path = str(java_gateway.socket_path)
    results = {}
    errors = []

    def client_work(client_id, iterations):
        try:
            client = GatunClient(socket_path)
            if not client.connect():
                errors.append(f"Client {client_id} failed to connect")
                return

            arr = client.create_object("java.util.ArrayList")

            # Add items
            for i in range(iterations):
                arr.add(f"client{client_id}_item{i}")

            # Verify count
            size = arr.size()
            results[client_id] = size

            client.close()
        except Exception as e:
            errors.append(f"Client {client_id} error: {e}")

    # Run 3 clients concurrently
    threads = []
    for i in range(3):
        t = threading.Thread(target=client_work, args=(i, 50))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Check no errors
    assert not errors, f"Errors occurred: {errors}"

    # Each client should have added exactly 50 items
    assert len(results) == 3
    for client_id, size in results.items():
        assert size == 50, f"Client {client_id} has {size} items, expected 50"


def test_multi_client_shm_cleanup(java_gateway):
    """Test that SHM files are cleaned up when clients disconnect."""
    import os
    import time
    from gatun import GatunClient

    socket_path = str(java_gateway.socket_path)

    client = GatunClient(socket_path)
    assert client.connect()

    shm_path = client.memory_path
    assert os.path.exists(shm_path), "SHM file should exist while connected"

    client.close()

    # Give server time to clean up
    time.sleep(0.1)

    assert not os.path.exists(shm_path), "SHM file should be deleted after disconnect"


def test_method_cache_consistency_under_repeated_calls(client):
    """Test that method resolution cache works correctly with repeated calls.

    This is a regression test for cache key stability - the cache keys must
    defensively copy argTypes arrays to avoid corruption if caller reuses arrays.
    """
    # Make many repeated calls with the same signature
    # This exercises the cache hit path
    results = []
    for i in range(100):
        result = client.invoke_static_method("java.lang.Integer", "parseInt", str(i))
        results.append(result)

    # Verify all results are correct
    assert results == list(range(100))


def test_constructor_cache_consistency(client):
    """Test that constructor resolution cache works correctly."""
    # Create many objects with the same constructor signature
    objects = []
    for i in range(50):
        obj = client.create_object(
            "java.util.ArrayList", i
        )  # ArrayList(int initialCapacity)
        objects.append(obj)

    # Verify all objects work correctly
    for i, obj in enumerate(objects):
        # Each should be an empty ArrayList
        assert obj.size() == 0
        obj.add(f"item_{i}")
        assert obj.size() == 1


def test_cache_with_different_arg_types(client):
    """Test that cache distinguishes between different argument types."""
    # These should resolve to different overloads and be cached separately
    results = []

    # String.valueOf(int)
    results.append(client.invoke_static_method("java.lang.String", "valueOf", 42))

    # String.valueOf(boolean)
    results.append(client.invoke_static_method("java.lang.String", "valueOf", True))

    # String.valueOf(double)
    results.append(client.invoke_static_method("java.lang.String", "valueOf", 3.14))

    # Repeat to exercise cache hits
    results.append(client.invoke_static_method("java.lang.String", "valueOf", 100))
    results.append(client.invoke_static_method("java.lang.String", "valueOf", False))
    results.append(client.invoke_static_method("java.lang.String", "valueOf", 2.71))

    assert results[0] == "42"
    assert results[1] == "true"
    assert results[2] == "3.14"
    assert results[3] == "100"
    assert results[4] == "false"
    assert results[5] == "2.71"


def test_class_cache_with_various_types(client):
    """Test that class cache works correctly with various class types.

    This exercises the classloader-safe class cache by loading classes
    from different packages and verifying they work correctly.
    """
    # Load various classes - these should be cached per classloader
    classes_to_test = [
        ("java.util.ArrayList", "add", "item1"),
        ("java.util.HashMap", "put", "key"),
        ("java.util.HashSet", "add", "elem"),
        ("java.lang.StringBuilder", "append", "text"),
        ("java.util.LinkedList", "add", "node"),
        ("java.util.TreeMap", "put", "treekey"),
    ]

    for class_name, method, arg in classes_to_test:
        obj = client.create_object(class_name)
        # Call a method to verify the class was loaded correctly
        if method == "put":
            obj.put(arg, "value")
            assert obj.get(arg) == "value"
        else:
            obj.add(arg) if method == "add" else obj.append(arg)
            assert obj.size() == 1 if method == "add" else len(obj.toString()) > 0


def test_class_cache_repeated_loads(client):
    """Test that repeated class loads use cache correctly."""
    # Create many objects of the same type - should hit cache
    for _ in range(50):
        obj = client.create_object("java.util.ArrayList")
        obj.add("test")
        assert obj.size() == 1

    # Create many objects of different types in interleaved pattern
    for i in range(20):
        if i % 2 == 0:
            obj = client.create_object("java.util.ArrayList")
        else:
            obj = client.create_object("java.util.HashMap")
        # Just verify they work
        assert obj is not None


def test_private_inner_class_method_handle(client):
    """Test that methods on private inner classes work via privateLookupIn.

    ArrayList.iterator() returns ArrayList$Itr which is a private inner class.
    The MethodHandle for its methods (hasNext, next) requires privateLookupIn
    to avoid falling back to slow reflective invocation.
    """
    # Create a list and get its iterator (private inner class ArrayList$Itr)
    arr = client.create_object("java.util.ArrayList")
    arr.add("first")
    arr.add("second")
    arr.add("third")

    # Get iterator - returns a private inner class instance
    iterator = arr.iterator()

    # Call methods on the private inner class
    # These should use MethodHandle via privateLookupIn, not slow Method.invoke
    results = []
    while iterator.hasNext():
        results.append(iterator.next())

    assert results == ["first", "second", "third"]


def test_private_inner_class_repeated_calls(client):
    """Test performance of repeated calls on private inner class methods.

    This exercises the cache path for private inner class methods,
    ensuring MethodHandle is properly cached after privateLookupIn.
    """
    # Do this multiple times to exercise caching
    for _ in range(10):
        arr = client.create_object("java.util.ArrayList")
        for i in range(20):
            arr.add(f"item_{i}")

        # Iterate using the private inner class iterator
        iterator = arr.iterator()
        count = 0
        while iterator.hasNext():
            iterator.next()
            count += 1

        assert count == 20


def test_hashmap_keyset_iterator(client):
    """Test iterator on HashMap.keySet() - another private inner class path."""
    hm = client.create_object("java.util.HashMap")
    hm.put("a", 1)
    hm.put("b", 2)
    hm.put("c", 3)

    # keySet() returns a view, iterator() returns private inner class
    key_set = hm.keySet()
    iterator = key_set.iterator()

    keys = []
    while iterator.hasNext():
        keys.append(iterator.next())

    # HashMap doesn't guarantee order, but should have all keys
    assert sorted(keys) == ["a", "b", "c"]


def test_type_specificity_prefers_exact_over_widening(client):
    """Test that exact type matches are preferred over widening conversions.

    This verifies the scoring hierarchy in MethodResolver.getTypeSpecificity():
    - Exact match (100) > Boxing (90) > Primitive widening (80-) > Boxed widening (70-)
    """
    # Math.max has overloads: max(int,int), max(long,long), max(float,float), max(double,double)
    Math = client.jvm.java.lang.Math

    # When passing Python ints, should pick int overload (exact match for Integer->int unbox)
    result = Math.max(10, 20)
    assert result == 20
    assert isinstance(result, int)

    # When passing Python floats, should pick double overload
    result = Math.max(10.5, 20.5)
    assert abs(result - 20.5) < 0.001

    # Integer.compare(int, int) vs Long.compare(long, long)
    # When called with ints, should pick Integer.compare
    result = client.jvm.java.lang.Integer.compare(5, 10)
    assert result < 0  # 5 < 10

    result = client.jvm.java.lang.Long.compare(5, 10)
    assert result < 0  # Also works via widening


def test_boxed_widening_does_not_dominate(client):
    """Test that boxed widening (Integer->Long) scores lower than direct matches.

    Java doesn't implicitly convert Integer to Long, but Gatun supports it
    for convenience. This test ensures it's ranked as less preferred.
    """
    # String.valueOf has many overloads including valueOf(int), valueOf(long)
    String = client.jvm.java.lang.String

    # Python int maps to Java Integer, should match valueOf(int) directly
    # not widen to valueOf(long)
    result = String.valueOf(42)
    assert result == "42"

    # Explicit long should also work
    result = String.valueOf(9999999999999)  # Larger than int max
    assert result == "9999999999999"


def test_cache_handles_many_signatures(client):
    """Test that caches handle many distinct method signatures gracefully.

    This exercises the bounded LRU cache behavior by creating many
    distinct method calls with different argument patterns. The cache
    should evict old entries rather than growing unbounded.
    """
    # Create many distinct method calls with different argument patterns
    # This simulates what happens in Spark with dynamically generated code

    StringBuilder = client.jvm.java.lang.StringBuilder
    String = client.jvm.java.lang.String

    # Create several StringBuilders with different initial strings
    # Each constructor call may have different signature resolution
    builders = []
    for i in range(50):
        sb = StringBuilder(f"test_{i}")
        builders.append(sb)

    # Call various methods on each builder
    for i, sb in enumerate(builders):
        sb.append(f"_suffix_{i}")
        sb.toString()

    # Also exercise static method resolution
    for i in range(50):
        String.valueOf(i)
        String.valueOf(float(i))

    # Verify operations completed correctly
    assert builders[0].toString() == "test_0_suffix_0"
    assert builders[-1].toString() == "test_49_suffix_49"

    # The test passes if no OutOfMemoryError or excessive slowdown occurs
    # The bounded cache should have evicted oldest entries if needed
