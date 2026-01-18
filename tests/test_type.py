import pytest

from gatun import JavaNoSuchMethodException


def test_primitive_types_and_math(client):
    """Verify that numbers return as ints/floats, not strings."""
    my_list = client.create_object("java.util.ArrayList")

    # 1. Add returns boolean (mapped to 1 or True in our schema)
    result = my_list.add("Item 1")
    assert result is True or result == 1

    my_list.add("Item 2")

    # 2. Size must be an Integer
    size = my_list.size()
    assert isinstance(size, int), f"Expected int, got {type(size)}"
    assert size == 2

    # 3. Verify Math works (proves it's not a string)
    calc = size * 10 + 5
    assert calc == 25


@pytest.mark.parametrize(
    "java_object, expected_str",
    [
        ("java.util.ArrayList", "[]"),
        ("java.util.HashMap", "{}"),
    ],
)
def test_object_creation(client, java_object, expected_str):
    """Verify we can create objects and get unique IDs."""
    obj1 = client.create_object(java_object)
    assert str(obj1) == expected_str


def test_error_propagation(client):
    """Verify Java exceptions are raised as typed Python exceptions."""
    my_list = client.create_object("java.util.ArrayList")

    # Trigger a Java-side error (calling a method that doesn't exist)
    with pytest.raises(JavaNoSuchMethodException) as excinfo:
        client.invoke_method(my_list.object_id, "methodThatDoesNotExist")

    # Should contain the method name in the error
    assert "methodThatDoesNotExist" in str(excinfo.value)


def test_stress_gc(client):
    """Create many objects to ensure GC doesn't crash the server."""
    for i in range(100):
        # Create and immediately discard
        _ = client.create_object("java.util.ArrayList")

    # If server is still alive, we are good.
    # We can verify by making one last valid call
    final_list = client.create_object("java.util.ArrayList")
    assert final_list.add("Survivor")


def test_context_manager(java_gateway):
    """Verify GatunClient works as a context manager."""
    from gatun.client import GatunClient

    socket_path = java_gateway.socket_path

    with GatunClient(socket_path) as client:
        # Verify connection works inside context
        obj = client.create_object("java.util.ArrayList")
        assert obj is not None
        assert str(obj) == "[]"

    # After exiting context, socket should be closed
    assert client.sock is None


def test_boolean_return_type(client):
    """Verify boolean returns as Python bool, not int."""
    my_list = client.create_object("java.util.ArrayList")

    result = my_list.add("test")
    assert result is True
    assert isinstance(result, bool)

    # contains() returns boolean
    assert my_list.contains("test") is True
    assert my_list.contains("nonexistent") is False


def test_list_operations_with_int_args(client):
    """Test List methods that take int arguments."""
    my_list = client.create_object("java.util.ArrayList")

    my_list.add("first")
    my_list.add("second")
    my_list.add("third")

    # get(int index) - returns the object at index
    item = my_list.get(1)
    assert item == "second"

    # remove(int index) - removes and returns the object
    removed = my_list.remove(0)
    assert removed == "first"
    assert my_list.size() == 2


def test_hashmap_operations(client):
    """Test HashMap with put/get operations."""
    my_map = client.create_object("java.util.HashMap")

    # put returns previous value (null for new key)
    result = my_map.put("key1", "value1")
    assert result is None

    # put returns previous value when overwriting
    result = my_map.put("key1", "value2")
    assert result == "value1"

    # get retrieves value
    value = my_map.get("key1")
    assert value == "value2"

    # size
    assert my_map.size() == 1

    # containsKey
    assert my_map.containsKey("key1") is True
    assert my_map.containsKey("nonexistent") is False


def test_stringbuilder_operations(client):
    """Test StringBuilder with chained operations."""
    sb = client.create_object("java.lang.StringBuilder")

    # append returns StringBuilder (object ref)
    result = sb.append("Hello")
    assert result is not None  # Returns the same StringBuilder

    sb.append(" ")
    sb.append("World")

    assert str(sb) == "Hello World"

    # length
    assert sb.length() == 11
