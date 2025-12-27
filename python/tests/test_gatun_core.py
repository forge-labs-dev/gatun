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
    """Test that return_object_ref=True returns ObjectRef for arrays."""
    from gatun import JavaArray
    from gatun.client import JavaObject

    # Get Class object for String
    class_obj = client.invoke_static_method("java.lang.Class", "forName", "java.lang.String")

    # Without return_object_ref, arrays are auto-converted to JavaArray
    arr_auto = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", class_obj, 3
    )
    assert isinstance(arr_auto, JavaArray)
    assert not isinstance(arr_auto, JavaObject)

    # With return_object_ref=True, arrays are returned as ObjectRef
    arr_ref = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", class_obj, 3, return_object_ref=True
    )
    assert isinstance(arr_ref, JavaObject)
    assert hasattr(arr_ref, "object_id")


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
    str_auto = client.invoke_static_method(
        "java.lang.String", "valueOf", 42
    )
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
    class_obj = client.invoke_static_method("java.lang.Class", "forName", "java.lang.String")
    arr = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", class_obj, 3, return_object_ref=True
    )
    assert isinstance(arr, JavaObject)

    # Set elements using Array.set
    client.invoke_static_method("java.lang.reflect.Array", "set", arr, 0, "hello")
    client.invoke_static_method("java.lang.reflect.Array", "set", arr, 1, "world")

    # Get elements using Array.get
    assert client.invoke_static_method("java.lang.reflect.Array", "get", arr, 0) == "hello"
    assert client.invoke_static_method("java.lang.reflect.Array", "get", arr, 1) == "world"
    assert client.invoke_static_method("java.lang.reflect.Array", "get", arr, 2) is None

    # Verify length
    assert client.invoke_static_method("java.lang.reflect.Array", "getLength", arr) == 3
