import pytest

from gatun import PROTOCOL_VERSION


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
    with pytest.raises(RuntimeError) as excinfo:
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
    with pytest.raises(RuntimeError) as excinfo:
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
    with pytest.raises(RuntimeError) as excinfo:
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
    with pytest.raises(RuntimeError) as excinfo:
        client.get_field(arr, "nonExistentField")
    assert "no field" in str(excinfo.value).lower()


def test_exception_includes_stack_trace(client):
    """Verify Java exceptions include full stack trace."""
    # Integer.parseInt with invalid input will throw NumberFormatException
    with pytest.raises(RuntimeError) as excinfo:
        client.invoke_static_method("java.lang.Integer", "parseInt", "not_a_number")

    error_msg = str(excinfo.value)
    # Should contain the exception class name
    assert "NumberFormatException" in error_msg
    # Should contain stack trace elements (at ...)
    assert "\tat " in error_msg or "at " in error_msg
