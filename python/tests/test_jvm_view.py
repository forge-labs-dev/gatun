"""Tests for JVM view semantics (jvm.java.util.ArrayList style access)."""

import pytest

from gatun import JavaSecurityException, java_import


def test_jvm_view_create_arraylist(client):
    """Test creating ArrayList via JVM view."""
    ArrayList = client.jvm.java.util.ArrayList
    my_list = ArrayList()

    assert my_list is not None
    assert my_list.size() == 0
    my_list.add("hello")
    assert my_list.size() == 1


def test_jvm_view_create_with_args(client):
    """Test creating objects with constructor arguments via JVM view."""
    # ArrayList with initial capacity
    ArrayList = client.jvm.java.util.ArrayList
    my_list = ArrayList(100)
    assert my_list.size() == 0

    # StringBuilder with initial string
    StringBuilder = client.jvm.java.lang.StringBuilder
    sb = StringBuilder("Hello")
    assert sb.toString() == "Hello"


def test_jvm_view_static_method(client):
    """Test calling static methods via JVM view."""
    Integer = client.jvm.java.lang.Integer
    result = Integer.parseInt("42")
    assert result == 42

    String = client.jvm.java.lang.String
    result = String.valueOf(123)
    assert result == "123"


def test_jvm_view_math_static_methods(client):
    """Test Math static methods via JVM view."""
    Math = client.jvm.java.lang.Math
    assert Math.max(10, 20) == 20
    assert Math.min(10, 20) == 10
    assert Math.abs(-42) == 42


def test_jvm_view_inline_usage(client):
    """Test inline JVM view usage without storing intermediate references."""
    # Create and use in one line
    result = client.jvm.java.util.ArrayList().size()
    assert result == 0

    # Static method inline
    result = client.jvm.java.lang.Integer.parseInt("99")
    assert result == 99


def test_jvm_view_chained_operations(client):
    """Test chained operations on JVM-created objects."""
    sb = client.jvm.java.lang.StringBuilder("Hello")
    sb.append(" ")
    sb.append("World")
    assert sb.toString() == "Hello World"


def test_jvm_view_hashmap(client):
    """Test HashMap via JVM view."""
    HashMap = client.jvm.java.util.HashMap
    my_map = HashMap()

    my_map.put("key1", "value1")
    assert my_map.get("key1") == "value1"
    assert my_map.size() == 1


def test_jvm_view_blocked_class(client):
    """Verify blocked classes still raise errors via JVM view."""
    Runtime = client.jvm.java.lang.Runtime
    with pytest.raises(JavaSecurityException) as excinfo:
        Runtime()  # Try to instantiate
    assert "not allowed" in str(excinfo.value).lower()


def test_jvm_view_repr(client):
    """Test repr of JVM view objects."""
    jvm = client.jvm
    assert "JVMView" in repr(jvm)

    node = jvm.java.util.ArrayList
    assert "java.util.ArrayList" in repr(node)


def test_jvm_view_reuse(client):
    """Test that JVM view can be reused to create multiple instances."""
    ArrayList = client.jvm.java.util.ArrayList

    list1 = ArrayList()
    list2 = ArrayList()

    list1.add("one")
    list2.add("two")

    assert list1.size() == 1
    assert list2.size() == 1
    assert list1.get(0) == "one"
    assert list2.get(0) == "two"


class TestJavaImport:
    """Tests for java_import functionality."""

    def test_java_import_wildcard(self, client):
        """Test wildcard import (java.util.*)."""
        java_import(client.jvm, "java.util.*")

        # Now ArrayList should be accessible directly
        arr = client.jvm.ArrayList()
        arr.add("test")
        assert arr.size() == 1

    def test_java_import_single_class(self, client):
        """Test single class import."""
        java_import(client.jvm, "java.lang.StringBuilder")

        sb = client.jvm.StringBuilder("Hello")
        assert sb.toString() == "Hello"

    def test_java_import_static_method(self, client):
        """Test static method access after import."""
        java_import(client.jvm, "java.lang.*")

        result = client.jvm.Integer.parseInt("42")
        assert result == 42

        result = client.jvm.Math.max(10, 20)
        assert result == 20

    def test_java_import_multiple_classes(self, client):
        """Test importing multiple specific classes."""
        java_import(client.jvm, "java.util.ArrayList")
        java_import(client.jvm, "java.lang.StringBuilder")

        # Use both imported classes
        arr = client.jvm.ArrayList()
        arr.add("hello")
        assert arr.size() == 1

        sb = client.jvm.StringBuilder("world")
        assert sb.toString() == "world"


class TestUppercaseStaticMethods:
    """Tests for uppercase static method detection (e.g., Encoders.INT()).

    This addresses a bug where uppercase method names like INT() were being
    treated as constructor calls instead of static method calls. The fix
    changes the heuristic to check if the parent segment looks like a class
    name (starts uppercase) rather than checking if the method name starts
    lowercase.
    """

    def test_uppercase_static_method_in_string(self, client):
        """Test calling Integer.MAX_VALUE (static field, but similar pattern)."""
        # Integer.MAX_VALUE is a static field that will be accessed via getattr
        # Using String.CASE_INSENSITIVE_ORDER would be ideal but it returns a Comparator
        # Let's test with a static method that has uppercase letters
        result = client.jvm.java.lang.Integer.MAX_VALUE
        # MAX_VALUE is a static field, should not try to call as constructor
        # The parent (Integer) looks like a class, so MAX_VALUE should be treated as field/method
        assert result is not None

    def test_all_uppercase_method_name_via_collections(self, client):
        """Test that uppercase static methods work via Collections."""
        # Collections.EMPTY_LIST is a static field with uppercase name
        empty = client.jvm.java.util.Collections.EMPTY_LIST
        # Should be treated as static field access, not constructor
        assert empty is not None

    def test_static_method_chain_parent_class_detection(self, client):
        """Test that parent class detection works for method chains."""
        # Integer.valueOf returns Integer, MAX_VALUE is accessed on it
        # But we want to test the _JVMNode detection
        # client.jvm.java.lang.Integer.valueOf("42") should work
        result = client.jvm.java.lang.Integer.valueOf("42")
        # This is a normal camelCase method, should work
        assert result == 42

    def test_mixed_case_static_method(self, client):
        """Test static methods with mixed case (normal camelCase)."""
        # parseInt is camelCase - should work
        result = client.jvm.java.lang.Integer.parseInt("123")
        assert result == 123

        # Same with String
        result = client.jvm.java.lang.String.valueOf(456)
        assert result == "456"

    def test_parent_detection_nested_package(self, client):
        """Test parent class detection through nested packages."""
        # java.util.Arrays is a class, asList should be detected as static method
        result = client.jvm.java.util.Arrays.asList("a", "b", "c")
        assert result is not None
        # Verify it's a list with 3 elements
        assert len(result) == 3


class TestStaticFields:
    """Tests for static field access (e.g., Integer.MAX_VALUE).

    This tests the GetStaticField action which allows accessing
    static fields on Java classes via the JVM view.
    """

    def test_integer_max_value(self, client):
        """Test accessing Integer.MAX_VALUE static field."""
        result = client.jvm.java.lang.Integer.MAX_VALUE
        assert result == 2147483647
        assert isinstance(result, int)

    def test_integer_min_value(self, client):
        """Test accessing Integer.MIN_VALUE static field."""
        result = client.jvm.java.lang.Integer.MIN_VALUE
        assert result == -2147483648

    def test_long_max_value(self, client):
        """Test accessing Long.MAX_VALUE static field."""
        result = client.jvm.java.lang.Long.MAX_VALUE
        assert result == 9223372036854775807

    def test_double_positive_infinity(self, client):
        """Test accessing Double.POSITIVE_INFINITY static field."""
        import math

        result = client.jvm.java.lang.Double.POSITIVE_INFINITY
        assert math.isinf(result)
        assert result > 0

    def test_collections_empty_list(self, client):
        """Test accessing Collections.EMPTY_LIST static field."""
        result = client.jvm.java.util.Collections.EMPTY_LIST
        assert result == []
        assert isinstance(result, list)

    def test_collections_empty_map(self, client):
        """Test accessing Collections.EMPTY_MAP static field."""
        result = client.jvm.java.util.Collections.EMPTY_MAP
        assert result == {}
        assert isinstance(result, dict)


class TestPrimitiveWidening:
    """Tests for primitive widening conversions (int -> double).

    This tests that int arguments are properly widened to double
    when calling methods that expect double parameters.
    """

    def test_math_pow_with_ints(self, client):
        """Test Math.pow with int arguments (should widen to double)."""
        result = client.jvm.java.lang.Math.pow(2, 10)
        assert result == 1024.0
        assert isinstance(result, float)

    def test_math_pow_with_floats(self, client):
        """Test Math.pow with float arguments (normal case)."""
        result = client.jvm.java.lang.Math.pow(2.0, 10.0)
        assert result == 1024.0

    def test_math_pow_mixed_args(self, client):
        """Test Math.pow with mixed int/float arguments."""
        result = client.jvm.java.lang.Math.pow(2, 3.0)
        assert result == 8.0

        result = client.jvm.java.lang.Math.pow(2.0, 3)
        assert result == 8.0

    def test_math_sqrt_with_int(self, client):
        """Test Math.sqrt with int argument."""
        result = client.jvm.java.lang.Math.sqrt(16)
        assert result == 4.0

    def test_math_floor_with_int(self, client):
        """Test Math.floor with int argument."""
        result = client.jvm.java.lang.Math.floor(5)
        assert result == 5.0
