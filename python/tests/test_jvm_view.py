"""Tests for JVM view semantics (jvm.java.util.ArrayList style access)."""

import pytest


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
    with pytest.raises(RuntimeError) as excinfo:
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
