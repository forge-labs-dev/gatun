"""Tests for varargs method support."""

import pytest


def test_arrays_aslist_single_arg(client):
    """Test Arrays.asList with single argument."""
    Arrays = client.jvm.java.util.Arrays
    result = Arrays.asList("hello")
    assert isinstance(result, list)
    assert result == ["hello"]


def test_arrays_aslist_multiple_args(client):
    """Test Arrays.asList with multiple arguments."""
    Arrays = client.jvm.java.util.Arrays
    result = Arrays.asList("a", "b", "c")
    assert isinstance(result, list)
    assert result == ["a", "b", "c"]


def test_arrays_aslist_empty(client):
    """Test Arrays.asList with no arguments."""
    Arrays = client.jvm.java.util.Arrays
    result = Arrays.asList()
    assert isinstance(result, list)
    assert result == []


def test_arrays_aslist_mixed_types(client):
    """Test Arrays.asList with mixed types."""
    Arrays = client.jvm.java.util.Arrays
    # All will be boxed to Object
    result = Arrays.asList("hello", 42, 3.14)
    assert isinstance(result, list)
    assert len(result) == 3
    assert result[0] == "hello"
    assert result[1] == 42
    assert abs(result[2] - 3.14) < 0.001


def test_string_format_varargs(client):
    """Test String.format with varargs."""
    String = client.jvm.java.lang.String
    result = String.format("Hello %s, you are %d years old", "Alice", 30)
    assert result == "Hello Alice, you are 30 years old"


def test_string_format_single_arg(client):
    """Test String.format with single vararg."""
    String = client.jvm.java.lang.String
    result = String.format("Hello %s!", "World")
    assert result == "Hello World!"


def test_string_format_no_varargs(client):
    """Test String.format with no varargs (just format string)."""
    String = client.jvm.java.lang.String
    result = String.format("No placeholders here")
    assert result == "No placeholders here"


def test_collections_addall_varargs(client):
    """Test Collections.addAll with varargs."""
    Collections = client.jvm.java.util.Collections
    ArrayList = client.jvm.java.util.ArrayList

    my_list = ArrayList()
    # addAll(Collection, T...) - returns boolean
    result = Collections.addAll(my_list, "one", "two", "three")
    assert result is True

    # Verify the list contents
    sub = my_list.subList(0, 3)
    assert sub == ["one", "two", "three"]


def test_varargs_with_integers(client):
    """Test varargs with integer arguments."""
    Arrays = client.jvm.java.util.Arrays
    result = Arrays.asList(1, 2, 3, 4, 5)
    assert isinstance(result, list)
    assert result == [1, 2, 3, 4, 5]


def test_nested_varargs_results(client):
    """Test that varargs results can be nested."""
    Arrays = client.jvm.java.util.Arrays

    inner1 = Arrays.asList("a", "b")
    inner2 = Arrays.asList("c", "d")

    # Now create outer list with inner lists
    outer = Arrays.asList(inner1, inner2)
    assert isinstance(outer, list)
    assert len(outer) == 2
    assert outer[0] == ["a", "b"]
    assert outer[1] == ["c", "d"]
