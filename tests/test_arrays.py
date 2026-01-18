"""Tests for Java array support."""

import array

import pytest

import pyarrow as pa


def test_array_fill(client):
    """Test Arrays.fill() modifies a Java array in place."""
    int_class = client.get_static_field("java.lang.Integer", "TYPE")
    java_array = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", int_class, 5, return_object_ref=True
    )

    Arrays = client.jvm.java.util.Arrays
    Arrays.fill(java_array, 7)

    assert Arrays.toString(java_array) == "[7, 7, 7, 7, 7]"


def test_array_sort_in_place(client):
    """Test Arrays.sort() modifies a Java array in place."""
    int_class = client.get_static_field("java.lang.Integer", "TYPE")
    java_array = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", int_class, 5, return_object_ref=True
    )

    Array = client.jvm.java.lang.reflect.Array
    for i, val in enumerate([5, 3, 1, 4, 2]):
        Array.setInt(java_array, i, val)

    Arrays = client.jvm.java.util.Arrays
    Arrays.sort(java_array)

    assert Arrays.toString(java_array) == "[1, 2, 3, 4, 5]"


@pytest.mark.parametrize(
    "key,expected",
    [
        (10, 0),
        (30, 2),
        (50, 4),
    ],
)
def test_array_binary_search_found(client, key, expected):
    """Test Arrays.binarySearch() finds elements at correct indices."""
    Arrays = client.jvm.java.util.Arrays
    arr = pa.array([10, 20, 30, 40, 50], type=pa.int32())
    assert Arrays.binarySearch(arr, key) == expected


def test_array_binary_search_not_found(client):
    """Test Arrays.binarySearch() returns negative for missing elements."""
    Arrays = client.jvm.java.util.Arrays
    arr = pa.array([10, 20, 30, 40, 50], type=pa.int32())
    assert Arrays.binarySearch(arr, 25) < 0


@pytest.mark.parametrize(
    "arr1,arr2,expected",
    [
        ([1, 2, 3], [1, 2, 3], True),
        ([1, 2, 3], [1, 2, 4], False),
        ([], [], True),
    ],
)
def test_array_equals(client, arr1, arr2, expected):
    """Test Arrays.equals() compares two arrays."""
    Arrays = client.jvm.java.util.Arrays
    a1 = pa.array(arr1, type=pa.int32())
    a2 = pa.array(arr2, type=pa.int32())
    assert Arrays.equals(a1, a2) is expected


@pytest.mark.parametrize(
    "start,end,expected",
    [
        (1, 4, [20, 30, 40]),
        (0, 2, [10, 20]),
        (3, 7, [40, 50, 0, 0]),  # Extends beyond array, pads with zeros
    ],
)
def test_array_copy_of_range(client, start, end, expected):
    """Test Arrays.copyOfRange() copies a slice."""
    Arrays = client.jvm.java.util.Arrays
    arr = pa.array([10, 20, 30, 40, 50], type=pa.int32())
    result = Arrays.copyOfRange(arr, start, end)
    assert list(result) == expected


def test_system_arraycopy(client):
    """Test System.arraycopy() copies between arrays."""
    int_class = client.get_static_field("java.lang.Integer", "TYPE")
    src = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", int_class, 5, return_object_ref=True
    )
    dst = client.invoke_static_method(
        "java.lang.reflect.Array", "newInstance", int_class, 5, return_object_ref=True
    )

    Array = client.jvm.java.lang.reflect.Array
    for i in range(5):
        Array.setInt(src, i, (i + 1) * 10)

    System = client.jvm.java.lang.System
    System.arraycopy(src, 1, dst, 0, 3)

    Arrays = client.jvm.java.util.Arrays
    assert Arrays.toString(dst) == "[20, 30, 40, 0, 0]"


def test_object_array_toarray(client):
    """Test ArrayList.toArray() returns iterable Object[]."""
    ArrayList = client.jvm.java.util.ArrayList
    arr = ArrayList()
    arr.add("hello")
    arr.add("world")
    result = arr.toArray()
    assert list(result) == ["hello", "world"]


@pytest.mark.parametrize(
    "data,expected",
    [
        (b"\x01\x02\x03", "[1, 2, 3]"),
        (bytearray([4, 5, 6]), "[4, 5, 6]"),
        (array.array("i", [1, 2, 3]), "[1, 2, 3]"),
        (array.array("d", [1.5, 2.5]), "[1.5, 2.5]"),
    ],
)
def test_python_array_types(client, data, expected):
    """Test Python bytes, bytearray, and array.array passed to Java."""
    Arrays = client.jvm.java.util.Arrays
    assert Arrays.toString(data) == expected


@pytest.mark.parametrize(
    "size",
    [0, 100],
)
def test_array_sizes(client, size):
    """Test empty and large arrays."""
    Arrays = client.jvm.java.util.Arrays
    arr = pa.array(range(size), type=pa.int32())
    result = Arrays.toString(arr)
    if size == 0:
        assert result == "[]"
    else:
        assert result.startswith("[0, 1, 2,")
        assert result.endswith("97, 98, 99]")
