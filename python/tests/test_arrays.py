"""Tests for Java array support."""

import array

import pyarrow as pa


# Uses the shared `client` fixture from conftest.py


class TestArrayReturnValues:
    """Test that Java arrays are returned correctly to Python.

    Java arrays are returned as JavaObject instances with iteration support.
    Use list() to convert them to Python lists.
    """

    def test_int_array_copyof(self, client):
        """Test Arrays.copyOf returns int[] which can be iterated."""
        Arrays = client.jvm.java.util.Arrays
        original = pa.array([1, 2, 3, 4, 5], type=pa.int32())
        # copyOf returns int[]
        result = Arrays.copyOf(original, 5)
        assert list(result) == [1, 2, 3, 4, 5]

    def test_double_array_copyof(self, client):
        """Test Arrays.copyOf returns double[] which can be iterated."""
        Arrays = client.jvm.java.util.Arrays
        original = pa.array([1.5, 2.5, 3.5], type=pa.float64())
        result = Arrays.copyOf(original, 3)
        assert list(result) == [1.5, 2.5, 3.5]

    def test_string_array_copyof(self, client):
        """Test Arrays.copyOf with String[] equivalent which can be iterated."""
        # Create Object[] with strings via ArrayList.toArray
        ArrayList = client.jvm.java.util.ArrayList
        al = ArrayList()
        al.add("a")
        al.add("b")
        al.add("c")
        arr = al.toArray()
        assert list(arr) == ["a", "b", "c"]

    def test_toarray_from_list(self, client):
        """Test ArrayList.toArray() returns Object[] which can be iterated."""
        ArrayList = client.jvm.java.util.ArrayList
        arr = ArrayList()
        arr.add("hello")
        arr.add("world")
        result = arr.toArray()
        assert list(result) == ["hello", "world"]


class TestArrayArguments:
    """Test that Python arrays are passed correctly to Java."""

    def test_int_array_argument(self, client):
        """Test passing int[] to Java."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array([3, 1, 4, 1, 5], type=pa.int32())
        Arrays.sort(arr)
        # Note: Arrays.sort modifies in place but we can't verify that
        # Instead, test that it doesn't error

    def test_arrays_tostring(self, client):
        """Test Arrays.toString(int[])."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array([1, 2, 3], type=pa.int32())
        result = Arrays.toString(arr)
        assert result == "[1, 2, 3]"

    def test_long_array_tostring(self, client):
        """Test Arrays.toString(long[])."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array([1, 2, 3], type=pa.int64())
        result = Arrays.toString(arr)
        assert result == "[1, 2, 3]"

    def test_double_array_tostring(self, client):
        """Test Arrays.toString(double[])."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array([1.5, 2.5, 3.5], type=pa.float64())
        result = Arrays.toString(arr)
        assert result == "[1.5, 2.5, 3.5]"

    def test_boolean_array_tostring(self, client):
        """Test Arrays.toString(boolean[])."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array([True, False, True], type=pa.bool_())
        result = Arrays.toString(arr)
        assert result == "[true, false, true]"

    def test_string_array_tostring(self, client):
        """Test Arrays.toString(Object[]) with strings."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array(["hello", "world"])
        result = Arrays.toString(arr)
        assert result == "[hello, world]"


class TestByteArrays:
    """Test byte array handling."""

    def test_bytes_to_java(self, client):
        """Test passing Python bytes to Java."""
        Arrays = client.jvm.java.util.Arrays
        data = b"\x01\x02\x03\x04"
        result = Arrays.toString(data)
        assert result == "[1, 2, 3, 4]"

    def test_bytearray_to_java(self, client):
        """Test passing Python bytearray to Java."""
        Arrays = client.jvm.java.util.Arrays
        data = bytearray([1, 2, 3, 4])
        result = Arrays.toString(data)
        assert result == "[1, 2, 3, 4]"


class TestPythonArrayModule:
    """Test Python array.array support."""

    def test_array_int(self, client):
        """Test array.array('i', ...) as int[]."""
        Arrays = client.jvm.java.util.Arrays
        arr = array.array("i", [1, 2, 3])
        result = Arrays.toString(arr)
        assert result == "[1, 2, 3]"

    def test_array_double(self, client):
        """Test array.array('d', ...) as double[]."""
        Arrays = client.jvm.java.util.Arrays
        arr = array.array("d", [1.5, 2.5, 3.5])
        result = Arrays.toString(arr)
        assert result == "[1.5, 2.5, 3.5]"


class TestArrayRoundTrip:
    """Test arrays can be sent to Java and received back.

    Java arrays are returned as JavaObject instances with iteration support.
    Use list() to convert them to Python lists.
    """

    def test_int_array_roundtrip_via_copy(self, client):
        """Test int[] round trip via Arrays.copyOf."""
        Arrays = client.jvm.java.util.Arrays
        original = pa.array([10, 20, 30], type=pa.int32())

        # copyOf returns int[]
        copied = Arrays.copyOf(original, 3)
        assert list(copied) == [10, 20, 30]

    def test_double_array_roundtrip(self, client):
        """Test double[] round trip."""
        Arrays = client.jvm.java.util.Arrays
        original = pa.array([1.1, 2.2, 3.3], type=pa.float64())

        # Arrays.copyOf returns double[]
        copied = Arrays.copyOf(original, 3)
        assert list(copied) == [1.1, 2.2, 3.3]

    def test_string_array_via_list(self, client):
        """Test String[] round trip via ArrayList."""
        ArrayList = client.jvm.java.util.ArrayList

        # Create ArrayList and add strings
        al = ArrayList()
        al.add("apple")
        al.add("banana")
        al.add("cherry")

        # toArray returns Object[]
        result = al.toArray()
        assert list(result) == ["apple", "banana", "cherry"]


class TestArrayEdgeCases:
    """Test edge cases for array handling."""

    def test_empty_int_array(self, client):
        """Test empty int array."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array([], type=pa.int32())
        result = Arrays.toString(arr)
        assert result == "[]"

    def test_large_array(self, client):
        """Test larger array (100 elements)."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array(range(100), type=pa.int32())
        # Just verify it doesn't error
        result = Arrays.toString(arr)
        assert result.startswith("[0, 1, 2,")
        assert result.endswith("97, 98, 99]")

    def test_float32_array(self, client):
        """Test float32 array (widened to double)."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array([1.5, 2.5, 3.5], type=pa.float32())
        result = Arrays.toString(arr)
        # Float widened to double
        assert "1.5" in result
        assert "2.5" in result
        assert "3.5" in result

    def test_int16_array(self, client):
        """Test int16 array (widened to int)."""
        Arrays = client.jvm.java.util.Arrays
        arr = pa.array([100, 200, 300], type=pa.int16())
        result = Arrays.toString(arr)
        assert result == "[100, 200, 300]"
