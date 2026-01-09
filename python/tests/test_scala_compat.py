"""Tests for Scala compatibility and reflection-based JVM navigation.

These tests cover issues discovered during Spark integration:
1. Scala object method calls (e.g., functions.col() in Spark)
2. Java array round-tripping (preserving array vs list semantics)
3. Reflection queries for distinguishing classes from methods

The reflection support enables proper handling of:
- Scala objects (compiled as classes with $ suffix, e.g., functions$)
- Static methods on Scala objects vs class instantiation
- Java arrays that need to be passed back to Java methods
"""

import pyarrow as pa

from gatun import JavaArray
from gatun.client import _reflect_cache


class TestReflection:
    """Tests for the Reflect action that queries Java for type information."""

    def test_reflect_class(self, client):
        """Test that reflect() returns 'class' for Java classes."""
        result = client.reflect("java.util.ArrayList")
        assert result == "class"

    def test_reflect_method(self, client):
        """Test that reflect() returns 'method' for static methods."""
        result = client.reflect("java.lang.Integer.parseInt")
        assert result == "method"

    def test_reflect_field(self, client):
        """Test that reflect() returns 'field' for static fields."""
        result = client.reflect("java.lang.Integer.MAX_VALUE")
        assert result == "field"

    def test_reflect_nonexistent(self, client):
        """Test that reflect() returns 'none' for nonexistent paths."""
        result = client.reflect("java.util.NonExistentClass")
        assert result == "none"

    def test_reflect_package(self, client):
        """Test that reflect() returns 'none' for packages (not classes)."""
        result = client.reflect("java.util")
        assert result == "none"

    def test_reflect_cache_used_by_jvm_node(self, client):
        """Test that JVM navigation uses reflection cache."""
        # Clear cache for this test
        test_path = "java.util.ArrayList"
        _reflect_cache.pop(test_path, None)

        # Access via JVM view triggers reflection and caches result
        _ = client.jvm.java.util.ArrayList()

        # Cache should be populated by _JVMNode._get_type()
        assert test_path in _reflect_cache
        assert _reflect_cache[test_path] == "class"


class TestScalaObjectMethods:
    """Tests for calling methods on Scala objects.

    Scala objects compile to Java classes with a $ suffix (e.g., functions$)
    with a singleton instance at the MODULE$ field. When accessing
    `jvm.package.object.method()`, we need to detect that `object` is a
    Scala object class and `method` is a method on it, not a nested class.

    This was discovered when Spark's `functions.col()` was being treated as
    instantiating a class `org.apache.spark.sql.functions.col` instead of
    calling the `col` method on the `functions` Scala object.
    """

    def test_arrays_as_list_is_static_method(self, client):
        """Test that Arrays.asList is detected as a method, not a class."""
        # java.util.Arrays is a class, asList is a static method on it
        # This should NOT try to instantiate "java.util.Arrays.asList"
        result = client.jvm.java.util.Arrays.asList("a", "b", "c")
        assert result == ["a", "b", "c"]

    def test_collections_sort_is_static_method(self, client):
        """Test that Collections.sort is detected as a method."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("c")
        arr.add("a")
        arr.add("b")

        # Collections.sort should be a static method call
        client.jvm.java.util.Collections.sort(arr)

        # Verify sorting worked
        assert arr.get(0) == "a"
        assert arr.get(1) == "b"
        assert arr.get(2) == "c"

    def test_math_static_method_not_constructor(self, client):
        """Test that Math.max is not treated as instantiating Math.max class."""
        result = client.jvm.java.lang.Math.max(10, 20)
        assert result == 20

    def test_string_format_static_method(self, client):
        """Test String.format as static method."""
        result = client.jvm.java.lang.String.format("Hello %s!", "World")
        assert result == "Hello World!"

    def test_integer_valueof_static_method(self, client):
        """Test Integer.valueOf as static method."""
        result = client.jvm.java.lang.Integer.valueOf("42")
        assert result == 42


class TestJavaArrayRoundTrip:
    """Tests for Java array preservation during round-trip.

    When Java returns an array (e.g., Object[], int[]), Gatun converts it to
    a JavaArray. When that JavaArray is passed back to Java, it should be
    serialized as an ArrayVal (Java array), not a ListVal (Java ArrayList).

    This was discovered when Spark's SQL methods expected Object[] but
    received ArrayList because arrays were being converted to regular Python
    lists and then back to ArrayLists.
    """

    def test_java_array_type_preserved(self, client):
        """Test that Object[] from Java are returned as JavaObject (reference to Java-side array).

        With the fix for Array.set/get, Object arrays are now kept as ObjectRef on the Java side
        rather than being auto-converted to ArrayVal. This allows Array.set/get to work properly.
        JavaObject supports len(), iteration, and indexing for arrays.
        """
        from gatun.client import JavaObject

        arr = client.jvm.java.util.ArrayList()
        arr.add("a")
        arr.add("b")

        # toArray() returns Object[] - now returned as JavaObject reference
        result = arr.toArray()

        # Object arrays are now JavaObject (reference to Java-side array)
        assert isinstance(result, JavaObject)
        # Can still access elements via Array.get or indexing
        assert result[0] == "a"
        assert result[1] == "b"

    def test_java_array_has_list_interface(self, client):
        """Test that Object[] from Java behaves like a Python list via JavaObject protocols.

        JavaObject implements __len__, __iter__, __getitem__ for arrays, allowing
        them to be used like Python lists.
        """
        arr = client.jvm.java.util.ArrayList()
        arr.add("x")
        arr.add("y")
        arr.add("z")

        result = arr.toArray()

        # Should behave like a list via JavaObject's __len__, __iter__, __getitem__
        assert len(result) == 3
        assert result[0] == "x"
        assert result[1] == "y"
        assert result[2] == "z"
        assert list(result) == ["x", "y", "z"]

    def test_java_array_round_trip_object(self, client):
        """Test Object[] round-trip preserves array semantics.

        Object arrays now return as JavaObject (reference to Java-side array).
        When passed back to Java, the ObjectRef is used directly.
        """
        from gatun.client import JavaObject

        # Create array from ArrayList
        arr = client.jvm.java.util.ArrayList()
        arr.add("hello")
        arr.add("world")
        java_array = arr.toArray()

        # Object arrays are now JavaObject (reference to Java-side array)
        assert isinstance(java_array, JavaObject)

        # Pass it back to Java - Arrays.toString expects Object[]
        # The JavaObject (ObjectRef) is passed directly to Java
        result = client.jvm.java.util.Arrays.toString(java_array)
        assert result == "[hello, world]"

    def test_java_int_array_preserved(self, client):
        """Test that int[] arrays have correct element type."""
        Arrays = client.jvm.java.util.Arrays
        original = pa.array([1, 2, 3, 4, 5], type=pa.int32())

        # copyOf returns int[]
        result = Arrays.copyOf(original, 5)

        assert isinstance(result, JavaArray)
        assert result.element_type == "Int"
        assert list(result) == [1, 2, 3, 4, 5]

    def test_java_long_array_preserved(self, client):
        """Test that long[] arrays have correct element type."""
        Arrays = client.jvm.java.util.Arrays
        original = pa.array([1, 2, 3], type=pa.int64())

        result = Arrays.copyOf(original, 3)

        assert isinstance(result, JavaArray)
        assert result.element_type == "Long"

    def test_java_double_array_preserved(self, client):
        """Test that double[] arrays have correct element type."""
        Arrays = client.jvm.java.util.Arrays
        original = pa.array([1.5, 2.5, 3.5], type=pa.float64())

        result = Arrays.copyOf(original, 3)

        assert isinstance(result, JavaArray)
        assert result.element_type == "Double"

    def test_java_string_array_preserved(self, client):
        """Test that Object[] with String elements is returned as JavaObject.

        toArray() returns Object[], which now stays as ObjectRef on Java side.
        The elements can be accessed via iteration or indexing.
        """
        from gatun.client import JavaObject

        arr = client.jvm.java.util.ArrayList()
        arr.add("a")
        arr.add("b")
        arr.add("c")

        result = arr.toArray()

        # Object[] is now JavaObject (reference to Java-side array)
        assert isinstance(result, JavaObject)
        # Elements accessible via iteration
        assert list(result) == ["a", "b", "c"]

    def test_java_array_len_function(self, client):
        """Test that len() works on Object[] returned as JavaObject.

        Object arrays are returned as JavaObject references. Use Python's
        len() function to get the array length.
        """
        arr = client.jvm.java.util.ArrayList()
        arr.add("item")
        java_array = arr.toArray()

        # Use len() for array length
        assert len(java_array) == 1

    def test_java_array_iteration(self, client):
        """Test that Object[] returned as JavaObject supports iteration.

        JavaObject implements __iter__ for arrays, allowing standard
        Python iteration patterns.
        """
        arr = client.jvm.java.util.ArrayList()
        arr.add("a")
        arr.add("b")
        java_array = arr.toArray()

        # len() works
        assert len(java_array) == 2
        # iteration works
        assert list(java_array) == ["a", "b"]


class TestJavaArraySerialization:
    """Tests for serializing JavaArray back to Java as arrays."""

    def test_java_array_passed_as_array_not_list(self, client):
        """Test that JavaArray is serialized as ArrayVal, not ListVal."""
        # Create a JavaArray manually
        java_array = JavaArray(["a", "b", "c"], element_type="Object")

        # Pass to Arrays.toString - expects Object[]
        result = client.jvm.java.util.Arrays.toString(java_array)
        assert result == "[a, b, c]"

    def test_int_java_array_serialization(self, client):
        """Test serializing JavaArray with Int element type."""
        java_array = JavaArray([1, 2, 3], element_type="Int")

        result = client.jvm.java.util.Arrays.toString(java_array)
        assert result == "[1, 2, 3]"

    def test_long_java_array_serialization(self, client):
        """Test serializing JavaArray with Long element type."""
        java_array = JavaArray([1, 2, 3], element_type="Long")

        result = client.jvm.java.util.Arrays.toString(java_array)
        assert result == "[1, 2, 3]"

    def test_double_java_array_serialization(self, client):
        """Test serializing JavaArray with Double element type."""
        java_array = JavaArray([1.5, 2.5, 3.5], element_type="Double")

        result = client.jvm.java.util.Arrays.toString(java_array)
        assert result == "[1.5, 2.5, 3.5]"

    def test_bool_java_array_serialization(self, client):
        """Test serializing JavaArray with Bool element type."""
        java_array = JavaArray([True, False, True], element_type="Bool")

        result = client.jvm.java.util.Arrays.toString(java_array)
        assert result == "[true, false, true]"

    def test_string_java_array_serialization(self, client):
        """Test serializing JavaArray with String element type."""
        java_array = JavaArray(["hello", "world"], element_type="String")

        result = client.jvm.java.util.Arrays.toString(java_array)
        assert result == "[hello, world]"

    def test_regular_list_still_becomes_arraylist(self, client):
        """Test that regular Python lists still become Java ArrayList."""
        # Regular list should become ArrayList (ListVal)
        arr = client.jvm.java.util.ArrayList()
        arr.addAll([1, 2, 3])  # Regular list becomes ArrayList

        # The ArrayList should have 3 elements
        assert arr.size() == 3

    def test_array_vs_list_distinction(self, client):
        """Test that JavaArray and list are handled differently."""
        # JavaArray should be passed as array
        java_array = JavaArray(["x"], element_type="Object")

        # Both should work but may behave differently in overloaded methods
        result1 = client.jvm.java.util.Arrays.toString(java_array)
        assert result1 == "[x]"

        # Regular list passed to Arrays.toString will be converted to ArrayList first
        # Then converted to Object[] by Java
        # This tests the distinction is preserved at the protocol level


class TestJavaListMethods:
    """Tests for JavaList methods (List returned from Java with Java-style accessors)."""

    def test_list_response_has_size_method(self, client):
        """Test that List responses from Java have size() method."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("a")
        arr.add("b")

        # subList returns a List view
        sub = arr.subList(0, 2)

        # Should have Java-style size() method
        assert hasattr(sub, "size")
        assert sub.size() == 2

    def test_list_response_has_isempty_method(self, client):
        """Test that List responses from Java have isEmpty() method."""
        arr = client.jvm.java.util.ArrayList()
        sub = arr.subList(0, 0)

        assert hasattr(sub, "isEmpty")
        assert sub.isEmpty() is True

    def test_list_response_has_get_method(self, client):
        """Test that List responses from Java have get() method."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("first")
        arr.add("second")

        sub = arr.subList(0, 2)

        assert hasattr(sub, "get")
        assert sub.get(0) == "first"
        assert sub.get(1) == "second"

    def test_list_response_has_contains_method(self, client):
        """Test that List responses from Java have contains() method."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("hello")

        sub = arr.subList(0, 1)

        assert hasattr(sub, "contains")
        assert sub.contains("hello") is True
        assert sub.contains("world") is False
