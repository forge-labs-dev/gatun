"""Tests demonstrating Gatun's compatibility with PySpark Py4J usage patterns.

This test suite covers the key patterns discovered in PySpark's use of Py4J:
1. JVM navigation and class access (gateway.jvm.package.Class)
2. Object instantiation with various constructor patterns
3. Instance and static method calls
4. Collection conversions (list -> Java List, dict -> Java Map)
5. Scala object/companion object access
6. Method chaining patterns
7. Callback/UDF registration patterns
8. Array operations and type preservation
9. Type checking with is_instance_of

These patterns are essential for Gatun to be a drop-in replacement for Py4J
in PySpark's Python-to-JVM communication layer.
"""

import pyarrow as pa

from gatun import JavaArray, java_import


class TestJVMNavigation:
    """Test JVM package navigation patterns used in PySpark.

    PySpark accesses classes via: sc._jvm.org.apache.spark.SparkConf
    """

    def test_nested_package_navigation(self, client):
        """Test accessing classes through nested package paths."""
        # PySpark pattern: sc._jvm.java.util.ArrayList
        ArrayList = client.jvm.java.util.ArrayList
        arr = ArrayList()
        assert arr.size() == 0

    def test_java_import_wildcard(self, client):
        """Test wildcard imports like java_import(jvm, 'java.util.*')."""
        java_import(client.jvm, "java.util.*")

        # After import, can access directly
        arr = client.jvm.ArrayList()
        hm = client.jvm.HashMap()

        arr.add("test")
        assert arr.size() == 1

        hm.put("key", "value")
        assert hm.get("key") == "value"

    def test_java_import_single_class(self, client):
        """Test single class import."""
        java_import(client.jvm, "java.lang.StringBuilder")

        sb = client.jvm.StringBuilder("hello")
        sb.append(" world")
        assert sb.toString() == "hello world"


class TestObjectInstantiation:
    """Test object creation patterns used in PySpark.

    PySpark creates objects like: JavaSparkContext(jconf)
    """

    def test_no_arg_constructor(self, client):
        """Test no-argument constructors."""
        arr = client.jvm.java.util.ArrayList()
        hm = client.jvm.java.util.HashMap()
        hs = client.jvm.java.util.HashSet()

        assert arr.size() == 0
        assert hm.size() == 0
        assert hs.size() == 0

    def test_constructor_with_capacity(self, client):
        """Test constructors with initial capacity (common in PySpark)."""
        # ArrayList(int initialCapacity)
        arr = client.jvm.java.util.ArrayList(100)
        assert arr.size() == 0  # Size is still 0, capacity is internal

        # HashMap(int initialCapacity)
        hm = client.jvm.java.util.HashMap(50)
        assert hm.size() == 0

    def test_constructor_with_string(self, client):
        """Test constructors with string arguments."""
        sb = client.jvm.java.lang.StringBuilder("initial")
        assert sb.toString() == "initial"

    def test_constructor_with_collection(self, client):
        """Test constructors that take collections."""
        # Create source list
        source = client.jvm.java.util.ArrayList()
        source.add("a")
        source.add("b")
        source.add("c")

        # Create new list from existing collection
        # ArrayList(Collection c)
        copy = client.jvm.java.util.ArrayList(source)
        assert copy.size() == 3
        assert list(copy) == ["a", "b", "c"]


class TestMethodCalls:
    """Test method call patterns used in PySpark."""

    def test_instance_methods(self, client):
        """Test basic instance method calls."""
        arr = client.jvm.java.util.ArrayList()

        # Various instance methods
        arr.add("first")
        arr.add("second")
        assert arr.size() == 2
        assert arr.get(0) == "first"
        assert arr.contains("first") is True
        assert arr.isEmpty() is False

        arr.remove("first")
        assert arr.size() == 1

    def test_static_methods(self, client):
        """Test static method calls (common in PySpark for utilities)."""
        # Math static methods
        assert client.jvm.java.lang.Math.max(10, 20) == 20
        assert client.jvm.java.lang.Math.min(10, 20) == 10
        assert client.jvm.java.lang.Math.abs(-42) == 42

        # Integer static methods
        assert client.jvm.java.lang.Integer.parseInt("123") == 123
        assert client.jvm.java.lang.Integer.valueOf(42) == 42

        # String static methods
        assert client.jvm.java.lang.String.valueOf(123) == "123"
        assert client.jvm.java.lang.String.format("Hello %s!", "World") == "Hello World!"

    def test_method_chaining(self, client):
        """Test method chaining (common in DataFrame API)."""
        sb = client.jvm.java.lang.StringBuilder()

        # Chain multiple append calls
        sb.append("Hello")
        sb.append(" ")
        sb.append("World")

        assert sb.toString() == "Hello World"

    def test_method_returning_object(self, client):
        """Test methods that return objects for further operations."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("a")
        arr.add("b")
        arr.add("c")

        # subList returns a view
        sub = arr.subList(0, 2)
        assert sub.size() == 2

    def test_overloaded_methods(self, client):
        """Test correct resolution of overloaded methods."""
        arr = client.jvm.java.util.ArrayList()

        # add(E e) vs add(int index, E element)
        arr.add("first")           # add(E)
        arr.add(0, "zeroth")       # add(int, E)

        assert arr.get(0) == "zeroth"
        assert arr.get(1) == "first"


class TestCollectionConversions:
    """Test Python <-> Java collection conversions.

    PySpark heavily uses PythonUtils.toSeq(), toList(), toScalaMap()
    """

    def test_python_list_to_java(self, client):
        """Test Python list passed as Java collection argument."""
        arr = client.jvm.java.util.ArrayList()

        # Pass Python list - should be converted to Java List
        arr.addAll([1, 2, 3, 4, 5])
        assert arr.size() == 5
        assert list(arr) == [1, 2, 3, 4, 5]

    def test_python_dict_to_java(self, client):
        """Test Python dict passed as Java Map."""
        hm = client.jvm.java.util.HashMap()

        # Put Python dict values
        hm.put("name", "Alice")
        hm.put("age", 30)
        hm.put("scores", [90, 85, 88])  # Nested list

        assert hm.get("name") == "Alice"
        assert hm.get("age") == 30
        # Nested list should be converted
        scores = hm.get("scores")
        assert list(scores) == [90, 85, 88]

    def test_nested_collections(self, client):
        """Test deeply nested collection conversion."""
        arr = client.jvm.java.util.ArrayList()

        # Add nested structure - simpler case that's more typical in PySpark
        nested = {"name": "Alice", "scores": [90, 85, 88]}
        arr.add(nested)

        result = arr.get(0)
        # Result is auto-converted back to Python dict
        assert result["name"] == "Alice"
        assert list(result["scores"]) == [90, 85, 88]


class TestScalaPatterns:
    """Test Scala-specific patterns used in PySpark.

    PySpark accesses Scala objects, companion objects, and uses
    Scala collections extensively.
    """

    def test_arrays_utility_class(self, client):
        """Test java.util.Arrays (similar to Scala Array companion)."""
        Arrays = client.jvm.java.util.Arrays

        # asList - commonly used in PySpark
        result = Arrays.asList("a", "b", "c")
        assert list(result) == ["a", "b", "c"]

    def test_collections_utility_class(self, client):
        """Test java.util.Collections (utility methods)."""
        Collections = client.jvm.java.util.Collections

        arr = client.jvm.java.util.ArrayList()
        arr.add(3)
        arr.add(1)
        arr.add(2)

        # Sort in place
        Collections.sort(arr)
        assert list(arr) == [1, 2, 3]

        # Reverse
        Collections.reverse(arr)
        assert list(arr) == [3, 2, 1]

    def test_treemap_sorted_keys(self, client):
        """Test TreeMap for sorted key access (Scala SortedMap pattern)."""
        tm = client.jvm.java.util.TreeMap()
        tm.put("zebra", 1)
        tm.put("apple", 2)
        tm.put("mango", 3)

        assert tm.firstKey() == "apple"
        assert tm.lastKey() == "zebra"


class TestCallbackPatterns:
    """Test callback/UDF patterns used in PySpark.

    PySpark registers Python functions as Java interface implementations
    for UDFs, custom comparators, etc.
    """

    def test_comparator_callback(self, client):
        """Test Comparator callback (used for custom sorting)."""
        def compare(a, b):
            # Reverse order comparator
            if a > b:
                return -1
            elif a < b:
                return 1
            return 0

        comparator = client.register_callback(compare, "java.util.Comparator")

        arr = client.jvm.java.util.ArrayList()
        arr.add(1)
        arr.add(3)
        arr.add(2)

        client.jvm.java.util.Collections.sort(arr, comparator)

        # Should be reverse sorted
        assert list(arr) == [3, 2, 1]

    def test_callback_with_strings(self, client):
        """Test callback processing string arguments."""
        def string_compare(a, b):
            # Case-insensitive comparison
            a_lower = a.lower() if a else ""
            b_lower = b.lower() if b else ""
            if a_lower < b_lower:
                return -1
            elif a_lower > b_lower:
                return 1
            return 0

        comparator = client.register_callback(string_compare, "java.util.Comparator")

        arr = client.jvm.java.util.ArrayList()
        arr.add("Banana")
        arr.add("apple")
        arr.add("Cherry")

        client.jvm.java.util.Collections.sort(arr, comparator)

        # Case-insensitive sorted
        assert list(arr) == ["apple", "Banana", "Cherry"]


class TestArrayOperations:
    """Test array operations and type preservation.

    PySpark uses arrays for efficient data transfer and needs
    to preserve array types when passing back to Java.
    """

    def test_pyarrow_int_array(self, client):
        """Test PyArrow int array to Java int[]."""
        Arrays = client.jvm.java.util.Arrays

        arr = pa.array([1, 2, 3, 4, 5], type=pa.int32())
        result = Arrays.toString(arr)
        assert result == "[1, 2, 3, 4, 5]"

    def test_pyarrow_double_array(self, client):
        """Test PyArrow double array to Java double[]."""
        Arrays = client.jvm.java.util.Arrays

        arr = pa.array([1.5, 2.5, 3.5], type=pa.float64())
        result = Arrays.toString(arr)
        assert result == "[1.5, 2.5, 3.5]"

    def test_pyarrow_string_array(self, client):
        """Test PyArrow string array to Java String[]."""
        Arrays = client.jvm.java.util.Arrays

        arr = pa.array(["hello", "world"])
        result = Arrays.toString(arr)
        assert result == "[hello, world]"

    def test_java_array_roundtrip(self, client):
        """Test Java array returned and passed back preserves type."""
        Arrays = client.jvm.java.util.Arrays

        # Create array, get it back as JavaArray
        original = pa.array([10, 20, 30], type=pa.int32())
        copied = Arrays.copyOf(original, 3)

        # Should be JavaArray
        assert isinstance(copied, JavaArray)
        assert copied.element_type == "Int"

        # Pass it back to Java method
        result = Arrays.toString(copied)
        assert result == "[10, 20, 30]"

    def test_toarray_from_collection(self, client):
        """Test ArrayList.toArray() returns proper JavaArray."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("x")
        arr.add("y")
        arr.add("z")

        java_array = arr.toArray()

        assert isinstance(java_array, JavaArray)
        assert list(java_array) == ["x", "y", "z"]

        # Should work when passed back to Java
        result = client.jvm.java.util.Arrays.toString(java_array)
        assert result == "[x, y, z]"


class TestTypeChecking:
    """Test is_instance_of patterns used in PySpark.

    PySpark uses instanceof checks for type validation.
    """

    def test_is_instance_of_interface(self, client):
        """Test checking if object implements an interface."""
        arr = client.jvm.java.util.ArrayList()

        assert client.is_instance_of(arr, "java.util.List") is True
        assert client.is_instance_of(arr, "java.util.Collection") is True
        assert client.is_instance_of(arr, "java.lang.Iterable") is True
        assert client.is_instance_of(arr, "java.util.Map") is False

    def test_is_instance_of_class(self, client):
        """Test checking against specific class."""
        arr = client.jvm.java.util.ArrayList()
        hm = client.jvm.java.util.HashMap()

        assert client.is_instance_of(arr, "java.util.ArrayList") is True
        assert client.is_instance_of(arr, "java.util.LinkedList") is False
        assert client.is_instance_of(hm, "java.util.HashMap") is True
        assert client.is_instance_of(hm, "java.util.Map") is True


class TestIterationPatterns:
    """Test iteration patterns used when processing Java collections."""

    def test_iterate_arraylist(self, client):
        """Test iterating over ArrayList."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("a")
        arr.add("b")
        arr.add("c")

        result = []
        for item in arr:
            result.append(item)

        assert result == ["a", "b", "c"]

    def test_iterate_hashset(self, client):
        """Test iterating over HashSet."""
        hs = client.jvm.java.util.HashSet()
        hs.add(1)
        hs.add(2)
        hs.add(3)

        result = set()
        for item in hs:
            result.add(item)

        assert result == {1, 2, 3}

    def test_len_on_collection(self, client):
        """Test len() on Java collections."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("x")
        arr.add("y")

        assert len(arr) == 2

    def test_indexing_on_list(self, client):
        """Test indexing on Java List."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("first")
        arr.add("second")
        arr.add("third")

        assert arr[0] == "first"
        assert arr[1] == "second"
        assert arr[2] == "third"


class TestBatchOperations:
    """Test batch operations for efficiency (important for PySpark performance)."""

    def test_batch_method_calls(self, client):
        """Test batching multiple method calls."""
        arr = client.jvm.java.util.ArrayList()

        # Batch add operations
        with client.batch() as b:
            for i in range(100):
                b.call(arr, "add", f"item_{i}")
            size_result = b.call(arr, "size")

        assert size_result.get() == 100
        assert arr.size() == 100

    def test_batch_mixed_operations(self, client):
        """Test batching mixed operations."""
        with client.batch() as b:
            arr_result = b.create("java.util.ArrayList")
            map_result = b.create("java.util.HashMap")

        arr = arr_result.get()
        hm = map_result.get()

        arr.add("test")
        hm.put("key", "value")

        assert arr.size() == 1
        assert hm.size() == 1

    def test_create_objects_vectorized(self, client):
        """Test vectorized object creation."""
        objects = client.create_objects([
            ("java.util.ArrayList", ()),
            ("java.util.HashMap", ()),
            ("java.util.HashSet", ()),
            ("java.util.LinkedList", ()),
        ])

        assert len(objects) == 4

        # All should be functional
        objects[0].add("test")
        objects[1].put("k", "v")
        objects[2].add(42)
        objects[3].add("item")

        assert objects[0].size() == 1
        assert objects[1].size() == 1
        assert objects[2].size() == 1
        assert objects[3].size() == 1

    def test_invoke_methods_vectorized(self, client):
        """Test vectorized method invocation."""
        arr = client.jvm.java.util.ArrayList()

        results = client.invoke_methods(arr, [
            ("add", ("a",)),
            ("add", ("b",)),
            ("add", ("c",)),
            ("size", ()),
            ("get", (0,)),
            ("get", (1,)),
            ("get", (2,)),
        ])

        assert results[0] is True  # add returns true
        assert results[1] is True
        assert results[2] is True
        assert results[3] == 3     # size
        assert results[4] == "a"   # get(0)
        assert results[5] == "b"   # get(1)
        assert results[6] == "c"   # get(2)


class TestEdgeCases:
    """Test edge cases that might occur in PySpark usage."""

    def test_null_handling(self, client):
        """Test None/null handling."""
        arr = client.jvm.java.util.ArrayList()
        arr.add(None)
        arr.add("not null")
        arr.add(None)

        assert arr.size() == 3
        assert arr.get(0) is None
        assert arr.get(1) == "not null"
        assert arr.get(2) is None

    def test_empty_string(self, client):
        """Test empty string handling."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("")
        arr.add("not empty")

        assert arr.get(0) == ""
        assert arr.get(1) == "not empty"

    def test_unicode_strings(self, client):
        """Test unicode string handling."""
        arr = client.jvm.java.util.ArrayList()
        arr.add("Hello 世界")
        arr.add("Привет мир")
        arr.add("🎉🎊🎁")

        assert arr.get(0) == "Hello 世界"
        assert arr.get(1) == "Привет мир"
        assert arr.get(2) == "🎉🎊🎁"

    def test_large_numbers(self, client):
        """Test large number handling."""
        arr = client.jvm.java.util.ArrayList()

        # Large long value
        large_val = 2**62
        arr.add(large_val)

        result = arr.get(0)
        assert result == large_val

    def test_float_precision(self, client):
        """Test float precision handling."""
        arr = client.jvm.java.util.ArrayList()

        arr.add(3.141592653589793)
        arr.add(2.718281828459045)

        assert abs(arr.get(0) - 3.141592653589793) < 1e-15
        assert abs(arr.get(1) - 2.718281828459045) < 1e-15
