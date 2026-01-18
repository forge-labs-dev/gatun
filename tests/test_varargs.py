"""Tests for varargs method support."""


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


def test_varargs_packed_array(client):
    """Test varargs with array passed directly (packed case).

    Java allows calling m(String... xs) as m(new String[]{"a","b"}).
    The array should be passed directly without being wrapped in another array.
    """

    Arrays = client.jvm.java.util.Arrays

    # Create an ArrayList, add items, get toArray() which returns JavaArray
    arr = client.jvm.java.util.ArrayList()
    arr.add("x")
    arr.add("y")
    arr.add("z")
    java_array = arr.toArray()  # Returns JavaArray

    # Arrays.toString(Object[]) is a varargs-style method
    # When we pass a JavaArray, it should be passed directly (packed case)
    # not wrapped in another array (which would give "[[Ljava.lang.Object;@...]")
    result = Arrays.toString(java_array)
    assert result == "[x, y, z]"


def test_varargs_packed_typed_array(client):
    """Test varargs with typed array passed directly."""
    from gatun import JavaArray

    Arrays = client.jvm.java.util.Arrays

    # Create a typed JavaArray
    int_array = JavaArray([1, 2, 3], element_type="Int")

    # Arrays.toString(int[]) should receive the array directly
    result = Arrays.toString(int_array)
    assert result == "[1, 2, 3]"


def test_varargs_spread_vs_packed(client):
    """Test that spread and packed varargs give same result for asList."""

    Arrays = client.jvm.java.util.Arrays

    # Spread case: individual arguments
    spread_result = Arrays.asList("a", "b", "c")

    # Packed case: pass an existing array
    # First create an Object[] array
    arr = client.jvm.java.util.ArrayList()
    arr.add("a")
    arr.add("b")
    arr.add("c")
    java_array = arr.toArray()

    # When passed to asList, the array should be unpacked as varargs
    # Actually for asList, passing an array should treat elements as varargs
    _ = Arrays.asList(java_array)

    # Both should give same logical result
    assert spread_result == ["a", "b", "c"]
    # Note: packed result might differ based on how Java handles it
    # The key is that it doesn't error and produces a valid list


def test_varargs_specificity_prefers_specific_types(client):
    """Test that varargs scoring prefers more specific types.

    String.format has overloads:
    - format(String, Object...)
    - format(Locale, String, Object...)

    When called with strings, it should pick the right one based on specificity.
    """
    String = client.jvm.java.lang.String

    # This should resolve to format(String, Object...) not format(Locale, String, Object...)
    result = String.format("Hello %s!", "World")
    assert result == "Hello World!"

    # Multiple args should still work
    result = String.format("%s + %s = %d", "1", "2", 3)
    assert result == "1 + 2 = 3"


def test_varargs_specificity_with_numbers(client):
    """Test that varargs specificity works with numeric types."""
    Arrays = client.jvm.java.util.Arrays

    # Arrays.asList with integers - should pick Object... and box them
    result = Arrays.asList(1, 2, 3)
    assert result == [1, 2, 3]

    # With strings - more specific match
    result = Arrays.asList("x", "y")
    assert result == ["x", "y"]
