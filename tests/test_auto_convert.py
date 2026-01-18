"""Tests for auto-convert between Java collections and Python list/dict."""


def test_java_list_to_python_list(client):
    """Test that Java List returned from method is auto-converted to Python list."""
    # Create an ArrayList and use subList which returns a List
    arr = client.jvm.java.util.ArrayList()
    arr.add("one")
    arr.add("two")
    arr.add("three")

    # subList returns a List - should be auto-converted to Python list
    sub = arr.subList(0, 2)
    assert isinstance(sub, list)
    assert sub == ["one", "two"]


def test_java_map_to_python_dict(client):
    """Test that Java HashMap returned from method is auto-converted."""
    # Create a map and return it via Collections method
    Collections = client.jvm.java.util.Collections

    # singletonMap returns a Map
    result = Collections.singletonMap("key", "value")
    assert isinstance(result, dict)
    assert result == {"key": "value"}


def test_java_empty_list(client):
    """Test empty list conversion."""
    Collections = client.jvm.java.util.Collections
    result = Collections.emptyList()
    assert isinstance(result, list)
    assert result == []


def test_java_empty_map(client):
    """Test empty map conversion."""
    Collections = client.jvm.java.util.Collections
    result = Collections.emptyMap()
    assert isinstance(result, dict)
    assert result == {}


def test_nested_list(client):
    """Test nested list conversion via subList."""
    # Create two ArrayLists and add them to an outer ArrayList
    outer = client.jvm.java.util.ArrayList()

    inner1 = client.jvm.java.util.ArrayList()
    inner1.add("a")
    inner1.add("b")

    inner2 = client.jvm.java.util.ArrayList()
    inner2.add("c")
    inner2.add("d")

    outer.add(inner1)
    outer.add(inner2)

    # subList returns a List view which should be auto-converted
    result = outer.subList(0, 2)
    assert isinstance(result, list)
    assert len(result) == 2
    # The inner lists should also be auto-converted
    assert result[0] == ["a", "b"]
    assert result[1] == ["c", "d"]


def test_list_with_mixed_types(client):
    """Test list with mixed primitive types."""
    arr = client.jvm.java.util.ArrayList()
    arr.add("hello")
    arr.add(42)
    arr.add(3.14)

    # subList returns a List that should be auto-converted
    result = arr.subList(0, 3)
    assert isinstance(result, list)
    assert result[0] == "hello"
    assert result[1] == 42
    assert abs(result[2] - 3.14) < 0.001


def test_map_with_multiple_entries(client):
    """Test map with multiple entries."""
    HashMap = client.jvm.java.util.HashMap
    my_map = HashMap()
    my_map.put("one", 1)
    my_map.put("two", 2)
    my_map.put("three", 3)

    Collections = client.jvm.java.util.Collections

    # Create an unmodifiable map from our map
    unmod = Collections.unmodifiableMap(my_map)
    # This should return as dict since it implements Map
    assert isinstance(unmod, dict)
    assert unmod == {"one": 1, "two": 2, "three": 3}


def test_list_singleton(client):
    """Test Collections.singletonList returns a Python list."""
    Collections = client.jvm.java.util.Collections
    result = Collections.singletonList("only")
    assert isinstance(result, list)
    assert result == ["only"]


def test_map_with_list_values(client):
    """Test map containing list values."""
    HashMap = client.jvm.java.util.HashMap
    my_map = HashMap()

    list1 = client.jvm.java.util.ArrayList()
    list1.add(1)
    list1.add(2)

    my_map.put("numbers", list1)
    my_map.put("name", "test")

    Collections = client.jvm.java.util.Collections
    result = Collections.unmodifiableMap(my_map)

    assert isinstance(result, dict)
    assert result["name"] == "test"
    assert result["numbers"] == [1, 2]
