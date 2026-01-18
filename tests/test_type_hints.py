"""Tests for TypeHint functionality for overload resolution."""

from gatun import TypeHint


class TestTypeHintBasics:
    """Basic TypeHint functionality tests."""

    def test_type_hint_with_long(self, client):
        """Test forcing long type instead of int."""
        # Without type hint, value 42 would be treated as int
        # With type hint, we can force it to be treated as long
        result = client.invoke_static_method(
            "java.lang.Long", "valueOf", TypeHint(42, "long")
        )
        assert result == 42

    def test_type_hint_with_integer(self, client):
        """Test forcing Integer boxed type."""
        result = client.invoke_static_method(
            "java.lang.Integer", "valueOf", TypeHint(42, "int")
        )
        assert result == 42

    def test_type_hint_with_double(self, client):
        """Test forcing double type for float values."""
        # Math.sqrt expects double
        result = client.invoke_static_method(
            "java.lang.Math", "sqrt", TypeHint(16, "double")
        )
        assert result == 4.0

    def test_type_hint_list_as_collection(self, client):
        """Test using List interface type instead of ArrayList."""
        arr = client.create_object("java.util.ArrayList")
        arr.add("a")
        arr.add("b")

        # Collections.unmodifiableList accepts List, not ArrayList specifically
        # Type hint ensures we match the List interface
        result = client.invoke_static_method(
            "java.util.Collections",
            "unmodifiableList",
            TypeHint(arr, "java.util.List"),
        )
        assert result.size() == 2

    def test_type_hint_preserved_through_jvm_view(self, client):
        """Test that type hints work through JVM view API."""
        Long = client.jvm.java.lang.Long
        # Use type hint to force long interpretation
        result = Long.valueOf(TypeHint(9223372036854775807, "long"))
        assert result == 9223372036854775807


class TestTypeHintOverloadResolution:
    """Tests for type hint impact on method overload resolution."""

    def test_string_valueof_with_int_hint(self, client):
        """Test String.valueOf with explicit int type hint."""
        # String.valueOf has overloads for int, long, double, etc.
        result = client.invoke_static_method(
            "java.lang.String", "valueOf", TypeHint(42, "int")
        )
        assert result == "42"

    def test_string_valueof_with_long_hint(self, client):
        """Test String.valueOf with explicit long type hint."""
        result = client.invoke_static_method(
            "java.lang.String", "valueOf", TypeHint(42, "long")
        )
        assert result == "42"

    def test_string_valueof_with_double_hint(self, client):
        """Test String.valueOf with explicit double type hint."""
        result = client.invoke_static_method(
            "java.lang.String", "valueOf", TypeHint(42.5, "double")
        )
        assert result == "42.5"

    def test_string_valueof_with_boolean_hint(self, client):
        """Test String.valueOf with explicit boolean type hint."""
        result = client.invoke_static_method(
            "java.lang.String", "valueOf", TypeHint(True, "boolean")
        )
        assert result == "true"


class TestTypeHintWithCollections:
    """Tests for type hints with collection types."""

    def test_arraylist_constructor_with_collection_hint(self, client):
        """Test ArrayList constructor accepting Collection."""
        source = client.create_object("java.util.ArrayList")
        source.add("x")
        source.add("y")

        # ArrayList(Collection) constructor
        copy = client.create_object(
            "java.util.ArrayList", TypeHint(source, "java.util.Collection")
        )
        assert copy.size() == 2
        assert copy.get(0) == "x"

    def test_collections_sort_with_list_hint(self, client):
        """Test Collections.sort with List type hint."""
        arr = client.create_object("java.util.ArrayList")
        arr.add("c")
        arr.add("a")
        arr.add("b")

        # Collections.sort(List) - type hint ensures List interface match
        client.invoke_static_method(
            "java.util.Collections", "sort", TypeHint(arr, "java.util.List")
        )

        assert arr.get(0) == "a"
        assert arr.get(1) == "b"
        assert arr.get(2) == "c"


class TestTypeHintEdgeCases:
    """Edge cases and error handling for type hints."""

    def test_type_hint_with_null_value(self, client):
        """Test type hint with None/null value."""
        hm = client.create_object("java.util.HashMap")
        # put(Object, Object) - type hint on null
        hm.put("key", TypeHint(None, "java.lang.Object"))
        assert hm.get("key") is None

    def test_type_hint_repr(self):
        """Test TypeHint string representation."""
        hint = TypeHint(42, "long")
        assert repr(hint) == "TypeHint(42, 'long')"

    def test_type_hint_with_object_ref(self, client):
        """Test type hint wrapping a JavaObject."""
        arr = client.create_object("java.util.ArrayList")
        arr.add("test")

        # Wrap JavaObject with interface type hint
        hint = TypeHint(arr, "java.util.List")
        assert hint.value is arr
        assert hint.java_type == "java.util.List"

    def test_type_hint_shortcut_names(self, client):
        """Test that short type names work (List vs java.util.List)."""
        arr = client.create_object("java.util.ArrayList")
        arr.add("test")

        # Both should work for interface type hints
        result1 = client.invoke_static_method(
            "java.util.Collections", "unmodifiableList", TypeHint(arr, "List")
        )
        result2 = client.invoke_static_method(
            "java.util.Collections", "unmodifiableList", TypeHint(arr, "java.util.List")
        )
        assert result1.size() == result2.size() == 1
