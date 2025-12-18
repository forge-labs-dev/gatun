"""Tests for Python callback support (Java calling back into Python)."""

import pytest

from gatun import JavaException


class TestCallbackBasics:
    """Basic callback registration and invocation tests."""

    def test_register_callback_returns_java_object(self, client):
        """Test that registering a callback returns a JavaObject."""
        from gatun import JavaObject

        def my_callback(*args):
            return sum(args) if args else 0

        callback_obj = client.register_callback(my_callback, "java.util.function.IntSupplier")
        assert callback_obj is not None
        assert isinstance(callback_obj, JavaObject)

    def test_comparator_for_sorting(self, client):
        """Test using a callback as a Comparator for sorting."""

        def reverse_compare(a, b):
            # Reverse order comparison
            if a < b:
                return 1
            elif a > b:
                return -1
            return 0

        comparator = client.register_callback(reverse_compare, "java.util.Comparator")

        # Create an ArrayList with some values
        arr = client.create_object("java.util.ArrayList")
        arr.add("banana")
        arr.add("apple")
        arr.add("cherry")

        # Sort using Collections.sort with our comparator
        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # Should be sorted in reverse order
        assert arr.get(0) == "cherry"
        assert arr.get(1) == "banana"
        assert arr.get(2) == "apple"

    def test_comparator_for_integer_sorting(self, client):
        """Test using a callback as a Comparator for sorting integers."""

        def compare_ints(a, b):
            # Sort in descending order
            return b - a

        comparator = client.register_callback(compare_ints, "java.util.Comparator")

        # Create an ArrayList with Integer objects
        arr = client.create_object("java.util.ArrayList")
        arr.add(client.invoke_static_method("java.lang.Integer", "valueOf", 5))
        arr.add(client.invoke_static_method("java.lang.Integer", "valueOf", 2))
        arr.add(client.invoke_static_method("java.lang.Integer", "valueOf", 8))
        arr.add(client.invoke_static_method("java.lang.Integer", "valueOf", 1))

        # Sort using Collections.sort with our comparator
        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # Should be sorted in descending order
        assert arr.get(0) == 8
        assert arr.get(1) == 5
        assert arr.get(2) == 2
        assert arr.get(3) == 1

    def test_comparator_natural_order(self, client):
        """Test using a callback as a Comparator for natural ordering."""

        def natural_compare(a, b):
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        comparator = client.register_callback(natural_compare, "java.util.Comparator")

        # Create a TreeSet with our comparator to test ordering
        # Note: TreeSet isn't in allowlist, using ArrayList + sort
        arr = client.create_object("java.util.ArrayList")
        arr.add("zebra")
        arr.add("alpha")
        arr.add("beta")

        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # Should be sorted alphabetically
        assert arr.get(0) == "alpha"
        assert arr.get(1) == "beta"
        assert arr.get(2) == "zebra"


class TestCallbackWithCollections:
    """Test callbacks used with Java collections."""

    def test_foreach_with_consumer(self, client):
        """Test using a callback with forEach."""
        collected = []

        def collect(item):
            collected.append(item)
            return None

        consumer = client.register_callback(collect, "java.util.function.Consumer")

        # Create a list and iterate with forEach
        arr = client.create_object("java.util.ArrayList")
        arr.add("one")
        arr.add("two")
        arr.add("three")

        arr.forEach(consumer)

        assert collected == ["one", "two", "three"]

    def test_replaceall_with_unaryoperator(self, client):
        """Test using a callback with replaceAll on ArrayList."""

        def uppercase(s):
            return s.upper() if isinstance(s, str) else str(s).upper()

        operator = client.register_callback(uppercase, "java.util.function.UnaryOperator")

        arr = client.create_object("java.util.ArrayList")
        arr.add("hello")
        arr.add("world")

        arr.replaceAll(operator)

        assert arr.get(0) == "HELLO"
        assert arr.get(1) == "WORLD"

    def test_removeif_with_predicate(self, client):
        """Test using a callback with removeIf on ArrayList."""

        def is_short(s):
            return len(s) <= 3

        predicate = client.register_callback(is_short, "java.util.function.Predicate")

        arr = client.create_object("java.util.ArrayList")
        arr.add("a")
        arr.add("hello")
        arr.add("hi")
        arr.add("world")
        arr.add("bye")

        arr.removeIf(predicate)

        # Should have removed "a", "hi", "bye" (length <= 3)
        assert arr.size() == 2
        assert arr.get(0) == "hello"
        assert arr.get(1) == "world"

    def test_compute_with_bifunction(self, client):
        """Test using a callback with compute on HashMap."""

        def combine(key, old_value):
            if old_value is None:
                return "new_" + key
            return old_value + "_updated"

        bifunction = client.register_callback(combine, "java.util.function.BiFunction")

        hm = client.create_object("java.util.HashMap")
        hm.put("key1", "value1")

        # compute should update existing value
        hm.compute("key1", bifunction)
        assert hm.get("key1") == "value1_updated"

        # compute should create new value
        hm.compute("key2", bifunction)
        assert hm.get("key2") == "new_key2"


class TestCallbackErrorHandling:
    """Test error handling in callbacks."""

    def test_callback_exception_propagates(self, client):
        """Test that exceptions in callbacks are propagated."""

        def failing_callback(a, b):
            raise ValueError("Intentional error from Python")

        comparator = client.register_callback(failing_callback, "java.util.Comparator")

        arr = client.create_object("java.util.ArrayList")
        arr.add("a")
        arr.add("b")

        # Sorting should raise an exception
        with pytest.raises(JavaException) as excinfo:
            client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        assert "Intentional error from Python" in str(excinfo.value)


class TestCallbackMultipleInvocations:
    """Test callbacks that are invoked multiple times."""

    def test_callback_invoked_multiple_times(self, client):
        """Test that a callback can be invoked multiple times."""
        call_count = [0]

        def counting_compare(a, b):
            call_count[0] += 1
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        comparator = client.register_callback(counting_compare, "java.util.Comparator")

        arr = client.create_object("java.util.ArrayList")
        arr.add("c")
        arr.add("a")
        arr.add("b")
        arr.add("d")

        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # The comparator should have been called multiple times during sorting
        assert call_count[0] > 0
        # Verify sorting worked
        assert arr.get(0) == "a"
        assert arr.get(1) == "b"
        assert arr.get(2) == "c"
        assert arr.get(3) == "d"

    def test_callback_state_preserved(self, client):
        """Test that callback state is preserved across invocations."""
        seen_values = []

        def tracking_consumer(value):
            seen_values.append(value)
            return None

        consumer = client.register_callback(tracking_consumer, "java.util.function.Consumer")

        arr = client.create_object("java.util.ArrayList")
        for i in range(5):
            arr.add(f"item_{i}")

        arr.forEach(consumer)

        assert seen_values == ["item_0", "item_1", "item_2", "item_3", "item_4"]


class TestCallbackCleanup:
    """Test callback cleanup and unregistration."""

    def test_unregister_callback(self, client):
        """Test that unregistered callbacks are cleaned up."""

        def my_callback(a, b):
            return 0

        callback = client.register_callback(my_callback, "java.util.Comparator")
        callback_id = callback.object_id

        # Verify callback is registered
        assert callback_id in client._callbacks

        # Unregister
        client.unregister_callback(callback_id)

        # Callback should be removed from client's registry
        assert callback_id not in client._callbacks
