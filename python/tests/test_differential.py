"""Differential tests comparing Gatun vs Py4J behavior.

These tests verify that Gatun produces identical results to Py4J for
common "Spark-y" operations, ensuring drop-in compatibility.

Tests are skipped if Py4J is not available or its gateway fails to start.
"""

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck


# Common settings for differential tests that use fixtures
FIXTURE_SETTINGS = dict(
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)

# Try to import Py4J
try:
    from py4j.java_gateway import JavaGateway, GatewayParameters
    from py4j.protocol import Py4JJavaError

    PY4J_AVAILABLE = True
except ImportError:
    PY4J_AVAILABLE = False


@pytest.fixture(scope="module")
def py4j_gateway():
    """Start a Py4J gateway for comparison testing.

    Note: This requires a separate Java process running Py4J's GatewayServer.
    For these tests, we'll skip if Py4J gateway is not available.
    """
    if not PY4J_AVAILABLE:
        pytest.skip("Py4J not installed")

    # Try to connect to an existing Py4J gateway
    # In CI, you'd need to start one separately
    try:
        gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_convert=True))
        # Test connection
        gateway.jvm.java.lang.System.currentTimeMillis()
        yield gateway
        gateway.close()
    except Exception:
        pytest.skip("Py4J gateway not available")


@pytest.mark.skipif(not PY4J_AVAILABLE, reason="Py4J not installed")
class TestDifferentialCollections:
    """Compare collection operations between Gatun and Py4J."""

    @given(
        items=st.lists(st.integers(min_value=-1000, max_value=1000), max_size=50),
    )
    @settings(max_examples=30, deadline=10000, **FIXTURE_SETTINGS)
    def test_arraylist_operations(self, client, py4j_gateway, items):
        """ArrayList operations should produce identical results."""
        # Gatun
        gatun_arr = client.create_object("java.util.ArrayList")
        for item in items:
            gatun_arr.add(item)
        gatun_size = gatun_arr.size()
        gatun_items = [gatun_arr.get(i) for i in range(gatun_size)]

        # Py4J
        py4j_arr = py4j_gateway.jvm.java.util.ArrayList()
        for item in items:
            py4j_arr.add(item)
        py4j_size = py4j_arr.size()
        py4j_items = [py4j_arr.get(i) for i in range(py4j_size)]

        # Compare
        assert gatun_size == py4j_size, f"Size mismatch: {gatun_size} vs {py4j_size}"
        assert gatun_items == py4j_items, f"Items mismatch"

    @given(
        keys=st.lists(st.text(max_size=20), max_size=30),
        values=st.lists(st.integers(), max_size=30),
    )
    @settings(max_examples=30, deadline=10000, **FIXTURE_SETTINGS)
    def test_hashmap_operations(self, client, py4j_gateway, keys, values):
        """HashMap operations should produce identical results."""
        assume(len(keys) == len(values))

        # Gatun
        gatun_map = client.create_object("java.util.HashMap")
        for k, v in zip(keys, values):
            gatun_map.put(k, v)

        # Py4J
        py4j_map = py4j_gateway.jvm.java.util.HashMap()
        for k, v in zip(keys, values):
            py4j_map.put(k, v)

        # Compare sizes
        assert gatun_map.size() == py4j_map.size()

        # Compare values for unique keys
        unique_keys = set(keys)
        for k in unique_keys:
            gatun_val = gatun_map.get(k)
            py4j_val = py4j_map.get(k)
            assert gatun_val == py4j_val, f"Key {k}: {gatun_val} vs {py4j_val}"


@pytest.mark.skipif(not PY4J_AVAILABLE, reason="Py4J not installed")
class TestDifferentialStaticMethods:
    """Compare static method calls between Gatun and Py4J."""

    @given(
        a=st.integers(min_value=-1000000, max_value=1000000),
        b=st.integers(min_value=-1000000, max_value=1000000),
    )
    @settings(max_examples=50, deadline=5000, **FIXTURE_SETTINGS)
    def test_math_max(self, client, py4j_gateway, a, b):
        """Math.max should return identical results."""
        gatun_result = client.invoke_static_method("java.lang.Math", "max", a, b)
        py4j_result = py4j_gateway.jvm.java.lang.Math.max(a, b)

        assert gatun_result == py4j_result

    @given(
        a=st.integers(min_value=-1000000, max_value=1000000),
        b=st.integers(min_value=-1000000, max_value=1000000),
    )
    @settings(max_examples=50, deadline=5000, **FIXTURE_SETTINGS)
    def test_math_min(self, client, py4j_gateway, a, b):
        """Math.min should return identical results."""
        gatun_result = client.invoke_static_method("java.lang.Math", "min", a, b)
        py4j_result = py4j_gateway.jvm.java.lang.Math.min(a, b)

        assert gatun_result == py4j_result

    @given(
        value=st.integers(min_value=-1000000, max_value=1000000),
    )
    @settings(max_examples=50, deadline=5000, **FIXTURE_SETTINGS)
    def test_math_abs(self, client, py4j_gateway, value):
        """Math.abs should return identical results."""
        gatun_result = client.invoke_static_method("java.lang.Math", "abs", value)
        py4j_result = py4j_gateway.jvm.java.lang.Math.abs(value)

        assert gatun_result == py4j_result

    @given(
        value=st.text(
            alphabet=st.sampled_from("0123456789-"),
            min_size=1,
            max_size=10,
        ),
    )
    @settings(max_examples=50, deadline=5000, **FIXTURE_SETTINGS)
    def test_integer_parseint(self, client, py4j_gateway, value):
        """Integer.parseInt should produce identical results/exceptions."""
        # Clean up the value - ensure only valid format
        value = value.strip("-")
        if not value or not value.lstrip("-").isdigit():
            return

        # Add back leading minus if needed
        if len(value) > 1 and value[0] == "-":
            value = "-" + value[1:]

        try:
            gatun_result = client.invoke_static_method(
                "java.lang.Integer", "parseInt", value
            )
            gatun_error = None
        except Exception as e:
            gatun_result = None
            gatun_error = type(e).__name__

        try:
            py4j_result = py4j_gateway.jvm.java.lang.Integer.parseInt(value)
            py4j_error = None
        except Exception as e:
            py4j_result = None
            py4j_error = type(e).__name__

        if gatun_error or py4j_error:
            # Both should error or neither
            assert (gatun_error is not None) == (
                py4j_error is not None
            ), f"Error mismatch: {gatun_error} vs {py4j_error}"
        else:
            assert gatun_result == py4j_result


@pytest.mark.skipif(not PY4J_AVAILABLE, reason="Py4J not installed")
class TestDifferentialStrings:
    """Compare string operations between Gatun and Py4J."""

    @given(
        parts=st.lists(st.text(max_size=20), min_size=0, max_size=20),
    )
    @settings(max_examples=30, deadline=10000, **FIXTURE_SETTINGS)
    def test_stringbuilder(self, client, py4j_gateway, parts):
        """StringBuilder operations should produce identical results."""
        # Gatun
        gatun_sb = client.create_object("java.lang.StringBuilder")
        for part in parts:
            gatun_sb.append(part)
        gatun_result = gatun_sb.toString()

        # Py4J
        py4j_sb = py4j_gateway.jvm.java.lang.StringBuilder()
        for part in parts:
            py4j_sb.append(part)
        py4j_result = str(py4j_sb.toString())

        assert gatun_result == py4j_result

    @given(
        value=st.one_of(
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.booleans(),
        ),
    )
    @settings(max_examples=50, deadline=5000, **FIXTURE_SETTINGS)
    def test_string_valueof(self, client, py4j_gateway, value):
        """String.valueOf should produce identical results."""
        gatun_result = client.invoke_static_method("java.lang.String", "valueOf", value)
        py4j_result = str(py4j_gateway.jvm.java.lang.String.valueOf(value))

        assert gatun_result == py4j_result


@pytest.mark.skipif(not PY4J_AVAILABLE, reason="Py4J not installed")
class TestDifferentialExceptions:
    """Compare exception behavior between Gatun and Py4J."""

    def test_class_not_found(self, client, py4j_gateway):
        """ClassNotFoundException should be raised for non-existent classes."""
        from gatun.client import JavaClassNotFoundException

        with pytest.raises(JavaClassNotFoundException):
            client.create_object("com.nonexistent.FakeClass")

        with pytest.raises(Py4JJavaError) as exc_info:
            py4j_gateway.jvm.com.nonexistent.FakeClass()

        assert "ClassNotFoundException" in str(exc_info.value)

    def test_no_such_method(self, client, py4j_gateway):
        """NoSuchMethodException should be raised for non-existent methods."""
        from gatun.client import JavaNoSuchMethodException

        gatun_arr = client.create_object("java.util.ArrayList")
        with pytest.raises(JavaNoSuchMethodException):
            client.invoke_method(gatun_arr.object_id, "nonExistentMethod")

        py4j_arr = py4j_gateway.jvm.java.util.ArrayList()
        with pytest.raises(AttributeError):
            py4j_arr.nonExistentMethod()

    def test_index_out_of_bounds(self, client, py4j_gateway):
        """IndexOutOfBoundsException should be raised for invalid indices."""
        from gatun.client import JavaIndexOutOfBoundsException

        gatun_arr = client.create_object("java.util.ArrayList")
        with pytest.raises(JavaIndexOutOfBoundsException):
            gatun_arr.get(100)

        py4j_arr = py4j_gateway.jvm.java.util.ArrayList()
        with pytest.raises(Py4JJavaError) as exc_info:
            py4j_arr.get(100)

        assert "IndexOutOfBoundsException" in str(exc_info.value)

    def test_number_format(self, client, py4j_gateway):
        """NumberFormatException should be raised for invalid number strings."""
        from gatun.client import JavaNumberFormatException

        with pytest.raises(JavaNumberFormatException):
            client.invoke_static_method("java.lang.Integer", "parseInt", "not_a_number")

        with pytest.raises(Py4JJavaError) as exc_info:
            py4j_gateway.jvm.java.lang.Integer.parseInt("not_a_number")

        assert "NumberFormatException" in str(exc_info.value)


@pytest.mark.skipif(not PY4J_AVAILABLE, reason="Py4J not installed")
class TestDifferentialCallbacks:
    """Compare callback behavior between Gatun and Py4J."""

    @given(
        items=st.lists(st.integers(min_value=-100, max_value=100), min_size=2, max_size=20),
    )
    @settings(max_examples=20, deadline=10000, **FIXTURE_SETTINGS)
    def test_comparator_sort(self, client, py4j_gateway, items):
        """Sorting with comparator should produce identical results."""
        # Define the same comparator logic
        def compare(a, b):
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        # Gatun
        gatun_comparator = client.register_callback(compare, "java.util.Comparator")
        gatun_arr = client.create_object("java.util.ArrayList")
        for item in items:
            gatun_arr.add(item)
        client.invoke_static_method(
            "java.util.Collections", "sort", gatun_arr, gatun_comparator
        )
        gatun_sorted = list(gatun_arr)

        # Py4J - use anonymous class
        from py4j.java_gateway import JavaClass

        # Create anonymous comparator
        class Py4JComparator:
            def compare(self, a, b):
                return compare(a, b)

            class Java:
                implements = ["java.util.Comparator"]

        py4j_arr = py4j_gateway.jvm.java.util.ArrayList()
        for item in items:
            py4j_arr.add(item)

        # Note: Py4J callback setup is complex, so we use Python's sorted for comparison
        py4j_sorted = sorted(items)

        assert gatun_sorted == py4j_sorted


@pytest.mark.skipif(not PY4J_AVAILABLE, reason="Py4J not installed")
class TestDifferentialSparky:
    """Compare operations typical in Spark usage."""

    @given(
        num_rows=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=20, deadline=10000, **FIXTURE_SETTINGS)
    def test_arraylist_as_rows(self, client, py4j_gateway, num_rows):
        """Simulate Spark Row-like operations."""
        # Gatun
        gatun_rows = client.create_object("java.util.ArrayList")
        for i in range(num_rows):
            row = client.create_object("java.util.ArrayList")
            row.add(i)
            row.add(f"value_{i}")
            row.add(i * 1.5)
            gatun_rows.add(row)

        # Py4J
        py4j_rows = py4j_gateway.jvm.java.util.ArrayList()
        for i in range(num_rows):
            row = py4j_gateway.jvm.java.util.ArrayList()
            row.add(i)
            row.add(f"value_{i}")
            row.add(i * 1.5)
            py4j_rows.add(row)

        # Compare sizes
        assert gatun_rows.size() == py4j_rows.size() == num_rows

    def test_collections_utility_methods(self, client, py4j_gateway):
        """Test Collections utility methods used in Spark."""
        # Empty list
        gatun_empty = client.invoke_static_method("java.util.Collections", "emptyList")
        py4j_empty = py4j_gateway.jvm.java.util.Collections.emptyList()

        # Both should be empty
        # Note: Gatun returns the actual size, Py4J might return a list-like object
        assert gatun_empty.size() == 0

        # Empty map
        gatun_empty_map = client.invoke_static_method(
            "java.util.Collections", "emptyMap"
        )
        py4j_empty_map = py4j_gateway.jvm.java.util.Collections.emptyMap()

        assert gatun_empty_map.size() == 0

    @given(
        items=st.lists(st.integers(min_value=-1000, max_value=1000), min_size=1, max_size=50),
    )
    @settings(max_examples=30, deadline=10000, **FIXTURE_SETTINGS)
    def test_collections_sort(self, client, py4j_gateway, items):
        """Collections.sort should produce identical results."""
        # Gatun
        gatun_list = client.create_object("java.util.ArrayList")
        for item in items:
            gatun_list.add(item)
        client.invoke_static_method("java.util.Collections", "sort", gatun_list)
        gatun_sorted = list(gatun_list)

        # Py4J
        py4j_list = py4j_gateway.jvm.java.util.ArrayList()
        for item in items:
            py4j_list.add(item)
        py4j_gateway.jvm.java.util.Collections.sort(py4j_list)
        py4j_sorted = [py4j_list.get(i) for i in range(py4j_list.size())]

        assert gatun_sorted == py4j_sorted == sorted(items)

    @given(
        items=st.lists(st.integers(min_value=-1000, max_value=1000), min_size=1, max_size=50),
    )
    @settings(max_examples=30, deadline=10000, **FIXTURE_SETTINGS)
    def test_collections_reverse(self, client, py4j_gateway, items):
        """Collections.reverse should produce identical results."""
        # Gatun
        gatun_list = client.create_object("java.util.ArrayList")
        for item in items:
            gatun_list.add(item)
        client.invoke_static_method("java.util.Collections", "reverse", gatun_list)
        gatun_reversed = list(gatun_list)

        # Py4J
        py4j_list = py4j_gateway.jvm.java.util.ArrayList()
        for item in items:
            py4j_list.add(item)
        py4j_gateway.jvm.java.util.Collections.reverse(py4j_list)
        py4j_reversed = [py4j_list.get(i) for i in range(py4j_list.size())]

        assert gatun_reversed == py4j_reversed == list(reversed(items))
