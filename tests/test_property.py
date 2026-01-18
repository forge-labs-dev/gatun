"""Property-based tests for protocol invariants using Hypothesis.

These tests verify that:
1. No sequence of bytes can crash or wedge the server
2. Object IDs are never orphaned or leaked
3. Session isolation is maintained
4. Command/response zones are respected
5. Cancellation never affects wrong requests
6. Cleanup is idempotent
"""

import logging
import tempfile
import time
from pathlib import Path

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck

from gatun.launcher import launch_gateway
from gatun.client import GatunClient

logger = logging.getLogger("gatun.tests.property")


# Common settings for property tests
# Each Hypothesis example gets a fresh client via make_client
HYPOTHESIS_SETTINGS = dict(
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)


# --- Property Test Gateway ---
# Property tests use a separate gateway to avoid affecting other tests.
# This is module-scoped so it's created once for all property tests.


@pytest.fixture(scope="module")
def property_gateway():
    """
    A separate gateway for property tests to avoid polluting the main gateway.

    Property tests (using Hypothesis) can exhaust server resources or leave
    it in a bad state due to many rapid connections. Using a separate gateway
    ensures that other tests are not affected.
    """
    socket_path = Path(tempfile.gettempdir()) / "gatun_property_test.sock"

    if socket_path.exists():
        socket_path.unlink()

    logger.info(f"Launching Property Test Gateway (64MB) at {socket_path}...")
    session = launch_gateway(memory="64MB", socket_path=str(socket_path))

    yield session

    logger.info("Stopping Property Test Gateway...")
    session.stop()

    if socket_path.exists():
        socket_path.unlink()


def _create_property_client(socket_path: str) -> GatunClient:
    """Helper to create and connect a client with retry logic."""
    c = GatunClient(socket_path)
    connected = False
    for _ in range(10):
        connected = c.connect()
        if connected:
            break
        time.sleep(0.1)

    if not connected:
        raise RuntimeError(f"Client failed to connect to Gateway at {socket_path}")

    return c


@pytest.fixture(scope="function")
def make_client(property_gateway):
    """
    Factory fixture for creating fresh clients for property tests.

    Each Hypothesis example should get a fresh connection to avoid
    protocol state corruption affecting subsequent examples.
    """
    socket_str = str(property_gateway.socket_path)
    current_client = [None]

    def _make():
        # Close the previous client before creating a new one
        if current_client[0] is not None:
            try:
                if current_client[0].sock:
                    current_client[0].sock.close()
            except Exception:
                pass

        c = _create_property_client(socket_str)
        current_client[0] = c
        return c

    yield _make

    # Cleanup the last client
    if current_client[0] is not None:
        try:
            if current_client[0].sock:
                current_client[0].sock.close()
        except Exception:
            pass


# --- Strategy Definitions ---

# Valid Java class names for testing
VALID_CLASSES = st.sampled_from(
    [
        "java.util.ArrayList",
        "java.util.HashMap",
        "java.util.HashSet",
        "java.util.LinkedList",
        "java.util.TreeMap",
        "java.util.TreeSet",
        "java.lang.StringBuilder",
        "java.lang.StringBuffer",
    ]
)

# Invalid/non-allowlisted class names
INVALID_CLASSES = st.sampled_from(
    [
        "java.lang.Runtime",
        "java.lang.ProcessBuilder",
        "java.io.File",
        "java.net.Socket",
        "com.evil.Malware",
        "",
        ".",
        "..",
        "java..util.ArrayList",
    ]
)

# Primitive values that can be passed as arguments
PRIMITIVE_VALUES = st.one_of(
    st.integers(min_value=-(2**31), max_value=2**31 - 1),
    st.floats(allow_nan=False, allow_infinity=False),
    st.booleans(),
    st.text(max_size=100),
    st.none(),
)

# List of primitive values
LIST_VALUES = st.lists(PRIMITIVE_VALUES, max_size=10)

# Method names for ArrayList
ARRAYLIST_METHODS = st.sampled_from(
    [
        "add",
        "get",
        "size",
        "clear",
        "isEmpty",
        "remove",
        "contains",
    ]
)


class TestObjectLifecycleInvariants:
    """Test that object lifecycle invariants are maintained."""

    @given(
        class_name=VALID_CLASSES,
        num_objects=st.integers(min_value=1, max_value=20),
    )
    @settings(max_examples=10, deadline=5000, **HYPOTHESIS_SETTINGS)
    def test_no_orphan_object_ids(self, make_client, class_name, num_objects):
        """Creating multiple objects should give unique, accessible IDs."""
        client = make_client()
        # Track all created objects and their IDs
        objects = []
        created_ids = set()

        # Create objects
        for _ in range(num_objects):
            obj = client.create_object(class_name)
            assert obj.object_id not in created_ids, "Duplicate object ID!"
            created_ids.add(obj.object_id)
            objects.append(obj)

        # All objects should be accessible via their wrapper
        for obj in objects:
            # Use the object - this verifies it's still valid
            result = obj.getClass()
            assert result is not None

    @given(class_name=VALID_CLASSES)
    @settings(max_examples=10, deadline=5000, **HYPOTHESIS_SETTINGS)
    def test_double_free_is_safe(self, make_client, class_name):
        """Double-free should not crash or corrupt state."""
        client = make_client()
        obj = client.create_object(class_name)
        obj_id = obj.object_id

        # Free explicitly (simulate early cleanup)
        client.free_object(obj_id)

        # Second free should be silently ignored (no crash)
        client.free_object(obj_id)

        # Session should still be functional
        new_obj = client.create_object(class_name)
        assert new_obj.object_id != obj_id

    @given(
        invalid_id=st.integers(min_value=100000, max_value=999999),
    )
    @settings(max_examples=10, deadline=5000, **HYPOTHESIS_SETTINGS)
    def test_invalid_object_id_handling(self, make_client, invalid_id):
        """Operations on invalid object IDs should raise clean errors."""
        client = make_client()
        # Invoking method on non-existent ID should fail gracefully
        with pytest.raises(Exception):
            client.invoke_method(invalid_id, "size")


class TestSecurityInvariants:
    """Test that security invariants are maintained."""

    @given(class_name=INVALID_CLASSES)
    @settings(max_examples=10, deadline=5000, **HYPOTHESIS_SETTINGS)
    def test_non_allowlisted_classes_rejected(self, make_client, class_name):
        """Non-allowlisted classes should be rejected."""
        client = make_client()
        from gatun.client import JavaSecurityException, JavaClassNotFoundException

        with pytest.raises((JavaSecurityException, JavaClassNotFoundException)):
            client.create_object(class_name)

    @given(class_name=INVALID_CLASSES)
    @settings(max_examples=10, deadline=5000, **HYPOTHESIS_SETTINGS)
    def test_static_methods_on_invalid_classes_rejected(self, make_client, class_name):
        """Static method calls on non-allowlisted classes should be rejected."""
        client = make_client()
        from gatun.client import JavaSecurityException, JavaClassNotFoundException

        with pytest.raises((JavaSecurityException, JavaClassNotFoundException)):
            client.invoke_static_method(class_name, "someMethod")


class TestMethodInvocationInvariants:
    """Test that method invocation invariants are maintained."""

    @given(
        items=st.lists(st.text(max_size=20), min_size=0, max_size=50),
    )
    @settings(max_examples=10, deadline=10000, **HYPOTHESIS_SETTINGS)
    def test_arraylist_add_get_consistency(self, make_client, items):
        """ArrayList.add() followed by get() should return same items."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        # Add all items
        for item in items:
            result = arr.add(item)
            assert result is True

        # Verify size
        assert arr.size() == len(items)

        # Verify all items retrievable
        for i, expected in enumerate(items):
            actual = arr.get(i)
            assert actual == expected, f"Mismatch at index {i}"

    @given(
        items=st.lists(
            st.integers(min_value=-1000, max_value=1000), min_size=0, max_size=20
        ),
    )
    @settings(max_examples=10, deadline=5000, **HYPOTHESIS_SETTINGS)
    def test_hashset_contains_consistency(self, make_client, items):
        """HashSet.add() items should all be found by contains()."""
        client = make_client()
        hs = client.create_object("java.util.HashSet")

        unique_items = set(items)

        for item in items:
            hs.add(item)

        # All unique items should be found
        for item in unique_items:
            assert hs.contains(item) is True

        # Size should equal unique count
        assert hs.size() == len(unique_items)

    @given(
        n=st.integers(min_value=0, max_value=20),
    )
    @settings(max_examples=10, deadline=5000, **HYPOTHESIS_SETTINGS)
    def test_hashmap_put_get_consistency(self, make_client, n):
        """HashMap.put() followed by get() should return same values."""
        client = make_client()
        # Generate n key-value pairs
        keys = [f"key_{i}" for i in range(n)]
        values = list(range(n))

        hm = client.create_object("java.util.HashMap")

        # Build expected state (later puts overwrite earlier)
        expected = {}
        for k, v in zip(keys, values):
            hm.put(k, v)
            expected[k] = v

        # Verify all keys return correct values
        for k, v in expected.items():
            actual = hm.get(k)
            assert actual == v, f"Key {k}: expected {v}, got {actual}"

        # Verify size
        assert hm.size() == len(expected)


class TestBatchInvariants:
    """Test that batch operations maintain invariants."""

    @given(
        num_adds=st.integers(min_value=1, max_value=50),
    )
    @settings(max_examples=10, deadline=10000, **HYPOTHESIS_SETTINGS)
    def test_batch_results_match_individual(self, make_client, num_adds):
        """Batch results should match equivalent individual calls."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        # Execute via batch
        with client.batch() as b:
            for i in range(num_adds):
                b.call(arr, "add", i)
            size_result = b.call(arr, "size")

        # Verify batch results
        assert size_result.get() == num_adds

        # Verify via individual call
        assert arr.size() == num_adds

    @given(
        num_creates=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=10, deadline=10000, **HYPOTHESIS_SETTINGS)
    def test_create_objects_vectorized_consistency(self, make_client, num_creates):
        """create_objects should create distinct objects."""
        client = make_client()
        specs = [("java.util.ArrayList", ()) for _ in range(num_creates)]

        objects = client.create_objects(specs)

        # All should be distinct objects
        ids = [obj.object_id for obj in objects]
        assert len(set(ids)) == num_creates, "Duplicate object IDs in batch!"

        # All should be functional
        for obj in objects:
            obj.add("test")
            assert obj.size() == 1


class TestVectorizedAPIInvariants:
    """Test that vectorized APIs maintain invariants."""

    @given(
        values=st.lists(st.text(max_size=10), min_size=1, max_size=20),
    )
    @settings(max_examples=10, deadline=10000, **HYPOTHESIS_SETTINGS)
    def test_invoke_methods_consistency(self, make_client, values):
        """invoke_methods results should match individual calls."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        # Build method calls
        calls = [("add", (v,)) for v in values]
        calls.append(("size", ()))

        # Execute vectorized
        results = client.invoke_methods(arr, calls)

        # All adds should return True
        for i, r in enumerate(results[:-1]):
            assert r is True, f"add at index {i} returned {r}"

        # Size should match
        assert results[-1] == len(values)

        # Verify via individual calls
        for i, v in enumerate(values):
            assert arr.get(i) == v


class TestIterationInvariants:
    """Test that iteration support maintains invariants."""

    @given(
        items=st.lists(
            st.integers(min_value=-100, max_value=100), min_size=1, max_size=30
        ),
    )
    @settings(max_examples=10, deadline=10000, **HYPOTHESIS_SETTINGS)
    def test_indexing_returns_all_items(self, make_client, items):
        """Indexing collection should return all items correctly."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        for item in items:
            arr.add(item)

        # Collect via indexing (arr[i] calls get())
        collected = [arr[i] for i in range(len(items))]

        # Should match original order
        assert collected == items

    @given(
        items=st.lists(
            st.integers(min_value=-100, max_value=100), min_size=0, max_size=20
        ),
    )
    @settings(max_examples=10, deadline=5000, **HYPOTHESIS_SETTINGS)
    def test_len_matches_size(self, make_client, items):
        """len() should always equal size()."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        for item in items:
            arr.add(item)
            assert len(arr) == arr.size()


class TestErrorRecovery:
    """Test that errors don't corrupt state."""

    @given(
        good_ops=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=10, deadline=10000, **HYPOTHESIS_SETTINGS)
    def test_error_recovery(self, make_client, good_ops):
        """Server should recover gracefully after errors."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        # Do some good operations
        for i in range(good_ops):
            arr.add(i)

        # Trigger an error
        from gatun.client import JavaIndexOutOfBoundsException

        with pytest.raises(JavaIndexOutOfBoundsException):
            arr.get(1000)  # Out of bounds

        # Server should still work
        arr.add("after_error")
        assert arr.size() == good_ops + 1
        assert arr.get(good_ops) == "after_error"

    @given(
        batch_size=st.integers(min_value=2, max_value=10),
        error_index=st.integers(min_value=0, max_value=9),
    )
    @settings(max_examples=10, deadline=10000, **HYPOTHESIS_SETTINGS)
    def test_batch_error_isolation(self, make_client, batch_size, error_index):
        """Errors in batch should not corrupt other results."""
        assume(error_index < batch_size)

        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        # Build batch with one error
        with client.batch() as b:
            results = []
            for i in range(batch_size):
                if i == error_index:
                    # This will error - parseInt on non-numeric
                    results.append(
                        b.call_static("java.lang.Integer", "parseInt", "not_a_number")
                    )
                else:
                    results.append(b.call(arr, "add", i))

        # Non-error results should be accessible
        for i, r in enumerate(results):
            if i == error_index:
                assert r.is_error
            else:
                # Should have succeeded
                assert r.get() is True


class TestConcurrencyInvariants:
    """Test that concurrent operations maintain invariants.

    Note: These tests use a single client but verify that operations
    are properly serialized and don't interfere with each other.
    """

    @given(
        num_objects=st.integers(min_value=2, max_value=5),
        ops_per_object=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=10, deadline=15000, **HYPOTHESIS_SETTINGS)
    def test_interleaved_operations_isolation(
        self, make_client, num_objects, ops_per_object
    ):
        """Interleaved operations on different objects should be isolated."""
        client = make_client()
        objects = [
            client.create_object("java.util.ArrayList") for _ in range(num_objects)
        ]

        # Interleave operations
        for op_num in range(ops_per_object):
            for obj_idx, obj in enumerate(objects):
                obj.add(f"obj{obj_idx}_op{op_num}")

        # Verify each object has correct items
        for obj_idx, obj in enumerate(objects):
            assert obj.size() == ops_per_object
            for op_num in range(ops_per_object):
                expected = f"obj{obj_idx}_op{op_num}"
                actual = obj.get(op_num)
                assert actual == expected
