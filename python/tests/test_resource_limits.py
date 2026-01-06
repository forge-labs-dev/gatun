"""Tests for server-side resource limits.

These tests verify that ResourceLimits are enforced by the Java server.
We use extra_jvm_flags to set low limits so we can test enforcement
without creating massive objects.
"""

import pytest
from gatun import connect, JavaException


class TestObjectCountLimit:
    """Test that object count limits are enforced."""

    def test_object_limit_exceeded(self):
        """Test that creating too many objects raises an error."""
        # Start server with very low object limit
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=5"])
        try:
            objects = []
            # Create objects up to the limit
            for i in range(5):
                obj = client.create_object("java.util.ArrayList")
                objects.append(obj)  # Keep references to prevent GC

            # The 6th object should fail
            with pytest.raises(JavaException) as exc_info:
                objects.append(client.create_object("java.util.ArrayList"))

            assert "Object limit exceeded" in str(exc_info.value)
            assert "max: 5" in str(exc_info.value)
        finally:
            client.close()

    def test_object_limit_after_free(self):
        """Test that freeing objects allows creating new ones."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=3"])
        try:
            # Create 3 objects (at limit)
            obj1 = client.create_object("java.util.ArrayList")
            obj2 = client.create_object("java.util.ArrayList")
            obj3 = client.create_object("java.util.ArrayList")

            # Free one object
            client.free_object(obj2.object_id)

            # Should now be able to create another
            obj4 = client.create_object("java.util.ArrayList")
            assert obj4 is not None

            # Keep references
            _ = (obj1, obj3, obj4)
        finally:
            client.close()


class TestStringLengthLimit:
    """Test that string length limits are enforced."""

    def test_string_too_long(self):
        """Test that strings exceeding the limit are rejected."""
        # Set a very low string limit (1KB)
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_string_length=1024"])
        try:
            # Create a string longer than 1KB
            long_string = "x" * 2000

            # Trying to pass this to Java should fail
            with pytest.raises(JavaException) as exc_info:
                client.jvm.java.lang.StringBuilder(long_string)

            assert "String too long" in str(exc_info.value)
        finally:
            client.close()

    def test_string_within_limit(self):
        """Test that strings within the limit work fine."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_string_length=1024"])
        try:
            # Create a string under 1KB
            short_string = "x" * 500
            sb = client.jvm.java.lang.StringBuilder(short_string)
            assert sb.toString() == short_string
        finally:
            client.close()


class TestCollectionSizeLimit:
    """Test that collection size limits are enforced."""

    def test_list_too_large(self):
        """Test that lists exceeding the limit are rejected."""
        # Set a very low collection limit
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_list_entries=10"])
        try:
            # Create a list with more than 10 entries
            big_list = list(range(20))

            # Trying to pass this to Java should fail
            with pytest.raises(JavaException) as exc_info:
                client.create_object("java.util.ArrayList", big_list)

            assert "Collection too large" in str(exc_info.value)
        finally:
            client.close()

    def test_dict_too_large(self):
        """Test that dicts exceeding the limit are rejected."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_list_entries=10"])
        try:
            # Create a dict with more than 10 entries
            big_dict = {f"key_{i}": i for i in range(20)}

            # Trying to pass this to Java should fail
            with pytest.raises(JavaException) as exc_info:
                client.create_object("java.util.HashMap", big_dict)

            assert "Collection too large" in str(exc_info.value)
        finally:
            client.close()

    def test_collection_within_limit(self):
        """Test that collections within the limit work fine."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_list_entries=10"])
        try:
            # Create a list under the limit
            small_list = list(range(5))
            arr = client.create_object("java.util.ArrayList", small_list)
            assert arr.size() == 5
        finally:
            client.close()


class TestBatchSizeLimit:
    """Test that batch size limits are enforced."""

    def test_batch_too_large(self):
        """Test that batches exceeding the limit are rejected."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_batch_size=5"])
        try:
            arr = client.create_object("java.util.ArrayList")

            # Create a batch with more than 5 commands
            with pytest.raises(JavaException) as exc_info:
                with client.batch() as b:
                    for i in range(10):
                        b.call(arr, "add", i)

            assert "Batch too large" in str(exc_info.value)
        finally:
            client.close()

    def test_batch_within_limit(self):
        """Test that batches within the limit work fine."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_batch_size=10"])
        try:
            arr = client.create_object("java.util.ArrayList")

            # Create a batch under the limit
            with client.batch() as b:
                for i in range(5):
                    b.call(arr, "add", i)
                size_result = b.call(arr, "size")

            assert size_result.get() == 5
        finally:
            client.close()


class TestMethodCallsLimit:
    """Test that invoke_methods limit is enforced."""

    def test_invoke_methods_too_many(self):
        """Test that too many method calls are rejected."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_method_calls=5"])
        try:
            arr = client.create_object("java.util.ArrayList")

            # Try to invoke more than 5 methods
            calls = [("add", (i,)) for i in range(10)]

            with pytest.raises(JavaException) as exc_info:
                client.invoke_methods(arr, calls)

            assert "Batch too large" in str(exc_info.value)
        finally:
            client.close()

    def test_invoke_methods_within_limit(self):
        """Test that method calls within the limit work fine."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_method_calls=10"])
        try:
            arr = client.create_object("java.util.ArrayList")

            # Invoke under the limit
            calls = [("add", (i,)) for i in range(5)]
            results = client.invoke_methods(arr, calls)

            assert len(results) == 5
            assert all(r is True for r in results)
        finally:
            client.close()


class TestCreateObjectsLimit:
    """Test that create_objects limit is enforced."""

    def test_create_objects_too_many(self):
        """Test that creating too many objects in one call is rejected."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_create_objects=5"])
        try:
            # Try to create more than 5 objects
            specs = [("java.util.ArrayList", ()) for _ in range(10)]

            with pytest.raises(JavaException) as exc_info:
                client.create_objects(specs)

            assert "Batch too large" in str(exc_info.value)
        finally:
            client.close()

    def test_create_objects_within_limit(self):
        """Test that create_objects within the limit work fine."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_create_objects=10"])
        try:
            # Create under the limit
            specs = [("java.util.ArrayList", ()) for _ in range(5)]
            objects = client.create_objects(specs)

            assert len(objects) == 5
            assert all(obj is not None for obj in objects)
        finally:
            client.close()


class TestNestingDepthLimit:
    """Test that nesting depth limits are enforced."""

    def test_nesting_too_deep(self):
        """Test that deeply nested structures are rejected."""
        # Set a very low nesting limit (2 levels)
        # The check is: if (depth > max) throw
        # So with max=2, depths 0, 1, 2 are OK, depth 3 fails
        # We need 4 levels of list nesting to reach depth 3
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_nesting_depth=2"])
        try:
            # Create a nested structure that exceeds the limit
            # [[[[x]]]] = 4 levels of nesting, which means depth reaches 3
            nested = [[[["too deep"]]]]

            # Trying to pass this to Java should fail
            with pytest.raises(JavaException) as exc_info:
                client.create_object("java.util.ArrayList", nested)

            assert "Nesting too deep" in str(exc_info.value)
        finally:
            client.close()

    def test_nesting_within_limit(self):
        """Test that nesting within the limit works fine."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_nesting_depth=5"])
        try:
            # Create a structure with 2 levels of nesting (within limit)
            nested = [["shallow"]]
            arr = client.create_object("java.util.ArrayList", nested)
            assert arr.size() == 1
        finally:
            client.close()


class TestMultipleLimits:
    """Test combining multiple limits."""

    def test_string_and_object_limits_independent(self):
        """Test that string and object limits are enforced independently."""
        # Test string limit first (in separate client to avoid object count issues)
        client1 = connect(extra_jvm_flags=["-Dgatun.limits.max_string_length=100"])
        try:
            with pytest.raises(JavaException) as exc_info:
                client1.jvm.java.lang.StringBuilder("x" * 200)
            assert "String too long" in str(exc_info.value)
        finally:
            client1.close()

        # Test object limit separately
        client2 = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=5"])
        try:
            # Create objects up to the limit
            objects = []
            for i in range(5):
                objects.append(client2.create_object("java.util.ArrayList"))

            # The 6th object should fail
            with pytest.raises(JavaException) as exc_info:
                objects.append(client2.create_object("java.util.ArrayList"))
            assert "Object limit exceeded" in str(exc_info.value)
        finally:
            client2.close()
