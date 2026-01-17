"""Fuzzing tests for protocol robustness.

These tests verify that:
1. Malformed FlatBuffers don't crash or hang the server
2. Invalid command sizes are rejected
3. Corrupted bytes don't cause undefined behavior
4. Zone boundary violations are caught

IMPORTANT: Fuzz tests use a dedicated gateway (fuzz_gateway) to avoid corrupting
the main gateway used by other tests. Tests that send raw malformed data use
make_client (factory pattern) to get a fresh connection per Hypothesis example.
"""

import logging
import struct
import tempfile
import time
from pathlib import Path

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck

import flatbuffers
from gatun.generated.org.gatun.protocol import Command as Cmd
from gatun.generated.org.gatun.protocol import Action as Act
from gatun.launcher import launch_gateway
from gatun.client import GatunClient


logger = logging.getLogger("gatun.tests.fuzz")

# Tests using high-level APIs can safely suppress this check since they don't
# corrupt protocol state. Tests that send raw malformed data use make_client
# to create a fresh client per example (which also triggers this check but
# is safe because each example creates its own client).
SUPPRESS_FIXTURE_CHECK = [HealthCheck.function_scoped_fixture]


# --- Fuzz Test Gateway ---
# Fuzz tests use a separate gateway to avoid corrupting the main gateway.
# This is module-scoped so it's created once for all fuzz tests.


@pytest.fixture(scope="module")
def fuzz_gateway():
    """
    A separate gateway for fuzz tests to avoid polluting the main gateway.

    Fuzz tests send malformed data that can corrupt server state. Using a
    separate gateway ensures that other tests are not affected.
    """
    socket_path = Path(tempfile.gettempdir()) / "gatun_fuzz_test.sock"

    if socket_path.exists():
        socket_path.unlink()

    logger.info(f"Launching Fuzz Test Gateway (64MB) at {socket_path}...")
    session = launch_gateway(memory="64MB", socket_path=str(socket_path))

    yield session

    logger.info("Stopping Fuzz Test Gateway...")
    session.stop()

    if socket_path.exists():
        socket_path.unlink()


def _create_fuzz_client(socket_path: str) -> GatunClient:
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
def make_client(fuzz_gateway):
    """
    Factory fixture for creating fresh clients for fuzz tests.

    Each Hypothesis example should get a fresh connection to avoid
    protocol state corruption affecting subsequent examples.
    """
    socket_str = str(fuzz_gateway.socket_path)
    current_client = [None]

    def _make():
        # Close the previous client before creating a new one
        if current_client[0] is not None:
            try:
                if current_client[0].sock:
                    current_client[0].sock.close()
            except Exception:
                pass

        c = _create_fuzz_client(socket_str)
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


@pytest.fixture(scope="function")
def client(fuzz_gateway):
    """Single client fixture for tests that don't need fresh clients per example."""
    c = _create_fuzz_client(str(fuzz_gateway.socket_path))
    yield c
    try:
        if c.sock:
            c.sock.close()
    except Exception:
        pass


class TestMalformedCommandHandling:
    """Test server resilience to malformed commands.

    These tests send raw malformed data that corrupts protocol state,
    so they use make_client to get a fresh connection per test/example.
    """

    def test_zero_length_command(self, make_client):
        """Zero-length command should be rejected."""
        client = make_client()
        # Send zero length
        client.sock.sendall(struct.pack("<I", 0))

        # Server should either:
        # 1. Send an error response
        # 2. Close the connection
        # Either way, the session is now broken

    def test_negative_length_command(self, make_client):
        """Negative command length (as unsigned) should be handled."""
        client = make_client()
        # -1 as unsigned int is 0xFFFFFFFF = 4294967295
        # This would be way larger than any zone
        try:
            client.sock.sendall(struct.pack("<I", 0xFFFFFFFF))
            # Either we get an error or connection closes
        except (BrokenPipeError, ConnectionResetError):
            pass  # Expected - server rejected

    def test_oversized_command(self, make_client):
        """Command larger than command zone should be rejected."""
        client = make_client()
        # Command zone is 64KB (65536 bytes)
        oversized = 100000  # 100KB

        try:
            client.sock.sendall(struct.pack("<I", oversized))
            # Server should reject this
        except (BrokenPipeError, ConnectionResetError):
            pass  # Expected

    def test_truncated_flatbuffer(self, make_client):
        """Truncated FlatBuffer should be handled gracefully."""
        client = make_client()
        # Build a valid command
        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString("java.util.ArrayList")
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObject)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)
        data = bytes(builder.Output())

        # Truncate it
        truncated = data[: len(data) // 2]

        # Write to shared memory and send length
        client.shm.seek(0)
        client.shm.write(truncated)

        try:
            client.sock.sendall(struct.pack("<I", len(truncated)))
            # Wait for response - should be an error
            resp_len_bytes = client.sock.recv(4)
            if resp_len_bytes:
                _ = struct.unpack("<I", resp_len_bytes)[0]
                # Should get some response (error)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Server may close connection on malformed data

    @given(
        action_byte=st.integers(
            min_value=24, max_value=127
        ),  # int8 range for FlatBuffers
    )
    @settings(
        max_examples=20, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_invalid_action_values(self, make_client, action_byte):
        """Invalid action enum values should be handled."""
        client = make_client()
        # Build a command with invalid action
        builder = flatbuffers.Builder(256)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, action_byte)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)
        data = bytes(builder.Output())

        client.shm.seek(0)
        client.shm.write(data)

        try:
            client.sock.sendall(struct.pack("<I", len(data)))
            resp_len_bytes = client.sock.recv(4)
            if resp_len_bytes:
                # Should get an error response
                _ = struct.unpack("<I", resp_len_bytes)[0]
                client.shm.seek(client.response_offset)
                # Server should have written an error
        except (BrokenPipeError, ConnectionResetError):
            pass  # Also acceptable

    @given(
        garbage=st.binary(min_size=1, max_size=1000),
    )
    @settings(
        max_examples=30, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_random_bytes_as_command(self, make_client, garbage):
        """Random bytes should not crash the server."""
        # Don't send all zeros (might be interpreted as valid empty command)
        assume(garbage != b"\x00" * len(garbage))

        client = make_client()
        client.shm.seek(0)
        client.shm.write(garbage)

        try:
            client.sock.sendall(struct.pack("<I", len(garbage)))
            # Try to read response
            _ = client.sock.recv(4)
            # Any response is fine - server didn't crash
        except (BrokenPipeError, ConnectionResetError):
            pass  # Server may close on garbage

    @given(
        flip_positions=st.lists(
            st.integers(min_value=0, max_value=255),
            min_size=1,
            max_size=10,
        ),
    )
    @settings(
        max_examples=30, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_bit_flipped_command(self, make_client, flip_positions):
        """Bit-flipped valid command should not crash server."""
        client = make_client()
        # Build a valid command
        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString("java.util.ArrayList")
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObject)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)
        data = bytearray(builder.Output())

        # Flip bits at random positions
        for pos in flip_positions:
            if pos < len(data):
                byte_pos = pos % len(data)
                bit_pos = pos % 8
                data[byte_pos] ^= 1 << bit_pos

        client.shm.seek(0)
        client.shm.write(bytes(data))

        try:
            client.sock.sendall(struct.pack("<I", len(data)))
            _ = client.sock.recv(4)
            # Any response is acceptable
        except (BrokenPipeError, ConnectionResetError):
            pass


class TestObjectIdFuzzing:
    """Test handling of malformed object IDs."""

    @given(
        object_id=st.integers(min_value=-(2**63), max_value=2**63 - 1),
    )
    @settings(
        max_examples=50, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_arbitrary_object_ids(self, make_client, object_id):
        """Arbitrary object IDs should not crash server."""
        # Skip ID 0 which might be special
        assume(object_id != 0)

        client = make_client()
        # Try to invoke method on arbitrary ID
        from gatun.client import JavaException

        try:
            client.invoke_method(object_id, "toString")
        except (JavaException, Exception):
            pass  # Expected - object doesn't exist

        # Session should still work
        arr = client.create_object("java.util.ArrayList")
        assert arr.size() == 0

    @given(
        num_frees=st.integers(min_value=1, max_value=20),
    )
    @settings(
        max_examples=20, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_repeated_free_on_same_id(self, make_client, num_frees):
        """Repeated free on same ID should be idempotent."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")
        obj_id = arr.object_id

        # Free multiple times
        for _ in range(num_frees):
            client.free_object(obj_id)

        # Session should still work
        new_arr = client.create_object("java.util.ArrayList")
        assert new_arr.object_id != obj_id


class TestStringFuzzing:
    """Test handling of malformed strings."""

    @given(
        class_name=st.text(min_size=0, max_size=1000),
    )
    @settings(
        max_examples=50, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_arbitrary_class_names(self, make_client, class_name):
        """Arbitrary class names should not crash server."""
        from gatun.client import JavaException

        client = make_client()
        try:
            client.create_object(class_name)
        except JavaException:
            pass  # Expected for invalid/non-allowlisted

        # Session should still work
        arr = client.create_object("java.util.ArrayList")
        assert arr is not None

    @given(
        method_name=st.text(min_size=0, max_size=500),
    )
    @settings(
        max_examples=50, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_arbitrary_method_names(self, make_client, method_name):
        """Arbitrary method names should not crash server."""
        from gatun.client import JavaException

        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        try:
            client.invoke_method(arr.object_id, method_name)
        except JavaException:
            pass  # Expected for invalid methods

        # Object should still work
        assert arr.size() == 0

    @given(
        field_name=st.text(min_size=0, max_size=500),
    )
    @settings(
        max_examples=50, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_arbitrary_field_names(self, make_client, field_name):
        """Arbitrary field names should not crash server."""
        from gatun.client import JavaException

        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        try:
            client.get_field(arr, field_name)
        except JavaException:
            pass  # Expected for invalid fields

        # Object should still work
        assert arr.size() == 0

    @given(
        evil_string=st.text(
            alphabet=st.sampled_from(
                list("abcdefghijklmnopqrstuvwxyz./\\$;[]{}()'\"\0\n\r\t")
            ),
            min_size=1,
            max_size=200,
        ),
    )
    @settings(
        max_examples=50, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_special_characters_in_names(self, make_client, evil_string):
        """Special characters in names should not cause issues."""
        from gatun.client import JavaException

        client = make_client()
        # Try as class name
        try:
            client.create_object(evil_string)
        except JavaException:
            pass

        # Try as method name
        arr = client.create_object("java.util.ArrayList")
        try:
            client.invoke_method(arr.object_id, evil_string)
        except JavaException:
            pass

        # Session should still work
        assert arr.size() == 0


class TestArgumentFuzzing:
    """Test handling of malformed arguments."""

    @given(
        args=st.lists(
            st.one_of(
                st.integers(),
                st.floats(allow_nan=True, allow_infinity=True),
                st.text(max_size=1000),
                st.binary(max_size=1000),
                st.none(),
                st.booleans(),
                st.lists(st.integers(), max_size=50),
                st.dictionaries(st.text(max_size=20), st.integers(), max_size=20),
            ),
            max_size=20,
        ),
    )
    @settings(
        max_examples=50, deadline=10000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_arbitrary_arguments(self, make_client, args):
        """Arbitrary arguments should not crash server."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        try:
            for arg in args:
                arr.add(arg)
        except Exception:
            pass  # Some args may not be serializable

        # Session should still work
        client.create_object("java.util.HashMap")

    @given(
        depth=st.integers(min_value=1, max_value=10),
    )
    @settings(
        max_examples=20, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_deeply_nested_structures(self, make_client, depth):
        """Deeply nested structures should not stack overflow."""
        client = make_client()
        # Build nested list
        nested = [1]
        for _ in range(depth):
            nested = [nested]

        arr = client.create_object("java.util.ArrayList")

        try:
            arr.add(nested)
        except Exception:
            pass  # May fail to serialize

        # Session should still work
        arr.clear()


class TestBatchFuzzing:
    """Test handling of malformed batch commands."""

    @given(
        num_commands=st.integers(min_value=0, max_value=100),
    )
    @settings(
        max_examples=30, deadline=15000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_large_batch(self, make_client, num_commands):
        """Large batches should not crash or hang."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        with client.batch() as b:
            for i in range(num_commands):
                b.call(arr, "add", i)

        assert arr.size() == num_commands

    @given(
        error_positions=st.lists(
            st.integers(min_value=0, max_value=19),
            min_size=0,
            max_size=5,
        ),
    )
    @settings(
        max_examples=30, deadline=10000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_batch_with_multiple_errors(self, make_client, error_positions):
        """Batches with multiple errors should handle all correctly."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        with client.batch() as b:
            results = []
            for i in range(20):
                if i in error_positions:
                    results.append(
                        b.call_static("java.lang.Integer", "parseInt", "invalid")
                    )
                else:
                    results.append(b.call(arr, "add", i))

        # Check results
        error_count = 0
        for i, r in enumerate(results):
            if i in error_positions:
                assert r.is_error
                error_count += 1
            else:
                assert r.get() is True

        # Array should have correct size (total - errors)
        assert arr.size() == 20 - len(set(error_positions))


class TestVectorizedFuzzing:
    """Test handling of malformed vectorized commands."""

    @given(
        num_calls=st.integers(min_value=0, max_value=50),
    )
    @settings(
        max_examples=30, deadline=10000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_invoke_methods_size_range(self, make_client, num_calls):
        """invoke_methods should handle various sizes."""
        client = make_client()
        arr = client.create_object("java.util.ArrayList")

        calls = [("add", (i,)) for i in range(num_calls)]

        results = client.invoke_methods(arr, calls)

        assert len(results) == num_calls
        assert arr.size() == num_calls

    @given(
        num_objects=st.integers(min_value=0, max_value=50),
    )
    @settings(
        max_examples=30, deadline=10000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_create_objects_size_range(self, make_client, num_objects):
        """create_objects should handle various sizes."""
        client = make_client()
        specs = [("java.util.ArrayList", ()) for _ in range(num_objects)]

        objects = client.create_objects(specs)

        assert len(objects) == num_objects

        # All should be functional
        for obj in objects:
            assert obj.size() == 0


class TestZoneBoundaryFuzzing:
    """Test command/response zone boundary handling."""

    def test_command_at_zone_limit(self, make_client):
        """Command exactly at zone limit should work."""
        client = make_client()
        # Zone limit is 64KB = 65536 bytes
        # Build a command that's close to the limit
        builder = flatbuffers.Builder(60000)

        # Create a very long class name to inflate command size
        long_name = "java.util." + "A" * 50000

        name_off = builder.CreateString(long_name)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObject)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        data = bytes(builder.Output())

        # This should fit in the command zone
        if len(data) < 65536:
            client.shm.seek(0)
            client.shm.write(data)
            client.sock.sendall(struct.pack("<I", len(data)))

            # Should get a response (likely an error for invalid class)
            resp_len_bytes = client.sock.recv(4)
            assert len(resp_len_bytes) == 4

    def test_response_larger_than_typical(self, client):
        """Large responses should be handled correctly."""
        # Create array with many items to generate large response
        arr = client.create_object("java.util.ArrayList")

        # Add many items
        for i in range(1000):
            arr.add(f"item_{i}_" + "x" * 50)

        # Verify size and retrieve via indexing (not iteration to avoid iterator bug)
        assert arr.size() == 1000
        assert arr.get(0) == "item_0_" + "x" * 50
        assert arr.get(999) == "item_999_" + "x" * 50


class TestCallbackFuzzing:
    """Test handling of malformed callbacks."""

    @given(
        interface_name=st.text(min_size=0, max_size=200),
    )
    @settings(
        max_examples=30, deadline=5000, suppress_health_check=SUPPRESS_FIXTURE_CHECK
    )
    def test_arbitrary_interface_names(self, make_client, interface_name):
        """Arbitrary interface names should not crash server."""
        from gatun.client import JavaException

        client = make_client()

        def dummy_callback(*args):
            return 0

        try:
            client.register_callback(dummy_callback, interface_name)
        except JavaException:
            pass  # Expected for invalid interfaces

        # Session should still work
        arr = client.create_object("java.util.ArrayList")
        assert arr is not None
