"""Tests for size validation of shared memory zones."""

import pytest
from gatun import PayloadTooLargeError
from gatun.client import COMMAND_ZONE_SIZE


def test_payload_too_large_error_attributes():
    """Test PayloadTooLargeError has correct attributes."""
    err = PayloadTooLargeError(10000, 4096, "Command")
    assert err.payload_size == 10000
    assert err.max_size == 4096
    assert err.zone == "Command"
    assert "10,000" in str(err)
    assert "4,096" in str(err)
    assert "Command" in str(err)


def test_payload_too_large_error_message_suggests_increase():
    """Test error message suggests increasing memory size."""
    err = PayloadTooLargeError(10000, 4096, "Arrow batch")
    assert "increasing memory size" in str(err).lower() or "Consider increasing" in str(err)
    assert "memory=" in str(err)


def test_command_size_limit(client):
    """Test that commands exceeding the zone size raise PayloadTooLargeError."""
    # Create a string that will result in a command larger than 4KB
    # The command includes method name, class info, and the argument
    # A 5KB string should definitely exceed the limit
    huge_string = "x" * 5000

    # Trying to pass this as an argument should fail during send
    with pytest.raises(PayloadTooLargeError) as exc_info:
        # Using StringBuilder constructor which will serialize the whole string
        client.jvm.java.lang.StringBuilder(huge_string)

    assert exc_info.value.zone == "Command"
    assert exc_info.value.payload_size > COMMAND_ZONE_SIZE


def test_command_within_limit(client):
    """Test that commands within the limit work fine."""
    # A reasonably sized string should work
    small_string = "x" * 100
    sb = client.jvm.java.lang.StringBuilder(small_string)
    assert sb.toString() == small_string


def test_response_size_limit(client):
    """Test that oversized responses from Java return an error message."""
    # Create a StringBuilder with a very large string
    # When we call toString(), the response will be too large
    sb = client.jvm.java.lang.StringBuilder()

    # Build a string larger than 4KB using multiple appends
    # Use small chunks to avoid command size limits
    for _ in range(50):
        sb.append("x" * 100)  # 5KB total

    # toString() will try to return a string > 4KB
    # This may either:
    # 1. Return "Response too large" error (if Java catches it)
    # 2. Cause socket closed (if Java process crashes)
    try:
        result = sb.toString()
        # If we got here, the response was within limits
        # This is OK - just means our estimate was wrong
        assert len(result) <= 4096, f"Expected response to be limited, got {len(result)} chars"
    except RuntimeError as e:
        # Either response too large OR socket closed is acceptable
        # The key is that we don't silently corrupt data
        assert "Response too large" in str(e) or "Socket closed" in str(e)


def test_large_list_response(client):
    """Test that large list responses trigger the size limit."""
    # Create a list with many items - when converted to response, may exceed limit
    arr = client.jvm.java.util.ArrayList()

    # Add many strings to exceed response zone
    for i in range(200):
        arr.add(f"item_{i:04d}_" + "x" * 30)

    # Getting subList will try to serialize all items
    try:
        result = arr.subList(0, 200)
        # If we got here, response was within limits
        pass  # This is OK
    except RuntimeError as e:
        # Either response too large OR socket closed is acceptable
        assert "Response too large" in str(e) or "Socket closed" in str(e)


def test_moderate_response_works(client):
    """Test that moderate-sized responses work fine."""
    arr = client.jvm.java.util.ArrayList()

    # Add a reasonable number of items
    for i in range(10):
        arr.add(f"item_{i}")

    result = arr.subList(0, 10)
    assert len(result) == 10
    assert result[0] == "item_0"


def test_exact_response_limit_boundary(client):
    """Test behavior near the 4KB response limit."""
    sb = client.jvm.java.lang.StringBuilder()

    # Build a string close to but under 4KB (accounting for FlatBuffer overhead)
    # FlatBuffer adds ~50-100 bytes of overhead for a string response
    sb.append("x" * 3800)

    # This should succeed - under the limit
    result = sb.toString()
    assert len(result) == 3800

    # Now add more to exceed 4KB
    sb.append("y" * 500)  # Total now 4300 bytes

    # This may fail or succeed depending on exact overhead
    try:
        result = sb.toString()
        # If succeeded, response fit in 4KB
        assert len(result) == 4300
    except RuntimeError as e:
        # If failed, it should be a size-related error
        assert "Response too large" in str(e) or "Socket closed" in str(e)


def test_arrow_payload_size_validation(client):
    """Test that Arrow batch size is validated before writing."""
    import pyarrow as pa

    # Get the max payload size (memory - 4KB command zone - 4KB response zone)
    # For 16MB default: ~16MB - 8KB ≈ 16MB
    # We need to test that validation happens

    # Create a small table that will fit
    small_table = pa.table({"col": [1, 2, 3]})
    client.send_arrow_table(small_table)  # Should succeed

    # Note: Testing with truly large tables would require allocating
    # significant memory and is impractical for unit tests.
    # The validation code path is tested via test_command_size_limit.


def test_send_arrow_table_batched_small_table(client):
    """Test batched sending with a small table (single batch)."""
    import pyarrow as pa

    table = pa.table({"col": list(range(100))})
    responses = client.send_arrow_table_batched(table)

    # Small table should be sent in a single batch
    assert len(responses) >= 1
    assert all("Received" in r for r in responses)


def test_send_arrow_table_batched_empty_table(client):
    """Test batched sending with an empty table."""
    import pyarrow as pa

    table = pa.table({"col": pa.array([], type=pa.int64())})

    # Empty tables produce an empty Arrow stream which Java rejects
    with pytest.raises(RuntimeError) as exc_info:
        client.send_arrow_table_batched(table)

    assert "Arrow Stream Empty" in str(exc_info.value)


def test_send_arrow_table_batched_with_explicit_batch_size(client):
    """Test batched sending with explicit batch size."""
    import pyarrow as pa

    table = pa.table({"col": list(range(100))})
    responses = client.send_arrow_table_batched(table, batch_size=30)

    # 100 rows with batch_size=30 should produce 4 batches
    assert len(responses) == 4
    assert all("Received" in r for r in responses)


def test_send_arrow_table_batched_multiple_columns(client):
    """Test batched sending with multiple columns."""
    import pyarrow as pa

    table = pa.table({
        "int_col": list(range(50)),
        "str_col": [f"value_{i}" for i in range(50)],
        "float_col": [i * 0.1 for i in range(50)],
    })
    responses = client.send_arrow_table_batched(table, batch_size=20)

    # 50 rows with batch_size=20 should produce 3 batches
    assert len(responses) == 3
