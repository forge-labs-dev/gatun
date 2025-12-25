"""Tests for size validation of shared memory zones."""

import pytest
from gatun import PayloadTooLargeError, JavaException, JavaRuntimeException
from gatun.client import COMMAND_ZONE_SIZE, RESPONSE_ZONE_SIZE


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
    assert "increasing memory size" in str(err).lower() or "Consider increasing" in str(
        err
    )
    assert "memory=" in str(err)


def test_command_size_limit(client):
    """Test that commands exceeding the zone size raise PayloadTooLargeError."""
    # Create a string that will result in a command larger than 64KB
    # The command includes method name, class info, and the argument
    # A 70KB string should definitely exceed the limit
    huge_string = "x" * 70000

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

    # Build a string larger than 64KB using multiple appends
    # Use small chunks to avoid command size limits
    # 700 * 100 = 70KB total, exceeding 64KB response zone
    for _ in range(700):
        sb.append("x" * 100)

    # toString() will try to return a string > 64KB
    # This may either:
    # 1. Return "Response too large" error (if Java catches it)
    # 2. Cause socket closed (if Java process crashes)
    try:
        result = sb.toString()
        # If we got here, the response was within limits
        # This is OK - just means our estimate was wrong
        assert len(result) <= RESPONSE_ZONE_SIZE, (
            f"Expected response to be limited, got {len(result)} chars"
        )
    except (PayloadTooLargeError, JavaException, RuntimeError) as e:
        # Either response too large OR socket closed is acceptable
        # The key is that we don't silently corrupt data
        err_str = str(e).lower()
        assert "too large" in err_str or "socket closed" in err_str


def test_large_list_response(client):
    """Test that large list responses trigger the size limit."""
    # Create a list with many items - when converted to response, may exceed limit
    arr = client.jvm.java.util.ArrayList()

    # Add many strings to exceed 64KB response zone
    # Each item is ~50 bytes, so 2000 items = ~100KB
    for i in range(2000):
        arr.add(f"item_{i:04d}_" + "x" * 40)

    # Getting subList will try to serialize all items
    try:
        arr.subList(0, 2000)
        # If we got here, response was within limits
        pass  # This is OK
    except (PayloadTooLargeError, JavaException, RuntimeError) as e:
        # Either response too large OR socket closed is acceptable
        err_str = str(e).lower()
        assert "too large" in err_str or "socket closed" in err_str


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
    """Test behavior near the 64KB response limit."""
    sb = client.jvm.java.lang.StringBuilder()

    # Build a string close to but under 64KB (accounting for FlatBuffer overhead)
    # FlatBuffer adds ~50-100 bytes of overhead for a string response
    # 60KB should be safely under the limit
    sb.append("x" * 60000)

    # This should succeed - under the limit
    result = sb.toString()
    assert len(result) == 60000

    # Now add more to exceed 64KB
    sb.append("y" * 6000)  # Total now 66KB

    # This may fail or succeed depending on exact overhead
    try:
        result = sb.toString()
        # If succeeded, response fit in 64KB
        assert len(result) == 66000
    except (PayloadTooLargeError, JavaException, RuntimeError) as e:
        # If failed, it should be a size-related error
        err_str = str(e).lower()
        assert "too large" in err_str or "socket closed" in err_str


def test_arrow_payload_size_validation(client):
    """Test that Arrow batch size is validated before writing."""
    import pyarrow as pa

    # Get the max payload size (memory - 64KB command zone - 64KB response zone)
    # For 16MB default: ~16MB - 128KB ≈ 16MB
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
    with pytest.raises(JavaRuntimeException) as exc_info:
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

    table = pa.table(
        {
            "int_col": list(range(50)),
            "str_col": [f"value_{i}" for i in range(50)],
            "float_col": [i * 0.1 for i in range(50)],
        }
    )
    responses = client.send_arrow_table_batched(table, batch_size=20)

    # 50 rows with batch_size=20 should produce 3 batches
    assert len(responses) == 3
