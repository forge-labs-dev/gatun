import pyarrow as pa
import pytest

from gatun import PayloadArena, StaleArenaError, UnsupportedArrowTypeError


def test_send_pyarrow_table(client):
    """Verify sending a raw PyArrow Table to Java."""

    # 1. Create Arrow Data
    # Schema: [name: string, age: int32, score: float64]
    names = pa.array(["Alice", "Bob", "Charlie", "David"])
    ages = pa.array([25, 30, 35, 40], type=pa.int32())
    scores = pa.array([88.5, 92.0, 79.9, 99.9])

    table = pa.Table.from_arrays([names, ages, scores], names=["name", "age", "score"])

    response = client.send_arrow_table(table)

    assert f"Received {table.num_rows} rows" in str(response)


def test_send_arrow_buffers(client):
    """Verify sending Arrow data via zero-copy buffer transfer."""

    # 1. Create Arrow Data
    names = pa.array(["Alice", "Bob", "Charlie"])
    ages = pa.array([25, 30, 35], type=pa.int64())

    table = pa.Table.from_arrays([names, ages], names=["name", "age"])

    arena = client.get_payload_arena()
    schema_cache = {}

    response = client.send_arrow_buffers(table, arena, schema_cache)
    assert f"Received {table.num_rows} rows" in str(response)

    # Verify schema was cached
    assert len(schema_cache) == 1

    # Reset arena and send again (should skip schema)
    arena.reset()
    response2 = client.send_arrow_buffers(table, arena, schema_cache)
    assert f"Received {table.num_rows} rows" in str(response2)
    assert len(schema_cache) == 1

    arena.close()


def test_payload_arena_basic(tmp_path):
    """Test PayloadArena allocation and reset."""
    arena_path = tmp_path / "test.shm"
    arena = PayloadArena.create(arena_path, 4096)

    # Test allocation
    info1 = arena.allocate(100)
    assert info1.offset == 0
    assert info1.length == 100
    assert info1.buffer is not None
    assert info1.buffer.size == 100

    # Test alignment (next alloc should be 64-byte aligned)
    info2 = arena.allocate(50)
    assert info2.offset == 128  # 100 rounded up to 128 (64-byte aligned)
    assert info2.length == 50

    # Test bytes tracking
    assert arena.bytes_used() == 128 + 50

    # Test reset
    arena.reset()
    assert arena.bytes_used() == 0

    # Can allocate from beginning again
    info3 = arena.allocate(200)
    assert info3.offset == 0

    arena.close()


def test_basic_roundtrip(client):
    """Test basic Arrow roundtrip (Python -> Java -> Python)."""
    original_table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie", "David", "Eve"]),
            "score": pa.array([95.5, 87.3, 92.1, 78.9, 99.0], type=pa.float64()),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    client.send_arrow_buffers(original_table, arena, schema_cache)
    received_table = client.get_arrow_data()

    assert received_table.num_rows == original_table.num_rows
    assert received_table.num_columns == original_table.num_columns
    assert received_table.schema == original_table.schema

    for name in original_table.schema.names:
        orig_col = original_table.column(name).to_pylist()
        recv_col = received_table.column(name).to_pylist()
        assert recv_col == orig_col, f"Mismatch in column {name}"

    arena.close()


def test_various_types_roundtrip(client):
    """Test roundtrip with various Arrow data types."""
    original_table = pa.table(
        {
            "int8_col": pa.array([1, 2, 3], type=pa.int8()),
            "int16_col": pa.array([100, 200, 300], type=pa.int16()),
            "int32_col": pa.array([1000, 2000, 3000], type=pa.int32()),
            "int64_col": pa.array([10000, 20000, 30000], type=pa.int64()),
            "float32_col": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
            "float64_col": pa.array([1.11, 2.22, 3.33], type=pa.float64()),
            "string_col": pa.array(["hello", "world", "test"]),
            "bool_col": pa.array([True, False, True]),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    client.send_arrow_buffers(original_table, arena, schema_cache)
    received_table = client.get_arrow_data()

    assert received_table.num_rows == original_table.num_rows
    assert received_table.num_columns == original_table.num_columns

    for name in original_table.schema.names:
        orig_col = original_table.column(name).to_pylist()
        recv_col = received_table.column(name).to_pylist()
        assert recv_col == orig_col, f"Mismatch in column {name}"

    arena.close()


def test_nulls_roundtrip(client):
    """Test roundtrip with null values."""

    original_table = pa.table(
        {
            "nullable_int": pa.array([1, None, 3, None, 5], type=pa.int64()),
            "nullable_string": pa.array(["a", None, "c", "d", None]),
            "nullable_float": pa.array([1.0, 2.0, None, 4.0, None], type=pa.float64()),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    client.send_arrow_buffers(original_table, arena, schema_cache)
    received_table = client.get_arrow_data()

    assert received_table.num_rows == original_table.num_rows

    # Verify null positions are preserved
    for name in original_table.schema.names:
        orig_col = original_table.column(name).to_pylist()
        recv_col = received_table.column(name).to_pylist()
        assert recv_col == orig_col, f"Mismatch in column {name}"

    arena.close()


def test_chunked_table_roundtrip(client):
    """Test roundtrip of chunked table (Python -> Java -> Python)."""

    # Create a chunked table
    table1 = pa.table(
        {
            "x": pa.array([1.0, 2.0], type=pa.float64()),
            "y": pa.array(["a", "b"]),
        }
    )
    table2 = pa.table(
        {
            "x": pa.array([3.0, 4.0, 5.0], type=pa.float64()),
            "y": pa.array(["c", "d", "e"]),
        }
    )
    chunked_table = pa.concat_tables([table1, table2])

    # Verify it's chunked
    assert chunked_table.column("x").num_chunks == 2

    arena = client.get_payload_arena()
    schema_cache = {}

    client.send_arrow_buffers(chunked_table, arena, schema_cache)

    # Get back from Java
    received_table = client.get_arrow_data()

    # Verify data matches (note: returned table will be unchunked)
    assert received_table.num_rows == chunked_table.num_rows
    assert received_table.num_columns == chunked_table.num_columns

    for name in chunked_table.schema.names:
        orig_col = chunked_table.column(name).to_pylist()
        recv_col = received_table.column(name).to_pylist()
        assert recv_col == orig_col, f"Mismatch in column {name}"

    arena.close()


def _make_list_table():
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "values": pa.array([[1, 2], [3, 4, 5], [6]], type=pa.list_(pa.int32())),
        }
    )


def _make_struct_table():
    struct_type = pa.struct([("x", pa.int32()), ("y", pa.int32())])
    return pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "point": pa.array([{"x": 1, "y": 2}, {"x": 3, "y": 4}], type=struct_type),
        }
    )


def _make_nested_list_of_structs_table():
    inner_struct = pa.struct([("x", pa.int32()), ("y", pa.string())])
    return pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "items": pa.array(
                [[{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], [{"x": 3, "y": "c"}]],
                type=pa.list_(inner_struct),
            ),
        }
    )


def _make_fixed_size_list_table():
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "coords": pa.array(
                [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                type=pa.list_(pa.int32(), 3),
            ),
        }
    )


def _make_large_list_table():
    return pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "values": pa.array(
                [[1, 2, 3], [4, 5]],
                type=pa.large_list(pa.int32()),
            ),
        }
    )


@pytest.mark.parametrize(
    "table_factory",
    [
        pytest.param(_make_list_table, id="list"),
        pytest.param(_make_struct_table, id="struct"),
        pytest.param(_make_nested_list_of_structs_table, id="nested_list_of_structs"),
        pytest.param(_make_fixed_size_list_table, id="fixed_size_list"),
        pytest.param(_make_large_list_table, id="large_list"),
    ],
)
def test_nested_type_roundtrip(client, table_factory):
    """Test nested Arrow types (list, struct, etc.) are transferred correctly."""
    table = table_factory()

    arena = client.get_payload_arena()
    schema_cache = {}

    response = client.send_arrow_buffers(table, arena, schema_cache)
    assert f"Received {table.num_rows} rows" in str(response)

    received_table = client.get_arrow_data()

    assert received_table.schema == table.schema
    for name in table.schema.names:
        assert received_table.column(name).to_pylist() == table.column(name).to_pylist()

    arena.close()


def test_map_type_roundtrip(client):
    """Test that map type is transferred correctly.

    Maps need special comparison since key order may change.
    """
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "attrs": pa.array(
                [{"a": 1, "b": 2}, {"c": 3}], type=pa.map_(pa.string(), pa.int32())
            ),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    response = client.send_arrow_buffers(table, arena, schema_cache)
    assert f"Received {table.num_rows} rows" in str(response)

    received_table = client.get_arrow_data()

    assert received_table.schema == table.schema
    orig_attrs = table.column("attrs").to_pylist()
    recv_attrs = received_table.column("attrs").to_pylist()
    for orig, recv in zip(orig_attrs, recv_attrs):
        assert dict(orig) == dict(recv)

    arena.close()


def test_dictionary_type_rejected(client):
    """Test that dictionary type is rejected (not yet supported)."""
    # Dictionary encoding requires special handling
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "category": pa.array(["a", "b", "a"]).dictionary_encode(),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    with pytest.raises(UnsupportedArrowTypeError) as exc_info:
        client.send_arrow_buffers(table, arena, schema_cache)

    assert "dictionary" in str(exc_info.value).lower()

    arena.close()


def test_stale_arena_error(client):
    """Test that StaleArenaError is raised when accessing data after arena reset."""

    # 1. Send data and get it back
    original_table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    client.send_arrow_buffers(original_table, arena, schema_cache)
    table_view = client.get_arrow_data()

    # 2. Verify we can access data before reset
    assert table_view.num_rows == 3
    assert table_view.column("id").to_pylist() == [1, 2, 3]

    # 3. Copy data before reset (safe pattern)
    safe_data = table_view.to_pydict()
    assert safe_data["id"] == [1, 2, 3]

    # 4. Reset the arena
    arena.reset()
    client.reset_payload_arena()

    # 5. Verify StaleArenaError is raised when accessing data after reset
    with pytest.raises(StaleArenaError) as exc_info:
        table_view.column("id")

    assert "stale" in str(exc_info.value).lower()
    assert "epoch" in str(exc_info.value).lower()

    # 6. Safe metadata access still works
    assert table_view.num_rows == 3  # num_rows doesn't touch buffer data
    assert table_view.schema == original_table.schema

    # 7. Safe data copy still works (was done before reset)
    assert safe_data["value"] == [10.0, 20.0, 30.0]

    arena.close()


def test_arrow_table_view_repr(client):
    """Test ArrowTableView repr shows epoch status."""

    original_table = pa.table(
        {
            "x": pa.array([1, 2], type=pa.int64()),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    client.send_arrow_buffers(original_table, arena, schema_cache)
    table_view = client.get_arrow_data()

    # Before reset: should show "valid"
    repr_before = repr(table_view)
    assert "valid" in repr_before
    assert "rows=2" in repr_before

    # After reset: should show "STALE"
    arena.reset()
    client.reset_payload_arena()

    repr_after = repr(table_view)
    assert "STALE" in repr_after

    arena.close()


def test_sliced_table_roundtrip(client):
    """Test that sliced arrays (with non-zero offsets) are handled correctly.

    Arrow arrays can have non-zero offsets when created via slicing. The protocol
    must normalize these to offset 0 before copying, otherwise the buffer data
    would be misaligned and reconstruction would fail.
    """
    # Create a table and slice it to create arrays with non-zero offsets
    full_table = pa.table(
        {
            "id": pa.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], type=pa.int64()),
            "name": pa.array(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]),
            "value": pa.array(
                [0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9], type=pa.float64()
            ),
        }
    )

    # Slice to get rows 3-6 (4 rows) - this creates arrays with offset=3
    sliced_table = full_table.slice(3, 4)

    # Verify the table is actually sliced with non-zero offsets
    for col in sliced_table.columns:
        chunk = col.chunks[0]
        assert chunk.offset == 3, f"Expected offset 3, got {chunk.offset}"

    arena = client.get_payload_arena()
    schema_cache = {}

    response = client.send_arrow_buffers(sliced_table, arena, schema_cache)
    assert f"Received {sliced_table.num_rows} rows" in str(response)

    # Roundtrip: get data back from Java
    received_table = client.get_arrow_data()

    # Verify data content matches the sliced portion
    assert received_table.num_rows == 4
    assert received_table.column("id").to_pylist() == [3, 4, 5, 6]
    assert received_table.column("name").to_pylist() == ["d", "e", "f", "g"]
    assert received_table.column("value").to_pylist() == [3.3, 4.4, 5.5, 6.6]

    arena.close()


def test_sliced_table_with_nulls_roundtrip(client):
    """Test sliced arrays with null values are handled correctly."""
    # Create a table with nulls and slice it
    full_table = pa.table(
        {
            "id": pa.array([0, 1, None, 3, 4, None, 6, 7, 8, None], type=pa.int64()),
            "name": pa.array(["a", None, "c", "d", None, "f", "g", None, "i", "j"]),
        }
    )

    # Slice rows 2-7 (6 rows) - includes various null positions
    sliced_table = full_table.slice(2, 6)

    # Verify non-zero offset
    for col in sliced_table.columns:
        chunk = col.chunks[0]
        assert chunk.offset == 2

    arena = client.get_payload_arena()
    schema_cache = {}

    response = client.send_arrow_buffers(sliced_table, arena, schema_cache)
    assert f"Received {sliced_table.num_rows} rows" in str(response)

    received_table = client.get_arrow_data()

    # Verify null positions are preserved correctly
    assert received_table.column("id").to_pylist() == [None, 3, 4, None, 6, 7]
    assert received_table.column("name").to_pylist() == ["c", "d", None, "f", "g", None]

    arena.close()


def test_date_time_types_roundtrip(client):
    """Test Arrow date and time types are transferred correctly."""
    from datetime import date, datetime

    table = pa.table(
        {
            "date32_col": pa.array(
                [date(2024, 1, 15), date(2024, 6, 30), date(2025, 12, 31)],
                type=pa.date32(),
            ),
            "date64_col": pa.array(
                [date(2024, 1, 15), date(2024, 6, 30), date(2025, 12, 31)],
                type=pa.date64(),
            ),
            "timestamp_us": pa.array(
                [
                    datetime(2024, 1, 15, 10, 30, 0),
                    datetime(2024, 6, 30, 23, 59, 59),
                    datetime(2025, 12, 31, 0, 0, 0),
                ],
                type=pa.timestamp("us"),
            ),
            "timestamp_ns": pa.array(
                [
                    datetime(2024, 1, 15, 10, 30, 0),
                    datetime(2024, 6, 30, 23, 59, 59),
                    datetime(2025, 12, 31, 0, 0, 0),
                ],
                type=pa.timestamp("ns"),
            ),
            "time32_s": pa.array([3600, 7200, 86399], type=pa.time32("s")),
            "time64_us": pa.array(
                [3600000000, 7200000000, 86399000000], type=pa.time64("us")
            ),
            "duration_us": pa.array(
                [1000000, 2000000, 3000000], type=pa.duration("us")
            ),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    response = client.send_arrow_buffers(table, arena, schema_cache)
    assert f"Received {table.num_rows} rows" in str(response)

    received_table = client.get_arrow_data()

    # Verify schema preserved
    assert received_table.schema == table.schema

    # Verify data for each column
    for name in table.schema.names:
        orig = table.column(name).to_pylist()
        recv = received_table.column(name).to_pylist()
        assert recv == orig, f"Mismatch in column {name}"

    arena.close()


def test_timestamp_with_timezone_roundtrip(client):
    """Test timezone-aware timestamps are transferred correctly."""
    from datetime import datetime

    table = pa.table(
        {
            "ts_utc": pa.array(
                [
                    datetime(2024, 1, 15, 10, 0),
                    datetime(2024, 6, 30, 12, 0),
                    datetime(2025, 12, 31, 23, 59),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "ts_us_eastern": pa.array(
                [
                    datetime(2024, 1, 15, 10, 0),
                    datetime(2024, 6, 30, 12, 0),
                    datetime(2025, 12, 31, 23, 59),
                ],
                type=pa.timestamp("us", tz="America/New_York"),
            ),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    response = client.send_arrow_buffers(table, arena, schema_cache)
    assert f"Received {table.num_rows} rows" in str(response)

    received_table = client.get_arrow_data()

    # Verify schema including timezone metadata
    assert received_table.schema == table.schema

    # Verify data
    for name in table.schema.names:
        orig = table.column(name).to_pylist()
        recv = received_table.column(name).to_pylist()
        assert recv == orig, f"Mismatch in column {name}"

    arena.close()


def test_date_time_with_nulls_roundtrip(client):
    """Test date/time types with null values."""
    from datetime import date, datetime

    table = pa.table(
        {
            "date_nullable": pa.array(
                [date(2024, 1, 15), None, date(2025, 12, 31)],
                type=pa.date32(),
            ),
            "ts_nullable": pa.array(
                [datetime(2024, 1, 15, 10, 0), None, datetime(2025, 12, 31, 23, 59)],
                type=pa.timestamp("us"),
            ),
            "ts_tz_nullable": pa.array(
                [datetime(2024, 1, 15, 10, 0), None, datetime(2025, 12, 31, 23, 59)],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    response = client.send_arrow_buffers(table, arena, schema_cache)
    assert f"Received {table.num_rows} rows" in str(response)

    received_table = client.get_arrow_data()

    # Verify nulls preserved
    for name in table.schema.names:
        orig = table.column(name).to_pylist()
        recv = received_table.column(name).to_pylist()
        assert recv == orig, f"Mismatch in column {name}"
        # Explicitly check null position
        assert recv[1] is None, f"Expected null at position 1 for {name}"

    arena.close()


def test_schema_serialization_consistency(client):
    """Verify schema serialization and caching consistency between Python and Java.

    This tests that:
    1. Python serialize -> Java deserialize works correctly (schema equality)
    2. Schema caching works across multiple roundtrips
    3. Cached schemas are reused correctly (no schema bytes sent on repeat)

    Note: Python and Java serialize schemas differently, so hashes of serialized
    bytes differ. However, we ensure Java uses Python's hash (cached from the
    original send) when sending back, so caching works correctly.
    """
    from gatun.arena import compute_schema_hash

    # Create a table with various types to test schema handling
    table = pa.table(
        {
            "int_col": pa.array([1, 2, 3], type=pa.int64()),
            "float_col": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
            "str_col": pa.array(["a", "b", "c"], type=pa.string()),
            "bool_col": pa.array([True, False, True], type=pa.bool_()),
        }
    )

    arena = client.get_payload_arena()

    # First send - Java should receive and cache the schema
    schema_cache = {}
    original_hash = compute_schema_hash(table.schema)
    client.send_arrow_buffers(table, arena, schema_cache)

    # Verify schema was cached on Python side
    assert original_hash in schema_cache

    # Get data back from Java - Java will serialize its cached schema
    received = client.get_arrow_data()

    # Verify schemas are equivalent (field names and types match)
    assert received.schema == table.schema, "Schema mismatch after roundtrip"

    # Verify all field names match
    assert received.schema.names == table.schema.names

    # Verify all field types match
    for i, field in enumerate(table.schema):
        received_field = received.schema.field(i)
        assert received_field.type == field.type, (
            f"Type mismatch for field {field.name}"
        )

    # Second send with same schema - should use cached schema (no schema bytes sent)
    arena.reset()
    client.reset_payload_arena()
    client.send_arrow_buffers(table, arena, schema_cache)

    # Get data back again
    received2 = client.get_arrow_data()

    # Verify schema is still correct when using cache
    assert received2.schema == table.schema, "Schema mismatch on cached roundtrip"

    # Third roundtrip to verify caching continues to work
    arena.reset()
    client.reset_payload_arena()
    client.send_arrow_buffers(table, arena, schema_cache)
    received3 = client.get_arrow_data()
    assert received3.schema == table.schema, "Schema mismatch on third roundtrip"

    arena.close()


def test_schema_hash_consistency_nested_types(client):
    """Verify nested type schemas work correctly across roundtrips.

    Tests that list, struct, and other nested types maintain schema equality
    after being sent to Java and received back.
    """
    # Create table with nested types
    table = pa.table(
        {
            "list_col": pa.array([[1, 2], [3, 4, 5], [6]], type=pa.list_(pa.int64())),
            "struct_col": pa.array(
                [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}],
                type=pa.struct([("x", pa.int64()), ("y", pa.string())]),
            ),
        }
    )

    arena = client.get_payload_arena()
    schema_cache = {}

    # Send to Java
    client.send_arrow_buffers(table, arena, schema_cache)

    # Get back
    received = client.get_arrow_data()

    # Verify schema equality (structural, not byte-level)
    assert received.schema == table.schema, "Nested type schema mismatch"

    # Verify data roundtrip
    assert (
        received.column("list_col").to_pylist() == table.column("list_col").to_pylist()
    )
    assert (
        received.column("struct_col").to_pylist()
        == table.column("struct_col").to_pylist()
    )

    arena.close()


def test_payload_arena_out_of_memory(tmp_path):
    """Test PayloadArena raises MemoryError when full."""
    arena_path = tmp_path / "test.shm"
    arena = PayloadArena.create(arena_path, 256)  # Small arena

    # First allocation should succeed
    info1 = arena.allocate(100)
    assert info1.length == 100

    # This should fail - not enough space
    with pytest.raises(MemoryError) as exc_info:
        arena.allocate(200)

    assert "full" in str(exc_info.value).lower()

    arena.close()


def test_payload_arena_context_manager(tmp_path):
    """Test PayloadArena works as a context manager."""
    arena_path = tmp_path / "test.shm"

    with PayloadArena.create(arena_path, 4096) as arena:
        info = arena.allocate(100)
        assert info.length == 100
        assert arena.bytes_used() == 100

    # After context exit, arena should be closed


def test_payload_arena_allocate_and_copy_bytes(tmp_path):
    """Test allocate_and_copy with Python bytes."""
    arena_path = tmp_path / "test.shm"
    arena = PayloadArena.create(arena_path, 4096)

    data = b"hello world"
    info = arena.allocate_and_copy(data)

    assert info.length == len(data)
    assert info.buffer is not None
    assert bytes(info.buffer) == data

    arena.close()


def test_payload_arena_allocate_and_copy_arrow_buffer(tmp_path):
    """Test allocate_and_copy with Arrow buffer."""
    arena_path = tmp_path / "test.shm"
    arena = PayloadArena.create(arena_path, 4096)

    # Create an Arrow buffer
    arr = pa.array([1, 2, 3, 4], type=pa.int32())
    arrow_buffer = arr.buffers()[1]  # Data buffer

    info = arena.allocate_and_copy(arrow_buffer)

    assert info.length == arrow_buffer.size
    assert info.buffer is not None

    arena.close()


def test_payload_arena_allocate_and_copy_zero_length(tmp_path):
    """Test allocate_and_copy with zero-length data."""
    arena_path = tmp_path / "test.shm"
    arena = PayloadArena.create(arena_path, 4096)

    info = arena.allocate_and_copy(b"")

    assert info.length == 0
    assert info.buffer is None  # Zero-length returns None buffer

    arena.close()


def test_payload_arena_bytes_available(tmp_path):
    """Test bytes_available tracking."""
    arena_path = tmp_path / "test.shm"
    arena = PayloadArena.create(arena_path, 1024)

    assert arena.bytes_available() == 1024

    arena.allocate(100)
    # After allocating 100 bytes (aligned to 64), available should decrease
    assert arena.bytes_available() < 1024

    arena.reset()
    assert arena.bytes_available() == 1024

    arena.close()
