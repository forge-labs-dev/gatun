import tempfile
from pathlib import Path

import pyarrow as pa

from gatun import PayloadArena


def test_send_pyarrow_table(client):
    """Verify sending a raw PyArrow Table to Java."""

    # 1. Create Arrow Data
    # Schema: [name: string, age: int32, score: float64]
    names = pa.array(["Alice", "Bob", "Charlie", "David"])
    ages = pa.array([25, 30, 35, 40], type=pa.int32())
    scores = pa.array([88.5, 92.0, 79.9, 99.9])

    table = pa.Table.from_arrays([names, ages, scores], names=["name", "age", "score"])

    print(f"\nSending Arrow Table: {table.num_rows} rows")

    # 2. Send via Shared Memory
    response = client.send_arrow_table(table)

    # 3. Verify
    print(f"Java Response: {response}")

    # Our Java server implementation returns "Received X rows"
    assert f"Received {table.num_rows} rows" in str(response)


def test_send_arrow_buffers(client):
    """Verify sending Arrow data via zero-copy buffer transfer."""

    # 1. Create Arrow Data
    names = pa.array(["Alice", "Bob", "Charlie"])
    ages = pa.array([25, 30, 35], type=pa.int64())

    table = pa.Table.from_arrays([names, ages], names=["name", "age"])

    # 2. Get payload arena from client (writes to the same shm Java reads from)
    arena = client.get_payload_arena()

    print(f"\nSending Arrow buffers: {table.num_rows} rows")

    # 3. Send via zero-copy buffer transfer
    schema_cache = {}
    response = client.send_arrow_buffers(table, arena, schema_cache)

    # 4. Verify
    print(f"Java Response: {response}")
    assert f"Received {table.num_rows} rows" in str(response)

    # Verify schema was cached
    assert len(schema_cache) == 1

    # 5. Reset arena and send again (should skip schema)
    arena.reset()
    response2 = client.send_arrow_buffers(table, arena, schema_cache)
    assert f"Received {table.num_rows} rows" in str(response2)

    # Schema cache should still have just one entry
    assert len(schema_cache) == 1

    arena.close()


def test_send_arrow_buffers_various_types(client):
    """Test zero-copy transfer with various Arrow types."""

    # Create table with multiple Arrow types
    table = pa.table({
        # Integer types
        "int8_col": pa.array([1, 2, 3], type=pa.int8()),
        "int16_col": pa.array([100, 200, 300], type=pa.int16()),
        "int32_col": pa.array([1000, 2000, 3000], type=pa.int32()),
        "int64_col": pa.array([10000, 20000, 30000], type=pa.int64()),
        # Float types
        "float32_col": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
        "float64_col": pa.array([1.11, 2.22, 3.33], type=pa.float64()),
        # String type
        "string_col": pa.array(["hello", "world", "test"]),
        # Boolean type
        "bool_col": pa.array([True, False, True]),
    })

    arena = client.get_payload_arena()
    schema_cache = {}

    print(f"\nSending Arrow buffers with various types: {table.num_rows} rows, {table.num_columns} columns")

    response = client.send_arrow_buffers(table, arena, schema_cache)
    print(f"Java Response: {response}")
    assert f"Received {table.num_rows} rows" in str(response)

    arena.close()


def test_send_arrow_buffers_with_nulls(client):
    """Test zero-copy transfer with null values."""

    # Create table with null values
    table = pa.table({
        "nullable_int": pa.array([1, None, 3, None, 5], type=pa.int64()),
        "nullable_string": pa.array(["a", None, "c", "d", None]),
        "nullable_float": pa.array([1.0, 2.0, None, 4.0, None], type=pa.float64()),
    })

    arena = client.get_payload_arena()
    schema_cache = {}

    print(f"\nSending Arrow buffers with nulls: {table.num_rows} rows")

    response = client.send_arrow_buffers(table, arena, schema_cache)
    print(f"Java Response: {response}")
    assert f"Received {table.num_rows} rows" in str(response)

    arena.close()


def test_payload_arena_basic():
    """Test PayloadArena allocation and reset."""

    with tempfile.NamedTemporaryFile(suffix=".shm", delete=False) as f:
        arena_path = Path(f.name)

    try:
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
    finally:
        arena_path.unlink(missing_ok=True)


def test_get_arrow_data_roundtrip(client):
    """Test Java -> Python Arrow transfer via get_arrow_data."""

    # 1. Create Arrow data and send to Java
    original_table = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(["Alice", "Bob", "Charlie", "David", "Eve"]),
        "score": pa.array([95.5, 87.3, 92.1, 78.9, 99.0], type=pa.float64()),
    })

    arena = client.get_payload_arena()
    schema_cache = {}

    print(f"\nSending Arrow table to Java: {original_table.num_rows} rows")
    client.send_arrow_buffers(original_table, arena, schema_cache)

    # 2. Request the data back from Java
    print("Requesting Arrow data back from Java...")
    received_table = client.get_arrow_data()

    # 3. Verify the data matches
    print(f"Received table: {received_table.num_rows} rows, {received_table.num_columns} columns")

    assert received_table.num_rows == original_table.num_rows
    assert received_table.num_columns == original_table.num_columns
    assert received_table.schema == original_table.schema

    # Verify data content
    for name in original_table.schema.names:
        orig_col = original_table.column(name).to_pylist()
        recv_col = received_table.column(name).to_pylist()
        assert recv_col == orig_col, f"Mismatch in column {name}"

    arena.close()


def test_get_arrow_data_various_types(client):
    """Test Java -> Python Arrow transfer with various data types."""

    original_table = pa.table({
        "int8_col": pa.array([1, 2, 3], type=pa.int8()),
        "int16_col": pa.array([100, 200, 300], type=pa.int16()),
        "int32_col": pa.array([1000, 2000, 3000], type=pa.int32()),
        "int64_col": pa.array([10000, 20000, 30000], type=pa.int64()),
        "float32_col": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
        "float64_col": pa.array([1.11, 2.22, 3.33], type=pa.float64()),
        "string_col": pa.array(["hello", "world", "test"]),
        "bool_col": pa.array([True, False, True]),
    })

    arena = client.get_payload_arena()
    schema_cache = {}

    print(f"\nSending Arrow table with various types: {original_table.num_rows} rows")
    client.send_arrow_buffers(original_table, arena, schema_cache)

    received_table = client.get_arrow_data()

    print(f"Received table: {received_table.num_rows} rows")

    assert received_table.num_rows == original_table.num_rows
    assert received_table.num_columns == original_table.num_columns

    # Verify each column
    for name in original_table.schema.names:
        orig_col = original_table.column(name).to_pylist()
        recv_col = received_table.column(name).to_pylist()
        assert recv_col == orig_col, f"Mismatch in column {name}"

    arena.close()


def test_get_arrow_data_with_nulls(client):
    """Test Java -> Python Arrow transfer with null values."""

    original_table = pa.table({
        "nullable_int": pa.array([1, None, 3, None, 5], type=pa.int64()),
        "nullable_string": pa.array(["a", None, "c", "d", None]),
        "nullable_float": pa.array([1.0, 2.0, None, 4.0, None], type=pa.float64()),
    })

    arena = client.get_payload_arena()
    schema_cache = {}

    print(f"\nSending Arrow table with nulls: {original_table.num_rows} rows")
    client.send_arrow_buffers(original_table, arena, schema_cache)

    received_table = client.get_arrow_data()

    print(f"Received table: {received_table.num_rows} rows")

    assert received_table.num_rows == original_table.num_rows

    # Verify null positions are preserved
    for name in original_table.schema.names:
        orig_col = original_table.column(name).to_pylist()
        recv_col = received_table.column(name).to_pylist()
        assert recv_col == orig_col, f"Mismatch in column {name}"

    arena.close()


def test_send_arrow_buffers_chunked_table(client):
    """Test zero-copy transfer with chunked columns.

    Arrow tables can have multiple chunks per column (e.g., from concatenation
    or reading large files). The protocol should handle these correctly by
    combining chunks before transfer.
    """

    # Create a chunked table by concatenating multiple tables
    table1 = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["Alice", "Bob", "Charlie"]),
    })
    table2 = pa.table({
        "id": pa.array([4, 5], type=pa.int64()),
        "name": pa.array(["David", "Eve"]),
    })
    table3 = pa.table({
        "id": pa.array([6], type=pa.int64()),
        "name": pa.array(["Frank"]),
    })

    # Concatenate creates a table with multiple chunks per column
    chunked_table = pa.concat_tables([table1, table2, table3])

    # Verify the table is actually chunked
    assert chunked_table.column("id").num_chunks == 3
    assert chunked_table.column("name").num_chunks == 3

    arena = client.get_payload_arena()
    schema_cache = {}

    print(f"\nSending chunked Arrow table: {chunked_table.num_rows} rows, "
          f"{chunked_table.column('id').num_chunks} chunks per column")

    response = client.send_arrow_buffers(chunked_table, arena, schema_cache)
    print(f"Java Response: {response}")
    assert f"Received {chunked_table.num_rows} rows" in str(response)

    arena.close()


def test_get_arrow_data_chunked_roundtrip(client):
    """Test roundtrip of chunked table (Python -> Java -> Python)."""

    # Create a chunked table
    table1 = pa.table({
        "x": pa.array([1.0, 2.0], type=pa.float64()),
        "y": pa.array(["a", "b"]),
    })
    table2 = pa.table({
        "x": pa.array([3.0, 4.0, 5.0], type=pa.float64()),
        "y": pa.array(["c", "d", "e"]),
    })
    chunked_table = pa.concat_tables([table1, table2])

    # Verify it's chunked
    assert chunked_table.column("x").num_chunks == 2

    arena = client.get_payload_arena()
    schema_cache = {}

    print(f"\nSending chunked table: {chunked_table.num_rows} rows")
    client.send_arrow_buffers(chunked_table, arena, schema_cache)

    # Get back from Java
    received_table = client.get_arrow_data()

    print(f"Received table: {received_table.num_rows} rows")

    # Verify data matches (note: returned table will be unchunked)
    assert received_table.num_rows == chunked_table.num_rows
    assert received_table.num_columns == chunked_table.num_columns

    for name in chunked_table.schema.names:
        orig_col = chunked_table.column(name).to_pylist()
        recv_col = received_table.column(name).to_pylist()
        assert recv_col == orig_col, f"Mismatch in column {name}"

    arena.close()
