import pyarrow as pa


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
