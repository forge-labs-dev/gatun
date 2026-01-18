#!/usr/bin/env python3
"""Benchmark Arrow data transfer performance.

This script compares different Arrow data transfer methods in Gatun:
1. send_arrow_table - IPC format serialization
2. send_arrow_buffers - Zero-copy buffer transfer
3. Traditional list/dict transfer for comparison

Run with:
    cd python
    JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_arrow.py
"""

import time
from statistics import mean, stdev

import pyarrow as pa

from gatun import connect


def benchmark(name: str, func, iterations: int = 100, warmup: int = 20):
    """Run a benchmark and print results."""
    # Warmup
    for _ in range(warmup):
        func()

    # Timed runs
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        elapsed = time.perf_counter() - start
        times.append(elapsed * 1_000_000)  # Convert to microseconds

    avg = mean(times)
    std = stdev(times) if len(times) > 1 else 0
    min_t = min(times)
    max_t = max(times)
    print(f"{name:50s}: {avg:10.1f} ± {std:8.1f} μs (min={min_t:.1f}, max={max_t:.1f})")
    return avg


def benchmark_throughput(
    name: str, func, data_size_bytes: int, iterations: int = 50, warmup: int = 10
):
    """Run a throughput benchmark and print MB/s."""
    # Warmup
    for _ in range(warmup):
        func()

    # Timed runs
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = mean(times)
    throughput_mbps = (data_size_bytes / (1024 * 1024)) / avg_time
    print(f"{name:50s}: {throughput_mbps:8.1f} MB/s (avg {avg_time * 1000:.2f} ms)")
    return throughput_mbps


def main():
    client = connect()

    print("=" * 70)
    print("Arrow Data Transfer Benchmarks")
    print("=" * 70)
    print()

    # --- Small data: Latency-focused ---
    print("### Small Data (Latency Focus) ###")
    print()

    # 100 rows - typical small batch
    small_table = pa.table(
        {
            "id": pa.array(range(100), type=pa.int64()),
            "name": pa.array([f"item_{i}" for i in range(100)]),
            "value": pa.array([float(i) * 1.5 for i in range(100)]),
        }
    )
    small_size = small_table.nbytes

    # Equivalent Python data
    small_list = list(range(100))
    small_dict = {f"key_{i}": f"value_{i}" for i in range(100)}

    print(f"Table: 100 rows, 3 columns, {small_size:,} bytes")
    print()

    # List transfer (baseline)
    def transfer_list():
        arr = client.create_object("java.util.ArrayList", small_list)
        arr.size()

    t_list = benchmark("List[int] (100 elements) via ArrayList", transfer_list)

    # Dict transfer (baseline)
    def transfer_dict():
        hm = client.create_object("java.util.HashMap", small_dict)
        hm.size()

    _ = benchmark("Dict[str,str] (100 entries) via HashMap", transfer_dict)

    # send_arrow_table (IPC format)
    def send_ipc():
        client.send_arrow_table(small_table)

    t_ipc = benchmark("Arrow Table (100 rows) via IPC", send_ipc)

    # send_arrow_buffers (zero-copy)
    arena = client.get_payload_arena()
    schema_cache = {}

    def send_buffers():
        arena.reset()
        client.send_arrow_buffers(small_table, arena, schema_cache)

    t_buffers = benchmark("Arrow Table (100 rows) via zero-copy buffers", send_buffers)

    print()
    print(f"  IPC vs List speedup: {t_list / t_ipc:.2f}x")
    print(f"  Zero-copy vs IPC speedup: {t_ipc / t_buffers:.2f}x")
    print(f"  Zero-copy vs List speedup: {t_list / t_buffers:.2f}x")
    print()

    # --- Medium data: Balance of latency and throughput ---
    print("### Medium Data (1K-10K rows) ###")
    print()

    for num_rows in [1000, 5000, 10000]:
        medium_table = pa.table(
            {
                "id": pa.array(range(num_rows), type=pa.int64()),
                "name": pa.array([f"item_{i}" for i in range(num_rows)]),
                "value": pa.array([float(i) * 1.5 for i in range(num_rows)]),
                "flag": pa.array([i % 2 == 0 for i in range(num_rows)]),
            }
        )
        data_size = medium_table.nbytes

        print(f"Table: {num_rows:,} rows, 4 columns, {data_size:,} bytes")

        def send_ipc_medium(t=medium_table):
            client.send_arrow_table(t)

        def send_buffers_medium(t=medium_table):
            arena.reset()
            client.send_arrow_buffers(t, arena, schema_cache)

        t_ipc_m = benchmark(
            f"  IPC format ({num_rows:,} rows)", send_ipc_medium, iterations=50
        )
        t_buf_m = benchmark(
            f"  Zero-copy buffers ({num_rows:,} rows)",
            send_buffers_medium,
            iterations=50,
        )
        print(f"    Speedup: {t_ipc_m / t_buf_m:.2f}x")
        print()

    # --- Large data: Throughput-focused ---
    print("### Large Data (Throughput Focus) ###")
    print()

    for num_rows in [50000, 100000, 500000]:
        large_table = pa.table(
            {
                "id": pa.array(range(num_rows), type=pa.int64()),
                "value": pa.array([float(i) for i in range(num_rows)]),
            }
        )
        data_size = large_table.nbytes

        print(
            f"Table: {num_rows:,} rows, 2 columns, {data_size:,} bytes ({data_size / (1024 * 1024):.1f} MB)"
        )

        def send_ipc_large(t=large_table):
            client.send_arrow_table(t)

        def send_buffers_large(t=large_table):
            arena.reset()
            client.send_arrow_buffers(t, arena, schema_cache)

        # Fewer iterations for large data
        iters = 20 if num_rows <= 100000 else 10
        benchmark_throughput(
            f"  IPC format ({num_rows:,} rows)",
            send_ipc_large,
            data_size,
            iterations=iters,
        )
        benchmark_throughput(
            f"  Zero-copy buffers ({num_rows:,} rows)",
            send_buffers_large,
            data_size,
            iterations=iters,
        )
        print()

    # --- PyArrow Array transfer (primitives) ---
    print("### PyArrow Array Transfer (to Java arrays) ###")
    print()

    # Note: Large arrays may exceed command zone size (64KB), so we limit to smaller sizes
    # For bulk data transfer, use send_arrow_table or send_arrow_buffers instead
    for size in [100, 1000, 5000]:
        int_array = pa.array(range(size), type=pa.int32())
        double_array = pa.array([float(i) for i in range(size)], type=pa.float64())

        def transfer_int_array(arr=int_array):
            # Pass pa.Array as argument - converts to Java int[]
            _ = client.jvm.java.util.Arrays.toString(arr)

        def transfer_double_array(arr=double_array):
            _ = client.jvm.java.util.Arrays.toString(arr)

        benchmark(
            f"pa.Array[int32] ({size:,}) -> int[]", transfer_int_array, iterations=50
        )
        benchmark(
            f"pa.Array[float64] ({size:,}) -> double[]",
            transfer_double_array,
            iterations=50,
        )
        print()

    # --- String data (more expensive) ---
    print("### String Column Transfer ###")
    print()

    for num_rows in [1000, 10000]:
        # Short strings
        short_str_table = pa.table(
            {
                "short": pa.array([f"s{i}" for i in range(num_rows)]),
            }
        )

        # Long strings (100 chars each)
        long_str_table = pa.table(
            {
                "long": pa.array(["x" * 100 + str(i) for i in range(num_rows)]),
            }
        )

        print(
            f"Short strings ({num_rows:,} rows, ~3 chars each): {short_str_table.nbytes:,} bytes"
        )

        def send_short(t=short_str_table):
            arena.reset()
            client.send_arrow_buffers(t, arena, schema_cache)

        benchmark("  Zero-copy buffers", send_short, iterations=50)

        print(
            f"Long strings ({num_rows:,} rows, ~100 chars each): {long_str_table.nbytes:,} bytes"
        )

        def send_long(t=long_str_table):
            arena.reset()
            client.send_arrow_buffers(t, arena, schema_cache)

        benchmark("  Zero-copy buffers", send_long, iterations=50)
        print()

    # --- Batch API comparison ---
    print("### Comparison: Arrow vs Batch API for bulk insert ###")
    print()

    # Use smaller size to fit within command zone (64KB limit)
    num_items = 500

    # Batch API: add items one by one
    arr = client.create_object("java.util.ArrayList")

    def batch_insert():
        arr.clear()
        with client.batch() as b:
            for i in range(num_items):
                b.call(arr, "add", i)

    t_batch = benchmark(
        f"Batch API: {num_items} add() calls", batch_insert, iterations=30
    )

    # Arrow: send all at once
    items_table = pa.table({"values": pa.array(range(num_items), type=pa.int64())})

    def arrow_insert():
        arena.reset()
        client.send_arrow_buffers(items_table, arena, schema_cache)

    t_arrow = benchmark(
        f"Arrow: send {num_items} values at once", arrow_insert, iterations=30
    )

    print()
    print(f"  Arrow vs Batch speedup: {t_batch / t_arrow:.1f}x")

    # --- Summary ---
    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print()
    print("Key findings:")
    print("  - Zero-copy buffers are faster than IPC for all sizes")
    print(
        "  - Arrow transfer is significantly faster than list/dict for structured data"
    )
    print("  - Use send_arrow_buffers() with get_payload_arena() for best performance")
    print("  - For single values or small objects, regular API is fine")
    print()
    print("Recommendations:")
    print("  - Small data (<100 rows): Either method works, zero-copy slightly faster")
    print("  - Medium data (100-10K rows): Use zero-copy buffers")
    print("  - Large data (>10K rows): Use zero-copy buffers for max throughput")
    print()

    arena.close()
    client.close()


if __name__ == "__main__":
    main()
