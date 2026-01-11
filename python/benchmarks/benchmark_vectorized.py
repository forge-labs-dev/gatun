#!/usr/bin/env python3
"""Benchmark vectorized APIs vs individual calls.

This script compares the performance of:
1. get_fields vs multiple get_field calls
2. invoke_methods vs multiple invoke_method calls
3. create_objects vs multiple create_object calls

Run with:
    JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_vectorized.py
"""

import time
from statistics import mean, stdev

from gatun import connect


def benchmark(name: str, func, iterations: int = 100, warmup: int = 10):
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
    print(f"{name:40s}: {avg:8.1f} ± {std:6.1f} μs")
    return avg


def main():
    client = connect()

    print("=" * 60)
    print("Vectorized API Benchmarks")
    print("=" * 60)
    print()

    # --- Benchmark invoke_methods ---
    print("### invoke_methods vs individual calls ###")
    print()

    arr = client.create_object("java.util.ArrayList")

    def individual_invoke_3():
        """3 individual method calls."""
        arr.add("x")
        arr.add("y")
        arr.size()
        # Clean up to keep size stable
        arr.clear()

    def vectorized_invoke_3():
        """3 method calls in one round-trip."""
        client.invoke_methods(
            arr,
            [
                ("add", ("x",)),
                ("add", ("y",)),
                ("size", ()),
            ],
        )
        arr.clear()

    t_individual_3 = benchmark("3 individual invoke_method calls", individual_invoke_3)
    t_vectorized_3 = benchmark("3 vectorized invoke_methods", vectorized_invoke_3)
    print(f"  Speedup: {t_individual_3 / t_vectorized_3:.2f}x")
    print()

    def individual_invoke_10():
        """10 individual method calls."""
        for i in range(10):
            arr.add(i)
        arr.clear()

    def vectorized_invoke_10():
        """10 method calls in one round-trip."""
        client.invoke_methods(arr, [("add", (i,)) for i in range(10)])
        arr.clear()

    t_individual_10 = benchmark(
        "10 individual invoke_method calls", individual_invoke_10
    )
    t_vectorized_10 = benchmark("10 vectorized invoke_methods", vectorized_invoke_10)
    print(f"  Speedup: {t_individual_10 / t_vectorized_10:.2f}x")
    print()

    # --- Benchmark create_objects ---
    print("### create_objects vs individual calls ###")
    print()

    def individual_create_3():
        """3 individual object creations."""
        client.create_object("java.util.ArrayList")
        client.create_object("java.util.HashMap")
        client.create_object("java.util.HashSet")

    def vectorized_create_3():
        """3 object creations in one round-trip."""
        client.create_objects(
            [
                ("java.util.ArrayList", ()),
                ("java.util.HashMap", ()),
                ("java.util.HashSet", ()),
            ]
        )

    t_individual_c3 = benchmark("3 individual create_object calls", individual_create_3)
    t_vectorized_c3 = benchmark("3 vectorized create_objects", vectorized_create_3)
    print(f"  Speedup: {t_individual_c3 / t_vectorized_c3:.2f}x")
    print()

    def individual_create_10():
        """10 individual object creations."""
        for _ in range(10):
            client.create_object("java.util.ArrayList")

    def vectorized_create_10():
        """10 object creations in one round-trip."""
        client.create_objects([("java.util.ArrayList", ()) for _ in range(10)])

    t_individual_c10 = benchmark(
        "10 individual create_object calls", individual_create_10
    )
    t_vectorized_c10 = benchmark("10 vectorized create_objects", vectorized_create_10)
    print(f"  Speedup: {t_individual_c10 / t_vectorized_c10:.2f}x")
    print()

    # --- Benchmark get_fields ---
    print("### get_fields vs individual calls ###")
    print()

    sb = client.create_object("java.lang.StringBuilder", "hello world test string")

    def individual_get_field():
        """Single get_field call."""
        client.get_field(sb, "count")

    def vectorized_get_field_1():
        """Single field via get_fields."""
        client.get_fields(sb, ["count"])

    t_individual_f1 = benchmark("1 individual get_field call", individual_get_field)
    t_vectorized_f1 = benchmark("1 vectorized get_fields", vectorized_get_field_1)
    print(f"  Overhead for single field: {(t_vectorized_f1 - t_individual_f1):.1f} μs")
    print()

    # --- Compare with batch API ---
    print("### Comparison with batch API ###")
    print()

    def batch_invoke_10():
        """10 method calls via batch API."""
        with client.batch() as b:
            for i in range(10):
                b.call(arr, "add", i)
        arr.clear()

    t_batch_10 = benchmark("10 batch API calls", batch_invoke_10)
    print(f"  Vectorized vs Batch speedup: {t_batch_10 / t_vectorized_10:.2f}x")
    print()

    # --- Summary ---
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print()
    print("Vectorized APIs provide significant speedups by reducing round-trips:")
    print(
        f"  - invoke_methods (3 calls):  {t_individual_3 / t_vectorized_3:.1f}x faster"
    )
    print(
        f"  - invoke_methods (10 calls): {t_individual_10 / t_vectorized_10:.1f}x faster"
    )
    print(
        f"  - create_objects (3 calls):  {t_individual_c3 / t_vectorized_c3:.1f}x faster"
    )
    print(
        f"  - create_objects (10 calls): {t_individual_c10 / t_vectorized_c10:.1f}x faster"
    )
    print()
    print("Use vectorized APIs when:")
    print("  - Calling multiple methods on the same object")
    print("  - Creating multiple objects at once")
    print("  - Reading multiple fields from one object")
    print()

    client.close()


if __name__ == "__main__":
    main()
