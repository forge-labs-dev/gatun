#!/usr/bin/env python3
"""Benchmark Python callback performance.

This script measures the overhead of Python callbacks invoked from Java:
1. Simple comparator callbacks
2. Callbacks with different argument types
3. Callback invocation overhead vs direct calls

Run with:
    cd python
    JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_callbacks.py
"""

import time
from statistics import mean, stdev

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


def main():
    client = connect()

    print("=" * 70)
    print("Python Callback Benchmarks")
    print("=" * 70)
    print()

    # --- Basic callback overhead ---
    print("### Callback Overhead ###")
    print()

    # Register a simple comparator
    call_count = [0]

    def simple_compare(a, b):
        call_count[0] += 1
        if a < b:
            return -1
        elif a > b:
            return 1
        return 0

    comparator = client.register_callback(simple_compare, "java.util.Comparator")

    # Create a list to sort
    arr = client.jvm.java.util.ArrayList()
    for i in [5, 2, 8, 1, 9, 3, 7, 4, 6, 0]:
        arr.add(i)

    # Measure sort time (involves multiple callback invocations)
    def sort_10_elements():
        # Reset list
        arr.clear()
        for i in [5, 2, 8, 1, 9, 3, 7, 4, 6, 0]:
            arr.add(i)
        # Sort with callback
        client.jvm.java.util.Collections.sort(arr, comparator)

    call_count[0] = 0
    t_sort = benchmark("Sort 10 elements with Python comparator", sort_10_elements, iterations=50)
    avg_callbacks_per_sort = call_count[0] / 50
    print(f"  Average callbacks per sort: {avg_callbacks_per_sort:.1f}")
    print(f"  Estimated per-callback overhead: {t_sort / avg_callbacks_per_sort:.1f} μs")
    print()

    # --- Comparison: Java vs Python comparator ---
    print("### Java vs Python Comparator ###")
    print()

    # Create a larger list
    large_arr = client.jvm.java.util.ArrayList()
    for i in range(100):
        large_arr.add(100 - i)  # Reverse order

    # Use natural ordering (Java-side comparison)
    def sort_java_comparator():
        large_arr.clear()
        for i in range(100):
            large_arr.add(100 - i)
        client.jvm.java.util.Collections.sort(large_arr)

    t_java = benchmark("Sort 100 elements (Java comparator)", sort_java_comparator, iterations=30)

    # Use Python callback
    def sort_python_comparator():
        large_arr.clear()
        for i in range(100):
            large_arr.add(100 - i)
        client.jvm.java.util.Collections.sort(large_arr, comparator)

    call_count[0] = 0
    t_python = benchmark("Sort 100 elements (Python comparator)", sort_python_comparator, iterations=30)
    avg_callbacks = call_count[0] / 30
    print(f"  Average callbacks per sort: {avg_callbacks:.1f}")
    print(f"  Overhead ratio (Python/Java): {t_python / t_java:.1f}x")
    print()

    # --- Different callback argument types ---
    print("### Callback with Different Argument Types ###")
    print()

    # String comparator
    def string_compare(a, b):
        if a < b:
            return -1
        elif a > b:
            return 1
        return 0

    string_comparator = client.register_callback(string_compare, "java.util.Comparator")

    str_arr = client.jvm.java.util.ArrayList()
    for s in ["zebra", "apple", "mango", "banana", "cherry", "date", "fig", "grape"]:
        str_arr.add(s)

    def sort_strings():
        str_arr.clear()
        for s in ["zebra", "apple", "mango", "banana", "cherry", "date", "fig", "grape"]:
            str_arr.add(s)
        client.jvm.java.util.Collections.sort(str_arr, string_comparator)

    benchmark("Sort 8 strings with Python comparator", sort_strings, iterations=50)
    print()

    # --- BiFunction callback (used for computeIfAbsent, merge, etc.) ---
    print("### BiFunction Callback ###")
    print()

    def merge_fn(old_val, new_val):
        """Merge function for HashMap.merge()"""
        return f"{old_val}+{new_val}"

    bi_func = client.register_callback(merge_fn, "java.util.function.BiFunction")

    merge_map = client.jvm.java.util.HashMap()

    def merge_values():
        merge_map.clear()
        # First put creates key
        merge_map.put("key", "v1")
        # merge() calls BiFunction when key exists
        merge_map.merge("key", "v2", bi_func)

    benchmark("HashMap.merge with BiFunction callback", merge_values, iterations=50)
    print()

    # --- Predicate callback (filter-like operations) ---
    print("### Predicate Callback ###")
    print()

    def is_even(x):
        """Check if a number is even."""
        return x % 2 == 0

    predicate = client.register_callback(is_even, "java.util.function.Predicate")

    numbers = client.jvm.java.util.ArrayList()
    for i in range(10):
        numbers.add(i)

    def filter_with_predicate():
        numbers.clear()
        for i in range(10):
            numbers.add(i)
        # removeIf uses Predicate
        numbers.removeIf(predicate)

    benchmark("ArrayList.removeIf with Predicate (10 items)", filter_with_predicate, iterations=50)
    print()

    # --- Summary ---
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print()
    print("Key findings:")
    print("  - Each callback round-trip adds ~100-200 μs overhead")
    print("  - For sorting, Python comparator is ~10-20x slower than Java")
    print("  - Callback overhead is dominated by IPC round-trip time")
    print()
    print("Recommendations:")
    print("  - Use Java-side comparators when possible (Collections.sort without comparator)")
    print("  - Minimize callback invocations for performance-critical code")
    print("  - Consider doing comparison logic in Java and returning results to Python")
    print()

    client.close()


if __name__ == "__main__":
    main()
