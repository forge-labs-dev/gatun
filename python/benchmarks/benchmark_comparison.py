#!/usr/bin/env python3
"""Comprehensive performance benchmarks comparing Gatun vs Py4J.

This script measures latency and throughput for all JVM bridge operations
used by PySpark, based on analysis of the PySpark codebase.

Usage:
    cd python
    uv run python benchmarks/benchmark_comparison.py

Categories tested:
    0. Baseline - Python overhead measurement for calibration
    1. Object Construction - no-arg, with primitives, with strings, with objects
    2. Static Method Calls - no args, primitives, strings, objects
    3. Instance Method Calls - simple, with args, chaining, returning objects
    4. Field Access - static fields, instance fields
    5. Data Transfer (Python -> Java) - lists, dicts, bytes with materialization verification
    6. Data Transfer (Java -> Python) - primitives, strings, object refs
    7. Error Propagation - bridge error transport vs Java-internal exceptions
    8. Type Checking - is_instance_of

Fairness notes:
    - Py4J is tested with auto_convert=True (PySpark default) and auto_convert=False
    - Baseline measurements allow computing net bridge cost
    - Batch timing used for fast operations to reduce Python overhead
"""

import glob
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import time
from typing import Callable

# Benchmark configuration
WARMUP_SECONDS = 2.0  # Time-based warmup for JIT + classloading
WARMUP_MIN_ITERATIONS = 1000  # Minimum warmup iterations
BENCHMARK_ITERATIONS = 1000
BATCH_SIZE = 100  # For batch timing of fast operations
BULK_SIZE = 10000


def get_cpu_info() -> str:
    """Get CPU model info."""
    try:
        if platform.system() == "Darwin":
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        elif platform.system() == "Linux":
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        return line.split(":")[1].strip()
    except Exception:
        pass
    return "Unknown"


def get_jvm_version() -> str:
    """Get JVM version."""
    try:
        java_home = os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@21")
        java_cmd = os.path.join(java_home, "bin", "java")
        result = subprocess.run(
            [java_cmd, "-version"],
            capture_output=True,
            text=True,
        )
        # Java outputs version to stderr
        lines = result.stderr.strip().split("\n")
        return lines[0] if lines else "Unknown"
    except Exception:
        return "Unknown"


def print_environment():
    """Print environment info for reproducibility."""
    print("=" * 80)
    print(" Environment")
    print("=" * 80)
    print(f"Python:     {sys.version}")
    print(f"Platform:   {platform.platform()}")
    print(f"CPU:        {get_cpu_info()}")
    print(f"JVM:        {get_jvm_version()}")
    print(f"JAVA_HOME:  {os.environ.get('JAVA_HOME', 'not set')}")

    # Gatun config
    try:
        from gatun.config import load_config

        config = load_config()
        print(f"Gatun memory:  {config.memory}")
        print(f"Gatun socket:  Unix domain socket (default)")
    except Exception as e:
        print(f"Gatun config:  Error loading: {e}")

    # Py4J version
    try:
        import py4j

        print(f"Py4J module:   {py4j.__version__}")
    except Exception:
        print("Py4J module:   Not installed or version unavailable")

    print()


def compute_percentile(sorted_times: list[float], p: float) -> float:
    """Compute percentile using linear interpolation (same as numpy)."""
    n = len(sorted_times)
    if n == 0:
        return 0.0
    # Use linear interpolation method (numpy default)
    idx = (n - 1) * p
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    weight = idx - lower
    return sorted_times[lower] * (1 - weight) + sorted_times[upper] * weight


def run_benchmark(
    name: str,
    func: Callable,
    iterations: int = BENCHMARK_ITERATIONS,
    warmup_seconds: float = WARMUP_SECONDS,
) -> dict:
    """Run a benchmark and return statistics.

    Uses time-based warmup to ensure JIT compilation and cache warming.
    """
    # Time-based warmup (minimum iterations + time threshold)
    warmup_count = 0
    warmup_start = time.perf_counter()
    while warmup_count < WARMUP_MIN_ITERATIONS or (time.perf_counter() - warmup_start) < warmup_seconds:
        func()
        warmup_count += 1

    # Benchmark
    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        func()
        elapsed = (time.perf_counter_ns() - start) / 1_000  # microseconds
        times.append(elapsed)

    sorted_times = sorted(times)
    return {
        "name": name,
        "iterations": iterations,
        "warmup_count": warmup_count,
        "mean_us": statistics.mean(times),
        "median_us": statistics.median(times),
        "stdev_us": statistics.stdev(times) if len(times) > 1 else 0,
        "min_us": min(times),
        "max_us": max(times),
        "p95_us": compute_percentile(sorted_times, 0.95),
        "p99_us": compute_percentile(sorted_times, 0.99),
    }


def run_cold_benchmark(
    name: str,
    setup_func: Callable,
    bench_func: Callable,
    iterations: int = 100,
) -> dict:
    """Run a cold-cache benchmark where setup is done inside each iteration.

    This measures the cost including cache misses, resolution, etc.
    Lower iteration count since each iteration is expensive.
    """
    times = []
    for _ in range(iterations):
        # Setup (creates fresh state, clears caches if applicable)
        ctx = setup_func()

        # Timed operation
        start = time.perf_counter_ns()
        bench_func(ctx)
        elapsed = (time.perf_counter_ns() - start) / 1_000  # microseconds
        times.append(elapsed)

    sorted_times = sorted(times)
    return {
        "name": name,
        "iterations": iterations,
        "warmup_count": 0,
        "cold": True,
        "mean_us": statistics.mean(times),
        "median_us": statistics.median(times),
        "stdev_us": statistics.stdev(times) if len(times) > 1 else 0,
        "min_us": min(times),
        "max_us": max(times),
        "p95_us": compute_percentile(sorted_times, 0.95),
        "p99_us": compute_percentile(sorted_times, 0.99),
    }


def run_batch_benchmark(
    name: str,
    func: Callable,
    iterations: int = BENCHMARK_ITERATIONS,
    batch_size: int = BATCH_SIZE,
    warmup_seconds: float = WARMUP_SECONDS,
) -> dict:
    """Run a benchmark using batch timing to reduce Python overhead.

    For fast operations (<10μs), per-iteration timing overhead dominates.
    This measures N calls in one timed block and divides by N.
    """
    # Time-based warmup
    warmup_count = 0
    warmup_start = time.perf_counter()
    while warmup_count < WARMUP_MIN_ITERATIONS or (time.perf_counter() - warmup_start) < warmup_seconds:
        func()
        warmup_count += 1

    # Benchmark in batches
    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        for _ in range(batch_size):
            func()
        elapsed = (time.perf_counter_ns() - start) / 1_000 / batch_size  # microseconds per call
        times.append(elapsed)

    sorted_times = sorted(times)
    return {
        "name": name,
        "iterations": iterations * batch_size,
        "warmup_count": warmup_count,
        "batch_size": batch_size,
        "mean_us": statistics.mean(times),
        "median_us": statistics.median(times),
        "stdev_us": statistics.stdev(times) if len(times) > 1 else 0,
        "min_us": min(times),
        "max_us": max(times),
        "p95_us": compute_percentile(sorted_times, 0.95),
        "p99_us": compute_percentile(sorted_times, 0.99),
    }


def _noop():
    """Empty function for measuring call overhead."""
    pass


def measure_baseline() -> dict:
    """Measure Python overhead for calibration."""
    results = {}

    # Measure lambda call overhead
    noop_result = run_benchmark("python_noop", lambda: None)
    results["lambda_noop"] = noop_result["mean_us"]

    # Measure function call overhead
    func_result = run_benchmark("python_func", _noop)
    results["func_call"] = func_result["mean_us"]

    return results


def print_category(name: str):
    """Print category header."""
    print(f"\n  {name}")
    print(f"  {'-' * 60}")


def print_results(results: list[dict], title: str):
    """Print benchmark results in a table."""
    print(f"\n{'=' * 90}")
    print(f" {title}")
    print(f"{'=' * 90}")
    print(f"{'Benchmark':<45} {'Mean (μs)':>10} {'Median':>10} {'P95':>10} {'P99':>10}")
    print(f"{'-' * 90}")

    current_category = None
    for r in results:
        # Extract category from name (e.g., "1.1 Foo" -> "1")
        cat = r["name"].split(".")[0] if "." in r["name"] else ""
        if cat != current_category:
            current_category = cat
            if cat:
                print()  # Blank line between categories

        p95 = r.get("p95_us", 0)
        print(
            f"{r['name']:<45} {r['mean_us']:>10.1f} {r['median_us']:>10.1f} {p95:>10.1f} {r['p99_us']:>10.1f}"
        )
    print()


def print_comparison(
    gatun_results: list[dict],
    py4j_results: list[dict],
    gatun_baseline: dict | None = None,
    py4j_baseline: dict | None = None,
):
    """Print comparison between Gatun and Py4J.

    If baselines are provided, also shows net latency (raw - baseline overhead).
    """
    import math

    # Verify alignment by benchmark name
    assert len(gatun_results) == len(py4j_results), (
        f"Result count mismatch: Gatun has {len(gatun_results)}, Py4J has {len(py4j_results)}"
    )
    for i, (g, p) in enumerate(zip(gatun_results, py4j_results)):
        assert g["name"] == p["name"], (
            f"Benchmark name mismatch at index {i}: Gatun '{g['name']}' vs Py4J '{p['name']}'"
        )

    # Use lambda_noop as the baseline overhead (most common call pattern)
    gatun_overhead = gatun_baseline["lambda_noop"] if gatun_baseline else 0
    py4j_overhead = py4j_baseline["lambda_noop"] if py4j_baseline else 0
    show_net = gatun_baseline is not None and py4j_baseline is not None

    print(f"\n{'=' * 102}")
    print(" Comparison: Gatun vs Py4J")
    if show_net:
        print(f" (Net = Raw - {gatun_overhead:.2f}μs Gatun baseline, {py4j_overhead:.2f}μs Py4J baseline)")
    print(f"{'=' * 102}")
    if show_net:
        print(
            f"{'Benchmark':<36} {'Gatun':>10} {'Net':>8} {'Py4J':>10} {'Net':>8} {'Speedup':>12} {'NetSpdup':>10}"
        )
    else:
        print(
            f"{'Benchmark':<40} {'Gatun (μs)':>12} {'Py4J (μs)':>12} {'Speedup':>12} {'Diff':>10}"
        )
    print(f"{'-' * 102}")

    speedups = []
    net_speedups = []
    current_category = None

    for g, p in zip(gatun_results, py4j_results):
        # Extract category
        cat = g["name"].split(".")[0] if "." in g["name"] else ""
        if cat != current_category:
            current_category = cat
            if cat:
                print()

        # Skip if either value is NaN (e.g., auto_convert=False can't do list transfer)
        if math.isnan(p["mean_us"]) or math.isnan(g["mean_us"]):
            if show_net:
                print(
                    f"{g['name']:<36} {g['mean_us']:>10.1f} {'N/A':>8} {'N/A':>10} {'N/A':>8} {'N/A':>12} {'N/A':>10}"
                )
            else:
                print(
                    f"{g['name']:<40} {g['mean_us']:>12.1f} {'N/A':>12} {'N/A':>12} {'N/A':>10}"
                )
            continue

        speedup = p["mean_us"] / g["mean_us"] if g["mean_us"] > 0 else 0
        if speedup > 0:
            speedups.append(speedup)

        # Compute net values (ensure non-negative)
        g_net = max(0, g["mean_us"] - gatun_overhead)
        p_net = max(0, p["mean_us"] - py4j_overhead)
        net_speedup = p_net / g_net if g_net > 0 else 0
        if net_speedup > 0:
            net_speedups.append(net_speedup)

        if speedup >= 1:
            speedup_str = f"{speedup:.2f}x"
        else:
            speedup_str = f"{1/speedup:.2f}x slower"

        if show_net:
            if net_speedup >= 1:
                net_str = f"{net_speedup:.2f}x"
            else:
                net_str = f"{1/net_speedup:.2f}x slow"
            print(
                f"{g['name']:<36} {g['mean_us']:>10.1f} {g_net:>8.1f} {p['mean_us']:>10.1f} {p_net:>8.1f} {speedup_str:>12} {net_str:>10}"
            )
        else:
            diff = p["mean_us"] - g["mean_us"]
            diff_str = f"{diff:+.1f}"
            print(
                f"{g['name']:<40} {g['mean_us']:>12.1f} {p['mean_us']:>12.1f} {speedup_str:>12} {diff_str:>10}"
            )

    if speedups:
        geo_mean = statistics.geometric_mean(speedups)
        print(f"{'-' * 102}")
        if show_net and net_speedups:
            net_geo_mean = statistics.geometric_mean(net_speedups)
            print(
                f"{'Geometric Mean':>36} {'':>10} {'':>8} {'':>10} {'':>8} {geo_mean:.2f}x faster {net_geo_mean:.2f}x net"
            )
        else:
            print(f"{'Geometric Mean':>40} {'':>12} {'':>12} {geo_mean:.2f}x faster {'':>10}")
    print()


def benchmark_gatun() -> tuple[list[dict], dict]:
    """Run comprehensive Gatun benchmarks.

    Returns:
        Tuple of (results list, baseline dict)
    """
    from gatun import connect

    client = connect()
    results = []

    # =========================================================================
    # 0. BASELINE MEASUREMENTS
    # =========================================================================
    baseline = measure_baseline()

    # =========================================================================
    # 1. OBJECT CONSTRUCTION
    # =========================================================================

    # 1.1 No-arg constructor
    results.append(
        run_benchmark(
            "1.1 new() no args",
            lambda: client.create_object("java.util.ArrayList"),
        )
    )

    # 1.2 Constructor with int arg
    results.append(
        run_benchmark(
            "1.2 new() int arg",
            lambda: client.create_object("java.util.ArrayList", 100),
        )
    )

    # 1.3 Constructor with string arg
    results.append(
        run_benchmark(
            "1.3 new() string arg",
            lambda: client.create_object("java.lang.StringBuilder", "hello world"),
        )
    )

    # 1.4 Constructor with int arg (HashMap with initial capacity)
    results.append(
        run_benchmark(
            "1.4 new() HashMap(int)",
            lambda: client.create_object("java.util.HashMap", 16),
        )
    )

    # =========================================================================
    # 2. STATIC METHOD CALLS
    # =========================================================================

    # 2.1 Static method - no args
    results.append(
        run_benchmark(
            "2.1 static() no args",
            lambda: client.jvm.java.lang.System.currentTimeMillis(),
        )
    )

    # 2.2 Static method - int args
    results.append(
        run_benchmark(
            "2.2 static() int args",
            lambda: client.jvm.java.lang.Math.max(10, 20),
        )
    )

    # 2.3 Static method - double args
    results.append(
        run_benchmark(
            "2.3 static() double args",
            lambda: client.jvm.java.lang.Math.pow(2.0, 10.0),
        )
    )

    # 2.4 Static method - string arg
    results.append(
        run_benchmark(
            "2.4 static() string arg",
            lambda: client.jvm.java.lang.Integer.parseInt("42"),
        )
    )

    # 2.5 Static method - string arg, string return
    results.append(
        run_benchmark(
            "2.5 static() string->string",
            lambda: client.jvm.java.lang.String.valueOf(12345),
        )
    )

    # =========================================================================
    # 3. INSTANCE METHOD CALLS
    # =========================================================================

    # 3.1 Instance method - no args, int return (fast op, use batch timing)
    arr = client.create_object("java.util.ArrayList")
    results.append(
        run_batch_benchmark(
            "3.1 call() no args->int",
            lambda: arr.size(),
        )
    )

    # 3.2 Instance method - int arg
    arr2 = client.create_object("java.util.ArrayList")
    counter = [0]

    def add_int():
        arr2.add(counter[0])
        counter[0] += 1

    results.append(run_benchmark("3.2 call() int arg", add_int))

    # 3.3 Instance method - string arg
    arr3 = client.create_object("java.util.ArrayList")
    results.append(
        run_benchmark(
            "3.3 call() string arg",
            lambda: arr3.add("test string"),
        )
    )

    # 3.4 Instance method - object return
    arr4 = client.create_object("java.util.ArrayList")
    arr4.add("item")
    results.append(
        run_benchmark(
            "3.4 call() object return",
            lambda: arr4.get(0),
        )
    )

    # 3.5 Instance method - string return
    sb = client.create_object("java.lang.StringBuilder", "hello")
    results.append(
        run_benchmark(
            "3.5 call() string return",
            lambda: sb.toString(),
        )
    )

    # 3.6 Two consecutive calls (simulates chaining pattern)
    sb_chain = client.create_object("java.lang.StringBuilder", "hello")

    def chain_call():
        # Append returns StringBuilder, then call toString
        sb_chain.append(" world")
        return sb_chain.length()

    results.append(run_benchmark("3.6 call() two calls", chain_call))

    # =========================================================================
    # 4. FIELD ACCESS (fast ops, use batch timing)
    # =========================================================================

    # 4.1 Static field - int
    results.append(
        run_batch_benchmark(
            "4.1 field static int",
            lambda: client.jvm.java.lang.Integer.MAX_VALUE,
        )
    )

    # 4.2 Static field - object (via class)
    results.append(
        run_batch_benchmark(
            "4.2 field static object",
            lambda: client.jvm.java.lang.System.out,
        )
    )

    # =========================================================================
    # 5. DATA TRANSFER - Python to Java (with materialization verification)
    # =========================================================================

    # 5.1 Transfer small list - use ArrayList(Collection) constructor to force iteration
    # Then force materialization by calling size() and get() (discard results, no assert)
    small_list = list(range(10))

    def transfer_list_10():
        java_list = client.create_object("java.util.ArrayList", small_list)
        # Force materialization: call size and get (results discarded)
        java_list.size()
        java_list.get(5)

    # Verify correctness once outside timed region
    _test_list = client.create_object("java.util.ArrayList", small_list)
    assert _test_list.size() == 10, "list(10) transfer verification failed"

    results.append(run_benchmark("5.1 list(10) materialized", transfer_list_10))

    # 5.2 Transfer medium list
    medium_list = list(range(100))

    def transfer_list_100():
        java_list = client.create_object("java.util.ArrayList", medium_list)
        # Force materialization: call size and get (results discarded)
        java_list.size()
        java_list.get(50)

    # Verify correctness once outside timed region
    _test_list = client.create_object("java.util.ArrayList", medium_list)
    assert _test_list.size() == 100, "list(100) transfer verification failed"

    results.append(run_benchmark("5.2 list(100) materialized", transfer_list_100))

    # 5.3 Transfer dict - use HashMap constructor with dict, verify with size + get
    test_dict = {"key1": "value1", "key2": 42, "key3": True}

    def transfer_dict():
        java_map = client.create_object("java.util.HashMap", test_dict)
        # Force materialization: call size and get (results discarded)
        java_map.size()
        java_map.get("key1")

    # Verify correctness once outside timed region
    _test_map = client.create_object("java.util.HashMap", test_dict)
    assert _test_map.size() == 3, "dict(3) transfer verification failed"

    results.append(run_benchmark("5.3 dict(3) materialized", transfer_dict))

    # 5.4 Transfer bytes (small) - store in ArrayList, verify length via Java-side only
    # Use Arrays.hashCode to force consumption without returning byte[] to Python
    small_bytes = b"hello world" * 10  # 110 bytes

    def transfer_bytes_small():
        arr = client.create_object("java.util.ArrayList")
        arr.add(small_bytes)
        # Force consumption entirely in Java: get ref and compute hash (no Python conversion)
        byte_arr = arr.get(0)
        client.jvm.java.util.Arrays.hashCode(byte_arr)

    # Verify correctness once outside timed region
    _test_arr = client.create_object("java.util.ArrayList")
    _test_arr.add(small_bytes)
    _test_bytes = _test_arr.get(0)
    assert client.jvm.java.lang.reflect.Array.getLength(_test_bytes) == 110, "bytes(110) verification failed"

    results.append(run_benchmark("5.4 bytes(110) verified", transfer_bytes_small))

    # 5.5 Transfer bytes (medium)
    medium_bytes = b"x" * 1000

    def transfer_bytes_1kb():
        arr = client.create_object("java.util.ArrayList")
        arr.add(medium_bytes)
        # Force consumption entirely in Java
        byte_arr = arr.get(0)
        client.jvm.java.util.Arrays.hashCode(byte_arr)

    # Verify correctness once outside timed region
    _test_arr = client.create_object("java.util.ArrayList")
    _test_arr.add(medium_bytes)
    _test_bytes = _test_arr.get(0)
    assert client.jvm.java.lang.reflect.Array.getLength(_test_bytes) == 1000, "bytes(1KB) verification failed"

    results.append(run_benchmark("5.5 bytes(1KB) verified", transfer_bytes_1kb))

    # =========================================================================
    # 6. DATA TRANSFER - Java to Python
    # =========================================================================

    # Setup: create Java objects with data
    arr_return = client.create_object("java.util.ArrayList")
    for i in range(10):
        arr_return.add(f"item{i}")

    # 6.1 Get size (int return)
    results.append(
        run_benchmark(
            "6.1 return int",
            lambda: arr_return.size(),
        )
    )

    # 6.2 Get string
    results.append(
        run_benchmark(
            "6.2 return string",
            lambda: arr_return.get(0),
        )
    )

    # 6.3 Get object reference
    hm = client.create_object("java.util.HashMap")
    hm.put("key", "value")
    results.append(
        run_benchmark(
            "6.3 return object ref",
            lambda: hm.entrySet(),
        )
    )

    # =========================================================================
    # 7. EXCEPTION HANDLING / ERROR PROPAGATION
    # =========================================================================

    # 7.1 Bridge error propagation - exception thrown in Java, caught in Python
    # This measures the full cost: Java exception creation + cross-language transport
    def catch_bridge_exception():
        try:
            client.jvm.java.lang.Integer.parseInt("not_a_number")
        except Exception:
            pass

    results.append(run_benchmark("7.1 bridge error propagation", catch_bridge_exception))

    # 7.2 Java-side try/catch (exception thrown and caught on Java side, call succeeds)
    # This isolates Java exception creation cost from cross-language transport
    # We use Integer.decode which catches NumberFormatException internally for hex/octal
    # and returns successfully for valid decimal input
    def java_internal_exception():
        # This triggers internal parsing attempts (hex, octal) that throw/catch exceptions
        # before successfully parsing as decimal. The call itself succeeds.
        client.jvm.java.lang.Integer.decode("12345")

    results.append(run_benchmark("7.2 java internal try/catch", java_internal_exception))

    # =========================================================================
    # 8. TYPE CHECKING (fast ops, use batch timing)
    # =========================================================================

    arr_check = client.create_object("java.util.ArrayList")
    results.append(
        run_batch_benchmark(
            "8.1 is_instance_of (true)",
            lambda: client.is_instance_of(arr_check, "java.util.List"),
        )
    )

    results.append(
        run_batch_benchmark(
            "8.2 is_instance_of (false)",
            lambda: client.is_instance_of(arr_check, "java.util.Map"),
        )
    )

    client.close()
    return results, baseline


def benchmark_payload_sweep() -> tuple[list[dict], list[dict]]:
    """Run payload size sweep benchmarks to find crossover points.

    Returns:
        Tuple of (string_results, bytes_results)
    """
    from gatun import connect

    client = connect()

    # String sizes to test: 0, 8, 32, 128, 512, 2K, 8K bytes
    string_sizes = [0, 8, 32, 128, 512, 2048, 8192]
    # Bytes sizes to test: 0, 16, 64, 256, 1K, 4K, 16K, 32K bytes
    # (64K exceeds default command zone size)
    bytes_sizes = [0, 16, 64, 256, 1024, 4096, 16384, 32768]

    string_results = []
    bytes_results = []

    # Fewer iterations for sweep (still enough for stable measurements)
    sweep_iterations = 500

    # =========================================================================
    # String argument sweep
    # =========================================================================
    for size in string_sizes:
        test_string = "x" * size

        def string_arg_test(s=test_string):
            client.create_object("java.lang.StringBuilder", s)

        result = run_benchmark(
            f"str_arg_{size}B",
            string_arg_test,
            iterations=sweep_iterations,
        )
        result["size_bytes"] = size
        string_results.append(result)

    # =========================================================================
    # String return sweep (use StringBuilder.toString with varying content)
    # =========================================================================
    for size in string_sizes:
        sb = client.create_object("java.lang.StringBuilder", "x" * size)

        def string_return_test(builder=sb):
            builder.toString()

        result = run_benchmark(
            f"str_ret_{size}B",
            string_return_test,
            iterations=sweep_iterations,
        )
        result["size_bytes"] = size
        string_results.append(result)

    # =========================================================================
    # Bytes send-only sweep (Python -> Java, returns boolean)
    # =========================================================================
    for size in bytes_sizes:
        test_bytes = b"x" * size
        arr = client.create_object("java.util.ArrayList")

        def bytes_send_test(data=test_bytes, container=arr):
            container.add(data)

        result = run_benchmark(
            f"bytes_send_{size}B",
            bytes_send_test,
            iterations=sweep_iterations,
        )
        result["size_bytes"] = size
        bytes_results.append(result)

    # =========================================================================
    # Bytes round-trip sweep (Python -> Java -> Python echo)
    # Store in ArrayList at index 0, then get(0) to retrieve
    # =========================================================================
    for size in bytes_sizes:
        test_bytes = b"x" * size
        # Pre-populate ArrayList with the bytes so get(0) returns them
        arr_echo = client.create_object("java.util.ArrayList")
        arr_echo.add(test_bytes)

        def bytes_echo_test(container=arr_echo):
            container.get(0)  # Returns byte[] which is converted to Python bytes

        result = run_benchmark(
            f"bytes_echo_{size}B",
            bytes_echo_test,
            iterations=sweep_iterations,
        )
        result["size_bytes"] = size
        bytes_results.append(result)

    client.close()
    return string_results, bytes_results


def print_sweep_results(
    string_results: list[dict],
    bytes_results: list[dict],
    py4j_sweep: dict | None = None,
):
    """Print payload sweep results showing size vs latency.

    If py4j_sweep is provided, shows side-by-side comparison.
    """
    py4j_str = py4j_sweep.get("string_results", []) if py4j_sweep else []
    py4j_bytes = py4j_sweep.get("bytes_results", []) if py4j_sweep else []

    # Build lookup by name for Py4J results
    py4j_str_by_name = {r["name"]: r for r in py4j_str}
    py4j_bytes_by_name = {r["name"]: r for r in py4j_bytes}

    has_py4j = bool(py4j_str or py4j_bytes)

    print(f"\n{'=' * 100}")
    print(" Payload Size Sweep (to identify crossover points)")
    print(f"{'=' * 100}")

    # String results
    print(f"\n  String Operations:")
    print(f"  {'-' * 90}")
    if has_py4j:
        print(f"  {'Operation':<18} {'Size':>8} {'Gatun':>10} {'Py4J':>10} {'Speedup':>10} {'μs/KB':>10}")
    else:
        print(f"  {'Operation':<20} {'Size':>10} {'Mean (μs)':>12} {'Median':>12} {'μs/KB':>12}")
    print(f"  {'-' * 90}")
    for r in string_results:
        size = r["size_bytes"]
        us_per_kb = (r["mean_us"] / size * 1024) if size > 0 else 0
        size_str = f"{size}B" if size < 1024 else f"{size // 1024}KB"

        if has_py4j and r["name"] in py4j_str_by_name:
            py4j_r = py4j_str_by_name[r["name"]]
            speedup = py4j_r["mean_us"] / r["mean_us"] if r["mean_us"] > 0 else 0
            speedup_str = f"{speedup:.2f}x" if speedup >= 1 else f"{1/speedup:.2f}x slow"
            print(
                f"  {r['name']:<18} {size_str:>8} {r['mean_us']:>10.1f} {py4j_r['mean_us']:>10.1f} {speedup_str:>10} {us_per_kb:>10.1f}"
            )
        else:
            print(
                f"  {r['name']:<20} {size_str:>10} {r['mean_us']:>12.1f} {r['median_us']:>12.1f} {us_per_kb:>12.1f}"
            )

    # Bytes results - separate send-only vs round-trip
    send_results = [r for r in bytes_results if "send" in r["name"]]
    echo_results = [r for r in bytes_results if "echo" in r["name"]]

    print(f"\n  Bytes Send-Only (Python -> Java, no return conversion):")
    print(f"  {'-' * 90}")
    if has_py4j:
        print(f"  {'Operation':<20} {'Size':>8} {'Gatun':>10} {'Py4J':>10} {'Speedup':>10} {'μs/KB':>10}")
    else:
        print(f"  {'Operation':<20} {'Size':>10} {'Mean (μs)':>12} {'Median':>12} {'μs/KB':>12}")
    print(f"  {'-' * 90}")
    for r in send_results:
        size = r["size_bytes"]
        us_per_kb = (r["mean_us"] / size * 1024) if size > 0 else 0
        size_str = f"{size}B" if size < 1024 else f"{size // 1024}KB"

        if has_py4j and r["name"] in py4j_bytes_by_name:
            py4j_r = py4j_bytes_by_name[r["name"]]
            speedup = py4j_r["mean_us"] / r["mean_us"] if r["mean_us"] > 0 else 0
            speedup_str = f"{speedup:.2f}x" if speedup >= 1 else f"{1/speedup:.2f}x slow"
            print(
                f"  {r['name']:<20} {size_str:>8} {r['mean_us']:>10.1f} {py4j_r['mean_us']:>10.1f} {speedup_str:>10} {us_per_kb:>10.1f}"
            )
        else:
            print(
                f"  {r['name']:<20} {size_str:>10} {r['mean_us']:>12.1f} {r['median_us']:>12.1f} {us_per_kb:>12.1f}"
            )

    print(f"\n  Bytes Round-Trip (Java -> Python, with return conversion):")
    print(f"  {'-' * 90}")
    if has_py4j:
        print(f"  {'Operation':<20} {'Size':>8} {'Gatun':>10} {'Py4J':>10} {'Speedup':>10} {'μs/KB':>10}")
    else:
        print(f"  {'Operation':<20} {'Size':>10} {'Mean (μs)':>12} {'Median':>12} {'μs/KB':>12}")
    print(f"  {'-' * 90}")
    for r in echo_results:
        size = r["size_bytes"]
        us_per_kb = (r["mean_us"] / size * 1024) if size > 0 else 0
        size_str = f"{size}B" if size < 1024 else f"{size // 1024}KB"

        if has_py4j and r["name"] in py4j_bytes_by_name:
            py4j_r = py4j_bytes_by_name[r["name"]]
            speedup = py4j_r["mean_us"] / r["mean_us"] if r["mean_us"] > 0 else 0
            speedup_str = f"{speedup:.2f}x" if speedup >= 1 else f"{1/speedup:.2f}x slow"
            print(
                f"  {r['name']:<20} {size_str:>8} {r['mean_us']:>10.1f} {py4j_r['mean_us']:>10.1f} {speedup_str:>10} {us_per_kb:>10.1f}"
            )
        else:
            print(
                f"  {r['name']:<20} {size_str:>10} {r['mean_us']:>12.1f} {r['median_us']:>12.1f} {us_per_kb:>12.1f}"
            )
    print()


def benchmark_gatun_throughput() -> list[dict]:
    """Run Gatun throughput benchmarks."""
    from gatun import connect

    client = connect()
    results = []

    # Bulk static calls
    start = time.perf_counter()
    for i in range(BULK_SIZE):
        client.jvm.java.lang.Math.abs(i)
    elapsed = time.perf_counter() - start
    results.append(
        {
            "name": f"Bulk static calls ({BULK_SIZE})",
            "total_ms": elapsed * 1000,
            "ops_per_sec": BULK_SIZE / elapsed,
        }
    )

    # Bulk object creation
    start = time.perf_counter()
    for _ in range(BULK_SIZE):
        client.create_object("java.util.ArrayList")
    elapsed = time.perf_counter() - start
    results.append(
        {
            "name": f"Bulk object creation ({BULK_SIZE})",
            "total_ms": elapsed * 1000,
            "ops_per_sec": BULK_SIZE / elapsed,
        }
    )

    # Bulk instance method calls
    arr = client.create_object("java.util.ArrayList")
    start = time.perf_counter()
    for i in range(BULK_SIZE):
        arr.add(i)
    elapsed = time.perf_counter() - start
    results.append(
        {
            "name": f"Bulk instance calls ({BULK_SIZE})",
            "total_ms": elapsed * 1000,
            "ops_per_sec": BULK_SIZE / elapsed,
        }
    )

    # Mixed workload (simulating real usage)
    # Each iteration = 5 bridge operations: create(1) + put(2) + get(1) + size(1)
    iterations = BULK_SIZE // 10
    ops_per_iteration = 5
    start = time.perf_counter()
    for i in range(iterations):
        # Create object (1 op)
        hm = client.create_object("java.util.HashMap")
        # Call instance methods (2 ops)
        hm.put("key1", i)
        hm.put("key2", f"value{i}")
        # Get values back (2 ops)
        hm.get("key1")
        hm.size()
    elapsed = time.perf_counter() - start
    total_ops = iterations * ops_per_iteration
    results.append(
        {
            "name": f"Mixed workload ({iterations} iter × {ops_per_iteration} ops)",
            "total_ms": elapsed * 1000,
            "iterations": iterations,
            "ops_per_iteration": ops_per_iteration,
            "iter_per_sec": iterations / elapsed,
            "ops_per_sec": total_ops / elapsed,
        }
    )

    client.close()
    return results


def print_throughput_results(results: list[dict], title: str):
    """Print throughput results."""
    print(f"\n{'=' * 90}")
    print(f" {title}")
    print(f"{'=' * 90}")
    print(f"{'Benchmark':<50} {'Total (ms)':>12} {'Iter/sec':>12} {'Ops/sec':>12}")
    print(f"{'-' * 90}")
    for r in results:
        # For mixed workload, show both iter/sec and ops/sec
        if "iter_per_sec" in r:
            print(
                f"{r['name']:<50} {r['total_ms']:>12.1f} {r['iter_per_sec']:>12,.0f} {r['ops_per_sec']:>12,.0f}"
            )
        else:
            # Single-op benchmarks: iter/sec == ops/sec
            print(
                f"{r['name']:<50} {r['total_ms']:>12.1f} {r['ops_per_sec']:>12,.0f} {r['ops_per_sec']:>12,.0f}"
            )
    print()


def print_throughput_comparison(
    gatun_results: list[dict], py4j_results: list[dict]
):
    """Print throughput comparison."""
    # Verify alignment by benchmark name
    assert len(gatun_results) == len(py4j_results), (
        f"Throughput result count mismatch: Gatun has {len(gatun_results)}, Py4J has {len(py4j_results)}"
    )
    for i, (g, p) in enumerate(zip(gatun_results, py4j_results)):
        assert g["name"] == p["name"], (
            f"Throughput benchmark name mismatch at index {i}: Gatun '{g['name']}' vs Py4J '{p['name']}'"
        )

    print(f"\n{'=' * 100}")
    print(" Throughput Comparison: Gatun vs Py4J (ops/sec)")
    print(f"{'=' * 100}")
    print(
        f"{'Benchmark':<55} {'Gatun ops/s':>14} {'Py4J ops/s':>14} {'Speedup':>12}"
    )
    print(f"{'-' * 100}")

    speedups = []
    for g, p in zip(gatun_results, py4j_results):
        speedup = g["ops_per_sec"] / p["ops_per_sec"] if p["ops_per_sec"] > 0 else 0
        speedups.append(speedup)
        print(
            f"{g['name']:<55} {g['ops_per_sec']:>14,.0f} {p['ops_per_sec']:>14,.0f} {speedup:>11.2f}x"
        )

    geo_mean = statistics.geometric_mean(speedups)
    print(f"{'-' * 100}")
    print(f"{'Geometric Mean':>55} {'':>14} {'':>14} {geo_mean:>11.2f}x")
    print()


# Py4J benchmark script (run in subprocess to avoid import conflicts)
# This script tests both auto_convert=True and auto_convert=False for fairness
PY4J_BENCHMARK_SCRIPT = '''
import json
import os
import socket
import statistics
import subprocess
import sys
import time

WARMUP_SECONDS = 2.0
WARMUP_MIN_ITERATIONS = 1000
BENCHMARK_ITERATIONS = 1000
BULK_SIZE = 10000


def _noop():
    pass


def compute_percentile(sorted_times, p):
    n = len(sorted_times)
    if n == 0:
        return 0.0
    idx = (n - 1) * p
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    weight = idx - lower
    return sorted_times[lower] * (1 - weight) + sorted_times[upper] * weight


def run_benchmark(name, func, iterations=BENCHMARK_ITERATIONS):
    # Time-based warmup
    warmup_count = 0
    warmup_start = time.perf_counter()
    while warmup_count < WARMUP_MIN_ITERATIONS or (time.perf_counter() - warmup_start) < WARMUP_SECONDS:
        func()
        warmup_count += 1

    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        func()
        elapsed = (time.perf_counter_ns() - start) / 1_000
        times.append(elapsed)
    sorted_times = sorted(times)
    return {
        "name": name,
        "iterations": iterations,
        "mean_us": statistics.mean(times),
        "median_us": statistics.median(times),
        "stdev_us": statistics.stdev(times) if len(times) > 1 else 0,
        "min_us": min(times),
        "max_us": max(times),
        "p95_us": compute_percentile(sorted_times, 0.95),
        "p99_us": compute_percentile(sorted_times, 0.99),
    }


BATCH_SIZE = 100

def run_batch_benchmark(name, func, iterations=BENCHMARK_ITERATIONS, batch_size=BATCH_SIZE):
    # Time-based warmup
    warmup_count = 0
    warmup_start = time.perf_counter()
    while warmup_count < WARMUP_MIN_ITERATIONS or (time.perf_counter() - warmup_start) < WARMUP_SECONDS:
        func()
        warmup_count += 1

    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        for _ in range(batch_size):
            func()
        elapsed = (time.perf_counter_ns() - start) / 1_000 / batch_size
        times.append(elapsed)
    sorted_times = sorted(times)
    return {
        "name": name,
        "iterations": iterations * batch_size,
        "mean_us": statistics.mean(times),
        "median_us": statistics.median(times),
        "stdev_us": statistics.stdev(times) if len(times) > 1 else 0,
        "min_us": min(times),
        "max_us": max(times),
        "p95_us": compute_percentile(sorted_times, 0.95),
        "p99_us": compute_percentile(sorted_times, 0.99),
    }


def measure_baseline():
    """Measure Python overhead for calibration."""
    noop_result = run_benchmark("python_noop", lambda: None)
    func_result = run_benchmark("python_func", _noop)
    return {
        "lambda_noop": noop_result["mean_us"],
        "func_call": func_result["mean_us"],
    }


def find_py4j_jar():
    """Find py4j JAR using glob, picking newest version."""
    import glob
    import py4j
    py4j_path = os.path.dirname(py4j.__file__)
    venv_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(py4j_path))))

    # Search with glob pattern, pick newest
    patterns = [
        os.path.join(venv_path, "share", "py4j", "py4j*.jar"),
        os.path.join(py4j_path, "share", "py4j", "py4j*.jar"),
    ]
    for pattern in patterns:
        jars = sorted(glob.glob(pattern), reverse=True)  # Newest first
        if jars:
            return jars[0]
    return None


def wait_for_port(port, timeout=10.0, interval=0.1):
    """Wait for a port to become available with retry loop."""
    import socket
    start = time.perf_counter()
    while time.perf_counter() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(interval)
            sock.connect(("127.0.0.1", port))
            sock.close()
            return True
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(interval)
    return False


def run_latency_benchmarks(gateway, auto_convert):
    """Run latency benchmarks with the given gateway.

    Args:
        gateway: Py4J gateway
        auto_convert: bool - whether auto_convert is enabled
    """
    results = []

    # 1. OBJECT CONSTRUCTION
    ArrayList = gateway.jvm.java.util.ArrayList
    StringBuilder = gateway.jvm.java.lang.StringBuilder
    HashMap = gateway.jvm.java.util.HashMap

    results.append(run_benchmark("1.1 new() no args", lambda: ArrayList()))
    results.append(run_benchmark("1.2 new() int arg", lambda: ArrayList(100)))
    results.append(run_benchmark("1.3 new() string arg", lambda: StringBuilder("hello world")))
    results.append(run_benchmark("1.4 new() HashMap(int)", lambda: HashMap(16)))

    # 2. STATIC METHOD CALLS
    System = gateway.jvm.java.lang.System
    Math = gateway.jvm.java.lang.Math
    Integer = gateway.jvm.java.lang.Integer
    String = gateway.jvm.java.lang.String

    results.append(run_benchmark("2.1 static() no args", lambda: System.currentTimeMillis()))
    results.append(run_benchmark("2.2 static() int args", lambda: Math.max(10, 20)))
    results.append(run_benchmark("2.3 static() double args", lambda: Math.pow(2.0, 10.0)))
    results.append(run_benchmark("2.4 static() string arg", lambda: Integer.parseInt("42")))
    results.append(run_benchmark("2.5 static() string->string", lambda: String.valueOf(12345)))

    # 3. INSTANCE METHOD CALLS
    arr = ArrayList()
    results.append(run_batch_benchmark("3.1 call() no args->int", lambda: arr.size()))

    arr2 = ArrayList()
    counter = [0]
    def add_int():
        arr2.add(counter[0])
        counter[0] += 1
    results.append(run_benchmark("3.2 call() int arg", add_int))

    arr3 = ArrayList()
    results.append(run_benchmark("3.3 call() string arg", lambda: arr3.add("test string")))

    arr4 = ArrayList()
    arr4.add("item")
    results.append(run_benchmark("3.4 call() object return", lambda: arr4.get(0)))

    sb = StringBuilder("hello")
    results.append(run_benchmark("3.5 call() string return", lambda: sb.toString()))

    sb_chain = StringBuilder("hello")
    def chain_call():
        sb_chain.append(" world")
        return sb_chain.length()
    results.append(run_benchmark("3.6 call() two calls", chain_call))

    # 4. FIELD ACCESS (fast ops, use batch timing)
    results.append(run_batch_benchmark("4.1 field static int", lambda: Integer.MAX_VALUE))
    results.append(run_batch_benchmark("4.2 field static object", lambda: System.out))

    # 5. DATA TRANSFER - Python to Java (with materialization verification)
    # Note: list/dict transfer only works with auto_convert=True
    Arrays = gateway.jvm.java.util.Arrays
    if auto_convert:
        small_list = list(range(10))
        def transfer_list_10():
            java_list = ArrayList(small_list)
            # Force materialization (no assert inside timed loop)
            java_list.size()
            java_list.get(5)
        results.append(run_benchmark("5.1 list(10) materialized", transfer_list_10))

        medium_list = list(range(100))
        def transfer_list_100():
            java_list = ArrayList(medium_list)
            java_list.size()
            java_list.get(50)
        results.append(run_benchmark("5.2 list(100) materialized", transfer_list_100))

        test_dict = {"key1": "value1", "key2": 42, "key3": True}
        def transfer_dict():
            java_map = HashMap(test_dict)
            java_map.size()
            java_map.get("key1")
        results.append(run_benchmark("5.3 dict(3) materialized", transfer_dict))
    else:
        # Placeholder results for comparison alignment (include all fields)
        nan_placeholder = {"mean_us": float("nan"), "median_us": float("nan"), "p95_us": float("nan"), "p99_us": float("nan"), "iterations": 0, "stdev_us": 0, "min_us": 0, "max_us": 0}
        results.append({"name": "5.1 list(10) materialized", **nan_placeholder})
        results.append({"name": "5.2 list(100) materialized", **nan_placeholder})
        results.append({"name": "5.3 dict(3) materialized", **nan_placeholder})

    # Bytes transfer - force consumption via Arrays.hashCode (no Python round-trip)
    small_bytes = b"hello world" * 10
    def transfer_bytes_small():
        arr = ArrayList()
        arr.add(small_bytes)
        byte_arr = arr.get(0)
        Arrays.hashCode(byte_arr)
    results.append(run_benchmark("5.4 bytes(110) verified", transfer_bytes_small))

    medium_bytes = b"x" * 1000
    def transfer_bytes_1kb():
        arr = ArrayList()
        arr.add(medium_bytes)
        byte_arr = arr.get(0)
        Arrays.hashCode(byte_arr)
    results.append(run_benchmark("5.5 bytes(1KB) verified", transfer_bytes_1kb))

    # 6. DATA TRANSFER - Java to Python
    arr_return = ArrayList()
    for i in range(10):
        arr_return.add(f"item{i}")

    results.append(run_benchmark("6.1 return int", lambda: arr_return.size()))
    results.append(run_benchmark("6.2 return string", lambda: arr_return.get(0)))

    hm = HashMap()
    hm.put("key", "value")
    results.append(run_benchmark("6.3 return object ref", lambda: hm.entrySet()))

    # 7. EXCEPTION HANDLING / ERROR PROPAGATION
    def catch_bridge_exception():
        try:
            Integer.parseInt("not_a_number")
        except Exception:
            pass
    results.append(run_benchmark("7.1 bridge error propagation", catch_bridge_exception))

    # 7.2 Java-side try/catch (exception thrown and caught internally, call succeeds)
    def java_internal_exception():
        Integer.decode("12345")
    results.append(run_benchmark("7.2 java internal try/catch", java_internal_exception))

    # 8. TYPE CHECKING (fast ops, use batch timing)
    from py4j.java_gateway import is_instance_of
    arr_check = ArrayList()
    results.append(run_batch_benchmark("8.1 is_instance_of (true)",
        lambda: is_instance_of(gateway, arr_check, "java.util.List")))
    results.append(run_batch_benchmark("8.2 is_instance_of (false)",
        lambda: is_instance_of(gateway, arr_check, "java.util.Map")))

    return results


def run_throughput_benchmarks(gateway):
    """Run throughput benchmarks."""
    ArrayList = gateway.jvm.java.util.ArrayList
    HashMap = gateway.jvm.java.util.HashMap
    Math = gateway.jvm.java.lang.Math

    throughput_results = []

    # Bulk static calls
    start = time.perf_counter()
    for i in range(BULK_SIZE):
        Math.abs(i)
    elapsed = time.perf_counter() - start
    throughput_results.append({
        "name": f"Bulk static calls ({BULK_SIZE})",
        "total_ms": elapsed * 1000,
        "ops_per_sec": BULK_SIZE / elapsed,
    })

    # Bulk object creation
    start = time.perf_counter()
    for _ in range(BULK_SIZE):
        ArrayList()
    elapsed = time.perf_counter() - start
    throughput_results.append({
        "name": f"Bulk object creation ({BULK_SIZE})",
        "total_ms": elapsed * 1000,
        "ops_per_sec": BULK_SIZE / elapsed,
    })

    # Bulk instance method calls
    arr = ArrayList()
    start = time.perf_counter()
    for i in range(BULK_SIZE):
        arr.add(i)
    elapsed = time.perf_counter() - start
    throughput_results.append({
        "name": f"Bulk instance calls ({BULK_SIZE})",
        "total_ms": elapsed * 1000,
        "ops_per_sec": BULK_SIZE / elapsed,
    })

    # Mixed workload
    # Each iteration = 5 bridge operations: create(1) + put(2) + get(1) + size(1)
    iterations = BULK_SIZE // 10
    ops_per_iteration = 5
    start = time.perf_counter()
    for i in range(iterations):
        hm = HashMap()
        hm.put("key1", i)
        hm.put("key2", f"value{i}")
        hm.get("key1")
        hm.size()
    elapsed = time.perf_counter() - start
    total_ops = iterations * ops_per_iteration
    throughput_results.append({
        "name": f"Mixed workload ({iterations} iter × {ops_per_iteration} ops)",
        "total_ms": elapsed * 1000,
        "iterations": iterations,
        "ops_per_iteration": ops_per_iteration,
        "iter_per_sec": iterations / elapsed,
        "ops_per_sec": total_ops / elapsed,
    })

    return throughput_results


def run_payload_sweep(gateway):
    """Run payload size sweep benchmarks."""
    StringBuilder = gateway.jvm.java.lang.StringBuilder
    ArrayList = gateway.jvm.java.util.ArrayList
    Arrays = gateway.jvm.java.util.Arrays

    string_sizes = [0, 8, 32, 128, 512, 2048, 8192]
    bytes_sizes = [0, 16, 64, 256, 1024, 4096, 16384, 32768]
    sweep_iterations = 500

    string_results = []
    bytes_results = []

    # String argument sweep
    for size in string_sizes:
        test_string = "x" * size
        def string_arg_test(s=test_string):
            StringBuilder(s)
        result = run_benchmark(f"str_arg_{size}B", string_arg_test, iterations=sweep_iterations)
        result["size_bytes"] = size
        string_results.append(result)

    # String return sweep
    for size in string_sizes:
        sb = StringBuilder("x" * size)
        def string_return_test(builder=sb):
            builder.toString()
        result = run_benchmark(f"str_ret_{size}B", string_return_test, iterations=sweep_iterations)
        result["size_bytes"] = size
        string_results.append(result)

    # Bytes send-only sweep (Python -> Java, returns boolean)
    for size in bytes_sizes:
        test_bytes = b"x" * size
        arr = ArrayList()
        def bytes_send_test(data=test_bytes, container=arr):
            container.add(data)
        result = run_benchmark(f"bytes_send_{size}B", bytes_send_test, iterations=sweep_iterations)
        result["size_bytes"] = size
        bytes_results.append(result)

    # Bytes round-trip sweep (Python -> Java -> Python echo)
    for size in bytes_sizes:
        test_bytes = b"x" * size
        arr_echo = ArrayList()
        arr_echo.add(test_bytes)  # Pre-populate
        def bytes_echo_test(container=arr_echo):
            container.get(0)  # Returns byte[] converted to Python bytes
        result = run_benchmark(f"bytes_echo_{size}B", bytes_echo_test, iterations=sweep_iterations)
        result["size_bytes"] = size
        bytes_results.append(result)

    return {"string_results": string_results, "bytes_results": bytes_results}


from py4j.java_gateway import JavaGateway, GatewayParameters

# Find free port
sock = socket.socket()
sock.bind(("", 0))
port = sock.getsockname()[1]
sock.close()

jar_path = find_py4j_jar()
if not jar_path:
    print(json.dumps({"error": "Could not find py4j JAR"}))
    sys.exit(1)

# Print env info for reproducibility
print("PY4J_JAR:" + jar_path)
print("PY4J_PORT:" + str(port))

java_home = os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@21")
java_cmd = os.path.join(java_home, "bin", "java")
proc = subprocess.Popen(
    [java_cmd, "-cp", jar_path, "py4j.GatewayServer", str(port)],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
)

# Wait for server to start with port polling (more reliable than sleep)
if not wait_for_port(port, timeout=10.0):
    print(json.dumps({"error": "Py4J server failed to start within 10s"}))
    proc.terminate()
    sys.exit(1)

try:
    # Measure baseline first
    baseline = measure_baseline()
    print("BASELINE:" + json.dumps(baseline))

    # Test with auto_convert=True (PySpark default)
    gateway_auto = JavaGateway(gateway_parameters=GatewayParameters(port=port, auto_convert=True))
    results_auto = run_latency_benchmarks(gateway_auto, auto_convert=True)
    print("LATENCY_RESULTS_AUTO_TRUE:" + json.dumps(results_auto))

    throughput_auto = run_throughput_benchmarks(gateway_auto)
    print("THROUGHPUT_RESULTS_AUTO_TRUE:" + json.dumps(throughput_auto))

    # Run payload sweep for side-by-side comparison
    sweep_results = run_payload_sweep(gateway_auto)
    print("PAYLOAD_SWEEP:" + json.dumps(sweep_results))

    gateway_auto.close()

    # Restart gateway for auto_convert=False test
    time.sleep(1)

    # Test with auto_convert=False (thin bridge baseline)
    gateway_no_auto = JavaGateway(gateway_parameters=GatewayParameters(port=port, auto_convert=False))
    results_no_auto = run_latency_benchmarks(gateway_no_auto, auto_convert=False)
    print("LATENCY_RESULTS_AUTO_FALSE:" + json.dumps(results_no_auto))

    throughput_no_auto = run_throughput_benchmarks(gateway_no_auto)
    print("THROUGHPUT_RESULTS_AUTO_FALSE:" + json.dumps(throughput_no_auto))
    gateway_no_auto.close()

finally:
    proc.terminate()
    proc.wait()
'''


def run_py4j_benchmarks() -> dict:
    """Run Py4J benchmarks in a subprocess.

    Returns a dict with keys:
        - baseline: Python overhead measurements
        - latency_auto_true: Results with auto_convert=True (PySpark default)
        - latency_auto_false: Results with auto_convert=False (thin bridge)
        - throughput_auto_true: Throughput with auto_convert=True
        - throughput_auto_false: Throughput with auto_convert=False
        - py4j_jar: Path to JAR used
        - py4j_port: Port used
    """
    cwd = os.path.dirname(os.path.dirname(__file__))
    print(f"  Subprocess cwd: {cwd}")

    result = subprocess.run(
        [sys.executable, "-c", PY4J_BENCHMARK_SCRIPT],
        capture_output=True,
        text=True,
        env={**os.environ},
        cwd=cwd,
    )

    if result.returncode != 0:
        print(f"  Py4J benchmark subprocess failed:")
        print(f"  stderr: {result.stderr[:500]}")
        return {}

    results = {}

    for line in result.stdout.split("\n"):
        if line.startswith("PY4J_JAR:"):
            results["py4j_jar"] = line[9:]
            print(f"  Py4J JAR: {results['py4j_jar']}")
        elif line.startswith("PY4J_PORT:"):
            results["py4j_port"] = line[10:]
        elif line.startswith("BASELINE:"):
            try:
                results["baseline"] = json.loads(line[9:])
            except json.JSONDecodeError:
                print("  Failed to parse baseline results")
        elif line.startswith("LATENCY_RESULTS_AUTO_TRUE:"):
            try:
                results["latency_auto_true"] = json.loads(line[26:])
            except json.JSONDecodeError:
                print("  Failed to parse latency results (auto_convert=True)")
        elif line.startswith("LATENCY_RESULTS_AUTO_FALSE:"):
            try:
                results["latency_auto_false"] = json.loads(line[27:])
            except json.JSONDecodeError:
                print("  Failed to parse latency results (auto_convert=False)")
        elif line.startswith("THROUGHPUT_RESULTS_AUTO_TRUE:"):
            try:
                results["throughput_auto_true"] = json.loads(line[29:])
            except json.JSONDecodeError:
                print("  Failed to parse throughput results (auto_convert=True)")
        elif line.startswith("THROUGHPUT_RESULTS_AUTO_FALSE:"):
            try:
                results["throughput_auto_false"] = json.loads(line[30:])
            except json.JSONDecodeError:
                print("  Failed to parse throughput results (auto_convert=False)")
        elif line.startswith("PAYLOAD_SWEEP:"):
            try:
                results["payload_sweep"] = json.loads(line[14:])
            except json.JSONDecodeError:
                print("  Failed to parse payload sweep results")

    return results


def print_baseline(gatun_baseline: dict, py4j_baseline: dict | None):
    """Print baseline measurements for calibration."""
    print(f"\n{'=' * 60}")
    print(" Baseline Measurements (Python Overhead)")
    print(f"{'=' * 60}")
    print(f"{'Measurement':<30} {'Gatun (μs)':>12} {'Py4J (μs)':>12}")
    print(f"{'-' * 60}")
    print(f"{'Lambda call (noop)':<30} {gatun_baseline['lambda_noop']:>12.2f}", end="")
    if py4j_baseline:
        print(f" {py4j_baseline['lambda_noop']:>12.2f}")
    else:
        print(" N/A")
    print(f"{'Function call (noop)':<30} {gatun_baseline['func_call']:>12.2f}", end="")
    if py4j_baseline:
        print(f" {py4j_baseline['func_call']:>12.2f}")
    else:
        print(" N/A")
    print()
    print("Note: These values represent Python overhead, not bridge cost.")
    print("Net bridge cost = measured latency - baseline overhead")
    print()


def main():
    # Print environment info first for reproducibility
    print_environment()

    print("=" * 80)
    print(" Comprehensive Gatun vs Py4J Performance Benchmarks")
    print("=" * 80)
    print(f"Warmup: {WARMUP_SECONDS}s (min {WARMUP_MIN_ITERATIONS} iterations)")
    print(f"Benchmark iterations: {BENCHMARK_ITERATIONS}")
    print(f"Bulk operation size: {BULK_SIZE}")
    print()
    print("Categories:")
    print("  0. Baseline (Python overhead)")
    print("  1. Object Construction")
    print("  2. Static Method Calls")
    print("  3. Instance Method Calls")
    print("  4. Field Access")
    print("  5. Data Transfer (Python -> Java)")
    print("  6. Data Transfer (Java -> Python)")
    print("  7. Exception Handling")
    print("  8. Type Checking")
    print()
    print("Fairness notes:")
    print("  - Py4J tested with both auto_convert=True (PySpark) and auto_convert=False")
    print("  - Baseline measurements included for computing net bridge cost")

    # Run Gatun benchmarks
    print("\n" + "-" * 80)
    print("[1/5] Running Gatun latency benchmarks...")
    gatun_latency, gatun_baseline = benchmark_gatun()

    print("[2/5] Running Gatun throughput benchmarks...")
    gatun_throughput = benchmark_gatun_throughput()

    # Run Py4J benchmarks
    print("[3/5] Running Py4J benchmarks (auto_convert=True)...")
    print("[4/5] Running Py4J benchmarks (auto_convert=False)...")
    py4j_results = run_py4j_benchmarks()

    # Extract Py4J results
    py4j_baseline = py4j_results.get("baseline")
    py4j_latency_auto = py4j_results.get("latency_auto_true")
    py4j_latency_no_auto = py4j_results.get("latency_auto_false")
    py4j_throughput_auto = py4j_results.get("throughput_auto_true")
    py4j_throughput_no_auto = py4j_results.get("throughput_auto_false")

    # Print baseline
    print_baseline(gatun_baseline, py4j_baseline)

    # Print results
    print_results(gatun_latency, "Gatun Latency Results")

    if py4j_latency_auto:
        print_results(py4j_latency_auto, "Py4J Latency Results (auto_convert=True, PySpark default)")
        print_comparison(gatun_latency, py4j_latency_auto, gatun_baseline, py4j_baseline)

    if py4j_latency_no_auto:
        print_results(py4j_latency_no_auto, "Py4J Latency Results (auto_convert=False, thin bridge)")
        print_comparison(gatun_latency, py4j_latency_no_auto, gatun_baseline, py4j_baseline)

    print_throughput_results(gatun_throughput, "Gatun Throughput Results")

    if py4j_throughput_auto:
        print_throughput_results(py4j_throughput_auto, "Py4J Throughput (auto_convert=True)")
        print_throughput_comparison(gatun_throughput, py4j_throughput_auto)

    # Summary
    print("=" * 80)
    print(" Summary")
    print("=" * 80)

    avg_gatun = statistics.mean(r["mean_us"] for r in gatun_latency)
    print(f"Gatun average latency: {avg_gatun:.1f} μs per operation")

    if py4j_latency_auto:
        avg_py4j_auto = statistics.mean(r["mean_us"] for r in py4j_latency_auto)
        print(f"Py4J (auto_convert=True) average latency:  {avg_py4j_auto:.1f} μs per operation")

        speedups = [
            p["mean_us"] / g["mean_us"]
            for g, p in zip(gatun_latency, py4j_latency_auto)
            if g["mean_us"] > 0
        ]
        geo_mean = statistics.geometric_mean(speedups)
        print(f"\nGatun vs Py4J (auto_convert=True): {geo_mean:.2f}x faster (geometric mean)")

    if py4j_latency_no_auto:
        import math
        valid_results = [r["mean_us"] for r in py4j_latency_no_auto if not math.isnan(r["mean_us"])]
        if valid_results:
            avg_py4j_no_auto = statistics.mean(valid_results)
            print(f"Py4J (auto_convert=False) average latency: {avg_py4j_no_auto:.1f} μs per operation")

            speedups = [
                p["mean_us"] / g["mean_us"]
                for g, p in zip(gatun_latency, py4j_latency_no_auto)
                if g["mean_us"] > 0 and not math.isnan(p["mean_us"])
            ]
            if speedups:
                geo_mean = statistics.geometric_mean(speedups)
                print(f"Gatun vs Py4J (auto_convert=False): {geo_mean:.2f}x faster (geometric mean)")

    if py4j_throughput_auto and gatun_throughput:
        gatun_ops = sum(r["ops_per_sec"] for r in gatun_throughput)
        py4j_ops = sum(r["ops_per_sec"] for r in py4j_throughput_auto)
        print(f"\nTotal throughput: Gatun {gatun_ops:,.0f} ops/s vs Py4J {py4j_ops:,.0f} ops/s")

    if py4j_throughput_no_auto and gatun_throughput:
        gatun_ops = sum(r["ops_per_sec"] for r in gatun_throughput)
        py4j_ops = sum(r["ops_per_sec"] for r in py4j_throughput_no_auto)
        print(f"Total throughput (no auto): Gatun {gatun_ops:,.0f} ops/s vs Py4J {py4j_ops:,.0f} ops/s")

    # Run payload sweep to identify crossover points
    print("\n" + "-" * 80)
    print("[5/5] Running payload size sweep...")
    string_sweep, bytes_sweep = benchmark_payload_sweep()
    py4j_sweep = py4j_results.get("payload_sweep")
    print_sweep_results(string_sweep, bytes_sweep, py4j_sweep)

    print()


if __name__ == "__main__":
    main()
