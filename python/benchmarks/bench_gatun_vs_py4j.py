#!/usr/bin/env python3
"""Benchmark comparing Gatun vs Py4J performance.

This benchmark measures:
1. Object creation latency
2. Method invocation latency
3. Data transfer throughput (lists, strings, arrays)
4. Round-trip latency for various operations

Usage:
    # Run with both Gatun and Py4J
    python benchmarks/bench_gatun_vs_py4j.py

    # Run Gatun only (if Py4J not installed)
    python benchmarks/bench_gatun_vs_py4j.py --gatun-only

Requirements:
    - Gatun: pip install gatun
    - Py4J: pip install py4j (optional)
"""

import argparse
import gc
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass

# Try to import both libraries
HAVE_GATUN = False
HAVE_PY4J = False

try:
    import gatun
    HAVE_GATUN = True
except ImportError:
    pass

try:
    from py4j.java_gateway import JavaGateway, GatewayParameters
    HAVE_PY4J = True
except ImportError:
    pass


@dataclass
class BenchmarkResult:
    """Result of a single benchmark."""
    name: str
    iterations: int
    total_time_ms: float
    mean_us: float
    median_us: float
    stddev_us: float
    min_us: float
    max_us: float
    p50_us: float
    p95_us: float
    p99_us: float
    ops_per_sec: float

    def __str__(self):
        return (
            f"{self.name}: {self.mean_us:.2f} µs/op "
            f"(p50={self.p50_us:.2f}, p95={self.p95_us:.2f}, p99={self.p99_us:.2f}) "
            f"[{self.ops_per_sec:,.0f} ops/sec]"
        )


def find_java() -> str | None:
    """Find Java executable path portably.

    Search order:
    1. JAVA_HOME environment variable (if set)
    2. 'java' on PATH via shutil.which()

    Returns:
        Path to java executable, or None if not found.
    """
    # Check JAVA_HOME first
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_path = os.path.join(java_home, "bin", "java")
        if os.path.isfile(java_path):
            return java_path

    # Fall back to PATH lookup
    java_path = shutil.which("java")
    if java_path:
        return java_path

    return None


def percentile(sorted_data: list[float], p: float) -> float:
    """Calculate percentile from sorted data."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def get_system_info() -> dict[str, str]:
    """Collect system information for benchmark reproducibility."""
    info = {
        "python_version": platform.python_version(),
        "python_impl": platform.python_implementation(),
        "os": platform.system(),
        "os_version": platform.release(),
        "machine": platform.machine(),
        "cpu": platform.processor() or "unknown",
    }

    # Get Java version
    java_path = find_java()
    if java_path:
        try:
            result = subprocess.run(
                [java_path, "-version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            # Java prints version to stderr
            version_output = result.stderr.strip().split("\n")[0]
            info["java_version"] = version_output
        except Exception:
            info["java_version"] = "unknown"
    else:
        info["java_version"] = "not found"

    # Get Gatun version/commit
    if HAVE_GATUN:
        try:
            info["gatun_version"] = getattr(gatun, "__version__", "dev")
        except Exception:
            info["gatun_version"] = "unknown"

        # Try to get git commit
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            )
            if result.returncode == 0:
                info["gatun_commit"] = result.stdout.strip()
        except Exception:
            pass

    # Get Py4J version
    if HAVE_PY4J:
        try:
            import py4j
            info["py4j_version"] = getattr(py4j, "__version__", "unknown")
        except Exception:
            info["py4j_version"] = "unknown"

    return info


def print_system_info(info: dict[str, str]):
    """Print system information header."""
    print("=" * 100)
    print("SYSTEM INFORMATION")
    print("=" * 100)
    print(f"  Python:    {info.get('python_version', '?')} ({info.get('python_impl', '?')})")
    print(f"  Java:      {info.get('java_version', '?')}")
    print(f"  OS:        {info.get('os', '?')} {info.get('os_version', '?')} ({info.get('machine', '?')})")
    print(f"  CPU:       {info.get('cpu', '?')}")
    if "gatun_version" in info:
        version_str = info["gatun_version"]
        if "gatun_commit" in info:
            version_str += f" (commit: {info['gatun_commit']})"
        print(f"  Gatun:     {version_str}")
    if "py4j_version" in info:
        print(f"  Py4J:      {info['py4j_version']}")
    print()
    print("  Note: For stable results, disable CPU frequency scaling (use 'performance' governor)")
    print("        and close other applications. Use --repeat N for statistical confidence.")
    print()


def run_benchmark(
    name: str, func, iterations: int = 1000, warmup: int = 100, batch: int = 100,
    tail_sample_rate: int = 10,
) -> BenchmarkResult:
    """Run a benchmark and collect statistics.

    Uses batched timing for mean/throughput (reduces timer noise) plus
    individual operation sampling for accurate tail latencies (p95/p99).

    Args:
        name: Benchmark name
        func: Function to benchmark
        iterations: Total number of operations to perform
        warmup: Number of warmup iterations
        batch: Number of operations per timed batch. Use 100 for fast ops (~10-100µs)
               to reduce timer overhead noise. Use 1 for slow ops or when you need
               precise per-op percentiles.
        tail_sample_rate: Sample 1 in N operations individually for tail latencies.
                         With batch=100 and tail_sample_rate=10, samples 10 ops per batch.
    """
    # Warmup
    for _ in range(warmup):
        func()

    # Force GC before measurement, then disable
    gc.collect()
    gc_was_enabled = gc.isenabled()
    gc.disable()

    try:
        # If batch == 1, just time everything individually (no batching)
        if batch == 1:
            individual_times = []
            for _ in range(iterations):
                start = time.perf_counter_ns()
                func()
                end = time.perf_counter_ns()
                individual_times.append((end - start) / 1000)  # microseconds
            batch_times = individual_times  # Same data for both
        else:
            # Batched timing for mean/throughput, plus sampled individual times for tails
            batch_times = []  # Per-op averages from batches (for mean)
            individual_times = []  # True per-op times (for tail latencies)
            num_batches = iterations // batch
            sample_counter = 0

            for _ in range(num_batches):
                start = time.perf_counter_ns()
                for _ in range(batch):
                    # Sample individual operations for tail latency
                    if sample_counter % tail_sample_rate == 0:
                        op_start = time.perf_counter_ns()
                        func()
                        op_end = time.perf_counter_ns()
                        individual_times.append((op_end - op_start) / 1000)
                    else:
                        func()
                    sample_counter += 1
                end = time.perf_counter_ns()
                # Record per-op average from batch (for mean calculation)
                batch_times.append((end - start) / 1000 / batch)  # µs per op
    finally:
        # Re-enable GC if it was enabled
        if gc_was_enabled:
            gc.enable()

    # Calculate statistics
    # Use batch averages for mean/throughput (lower noise)
    actual_iterations = len(batch_times) * batch if batch > 1 else len(batch_times)
    total_ms = sum(batch_times) * (batch if batch > 1 else 1) / 1000
    mean = statistics.mean(batch_times)
    median = statistics.median(batch_times)
    stddev = statistics.stdev(batch_times) if len(batch_times) > 1 else 0
    ops_per_sec = 1_000_000 / mean if mean > 0 else 0

    # Use individual times for true tail latencies (captures outliers)
    sorted_individual = sorted(individual_times)
    min_time = min(individual_times)
    max_time = max(individual_times)
    p50 = percentile(sorted_individual, 50)
    p95 = percentile(sorted_individual, 95)
    p99 = percentile(sorted_individual, 99)

    return BenchmarkResult(
        name=name,
        iterations=actual_iterations,
        total_time_ms=total_ms,
        mean_us=mean,
        median_us=median,
        stddev_us=stddev,
        min_us=min_time,
        max_us=max_time,
        p50_us=p50,
        p95_us=p95,
        p99_us=p99,
        ops_per_sec=ops_per_sec,
    )


class GatunBenchmark:
    """Benchmarks for Gatun."""

    def __init__(self):
        self.client = None
        self.socket_path = f"/tmp/gatun_bench_{os.getpid()}.sock"

    def setup(self):
        """Start Gatun server and connect."""
        # Clean up any stale files
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        if os.path.exists(self.socket_path + ".shm"):
            os.unlink(self.socket_path + ".shm")

        # gatun.connect() launches server and returns connected client
        self.client = gatun.connect(socket_path=self.socket_path)

    def teardown(self):
        """Stop Gatun server."""
        if self.client:
            # Client has _server_session attached that stops the server
            if hasattr(self.client, "_server_session"):
                self.client._server_session.stop()
            self.client.close()
        # Clean up socket files
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        if os.path.exists(self.socket_path + ".shm"):
            os.unlink(self.socket_path + ".shm")

    def warmup_jvm(self, iterations=5000):
        """Warm up the JVM to trigger JIT compilation.

        This runs each operation type many times to ensure the JVM has
        compiled hot paths before we start measuring.
        """
        print("  Warming up JVM (5000 iterations per operation)...")

        # Warm up object creation
        for _ in range(iterations):
            arr = self.client.create_object("java.util.ArrayList")
            arr.add("item")
            arr.size()

        # Warm up static methods
        for _ in range(iterations):
            self.client.invoke_static_method("java.lang.Math", "max", 10, 20)

        # Warm up RPC overhead (Integer.valueOf uses cached values)
        for _ in range(iterations):
            self.client.invoke_static_method("java.lang.Integer", "valueOf", 1)

        # Warm up string operations
        sb = self.client.create_object("java.lang.StringBuilder")
        for _ in range(iterations):
            sb.append("x")
            sb.setLength(0)

        # Warm up JVM view
        for _ in range(iterations):
            self.client.jvm.java.util.ArrayList()

        # Force GC after warmup
        gc.collect()
        print("  JVM warmup complete.")

    def bench_create_object(self, iterations=1000) -> BenchmarkResult:
        """Benchmark object creation.

        Objects are kept in a list to prevent GC/FreeObject calls during measurement.
        Both Gatun and Py4J benchmarks append to a list (minimal, equal overhead).
        """
        created_objects = []

        def create():
            obj = self.client.create_object("java.util.ArrayList")
            created_objects.append(obj)
            return obj

        result = run_benchmark("gatun_create_object", create, iterations)

        # Detach and cleanup after benchmark (outside timing)
        for obj in created_objects:
            try:
                obj.detach()
                self.client.free_object(obj.object_id)
            except Exception:
                pass
        created_objects.clear()

        return result

    def bench_method_call_void(self, iterations=1000) -> BenchmarkResult:
        """Benchmark void method call (StringBuilder.setLength)."""
        sb = self.client.create_object("java.lang.StringBuilder", "hello")

        def call():
            sb.setLength(0)

        return run_benchmark("gatun_method_void", call, iterations)

    def bench_method_call_return_int(self, iterations=1000) -> BenchmarkResult:
        """Benchmark method call returning int (ArrayList.size)."""
        arr = self.client.create_object("java.util.ArrayList")
        arr.add("item")

        def call():
            return arr.size()

        return run_benchmark("gatun_method_return_int", call, iterations)

    def bench_method_call_return_string(self, iterations=1000) -> BenchmarkResult:
        """Benchmark method call returning string (StringBuilder.toString)."""
        sb = self.client.create_object("java.lang.StringBuilder", "hello world")

        def call():
            return sb.toString()

        return run_benchmark("gatun_method_return_string", call, iterations)

    def bench_static_method(self, iterations=1000) -> BenchmarkResult:
        """Benchmark static method call (Math.max)."""
        def call():
            return self.client.invoke_static_method("java.lang.Math", "max", 10, 20)

        return run_benchmark("gatun_static_method", call, iterations)

    def bench_rpc_overhead(self, iterations=1000) -> BenchmarkResult:
        """Benchmark pure RPC overhead with minimal Java work.

        Uses Integer.valueOf(1) which returns a cached Integer object,
        making Java-side work essentially zero. This measures the pure
        bridge overhead: serialization, IPC, deserialization.
        """
        def call():
            return self.client.invoke_static_method("java.lang.Integer", "valueOf", 1)

        return run_benchmark("gatun_rpc_overhead", call, iterations)

    def bench_many_small_calls(self, iterations=100) -> BenchmarkResult:
        """Benchmark many small method calls (10 adds per iteration).

        This tests control-plane overhead with many round-trips.
        Both Gatun and Py4J do the same per-item loop.
        """
        small_list = list(range(10))

        def call():
            arr = self.client.create_object("java.util.ArrayList")
            for item in small_list:
                arr.add(item)
            return arr.size()

        # Use batch=10 for slower ops (each iteration is ~700µs)
        return run_benchmark("gatun_many_small_calls_10", call, iterations, batch=10)

    def bench_bulk_transfer(self, iterations=1000) -> BenchmarkResult:
        """Benchmark bulk argument transfer (single call with list payload).

        Passes a Python list to ArrayList.addAll() via Collections.nCopies().
        This tests serialization efficiency - the list is passed as one argument.

        Note: Gatun's FlatBuffers packs lists efficiently,
        while Py4J serializes elements over TCP.
        """
        # Create a list with 100 items to transfer
        items = list(range(100))

        def call():
            # Create ArrayList and add all items via addAll (single call with list arg)
            arr = self.client.create_object("java.util.ArrayList")
            arr.addAll(items)
            return arr.size()

        # Use batch=10 for slower ops (each iteration is ~800µs)
        return run_benchmark("gatun_bulk_transfer_100", call, iterations, batch=10)

    def bench_transfer_string(self, iterations=1000) -> BenchmarkResult:
        """Benchmark transferring string (1KB)."""
        sb = self.client.create_object("java.lang.StringBuilder")
        test_string = "x" * 1024

        def call():
            sb.setLength(0)
            sb.append(test_string)

        return run_benchmark("gatun_transfer_string_1kb", call, iterations)

    def bench_jvm_view_navigation(self, iterations=1000) -> BenchmarkResult:
        """Benchmark JVM view object creation."""
        def call():
            return self.client.jvm.java.util.ArrayList()

        return run_benchmark("gatun_jvm_view_create", call, iterations)

    def bench_roundtrip_chain(self, iterations=500) -> BenchmarkResult:
        """Benchmark chain of operations (create, add 3 items, get size)."""
        def call():
            arr = self.client.create_object("java.util.ArrayList")
            arr.add("one")
            arr.add("two")
            arr.add("three")
            return arr.size()

        # Use batch=10 for slower ops (each iteration is ~280µs)
        return run_benchmark("gatun_roundtrip_chain", call, iterations, batch=10)

    def bench_arrow_transfer(self, size_mb: int, iterations: int = 100) -> BenchmarkResult:
        """Benchmark Arrow table transfer via shared memory.

        This measures Gatun's bulk data transfer using Arrow IPC format.

        What happens:
        1. Python serializes Arrow table to IPC format directly into shared memory
           (using pa.memory_map for direct writes, no intermediate Python buffer)
        2. Python signals Java via socket (tiny message)
        3. Java reads Arrow IPC from shared memory (mmap, no copy)
        4. Java parses IPC format and returns row count
        5. Python receives acknowledgement

        Important caveats:
        - Arrow IPC serialization still occurs (table → IPC bytes)
        - This is NOT true zero-copy (which would share raw Arrow buffer pointers)
        - But IPC bytes go directly to shm, and Java reads via mmap (no copies)
        - The "handoff rate" metric shows effective throughput, not memory bandwidth

        Args:
            size_mb: Target size in MB for the Arrow table
            iterations: Number of iterations (fewer for large sizes)
        """
        import numpy as np
        import pyarrow as pa

        # Create a table of approximately the target size
        # Each row: 1 int64 (8 bytes) + 1 float64 (8 bytes) = 16 bytes
        # Plus Arrow overhead (~10-20%), so estimate 20 bytes per row
        rows = (size_mb * 1024 * 1024) // 20

        # Pre-create the table (not part of timing)
        data = {
            "id": np.arange(rows, dtype=np.int64),
            "value": np.random.random(rows).astype(np.float64),
        }
        table = pa.table(data)
        actual_size_mb = table.nbytes / (1024 * 1024)

        def transfer():
            return self.client.send_arrow_table(table)

        result = run_benchmark(
            f"gatun_arrow_{size_mb}mb",
            transfer,
            iterations,
            warmup=min(10, iterations // 2),
            batch=1,  # Don't batch large transfers
        )

        # Calculate effective handoff rate
        # actual_size_mb / (p50_us / 1e6) = MB/s, then / 1024 = GB/s
        handoff_rate_gbs = (actual_size_mb / (result.p50_us / 1_000_000)) / 1024
        result.name = f"gatun_arrow_{size_mb}mb ({actual_size_mb:.1f}MB @ {handoff_rate_gbs:.2f} GB/s)"

        return result


class Py4JBenchmark:
    """Benchmarks for Py4J."""

    def __init__(self):
        self.gateway = None

    def setup(self):
        """Start Py4J gateway using the official launch_gateway() helper."""
        from py4j.java_gateway import launch_gateway

        # Use Py4J's official launch_gateway() which handles:
        # - Finding the jar
        # - Starting the JVM with correct classpath
        # - Capturing the port from stdout
        # - Setting up proper gateway parameters
        java_path = find_java()
        if not java_path:
            raise RuntimeError(
                "Java not found. Set JAVA_HOME or ensure 'java' is on PATH."
            )

        port = launch_gateway(java_path=java_path, die_on_exit=True)

        # Connect with auto_convert for fair comparison (Gatun also auto-converts)
        self.gateway = JavaGateway(
            gateway_parameters=GatewayParameters(port=port, auto_convert=True)
        )

    def teardown(self):
        """Stop Py4J gateway."""
        if self.gateway:
            self.gateway.shutdown()

    def warmup_jvm(self, iterations=5000):
        """Warm up the JVM to trigger JIT compilation.

        This runs each operation type many times to ensure the JVM has
        compiled hot paths before we start measuring.
        """
        print("  Warming up JVM (5000 iterations per operation)...")

        # Warm up object creation
        for _ in range(iterations):
            arr = self.gateway.jvm.java.util.ArrayList()
            arr.add("item")
            arr.size()

        # Warm up static methods
        Math = self.gateway.jvm.java.lang.Math
        for _ in range(iterations):
            Math.max(10, 20)

        # Warm up RPC overhead (Integer.valueOf uses cached values)
        Integer = self.gateway.jvm.java.lang.Integer
        for _ in range(iterations):
            Integer.valueOf(1)

        # Warm up string operations
        sb = self.gateway.jvm.java.lang.StringBuilder()
        for _ in range(iterations):
            sb.append("x")
            sb.setLength(0)

        # Force GC after warmup
        gc.collect()
        print("  JVM warmup complete.")

    def bench_create_object(self, iterations=1000) -> BenchmarkResult:
        """Benchmark object creation.

        Objects are kept in a list to prevent GC during measurement.
        """
        created_objects = []

        def create():
            obj = self.gateway.jvm.java.util.ArrayList()
            created_objects.append(obj)
            return obj

        result = run_benchmark("py4j_create_object", create, iterations)

        # Clear references after benchmark (Py4J will GC them)
        created_objects.clear()

        return result

    def bench_method_call_void(self, iterations=1000) -> BenchmarkResult:
        """Benchmark void method call (StringBuilder.setLength)."""
        sb = self.gateway.jvm.java.lang.StringBuilder("hello")

        def call():
            sb.setLength(0)

        return run_benchmark("py4j_method_void", call, iterations)

    def bench_method_call_return_int(self, iterations=1000) -> BenchmarkResult:
        """Benchmark method call returning int (ArrayList.size)."""
        arr = self.gateway.jvm.java.util.ArrayList()
        arr.add("item")

        def call():
            return arr.size()

        return run_benchmark("py4j_method_return_int", call, iterations)

    def bench_method_call_return_string(self, iterations=1000) -> BenchmarkResult:
        """Benchmark method call returning string (StringBuilder.toString)."""
        sb = self.gateway.jvm.java.lang.StringBuilder("hello world")

        def call():
            return sb.toString()

        return run_benchmark("py4j_method_return_string", call, iterations)

    def bench_static_method(self, iterations=1000) -> BenchmarkResult:
        """Benchmark static method call (Math.max)."""
        Math = self.gateway.jvm.java.lang.Math

        def call():
            return Math.max(10, 20)

        return run_benchmark("py4j_static_method", call, iterations)

    def bench_rpc_overhead(self, iterations=1000) -> BenchmarkResult:
        """Benchmark pure RPC overhead with minimal Java work.

        Uses Integer.valueOf(1) which returns a cached Integer object,
        making Java-side work essentially zero. This measures the pure
        bridge overhead: serialization, IPC, deserialization.
        """
        Integer = self.gateway.jvm.java.lang.Integer

        def call():
            return Integer.valueOf(1)

        return run_benchmark("py4j_rpc_overhead", call, iterations)

    def bench_many_small_calls(self, iterations=100) -> BenchmarkResult:
        """Benchmark many small method calls (10 adds per iteration).

        This tests control-plane overhead with many round-trips.
        Both Gatun and Py4J do the same per-item loop.
        """
        small_list = list(range(10))

        def call():
            arr = self.gateway.jvm.java.util.ArrayList()
            for item in small_list:
                arr.add(item)
            return arr.size()

        # Use batch=10 for slower ops
        return run_benchmark("py4j_many_small_calls_10", call, iterations, batch=10)

    def bench_bulk_transfer(self, iterations=1000) -> BenchmarkResult:
        """Benchmark bulk argument transfer (single call with list payload).

        Passes a Python list to ArrayList.addAll().
        This tests serialization efficiency - the list is passed as one argument.

        Note: Py4J serializes elements over TCP,
        while Gatun's FlatBuffers packs lists efficiently.
        """
        # Create a list with 100 items to transfer
        items = list(range(100))

        def call():
            # Create ArrayList and add all items via addAll (single call with list arg)
            arr = self.gateway.jvm.java.util.ArrayList()
            arr.addAll(items)
            return arr.size()

        # Use batch=10 for slower ops
        return run_benchmark("py4j_bulk_transfer_100", call, iterations, batch=10)

    def bench_transfer_string(self, iterations=1000) -> BenchmarkResult:
        """Benchmark transferring string (1KB)."""
        sb = self.gateway.jvm.java.lang.StringBuilder()
        test_string = "x" * 1024

        def call():
            sb.setLength(0)
            sb.append(test_string)

        return run_benchmark("py4j_transfer_string_1kb", call, iterations)

    def bench_jvm_view_navigation(self, iterations=1000) -> BenchmarkResult:
        """Benchmark JVM view object creation."""
        def call():
            return self.gateway.jvm.java.util.ArrayList()

        return run_benchmark("py4j_jvm_view_create", call, iterations)

    def bench_roundtrip_chain(self, iterations=500) -> BenchmarkResult:
        """Benchmark chain of operations (create, add 3 items, get size)."""
        def call():
            arr = self.gateway.jvm.java.util.ArrayList()
            arr.add("one")
            arr.add("two")
            arr.add("three")
            return arr.size()

        # Use batch=10 for slower ops
        return run_benchmark("py4j_roundtrip_chain", call, iterations, batch=10)


def print_comparison(gatun_results: list[BenchmarkResult], py4j_results: list[BenchmarkResult]):
    """Print side-by-side comparison."""
    print("\n" + "=" * 100)
    print("BENCHMARK COMPARISON: Gatun vs Py4J")
    print("=" * 100)

    # Create lookup for py4j results
    py4j_lookup = {}
    for r in py4j_results:
        # Extract benchmark name without prefix
        name = r.name.replace("py4j_", "")
        py4j_lookup[name] = r

    # Summary table
    print(f"\n{'Benchmark':<25} {'Gatun p50':<12} {'Py4J p50':<12} {'Speedup':<10}")
    print("-" * 60)

    speedups = []
    for gr in gatun_results:
        name = gr.name.replace("gatun_", "")
        py4j_r = py4j_lookup.get(name)

        if py4j_r:
            speedup = py4j_r.p50_us / gr.p50_us
            speedups.append(speedup)
            speedup_str = f"{speedup:.1f}x"
            if speedup > 1:
                speedup_str = f"\033[92m{speedup_str}\033[0m"  # Green
            else:
                speedup_str = f"\033[91m{speedup_str}\033[0m"  # Red
            print(f"{name:<25} {gr.p50_us:<12.1f} {py4j_r.p50_us:<12.1f} {speedup_str}")
        else:
            print(f"{name:<25} {gr.p50_us:<12.1f} {'N/A':<12} {'N/A'}")

    if speedups:
        avg_speedup = statistics.mean(speedups)
        print("-" * 60)
        print(f"{'Average Speedup (p50)':<25} {'':<12} {'':<12} \033[1m{avg_speedup:.1f}x\033[0m")

    # Detailed table with percentiles
    print("\n" + "=" * 100)
    print("LATENCY PERCENTILES (µs)")
    print("=" * 100)

    print(f"\n{'Benchmark':<25} {'Library':<8} {'p50':<10} {'p95':<10} {'p99':<10} {'max':<10}")
    print("-" * 75)

    for gr in gatun_results:
        name = gr.name.replace("gatun_", "")
        py4j_r = py4j_lookup.get(name)

        print(f"{name:<25} {'Gatun':<8} {gr.p50_us:<10.1f} {gr.p95_us:<10.1f} {gr.p99_us:<10.1f} {gr.max_us:<10.1f}")
        if py4j_r:
            print(f"{'':<25} {'Py4J':<8} {py4j_r.p50_us:<10.1f} {py4j_r.p95_us:<10.1f} {py4j_r.p99_us:<10.1f} {py4j_r.max_us:<10.1f}")

    # Throughput summary
    print("\n" + "=" * 100)
    print("THROUGHPUT (ops/sec)")
    print("=" * 100)

    print(f"\n{'Benchmark':<25} {'Gatun':<15} {'Py4J':<15}")
    print("-" * 55)

    for gr in gatun_results:
        name = gr.name.replace("gatun_", "")
        py4j_r = py4j_lookup.get(name)

        if py4j_r:
            print(f"{name:<25} {gr.ops_per_sec:<15,.0f} {py4j_r.ops_per_sec:<15,.0f}")
        else:
            print(f"{name:<25} {gr.ops_per_sec:<15,.0f} {'N/A':<15}")


def print_arrow_results(results: list[BenchmarkResult]):
    """Print Arrow benchmark results with throughput focus."""
    print("\n" + "=" * 100)
    print("ARROW BULK DATA TRANSFER (Gatun Only)")
    print("=" * 100)
    print("\nThis benchmark measures Arrow IPC transfer latency via shared memory.")
    print("Data flow: Python serializes Arrow IPC directly to mmap → Java reads via mmap")
    print("Note: IPC serialization still occurs; this is not true zero-copy buffer sharing.")
    print("'Handoff rate' = size / latency (effective throughput, not memory bandwidth).\n")

    print(f"{'Size':<20} {'Latency (p50)':<15} {'Handoff Rate':<15} {'p95':<12} {'p99':<12}")
    print("-" * 75)

    for r in results:
        # Extract size from name like "gatun_arrow_8mb (8.5MB @ 2.1 GB/s)"
        name_parts = r.name.split(" ")
        size_part = name_parts[0].replace("gatun_arrow_", "").upper()

        # Calculate effective handoff rate from actual data
        # Name format: "gatun_arrow_Xmb (Y.YMB @ Z.ZZ GB/s)"
        if "(" in r.name and "@" in r.name:
            info = r.name.split("(")[1].rstrip(")")
            actual_mb = float(info.split("MB")[0])
            # MB / (µs / 1e6) = MB/s, then / 1024 = GB/s
            handoff_rate_gbs = (actual_mb / (r.p50_us / 1_000_000)) / 1024
            rate_str = f"{handoff_rate_gbs:.2f} GB/s"
        else:
            rate_str = "N/A"

        print(f"{size_part:<20} {r.p50_us:>10.0f} µs   {rate_str:<15} {r.p95_us:>8.0f} µs   {r.p99_us:>8.0f} µs")


def run_gatun_benchmarks() -> list[BenchmarkResult]:
    """Run all Gatun benchmarks."""
    bench = GatunBenchmark()
    results = []

    try:
        print("Setting up Gatun...")
        bench.setup()
        bench.warmup_jvm()
        print("Running Gatun benchmarks...")

        results.append(bench.bench_create_object())
        results.append(bench.bench_method_call_void())
        results.append(bench.bench_method_call_return_int())
        results.append(bench.bench_method_call_return_string())
        results.append(bench.bench_static_method())
        results.append(bench.bench_rpc_overhead())
        results.append(bench.bench_many_small_calls())
        results.append(bench.bench_bulk_transfer())
        results.append(bench.bench_transfer_string())
        results.append(bench.bench_jvm_view_navigation())
        results.append(bench.bench_roundtrip_chain())

    finally:
        bench.teardown()

    return results


def run_arrow_benchmarks(memory_mb: int = 128) -> list[BenchmarkResult]:
    """Run Arrow bulk transfer benchmarks (Gatun only).

    Args:
        memory_mb: Shared memory size in MB. Determines max transfer size.

    Returns:
        List of benchmark results for different transfer sizes.
    """
    import numpy as np  # noqa: F401 - verify numpy is available

    socket_path = f"/tmp/gatun_arrow_bench_{os.getpid()}.sock"
    results = []

    try:
        # Clean up any stale files
        if os.path.exists(socket_path):
            os.unlink(socket_path)
        if os.path.exists(socket_path + ".shm"):
            os.unlink(socket_path + ".shm")

        # Launch with specified memory size
        print(f"Setting up Gatun with {memory_mb}MB shared memory for Arrow benchmarks...")
        client = gatun.connect(memory=f"{memory_mb}MB", socket_path=socket_path)

        # Warmup
        print("  Warming up...")
        bench = GatunBenchmark()
        bench.client = client
        bench.socket_path = socket_path

        # Calculate max payload size (memory - command zone - response zone)
        # Command zone: 4KB, Response zone: 4KB, so payload ~ memory - 8KB
        max_payload_mb = memory_mb - 1  # Leave some margin

        # Test sizes: 1MB, 8MB, then scale up based on available memory
        sizes = [1, 8]
        if max_payload_mb >= 32:
            sizes.append(32)
        if max_payload_mb >= 64:
            sizes.append(64)
        if max_payload_mb >= 100:
            sizes.append(100)

        print(f"Running Arrow benchmarks for sizes: {sizes} MB...")
        for size_mb in sizes:
            if size_mb > max_payload_mb:
                print(f"  Skipping {size_mb}MB (exceeds {max_payload_mb}MB payload limit)")
                continue

            print(f"  Testing {size_mb}MB transfer...")
            # Fewer iterations for larger sizes
            iterations = max(10, 100 // size_mb)
            try:
                result = bench.bench_arrow_transfer(size_mb, iterations=iterations)
                results.append(result)
            except Exception as e:
                print(f"  Error with {size_mb}MB: {e}")

    finally:
        # Cleanup
        if "client" in dir():
            if hasattr(client, "_server_session"):
                client._server_session.stop()
            client.close()
        if os.path.exists(socket_path):
            os.unlink(socket_path)
        if os.path.exists(socket_path + ".shm"):
            os.unlink(socket_path + ".shm")

    return results


def run_py4j_benchmarks() -> list[BenchmarkResult]:
    """Run all Py4J benchmarks."""
    bench = Py4JBenchmark()
    results = []

    try:
        print("Setting up Py4J...")
        bench.setup()
        bench.warmup_jvm()
        print("Running Py4J benchmarks...")

        results.append(bench.bench_create_object())
        results.append(bench.bench_method_call_void())
        results.append(bench.bench_method_call_return_int())
        results.append(bench.bench_method_call_return_string())
        results.append(bench.bench_static_method())
        results.append(bench.bench_rpc_overhead())
        results.append(bench.bench_many_small_calls())
        results.append(bench.bench_bulk_transfer())
        results.append(bench.bench_transfer_string())
        results.append(bench.bench_jvm_view_navigation())
        results.append(bench.bench_roundtrip_chain())

    finally:
        bench.teardown()

    return results


def merge_results(
    all_runs: list[list[BenchmarkResult]], mode: str = "median"
) -> list[BenchmarkResult]:
    """Merge multiple benchmark runs.

    Args:
        all_runs: List of benchmark run results
        mode: Aggregation mode:
            - "median": Take the run with median p50 (default, statistically sound)
            - "best": Take the run with best (minimum) p50 (cherry-picking)

    Returns:
        List of merged BenchmarkResult objects
    """
    if not all_runs:
        return []
    if len(all_runs) == 1:
        return all_runs[0]

    # Group by benchmark name
    by_name: dict[str, list[BenchmarkResult]] = {}
    for run in all_runs:
        for r in run:
            if r.name not in by_name:
                by_name[r.name] = []
            by_name[r.name].append(r)

    merged = []
    for name in by_name:
        results = by_name[name]
        if mode == "best":
            # Cherry-pick best result (not recommended for publication)
            selected = min(results, key=lambda r: r.p50_us)
        else:
            # Median: sort by p50 and take middle element
            sorted_results = sorted(results, key=lambda r: r.p50_us)
            mid = len(sorted_results) // 2
            selected = sorted_results[mid]
        merged.append(selected)

    return merged


def main():
    parser = argparse.ArgumentParser(description="Benchmark Gatun vs Py4J")
    parser.add_argument("--gatun-only", action="store_true", help="Only run Gatun benchmarks")
    parser.add_argument("--py4j-only", action="store_true", help="Only run Py4J benchmarks")
    parser.add_argument("--arrow", action="store_true",
                        help="Run Arrow bulk transfer benchmarks (Gatun only)")
    parser.add_argument("--arrow-memory", type=int, default=128, metavar="MB",
                        help="Shared memory size for Arrow benchmarks (default: 128MB)")
    parser.add_argument("--repeat", type=int, default=1, metavar="N",
                        help="Run benchmarks N times and report median results")
    parser.add_argument("--best", action="store_true",
                        help="Report best (minimum) p50 instead of median (cherry-picking mode)")
    args = parser.parse_args()

    if not HAVE_GATUN and not args.py4j_only:
        print("Error: gatun not installed. Install with: pip install gatun")
        sys.exit(1)

    if not HAVE_PY4J and not args.gatun_only and not args.arrow:
        print("Warning: py4j not installed. Running Gatun-only benchmarks.")
        print("Install py4j with: pip install py4j")
        args.gatun_only = True

    # Print system information
    system_info = get_system_info()
    print_system_info(system_info)

    # Arrow-only mode
    if args.arrow:
        arrow_results = run_arrow_benchmarks(memory_mb=args.arrow_memory)
        if arrow_results:
            print_arrow_results(arrow_results)
        return

    all_gatun_runs = []
    all_py4j_runs = []

    for i in range(args.repeat):
        if args.repeat > 1:
            print(f"\n{'='*40} Run {i+1}/{args.repeat} {'='*40}")

        if not args.py4j_only:
            all_gatun_runs.append(run_gatun_benchmarks())

        if not args.gatun_only:
            all_py4j_runs.append(run_py4j_benchmarks())

    # Merge results across runs
    merge_mode = "best" if args.best else "median"
    gatun_results = merge_results(all_gatun_runs, mode=merge_mode)
    py4j_results = merge_results(all_py4j_runs, mode=merge_mode)

    if args.repeat > 1:
        mode_label = "BEST" if args.best else "MEDIAN"
        print(f"\n\n{'='*40} {mode_label} RESULTS (across {args.repeat} runs) {'='*40}")

    if gatun_results and py4j_results:
        print_comparison(gatun_results, py4j_results)
    elif gatun_results:
        print("\n--- Gatun Results ---")
        for r in gatun_results:
            print(f"  {r}")
    elif py4j_results:
        print("\n--- Py4J Results ---")
        for r in py4j_results:
            print(f"  {r}")


if __name__ == "__main__":
    main()
