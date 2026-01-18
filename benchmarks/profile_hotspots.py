#!/usr/bin/env python
"""Profile Gatun hot paths to identify optimization opportunities."""

import time
import cProfile
import pstats
from io import StringIO

from gatun import connect


def profile_instance_method_calls(client, iterations=5000):
    """Profile instance method calls - one of the slowest paths."""
    arr = client.create_object("java.util.ArrayList")

    # Warmup
    for _ in range(1000):
        arr.size()

    # Profile
    pr = cProfile.Profile()
    pr.enable()

    for i in range(iterations):
        arr.size()

    pr.disable()

    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats(30)
    print("=== Instance Method Calls (arr.size()) ===")
    print(s.getvalue())

    # Timed measurement
    start = time.perf_counter_ns()
    for _ in range(iterations):
        arr.size()
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"Average: {elapsed:.1f} μs per call\n")


def profile_static_method_calls(client, iterations=5000):
    """Profile static method calls."""
    # Warmup
    for _ in range(1000):
        client.jvm.java.lang.System.currentTimeMillis()

    # Profile
    pr = cProfile.Profile()
    pr.enable()

    for i in range(iterations):
        client.jvm.java.lang.System.currentTimeMillis()

    pr.disable()

    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats(30)
    print("=== Static Method Calls (System.currentTimeMillis()) ===")
    print(s.getvalue())

    # Timed measurement
    start = time.perf_counter_ns()
    for _ in range(iterations):
        client.jvm.java.lang.System.currentTimeMillis()
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"Average: {elapsed:.1f} μs per call\n")


def profile_object_creation(client, iterations=5000):
    """Profile object creation."""
    # Warmup
    for _ in range(1000):
        client.create_object("java.util.ArrayList")

    # Profile
    pr = cProfile.Profile()
    pr.enable()

    for i in range(iterations):
        client.create_object("java.util.ArrayList")

    pr.disable()

    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats(30)
    print("=== Object Creation (new ArrayList()) ===")
    print(s.getvalue())

    # Timed measurement
    start = time.perf_counter_ns()
    for _ in range(iterations):
        client.create_object("java.util.ArrayList")
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"Average: {elapsed:.1f} μs per call\n")


def profile_string_return(client, iterations=5000):
    """Profile returning strings from Java."""
    sb = client.create_object("java.lang.StringBuilder", "hello world")

    # Warmup
    for _ in range(1000):
        sb.toString()

    # Profile
    pr = cProfile.Profile()
    pr.enable()

    for i in range(iterations):
        sb.toString()

    pr.disable()

    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats(30)
    print("=== String Return (sb.toString()) ===")
    print(s.getvalue())

    # Timed measurement
    start = time.perf_counter_ns()
    for _ in range(iterations):
        sb.toString()
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"Average: {elapsed:.1f} μs per call\n")


def profile_breakdown():
    """Profile individual components to identify bottlenecks."""
    from gatun.generated.org.gatun.protocol import Command as Cmd
    from gatun.generated.org.gatun.protocol import Action as Act
    from gatun.generated.org.gatun.protocol import Response
    import struct

    client = connect()
    arr = client.create_object("java.util.ArrayList")

    iterations = 10000

    # 1. FlatBuffer builder creation/reuse
    print("=== Component Timing Breakdown ===\n")

    # Builder reuse (should be ~0)
    start = time.perf_counter_ns()
    for _ in range(iterations):
        builder = client._get_builder()
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"Builder get/clear: {elapsed:.3f} μs")

    # String creation (cached)
    start = time.perf_counter_ns()
    for _ in range(iterations):
        builder = client._get_builder()
        client._create_string(builder, "size")
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"String creation (cached): {elapsed:.3f} μs")

    # String encoding (uncached)
    unique_names = [f"method_{i}" for i in range(iterations)]
    client._string_cache._cache.clear()  # Access underlying OrderedDict
    start = time.perf_counter_ns()
    for name in unique_names:
        builder = client._get_builder()
        client._create_string(builder, name)
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"String creation (uncached): {elapsed:.3f} μs")

    # Full command build (no args)
    start = time.perf_counter_ns()
    for _ in range(iterations):
        builder = client._get_builder()
        meth_off = client._create_string(builder, "size")
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeMethod)
        Cmd.CommandAddTargetId(builder, arr.object_id)
        Cmd.CommandAddTargetName(builder, meth_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)
        data = builder.Output()
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"Full command build (no args): {elapsed:.3f} μs")

    # SHM write
    data = builder.Output()
    start = time.perf_counter_ns()
    for _ in range(iterations):
        client.shm.seek(0)
        client.shm.write(data)
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"SHM write ({len(data)} bytes): {elapsed:.3f} μs")

    # Socket send (length only)
    length_bytes = struct.pack("<I", len(data))
    start = time.perf_counter_ns()
    for _ in range(iterations):
        client.sock.sendall(length_bytes)
        # Receive response length (4 bytes)
        client.sock.recv(4)
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"Socket round-trip (8 bytes): {elapsed:.3f} μs")

    # Response parsing
    client.shm.seek(client.response_offset)
    resp_buf = client.shm.read(100)  # Read some response data
    start = time.perf_counter_ns()
    for _ in range(iterations):
        resp = Response.Response.GetRootAsResponse(resp_buf, 0)
        _ = resp.IsError()
        _ = resp.ReturnValType()
    elapsed = (time.perf_counter_ns() - start) / iterations / 1000
    print(f"Response parse: {elapsed:.3f} μs")

    print()
    client.close()


def main():
    print("Gatun Hot Path Profiler")
    print("=" * 60)
    print()

    # First do component breakdown
    profile_breakdown()

    # Then full profiles
    client = connect()

    profile_static_method_calls(client, iterations=3000)
    profile_instance_method_calls(client, iterations=3000)
    profile_object_creation(client, iterations=3000)
    profile_string_return(client, iterations=3000)

    client.close()


if __name__ == "__main__":
    main()
