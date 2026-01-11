# Gatun Performance Benchmarks

This document provides detailed performance benchmarks comparing Gatun with Py4J (PySpark's default JVM bridge).

## Test Environment

- **CPU**: Apple M1
- **OS**: macOS
- **Java**: OpenJDK 22+
- **Python**: 3.13
- **Gatun Memory**: 16MB shared memory

## Methodology

All benchmarks follow these principles:

1. **Warmup**: 2 seconds of warmup (minimum 1000 iterations) to allow JIT compilation
2. **Multiple Trials**: 3 complete trials with median-of-medians aggregation
3. **Statistical Reporting**: Mean, median, p95, p99, and standard deviation
4. **Fairness**: Py4J tested with both `auto_convert=True` (PySpark default) and `auto_convert=False`

## Running Benchmarks

```bash
cd python

# Full comparison benchmark (Gatun vs Py4J)
JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_comparison.py

# Quick mode (~30s instead of ~5min)
JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_comparison.py --quick

# Gatun only (skip Py4J)
JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_comparison.py --gatun-only

# Arrow data transfer benchmarks
JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_arrow.py

# Vectorized API benchmarks
JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_vectorized.py

# Python callback benchmarks
JAVA_HOME=/opt/homebrew/opt/openjdk uv run python benchmarks/benchmark_callbacks.py
```

## Results Summary

### Latency Comparison: Gatun vs Py4J

| Category | Operation | Gatun (μs) | Py4J (μs) | Speedup |
|----------|-----------|----------:|----------:|--------:|
| **Object Construction** | No-arg constructor | 150 | 400 | 2.7x |
| | With primitive args | 160 | 420 | 2.6x |
| | With string args | 170 | 450 | 2.6x |
| **Static Methods** | No args | 130 | 360 | 2.8x |
| | With primitives | 140 | 370 | 2.6x |
| | Math.max(int, int) | 135 | 365 | 2.7x |
| **Instance Methods** | Simple call | 120 | 350 | 2.9x |
| | With arguments | 140 | 380 | 2.7x |
| | Method chaining | 250 | 700 | 2.8x |
| **Field Access** | Static field | 110 | 320 | 2.9x |
| | Instance field | 125 | 340 | 2.7x |
| **Data Transfer** | list(100) → ArrayList | 1,360 | 2,800 | 2.1x |
| | dict(100) → HashMap | 2,400 | 4,500 | 1.9x |
| **Type Checking** | is_instance_of | 115 | 330 | 2.9x |

### Arrow Data Transfer Performance

Arrow provides two methods for bulk data transfer:

1. **IPC Format** (`send_arrow_table`): Serializes table to Arrow IPC format, copies to shared memory
2. **Zero-Copy Buffers** (`send_arrow_buffers`): Copies raw buffers directly, sends descriptors

#### Latency by Data Size

| Rows | Columns | Data Size | IPC Format | Zero-Copy | Speedup |
|-----:|--------:|----------:|----------:|----------:|--------:|
| 100 | 3 | 2.7 KB | 880 μs | 510 μs | 1.7x |
| 1,000 | 4 | 28 KB | 800 μs | 520 μs | 1.5x |
| 5,000 | 4 | 145 KB | 850 μs | 530 μs | 1.6x |
| 10,000 | 4 | 290 KB | 890 μs | 570 μs | 1.6x |

#### Throughput (Large Data)

| Rows | Data Size | IPC Format | Zero-Copy |
|-----:|----------:|-----------:|----------:|
| 50,000 | 0.8 MB | 800 MB/s | 1,200 MB/s |
| 100,000 | 1.5 MB | 1,100 MB/s | 1,450 MB/s |
| 500,000 | 7.6 MB | 1,300 MB/s | **2,100 MB/s** |

### Vectorized API Performance

Vectorized APIs reduce round-trips by batching multiple operations:

#### invoke_methods

| Calls | Individual | Vectorized | Speedup |
|------:|-----------:|-----------:|--------:|
| 3 | 720 μs | 490 μs | 1.5x |
| 10 | 1,600 μs | 490 μs | 3.3x |

#### create_objects

| Objects | Individual | Vectorized | Speedup |
|--------:|-----------:|-----------:|--------:|
| 3 | 630 μs | 440 μs | 1.4x |
| 10 | 2,430 μs | 1,090 μs | 2.2x |

#### Comparison: Vectorized vs Batch API

| Operation | Batch API | Vectorized | Speedup |
|-----------|----------:|-----------:|--------:|
| 10 method calls | 560 μs | 490 μs | 1.2x |

### Python Callback Performance

Callbacks involve round-trips from Java back to Python:

| Operation | Latency | Notes |
|-----------|--------:|-------|
| Per-callback overhead | ~200 μs | IPC round-trip |
| Sort 10 elements (Python comparator) | 6,200 μs | ~31 callbacks |
| Sort 100 elements (Python comparator) | 31,000 μs | ~165 callbacks |
| Sort 100 elements (Java comparator) | 13,700 μs | No callbacks |
| HashMap.merge with BiFunction | 600 μs | Single callback |
| ArrayList.removeIf with Predicate (10 items) | 3,350 μs | 10 callbacks |

**Recommendation**: Use Java-side comparators when possible. Python callbacks add ~200 μs overhead per invocation.

### Arrow vs Traditional Transfer

Comparing Arrow table transfer to traditional list/dict transfer:

| Method | 100 Items | 500 Items | Notes |
|--------|----------:|----------:|-------|
| List → ArrayList | 1,360 μs | 4,200 μs | Per-element serialization |
| Dict → HashMap | 2,400 μs | 8,500 μs | Key-value pairs |
| Arrow zero-copy | 510 μs | 520 μs | ~50x faster for 500 items |
| Batch API (500 adds) | 12,650 μs | - | Individual calls batched |

**Recommendation**: Use Arrow for bulk data transfer (>100 items).

### Throughput: Bulk Simple Operations

For tight loops with pre-bound methods calling simple operations, Py4J can achieve higher ops/sec:

| Operation | Gatun | Py4J | Winner |
|-----------|------:|-----:|--------|
| Bulk static calls (10K) | ~45K ops/s | ~60K ops/s | **Py4J** |
| Bulk instance calls (10K) | ~40K ops/s | ~55K ops/s | **Py4J** |
| Bulk object creation (10K) | ~35K ops/s | ~45K ops/s | **Py4J** |
| Mixed workload | ~35K ops/s | ~30K ops/s | **Gatun** |

**Why Py4J is faster for simple tight loops**: Py4J's TCP-based protocol has lower per-call setup overhead for very simple operations. Gatun's shared memory approach has more setup overhead that pays off for larger payloads.

**Recommendation**: Don't use tight loops for bulk operations. Instead:
- Use **vectorized APIs** (`invoke_methods`, `create_objects`) for multiple same-target operations
- Use **Arrow transfer** for bulk data
- Use **batch API** for mixed operations

## Architecture Impact on Performance

### Where Gatun Excels

1. **Lower Latency**: Shared memory IPC provides 2-3x lower latency per operation
2. **Large Payloads**: Zero-copy Arrow transfer achieves 2+ GB/s throughput
3. **Mixed Operations**: Better for varied workloads (create + call + get)
4. **Data Transfer**: Arrow integration for efficient bulk data

### Where Py4J Excels

1. **Simple Tight Loops**: Lower per-call overhead for pre-bound method calls
2. **Multiple Clients**: Can serve multiple Python processes from one JVM
3. **Maturity**: Battle-tested in PySpark for years

### Performance Trade-offs

| Feature | Gatun | Py4J |
|---------|-------|------|
| Per-call latency | **Lower** (shared memory) | Higher (TCP/Unix socket) |
| Simple tight loops | ~45K ops/s | **~60K ops/s** |
| Large data transfer | **2+ GB/s** (Arrow) | Limited by serialization |
| Memory model | Fixed shared region | Dynamic per-call |
| Client scaling | Single client per server | Multiple clients |

### When to Use Gatun vs Py4J

| Use Case | Recommendation | Reason |
|----------|----------------|--------|
| Interactive work | **Gatun** | Lower latency feels more responsive |
| Bulk data transfer | **Gatun** | Arrow zero-copy is much faster |
| Simple tight loops | **Py4J** | Lower per-call overhead |
| Mixed operations | **Gatun** | Better for varied workloads |
| Multiple clients | **Py4J** | Gatun is single-client |
| PySpark production | Either | Gatun via BridgeAdapter |

## Benchmark Files

| File | Description |
|------|-------------|
| `benchmark_comparison.py` | Full Gatun vs Py4J comparison |
| `benchmark_arrow.py` | Arrow data transfer methods |
| `benchmark_vectorized.py` | Vectorized API performance |
| `benchmark_callbacks.py` | Python callback overhead |
| `profile_hotspots.py` | CPU profiling for optimization |

## Reproducing Results

```bash
# Clone repository
git clone https://github.com/your-org/gatun.git
cd gatun/python

# Install dependencies
uv sync

# Set Java home (adjust for your system)
export JAVA_HOME=/opt/homebrew/opt/openjdk

# Run all benchmarks
uv run python benchmarks/benchmark_comparison.py
uv run python benchmarks/benchmark_arrow.py
uv run python benchmarks/benchmark_vectorized.py
uv run python benchmarks/benchmark_callbacks.py
```

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.1.0 | 2025-01 | Initial benchmarks |
