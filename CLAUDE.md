# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Status: Alpha (experimental)** - APIs may change without notice.

Gatun is a high-performance Python-to-Java bridge using shared memory (mmap) and Unix domain sockets for inter-process communication. It uses FlatBuffers for serialization and Apache Arrow for efficient bulk data transfer.

## Architecture

### Components
- **gatun-core** (Java): Server that manages Java objects and handles RPC requests
- **src/gatun** (Python): Client library with sync/async clients, launcher, and config
- **schemas/commands.fbs**: FlatBuffers schema defining the wire protocol

### Communication Flow
1. Python launcher starts Java server subprocess
2. Client connects via Unix domain socket
3. Server creates per-client shared memory file and sends handshake with SHM path
4. Commands are serialized to FlatBuffers, written to shared memory
5. Length prefix sent over socket signals Java to process command
6. Response written to shared memory, length sent back to Python
7. On disconnect, server cleans up the client's shared memory file

### Memory Layout
- Command zone: offset 0
- Payload zone (Arrow data): offset 4096
- Response zone: last 4KB of shared memory

### Protocol Handshake
Format: `[4 bytes: version][4 bytes: arena_epoch][8 bytes: memory size][2 bytes: shm_path_len][N bytes: shm_path]`
- Protocol version (currently 2) is verified on connect
- Version mismatch raises RuntimeError with upgrade instructions
- SHM path is the per-client shared memory file path (e.g., `/tmp/gatun.sock.1.shm`)

## Build Commands

### Python (from repository root)
```bash
JAVA_HOME=/opt/homebrew/opt/openjdk uv sync    # Install deps and build JAR
JAVA_HOME=/opt/homebrew/opt/openjdk uv sync --reinstall-package gatun  # Rebuild after schema/Java changes
JAVA_HOME=/opt/homebrew/opt/openjdk uv build   # Build wheel and sdist
uv run pytest                        # Run all tests (with coverage)
uv run pytest tests/test_gatun_core.py::test_name  # Run single test
uv run pytest --no-cov               # Run without coverage (faster)
uv run ruff check .                  # Lint
uv run ruff format .                 # Format
```

Coverage reports are generated automatically:
- Terminal: shows missing lines after test run
- HTML: `htmlcov/index.html` for detailed browsing
- Config in `pyproject.toml` under `[tool.coverage.*]`

### Java (from repository root, if needed separately)
```bash
./gradlew :gatun-core:build          # Build the Java server
./gradlew :gatun-core:shadowJar      # Build fat JAR (gatun-server-all.jar)
./gradlew :gatun-core:run            # Run the server directly
```

## Generated Code

FlatBuffers code is generated from `schemas/commands.fbs`:
- Java: `gatun-core/src/main/java/org/gatun/protocol/`
- Python: `src/gatun/generated/org/gatun/protocol/`

The build backend (`gatun_build_backend.py`) automatically regenerates FlatBuffers code when the schema changes.

## Key Implementation Details

- Java 22+ required
- Python 3.13+ required (3.14 also supported)
- Arrow memory requires JVM flags: `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
- Python client uses `weakref.finalize` for automatic Java object cleanup
- Socket path: Generated randomly in `/tmp/gatun_<pid>_<random>.sock` by default
- Use `T | None` syntax instead of `Optional[T]`
- No fallback imports (e.g., no `tomli` for older Python)

## Quick Start

```python
from gatun import connect

# Start server and connect
client = connect()

# Create Java objects via JVM view
ArrayList = client.jvm.java.util.ArrayList
my_list = ArrayList()
my_list.add("hello")
my_list.add("world")
print(my_list.size())  # 2

# Call static methods
result = client.jvm.java.lang.Integer.parseInt("42")  # 42

# Clean up
client.close()
```

## Supported Operations

### JVM View API (Recommended)
The `client.jvm` property provides package-style navigation:
```python
# Access classes via package path
ArrayList = client.jvm.java.util.ArrayList
HashMap = client.jvm.java.util.HashMap

# Instantiate classes
my_list = ArrayList()           # No-arg constructor
my_list = ArrayList(100)        # With initial capacity

# Call static methods
result = client.jvm.java.lang.Integer.parseInt("42")  # 42
result = client.jvm.java.lang.Math.max(10, 20)        # 20
result = client.jvm.java.lang.Math.sqrt(16.0)         # 4.0 (note: use floats for double params)

# String operations
result = client.jvm.java.lang.String.valueOf(123)     # "123"
result = client.jvm.java.lang.String.format("Hello %s!", "World")  # "Hello World!"
```

### java_import
Import classes for shorter access paths:
```python
from gatun import java_import

# Wildcard import
java_import(client.jvm, "java.util.*")
arr = client.jvm.ArrayList()  # instead of client.jvm.java.util.ArrayList()

# Single class import
java_import(client.jvm, "java.lang.StringBuilder")
sb = client.jvm.StringBuilder("hello")
```

### Collections Examples
```python
# HashMap
hm = client.jvm.java.util.HashMap()
hm.put("key1", "value1")
hm.put("key2", 42)
hm.get("key1")  # "value1"
hm.size()       # 2

# TreeMap (sorted keys)
tm = client.jvm.java.util.TreeMap()
tm.put("zebra", 1)
tm.put("apple", 2)
tm.firstKey()  # "apple"
tm.lastKey()   # "zebra"

# HashSet (no duplicates)
hs = client.jvm.java.util.HashSet()
hs.add("a")
hs.add("b")
hs.add("a")  # duplicate ignored
hs.size()    # 2
hs.contains("a")  # True

# StringBuilder for efficient string building
sb = client.jvm.java.lang.StringBuilder("Hello")
sb.append(" ")
sb.append("World!")
sb.toString()  # "Hello World!"

# Collections utility methods
java_import(client.jvm, "java.util.*")
arr = client.jvm.ArrayList()
arr.add("banana")
arr.add("apple")
arr.add("cherry")
client.jvm.Collections.sort(arr)     # Sorts in place
client.jvm.Collections.reverse(arr)  # Reverses in place

# Arrays.asList returns a Python list (auto-converted)
result = client.jvm.java.util.Arrays.asList("a", "b", "c")  # ['a', 'b', 'c']
```

### Passing Python Collections
Python lists and dicts are automatically converted to Java collections:
```python
arr = client.jvm.java.util.ArrayList()
arr.add([1, 2, 3])                    # Converted to Java List
arr.add({"name": "Alice", "age": 30}) # Converted to Java Map
```

### Async Client
```python
from gatun import aconnect
import asyncio

async def main():
    client = await aconnect()

    # All operations are async
    arr = await client.jvm.java.util.ArrayList()
    await arr.add("hello")
    size = await arr.size()  # 1

    # Static methods
    result = await client.jvm.java.lang.Integer.parseInt("42")  # 42

    await client.close()

asyncio.run(main())
```

### Python Callbacks
Register Python functions as Java interface implementations:
```python
def compare(a, b):
    return -1 if a < b else (1 if a > b else 0)

comparator = client.register_callback(compare, "java.util.Comparator")

arr = client.jvm.java.util.ArrayList()
arr.add(3)
arr.add(1)
arr.add(2)
client.jvm.java.util.Collections.sort(arr, comparator)
# arr is now [1, 2, 3]
```

Async callbacks are also supported:
```python
async def async_compare(a, b):
    await asyncio.sleep(0.01)
    return -1 if a < b else (1 if a > b else 0)

comparator = await async_client.register_callback(async_compare, "java.util.Comparator")
```

**Important: Reentrancy Policy**

Gatun does **not** support nested Java calls from within callbacks. Attempting to call Java from inside a callback raises `ReentrancyError`:

```python
from gatun import ReentrancyError

def bad_callback(a, b):
    # This will raise ReentrancyError - cannot call Java from callback
    client.create_object("java.util.ArrayList")
    return 0

comparator = client.register_callback(bad_callback, "java.util.Comparator")
arr = client.jvm.java.util.ArrayList()
arr.add("b")
arr.add("a")

# This raises JavaException containing ReentrancyError
client.jvm.java.util.Collections.sort(arr, comparator)
```

Why this restriction exists: Gatun uses a single shared memory channel per client. When Java calls back into Python, the original request is still waiting for a response. A nested Java call would try to use the same channel, causing a deadlock.

Workarounds:
- Queue work for later instead of calling Java immediately in the callback
- Use pure Python logic in callbacks (no Java calls)
- Pre-create any Java objects you need before the callback is invoked

### Request Cancellation
Cancel long-running requests:
```python
from gatun import CancelledException

request_id = client._get_request_id()
# ... start long operation in another thread ...
client.cancel(request_id)  # Returns True on acknowledgement

# Java side can check: GatunServer.checkCancelled() throws InterruptedException
# Maps to CancelledException in Python
```

### is_instance_of
Check if a Java object is an instance of a class (equivalent to `instanceof`):
```python
arr = client.create_object("java.util.ArrayList")
client.is_instance_of(arr, "java.util.List")       # True
client.is_instance_of(arr, "java.util.Collection") # True
client.is_instance_of(arr, "java.util.Map")        # False
```

### Batch API
Execute multiple commands in a single round-trip (6x speedup for bulk operations):
```python
arr = client.create_object("java.util.ArrayList")

# Batch operations - single round-trip for all commands
with client.batch() as b:
    for i in range(100):
        b.call(arr, "add", i)
    size_result = b.call(arr, "size")

print(size_result.get())  # 100

# Available batch methods:
# b.create(class_name, *args)           - Create object
# b.call(obj, method_name, *args)       - Instance method
# b.call_static(class_name, method, *args) - Static method
# b.get_field(obj, field_name)          - Get field
# b.set_field(obj, field_name, value)   - Set field

# Error handling
with client.batch(stop_on_error=True) as b:  # Stop on first error
    r1 = b.call(arr, "add", "valid")
    r2 = b.call_static("java.lang.Integer", "parseInt", "invalid")  # Errors
    r3 = b.call(arr, "size")  # Skipped

# Results via BatchResult.get() - raises exception if command failed
print(r1.get())      # True
print(r2.is_error)   # True
```

### Vectorized APIs
For even faster bulk operations on the same target, use vectorized APIs (2-5x speedup):

```python
# invoke_methods - Multiple method calls on same object in one round-trip
arr = client.create_object("java.util.ArrayList")
results = client.invoke_methods(arr, [
    ("add", ("a",)),
    ("add", ("b",)),
    ("add", ("c",)),
    ("size", ()),
])
# results = [True, True, True, 3]

# With return_object_ref for specific calls
results = client.invoke_methods(arr, [
    ("size", ()),
    ("subList", (0, 2)),
], return_object_refs=[False, True])  # Second result as JavaObject

# create_objects - Create multiple objects in one round-trip
list1, map1, set1 = client.create_objects([
    ("java.util.ArrayList", ()),
    ("java.util.HashMap", ()),
    ("java.util.HashSet", ()),
])

# With constructor arguments
objects = client.create_objects([
    ("java.util.ArrayList", (100,)),      # Initial capacity
    ("java.lang.StringBuilder", ("hi",)), # Initial string
])

# get_fields - Read multiple fields from one object in one round-trip
sb = client.create_object("java.lang.StringBuilder", "hello")
values = client.get_fields(sb, ["count"])  # [5]
```

**When to use which API:**
| API | Best For |
|-----|----------|
| `invoke_methods` | Multiple method calls on **same object** |
| `create_objects` | Creating multiple objects at startup |
| `get_fields` | Reading multiple fields from one object |
| `batch` | Mixed operations on **different objects** |

### Low-Level API
For direct control:
```python
client.create_object("java.util.ArrayList")           # No-arg constructor
client.create_object("java.util.ArrayList", 100)      # With initial capacity
client.invoke_method(object_id, "methodName", arg1)   # Direct method call
client.invoke_static_method("java.lang.Math", "max", 10, 20)
client.get_field(obj, "fieldName")                    # Get field value
client.set_field(obj, "fieldName", value)             # Set field value
client.is_instance_of(obj, "java.util.List")          # Check instance type
client.ping()                                         # Health check (returns True if alive)
client.get_metrics()                                  # Get server metrics report

# Vectorized operations (single round-trip for multiple operations)
client.get_fields(obj, ["field1", "field2"])          # Multiple field reads
client.invoke_methods(obj, [("method1", (arg,)), ("method2", ())])  # Multiple calls
client.create_objects([("class1", ()), ("class2", (arg,))])  # Multiple creations
```

### Arrow Data Transfer
Multiple methods for transferring Arrow data:

#### IPC Format (Simple)
```python
import pyarrow as pa

table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
result = client.send_arrow_table(table)  # "Received 3 rows"
```

#### Scoped Context Manager (Recommended)
Handles arena lifecycle automatically with proper cleanup:
```python
import pyarrow as pa

# Send and receive with automatic cleanup
with client.arrow_context() as ctx:
    ctx.send(table1)          # Auto-resets arena between sends
    ctx.send(table2)          # Safe to send multiple tables
    result = ctx.receive()    # Get data back as PyArrow table
# Arena automatically closed on exit

# Async version
async with client.arrow_context() as ctx:
    await ctx.send(table)
```

#### Size Validation
Check size before transfer to avoid errors:
```python
from gatun import estimate_arrow_size, PayloadTooLargeError

# Estimate buffer size needed
estimated = estimate_arrow_size(large_table)
print(f"Estimated: {estimated:,} bytes")

# Size is validated automatically - raises PayloadTooLargeError if too large
try:
    client.send_arrow_buffers(large_table, arena, {})
except PayloadTooLargeError as e:
    print(f"Too large: {e.payload_size:,} > {e.max_size:,} bytes")
```

#### Zero-Copy Buffer Transfer (Manual Control)
For maximum performance with fine-grained control:
```python
import pyarrow as pa

table = pa.table({"name": ["Alice", "Bob"], "age": [25, 30]})

# Get arena backed by client's shared memory
arena = client.get_payload_arena()
schema_cache = {}

# Send table via zero-copy buffer transfer
client.send_arrow_buffers(table, arena, schema_cache)

# For multiple batches, reset arena between sends
for batch in table.to_batches():
    arena.reset()  # Reuse arena for each batch
    client.send_arrow_buffers(batch, arena, schema_cache)

arena.close()
```

Data flow (Python -> Java):
1. Python copies Arrow buffers into shared memory (one memcpy)
2. Python sends buffer descriptors (offsets/lengths) to Java
3. Java wraps buffers directly as ArrowBuf (zero-copy read)

### JavaObject Features
JavaObject wrappers support Python protocols for seamless integration:
```python
# Iteration - iterate over Java collections
arr = client.jvm.java.util.ArrayList()
arr.add("a")
arr.add("b")
for item in arr:
    print(item)  # "a", "b"

# Indexing - access elements by index
print(arr[0])  # "a"
print(arr[1])  # "b"

# Length - get collection size
print(len(arr))  # 2

# Works with any Java Iterable/Collection
hashset = client.jvm.java.util.HashSet()
hashset.add(1)
hashset.add(2)
print(list(hashset))  # [1, 2]
```

### JavaArray for Primitive Arrays
Primitive arrays (`int[]`, `long[]`, `double[]`, etc.) are returned as `JavaArray`:
```python
from gatun import JavaArray
import pyarrow as pa

# Primitive arrays from Java are JavaArray instances
original = pa.array([1, 2, 3], type=pa.int32())
int_array = client.jvm.java.util.Arrays.copyOf(original, 3)
print(isinstance(int_array, JavaArray))  # True
print(int_array.element_type)  # "Int"
print(list(int_array))  # [1, 2, 3]

# Create JavaArray manually for specific element types
int_array = JavaArray([1, 2, 3], element_type="Int")
str_array = JavaArray(["a", "b"], element_type="String")
result = client.jvm.java.util.Arrays.toString(int_array)  # "[1, 2, 3]"
```

### Object Arrays as JavaObject
Object arrays (`Object[]`, `String[]`) are returned as `JavaObject` references to allow `Array.set/get`:
```python
# Object arrays from toArray() are JavaObject (not JavaArray)
arr = client.jvm.java.util.ArrayList()
arr.add("x")
arr.add("y")
java_array = arr.toArray()  # Returns JavaObject

# Use len() and iteration (not .size() or .length)
print(len(java_array))    # 2
print(java_array[0])      # "x"
print(list(java_array))   # ["x", "y"]

# Can still pass back to Java methods
result = client.jvm.java.util.Arrays.toString(java_array)  # "[x, y]"

# Array.set/get work on Object arrays
Array = client.jvm.java.lang.reflect.Array
Array.set(java_array, 0, "modified")
print(Array.get(java_array, 0))  # "modified"
```

### Supported Argument/Return Types
- Primitives: `int`, `long`, `double`, `boolean`
- `String`
- `list` -> Java `List` (ArrayList)
- `dict` -> Java `Map` (HashMap)
- `bytes` -> Java `byte[]`
- `JavaArray` -> Java primitive arrays (`int[]`, `double[]`, etc.)
- `pyarrow.Array` -> Java arrays (int32->int[], int64->long[], float64->double[], bool->boolean[], string->String[])
- `array.array` -> Java arrays (typecode 'i'->int[], 'q'->long[], 'd'->double[])
- Object references (returned as `JavaObject` wrappers, including Object arrays)
- `null`/`None`

**Note:** Object arrays (`Object[]`, `String[]`) return as `JavaObject`, not `JavaArray`. Use `len()` instead of `.size()` or `.length`.

## Exception Handling

Java exceptions are mapped to Python exceptions:
- `java.lang.SecurityException` -> `JavaSecurityException`
- `java.lang.IllegalArgumentException` -> `JavaIllegalArgumentException`
- `java.lang.NoSuchMethodException` -> `JavaNoSuchMethodException`
- `java.lang.NoSuchFieldException` -> `JavaNoSuchFieldException`
- `java.lang.ClassNotFoundException` -> `JavaClassNotFoundException`
- `java.lang.NullPointerException` -> `JavaNullPointerException`
- `java.lang.IndexOutOfBoundsException` -> `JavaIndexOutOfBoundsException`
- `java.lang.NumberFormatException` -> `JavaNumberFormatException`
- `java.lang.InterruptedException` -> `CancelledException`
- Other exceptions -> `JavaRuntimeException`

Python-side exceptions:
- `ReentrancyError` - Raised when attempting nested Java calls from within a callback
- `PayloadTooLargeError` - Raised when data exceeds shared memory capacity
- `StaleArenaError` - Raised when using an arena after client reconnected

## Security

### Class Allowlist
Only these classes can be instantiated or used for static methods (hardcoded in GatunServer.java):
- Collections: `java.util.ArrayList`, `LinkedList`, `HashMap`, `LinkedHashMap`, `TreeMap`, `HashSet`, `LinkedHashSet`, `TreeSet`, `Collections`, `Arrays`
- Strings: `java.lang.String`, `StringBuilder`, `StringBuffer`
- Primitives: `java.lang.Integer`, `Long`, `Double`, `Float`, `Boolean`, `Byte`, `Short`, `Math`
- Reflection: `java.lang.Class`, `java.lang.reflect.Array` (for array operations)
- Spark/Scala: Classes under `org.apache.spark.*` and `scala.*` prefixes are allowed

Attempting to use non-allowlisted classes (e.g., `Runtime`, `ProcessBuilder`) raises `SecurityException`.

### Session Isolation
- Each client session gets its own shared memory file (per-client SHM)
- Multiple clients can connect concurrently without data races
- Each client session tracks its own object IDs
- Objects are automatically cleaned up when session ends
- SHM files are deleted when the client disconnects
- Double-free attempts are silently ignored

## Observability

Gatun includes comprehensive observability features for debugging and monitoring.

### Enabling from Python
The easiest way to enable observability features:
```python
from gatun import connect

# Enable trace mode for method resolution debugging
client = connect(trace=True)

# Enable verbose logging
client = connect(log_level="FINE")

# Both
client = connect(trace=True, log_level="FINE")
```

Or via environment variables:
```bash
export GATUN_TRACE=true
export GATUN_LOG_LEVEL=FINE
python my_script.py
```

### Structured Logging
Log format: `sessionId=N requestId=N action=X target=Y latency_us=Z status=OK|ERROR`

Log levels:
- `INFO` - Server start/stop only (default)
- `FINE` - Request/response logging with timing
- `FINER` - Object registry add/remove events
- `FINEST` - All internal details

### Trace Mode
When enabled (`trace=True` or `GATUN_TRACE=true`), logs detailed method resolution:
- Class and method name
- Number of overload candidates
- Chosen method signature
- Specificity score
- Actual argument types

Useful for debugging "wrong method called" or overload resolution issues.

### Metrics
Access server metrics programmatically from Python:
```python
# Get server metrics report
metrics = client.get_metrics()
print(metrics)
```

The report includes:
- Request counts and rates per action type
- Latency percentiles (p50, p99) per action
- Object registry counts (current and peak)
- Arrow transfer metrics (rows, bytes)
- Callback invocation counts

Async clients also support `await client.get_metrics()`.

### JFR Events
Gatun emits JFR (Java Flight Recorder) events for profiling:
```bash
java -XX:StartFlightRecording=filename=gatun.jfr,settings=profile -jar gatun-server.jar
```

Events:
- `org.gatun.Request` - Request start/end with timing
- `org.gatun.MethodResolution` - Method overload resolution decisions
- `org.gatun.ObjectRegistry` - Object add/remove events
- `org.gatun.Callback` - Python callback round-trips
- `org.gatun.ArrowTransfer` - Arrow data transfers
- `org.gatun.Session` - Session lifecycle

### Request Ring Buffer
Each session maintains a ring buffer of the last 64 requests. On errors, recent request history is logged to help diagnose failures.

## Configuration

Configure via `pyproject.toml`:
```toml
[tool.gatun]
memory = "64MB"
socket_path = "/tmp/gatun.sock"  # Optional: fixed path instead of random
```

Or environment variables:
```bash
GATUN_MEMORY=64MB
GATUN_SOCKET_PATH=/tmp/gatun.sock
```

## BridgeAdapter (PySpark Integration)

The `BridgeAdapter` abstract class provides a unified interface for JVM communication:

```python
from gatun.bridge import BridgeAdapter
from gatun.bridge_adapters import GatunAdapter

# Create adapter (launches Gatun server)
bridge = GatunAdapter(memory="256MB")

# Object lifecycle
obj = bridge.new("java.util.ArrayList")  # Create object
bridge.detach(obj)                        # Prevent auto-cleanup
bridge.close()                            # Close connection

# Method calls
bridge.call(obj, "add", "hello")          # Instance method
result = bridge.call_static("java.lang.Math", "max", 10, 20)  # Static method

# Field access
value = bridge.get_field(obj, "fieldName")
bridge.set_field(obj, "fieldName", value)

# Array operations (for Java array manipulation)
arr = bridge.new_array("java.lang.String", 3)  # Create String[3]
bridge.array_set(arr, 0, "hello")              # Set element
value = bridge.array_get(arr, 0)               # Get element
length = bridge.array_length(arr)              # Get length

# Type checking
bridge.is_instance_of(obj, "java.util.List")  # True
```

### PySpark Usage
To use Gatun with PySpark, set the environment variable before importing PySpark:
```bash
export PYSPARK_USE_GATUN=true
export GATUN_MEMORY=256MB
```

Then use PySpark normally - it will use Gatun for JVM communication.

## Logging

- Java: `java.util.logging` (Logger for `GatunServer`)
- Python: `logging` module (loggers for `gatun.client` and `gatun.launcher`)

## Method Overload Resolution

Gatun uses specificity scoring to resolve overloaded Java methods:
1. Exact type matches get highest priority
2. Compatible types (e.g., `String` matching `CharSequence`) get medium priority
3. `Object` parameters get lowest priority
4. Non-varargs methods are preferred over varargs

Note: For methods with `double` parameters (like `Math.pow`), pass Python floats:
```python
Math.pow(2.0, 10.0)  # Works: 1024.0
Math.pow(2, 10)      # May fail: int args don't match double params
```
