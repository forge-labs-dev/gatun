# Gatun

High-performance Python-to-Java bridge using shared memory and Unix domain sockets.

## Features

- **Shared Memory IPC**: Zero-copy data transfer via mmap
- **FlatBuffers Protocol**: Efficient binary serialization
- **Apache Arrow Integration**: Zero-copy array/table transfer
- **Sync & Async Clients**: Both blocking and asyncio support
- **Python Callbacks**: Register Python functions as Java interfaces
- **Request Cancellation**: Cancel long-running operations
- **JVM View API**: Pythonic package-style navigation (`client.jvm.java.util.ArrayList`)
- **PySpark Integration**: Use as backend for PySpark via BridgeAdapter
- **Pythonic JavaObjects**: Iteration, indexing, and len() support on Java collections
- **Batch API**: Execute multiple commands in a single round-trip (6x speedup for bulk ops)
- **Vectorized APIs**: invoke_methods, create_objects, get_fields for 2-5x additional speedup
- **Observability**: Server metrics, structured logging, and JFR events for debugging and monitoring

## Installation

```bash
pip install gatun
```

Requires Java 21+.

## Quick Start

```python
from gatun import connect

# Auto-launch server and connect
client = connect()

# Create Java objects via JVM view
ArrayList = client.jvm.java.util.ArrayList
my_list = ArrayList()
my_list.add("hello")
my_list.add("world")
print(my_list.size())  # 2

# Call static methods
result = client.jvm.java.lang.Integer.parseInt("42")  # 42
result = client.jvm.java.lang.Math.max(10, 20)        # 20

# Clean up
client.close()
```

## Examples

### java_import for Shorter Paths

```python
from gatun import connect, java_import

client = connect()

# Wildcard import
java_import(client.jvm, "java.util.*")
arr = client.jvm.ArrayList()  # instead of client.jvm.java.util.ArrayList()
arr.add("hello")

# Single class import
java_import(client.jvm, "java.lang.StringBuilder")
sb = client.jvm.StringBuilder("hello")
print(sb.toString())  # "hello"
```

### Collections

```python
# HashMap
hm = client.jvm.java.util.HashMap()
hm.put("key1", "value1")
hm.put("key2", 42)
print(hm.get("key1"))  # "value1"
print(hm.size())       # 2

# TreeMap (sorted keys)
tm = client.jvm.java.util.TreeMap()
tm.put("zebra", 1)
tm.put("apple", 2)
tm.put("mango", 3)
print(tm.firstKey())  # "apple"
print(tm.lastKey())   # "zebra"

# HashSet (no duplicates)
hs = client.jvm.java.util.HashSet()
hs.add("a")
hs.add("b")
hs.add("a")  # duplicate ignored
print(hs.size())        # 2
print(hs.contains("a")) # True

# Collections utility methods
java_import(client.jvm, "java.util.*")
arr = client.jvm.ArrayList()
arr.add("banana")
arr.add("apple")
arr.add("cherry")
client.jvm.Collections.sort(arr)     # ["apple", "banana", "cherry"]
client.jvm.Collections.reverse(arr)  # ["cherry", "banana", "apple"]

# Arrays.asList (returns Python list)
result = client.jvm.java.util.Arrays.asList("a", "b", "c")  # ['a', 'b', 'c']
```

### String Operations

```python
# StringBuilder
sb = client.jvm.java.lang.StringBuilder("Hello")
sb.append(" ")
sb.append("World!")
print(sb.toString())  # "Hello World!"

# String static methods
result = client.jvm.java.lang.String.valueOf(123)  # "123"
result = client.jvm.java.lang.String.format("Hello %s, you have %d messages", "Alice", 5)
# "Hello Alice, you have 5 messages"
```

### Math Operations

```python
Math = client.jvm.java.lang.Math
print(Math.abs(-42))        # 42
print(Math.min(5, 3))       # 3
print(Math.max(10, 20))     # 20
print(Math.pow(2.0, 10.0))  # 1024.0 (note: use floats for double params)
print(Math.sqrt(16.0))      # 4.0
```

### Integer Utilities

```python
Integer = client.jvm.java.lang.Integer
print(Integer.parseInt("42"))        # 42
print(Integer.valueOf("123"))        # 123
print(Integer.toBinaryString(255))   # "11111111"
print(Integer.MAX_VALUE)             # 2147483647 (static field)
```

### Passing Python Collections

Python lists and dicts are automatically converted to Java collections:

```python
arr = client.jvm.java.util.ArrayList()
arr.add([1, 2, 3])                    # Converted to Java List
arr.add({"name": "Alice", "age": 30}) # Converted to Java Map
print(arr.size())  # 2
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
    await arr.add("world")
    size = await arr.size()  # 2

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

Async callbacks work too:

```python
async def async_compare(a, b):
    await asyncio.sleep(0.01)  # Simulate async work
    return -1 if a < b else (1 if a > b else 0)

comparator = await client.register_callback(async_compare, "java.util.Comparator")
```

### Type Checking with is_instance_of

```python
arr = client.create_object("java.util.ArrayList")
client.is_instance_of(arr, "java.util.List")       # True
client.is_instance_of(arr, "java.util.Collection") # True
client.is_instance_of(arr, "java.util.Map")        # False
```

### Pythonic Java Collections

JavaObject wrappers support iteration, indexing, and length:

```python
arr = client.jvm.java.util.ArrayList()
arr.add("a")
arr.add("b")
arr.add("c")

# Iterate
for item in arr:
    print(item)  # "a", "b", "c"

# Index access
print(arr[0])  # "a"
print(arr[1])  # "b"

# Length
print(len(arr))  # 3

# Convert to Python list
items = list(arr)  # ["a", "b", "c"]
```

### Batch API

Execute multiple commands in a single round-trip to reduce per-call overhead:

```python
arr = client.create_object("java.util.ArrayList")

# Batch 100 operations in one round-trip (6x faster than individual calls)
with client.batch() as b:
    for i in range(100):
        b.call(arr, "add", i)
    size_result = b.call(arr, "size")

print(size_result.get())  # 100

# Mix different operation types
with client.batch() as b:
    obj = b.create("java.util.HashMap")
    r1 = b.call_static("java.lang.Integer", "parseInt", "42")
    r2 = b.call_static("java.lang.Math", "max", 10, 20)

print(r1.get())  # 42
print(r2.get())  # 20

# Error handling: continue on error (default) or stop on first error
with client.batch(stop_on_error=True) as b:
    r1 = b.call(arr, "add", "valid")
    r2 = b.call_static("java.lang.Integer", "parseInt", "invalid")  # Will error
    r3 = b.call(arr, "size")  # Skipped when stop_on_error=True
```

### Vectorized APIs

For even faster bulk operations on the same target (2-5x speedup over batch):

```python
# invoke_methods - Multiple calls on same object in one round-trip
arr = client.create_object("java.util.ArrayList")
results = client.invoke_methods(arr, [
    ("add", ("a",)),
    ("add", ("b",)),
    ("add", ("c",)),
    ("size", ()),
])
# results = [True, True, True, 3]

# create_objects - Create multiple objects in one round-trip
list1, map1, set1 = client.create_objects([
    ("java.util.ArrayList", ()),
    ("java.util.HashMap", ()),
    ("java.util.HashSet", ()),
])

# get_fields - Read multiple fields from one object
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

### JavaArray for Array Round-Tripping

When Java returns arrays, they're wrapped as `JavaArray` to preserve type when passed back:

```python
from gatun import JavaArray

# Arrays from Java are JavaArray instances
arr = client.jvm.java.util.ArrayList()
arr.add("x")
arr.add("y")
java_array = arr.toArray()  # Returns JavaArray

# JavaArray acts like a list
print(list(java_array))  # ["x", "y"]

# But preserves array type for Java methods
result = client.jvm.java.util.Arrays.toString(java_array)  # "[x, y]"

# Create typed arrays manually
int_array = JavaArray([1, 2, 3], element_type="Int")
```

### Arrow Data Transfer

```python
import pyarrow as pa

# Simple: IPC format
table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
result = client.send_arrow_table(table)  # "Received 3 rows"

# Zero-copy buffer transfer (optimal for large data)
arena = client.get_payload_arena()
schema_cache = {}
for batch in batches:
    arena.reset()
    client.send_arrow_buffers(batch, arena, schema_cache)
arena.close()
```

### Low-Level API

For direct control:

```python
# Create objects
obj = client.create_object("java.util.ArrayList")
obj = client.create_object("java.util.ArrayList", 100)  # with capacity

# Invoke methods
client.invoke_method(obj.object_id, "add", "item")
result = client.invoke_static_method("java.lang.Math", "max", 10, 20)

# Access fields
size = client.get_field(obj, "size")
client.set_field(obj, "fieldName", value)

# Vectorized operations (single round-trip for multiple operations)
client.get_fields(obj, ["field1", "field2"])
client.invoke_methods(obj, [("method1", (arg,)), ("method2", ())])
client.create_objects([("class1", ()), ("class2", (arg,))])
```

### Observability

Get server metrics for debugging and monitoring:

```python
# Get server metrics report
metrics = client.get_metrics()
print(metrics)
# === Gatun Server Metrics ===
# Global:
#   total_requests: 150
#   total_errors: 0
#   requests_per_sec: 45.23
#   current_sessions: 1
#   current_objects: 12
#   peak_objects: 25
# ...
```

Enable trace mode for method resolution debugging:

```python
# Enable trace mode
client = connect(trace=True)

# Enable verbose logging
client = connect(log_level="FINE")
```

Or via environment variables:

```bash
export GATUN_TRACE=true
export GATUN_LOG_LEVEL=FINE
```

## PySpark Integration

Use Gatun as the JVM communication backend for PySpark:

```bash
# Enable Gatun backend
export PYSPARK_USE_GATUN=true
export GATUN_MEMORY=256MB

# Run PySpark normally
python my_spark_app.py
```

Or use the BridgeAdapter API directly:

```python
from gatun.bridge_adapters import GatunAdapter

# Create bridge (launches JVM)
bridge = GatunAdapter(memory="256MB")

# Use bridge API
obj = bridge.new("java.util.ArrayList")
bridge.call(obj, "add", "hello")
result = bridge.call_static("java.lang.Math", "max", 10, 20)

# Array operations
arr = bridge.new_array("java.lang.String", 3)
bridge.array_set(arr, 0, "hello")
bridge.array_get(arr, 0)  # "hello"

bridge.close()
```

## Configuration

Configure via `pyproject.toml`:

```toml
[tool.gatun]
memory = "64MB"
socket_path = "/tmp/gatun.sock"  # Optional: uses random path by default
```

Or environment variables:

```bash
export GATUN_MEMORY=64MB
export GATUN_SOCKET_PATH=/tmp/gatun.sock
```

## Supported Types

| Python | Java |
|--------|------|
| `int` | `int`, `long` |
| `float` | `double` |
| `bool` | `boolean` |
| `str` | `String` |
| `list` | `List` (ArrayList) |
| `dict` | `Map` (HashMap) |
| `bytes` | `byte[]` |
| `JavaArray` | Arrays (`int[]`, `String[]`, etc.) |
| `numpy.ndarray` | Typed arrays |
| `None` | `null` |
| `JavaObject` | Object reference |

## Exception Handling

Java exceptions are mapped to Python exceptions:

```python
from gatun import (
    JavaException,
    JavaSecurityException,
    JavaIllegalArgumentException,
    JavaNoSuchMethodException,
    JavaClassNotFoundException,
    JavaNullPointerException,
    JavaIndexOutOfBoundsException,
    JavaNumberFormatException,
)

try:
    client.jvm.java.lang.Integer.parseInt("not_a_number")
except JavaNumberFormatException as e:
    print(f"Parse error: {e}")
```

## Architecture

Gatun uses a client-server architecture with shared memory for high-performance IPC:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Python Client                            │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐  │
│  │ GatunClient │  │ AsyncClient  │  │    BridgeAdapter       │  │
│  └──────┬──────┘  └──────┬───────┘  └───────────┬────────────┘  │
│         └────────────────┼──────────────────────┘               │
│                          ▼                                       │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              FlatBuffers Serialization                     │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │ Unix Domain Socket (length prefix)
                             │ + Shared Memory (command/response data)
┌────────────────────────────▼────────────────────────────────────┐
│                          Java Server                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                      GatunServer                           │  │
│  │  - Command dispatch (create, invoke, field access, etc.)  │  │
│  │  - Object registry and session management                  │  │
│  │  - Security allowlist enforcement                          │  │
│  └───────────────────────────────────────────────────────────┘  │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────────┐  │
│  │ ReflectionCache │ │ MethodResolver  │ │ ArrowMemoryHandler│  │
│  │ - Method cache  │ │ - Overload res. │ │ - Arrow IPC       │  │
│  │ - Constructor   │ │ - Varargs       │ │ - Zero-copy xfer  │  │
│  │ - Field cache   │ │ - Type compat.  │ │                   │  │
│  └─────────────────┘ └─────────────────┘ └──────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Communication Flow

1. Python serializes command to FlatBuffers, writes to shared memory
2. Length prefix sent over Unix socket signals Java to process
3. Java reads command from shared memory, executes, writes response
4. Response length sent back over socket
5. Python reads response from shared memory

### Memory Layout

- Command zone: offset 0 (Python writes, Java reads)
- Payload zone: offset 4096 (Arrow data)
- Response zone: last 4KB (Java writes, Python reads)

## Development

```bash
cd python
JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv sync  # Install deps and build JAR
uv run pytest              # Run tests
uv run ruff check .        # Lint
uv run ruff format .       # Format
```

The `uv sync` command automatically builds the Java JAR via the custom build backend.

### Project Structure

```
gatun/
├── python/
│   └── src/gatun/         # Python client library
│       ├── client.py      # Sync client
│       ├── async_client.py# Async client
│       ├── launcher.py    # Server process management
│       └── bridge.py      # BridgeAdapter interface
├── gatun-core/
│   └── src/main/java/org/gatun/server/
│       ├── GatunServer.java       # Main server
│       ├── ReflectionCache.java   # Caching layer
│       ├── MethodResolver.java    # Method resolution
│       └── ArrowMemoryHandler.java# Arrow integration
└── schemas/
    └── commands.fbs       # FlatBuffers protocol schema
```

## License

Apache 2.0
