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
- **Py4J Compatible**: Drop-in replacement for Py4J in many cases
- **PySpark Integration**: Use as backend for PySpark via BridgeAdapter
- **Pythonic JavaObjects**: Iteration, indexing, and len() support on Java collections

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
```

## Py4J Compatibility

For migrating from Py4J (e.g., PySpark):

```python
from gatun.py4j_compat import JavaGateway, launch_gateway, java_import

# Launch gateway (starts Gatun server)
gateway = launch_gateway()

# Or use context manager
with JavaGateway() as gateway:
    ArrayList = gateway.jvm.java.util.ArrayList
    arr = ArrayList()
    arr.add("hello")
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

## Development

```bash
cd python
JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv sync  # Install deps and build JAR
uv run pytest              # Run tests
uv run ruff check .        # Lint
uv run ruff format .       # Format
```

## License

Apache 2.0
