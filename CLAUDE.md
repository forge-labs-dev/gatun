# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Gatun is a high-performance Python-to-Java bridge using shared memory (mmap) and Unix domain sockets for inter-process communication. It uses FlatBuffers for serialization and Apache Arrow for efficient bulk data transfer.

## Architecture

### Components
- **gatun-core** (Java): Server that manages Java objects and handles RPC requests
- **python/src/gatun** (Python): Client library with sync/async clients, launcher, and config
- **schemas/commands.fbs**: FlatBuffers schema defining the wire protocol

### Communication Flow
1. Python launcher starts Java server subprocess with shared memory file
2. Client connects via Unix domain socket, receives protocol version and memory size in handshake
3. Commands are serialized to FlatBuffers, written to shared memory
4. Length prefix sent over socket signals Java to process command
5. Response written to shared memory, length sent back to Python

### Memory Layout
- Command zone: offset 0
- Payload zone (Arrow data): offset 4096
- Response zone: last 4KB of shared memory

### Protocol Handshake
Format: `[4 bytes: version][4 bytes: reserved][8 bytes: memory size]`
- Protocol version (currently 1) is verified on connect
- Version mismatch raises RuntimeError with upgrade instructions

## Build Commands

### Python (from python/ directory)
```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv sync    # Install deps and build JAR
JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv sync --reinstall-package gatun  # Rebuild after schema/Java changes
uv run pytest                        # Run all tests
uv run pytest tests/test_gatun_core.py::test_name  # Run single test
uv run ruff check .                  # Lint
uv run ruff format .                 # Format
```

### Java (from repository root, if needed separately)
```bash
./gradlew :gatun-core:build          # Build the Java server
./gradlew :gatun-core:shadowJar      # Build fat JAR (gatun-server-all.jar)
./gradlew :gatun-core:run            # Run the server directly
```

## Generated Code

FlatBuffers code is generated from `schemas/commands.fbs`:
- Java: `gatun-core/src/main/java/org/gatun/protocol/`
- Python: `python/src/gatun/generated/org/gatun/protocol/`

The build backend (`python/gatun_build_backend.py`) automatically regenerates FlatBuffers code when the schema changes.

## Key Implementation Details

- Java 21 with preview features required (`--enable-preview`)
- Arrow memory requires JVM flags: `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
- Python client uses `weakref.finalize` for automatic Java object cleanup
- Socket path: Generated randomly in `/tmp/gatun_<pid>_<random>.sock` by default
- Use `T | None` syntax instead of `Optional[T]`
- Python 3.13+ only (no fallback imports like `tomli`)

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
The `client.jvm` property provides package-style navigation similar to Py4J:
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

### java_import (Py4J Compatible)
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

### is_instance_of (Py4J Compatible)
Check if a Java object is an instance of a class (equivalent to `instanceof`):
```python
arr = client.create_object("java.util.ArrayList")
client.is_instance_of(arr, "java.util.List")       # True
client.is_instance_of(arr, "java.util.Collection") # True
client.is_instance_of(arr, "java.util.Map")        # False
```

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
```

### Arrow Data Transfer
Two methods for transferring Arrow data:

#### IPC Format (Simple)
```python
import pyarrow as pa

table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
result = client.send_arrow_table(table)  # "Received 3 rows"
```

#### Zero-Copy Buffer Transfer (Optimal)
For maximum performance, use buffer descriptors to avoid IPC serialization:
```python
import pyarrow as pa

table = pa.table({"name": ["Alice", "Bob"], "age": [25, 30]})

# Get arena backed by client's shared memory
arena = client.get_payload_arena()
schema_cache = {}

# Send multiple tables efficiently
for batch in batches:
    arena.reset()  # Reuse arena for each batch
    client.send_arrow_buffers(batch, arena, schema_cache)

arena.close()
```

Data flow (Python -> Java):
1. Python copies Arrow buffers into shared memory (one copy)
2. Python sends buffer descriptors (offsets/lengths) to Java
3. Java wraps buffers directly as ArrowBuf (zero-copy read)

### Supported Argument/Return Types
- Primitives: `int`, `long`, `double`, `boolean`
- `String`
- `list` -> Java `List`
- `dict` -> Java `Map`
- `bytes` -> Java `byte[]`
- Object references (returned as `JavaObject` wrappers)
- `null`/`None`

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

## Security

### Class Allowlist
Only these classes can be instantiated or used for static methods (hardcoded in GatunServer.java):
- Collections: `java.util.ArrayList`, `LinkedList`, `HashMap`, `LinkedHashMap`, `TreeMap`, `HashSet`, `LinkedHashSet`, `TreeSet`, `Collections`, `Arrays`
- Strings: `java.lang.String`, `StringBuilder`, `StringBuffer`
- Primitives: `java.lang.Integer`, `Long`, `Double`, `Boolean`, `Math`
- Spark/Scala: Classes under `org.apache.spark.*` and `scala.*` prefixes are allowed

Attempting to use non-allowlisted classes (e.g., `Runtime`, `ProcessBuilder`) raises `SecurityException`.

### Session Isolation
- Each client session tracks its own object IDs
- Objects are automatically cleaned up when session ends
- Double-free attempts are silently ignored

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

## Py4J Compatibility Layer

For migrating from Py4J (e.g., PySpark integration):
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
