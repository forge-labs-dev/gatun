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

## Installation

```bash
pip install gatun
```

Requires Java 21+.

## Quick Start

```python
import gatun

# Auto-launch server and connect
client = gatun.connect()

# Create Java objects via JVM view
ArrayList = client.jvm.java.util.ArrayList
my_list = ArrayList()
my_list.add("hello")
my_list.add("world")
print(my_list.size())  # 2

# Static methods
result = client.jvm.java.lang.Math.max(10, 20)  # 20

# Cleanup
client.close()
```

### Async Client

```python
import asyncio
import gatun

async def main():
    client = await gatun.aconnect()

    arr = await client.jvm.java.util.ArrayList()
    await arr.add("async")
    print(await arr.size())  # 1

    await client.close()

asyncio.run(main())
```

### Python Callbacks

```python
# Register Python function as Java Comparator
def compare(a, b):
    return -1 if a < b else (1 if a > b else 0)

comparator = client.register_callback(compare, "java.util.Comparator")

# Use with Java Collections
arr = client.jvm.java.util.ArrayList()
arr.add("c")
arr.add("a")
arr.add("b")
client.jvm.java.util.Collections.sort(arr, comparator)
# arr is now ["a", "b", "c"]
```

### Arrow Data Transfer

```python
import pyarrow as pa

# Send Arrow table to Java
table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
client.send_arrow_batch(table)
```

## Configuration

Configure via `pyproject.toml`:

```toml
[tool.gatun]
memory = "64MB"
socket_path = "/tmp/gatun.sock"
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
| `list` | `List` |
| `dict` | `Map` |
| `bytes` | `byte[]` |
| `None` | `null` |
| `JavaObject` | Object reference |

## Development

```bash
cd python
uv sync                    # Install dependencies and build
uv run pytest              # Run tests
uv run ruff check .        # Lint
uv run ruff format .       # Format
```

## License

Apache 2.0
