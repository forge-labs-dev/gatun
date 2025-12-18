# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Gatun is a high-performance Python-to-Java bridge using shared memory (mmap) and Unix domain sockets for inter-process communication. It uses FlatBuffers for serialization and Apache Arrow for zero-copy data transfer.

## Architecture

### Components
- **gatun-core** (Java): Server that manages Java objects and handles RPC requests
- **python/src/gatun** (Python): Client library with launcher and client modules
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
JAVA_HOME=/opt/homebrew/opt/openjdk@21 uv sync --reinstall-package gatun  # Rebuild JAR after Java changes
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

## Key Implementation Details

- Java 21 with preview features required (`--enable-preview`)
- Arrow memory requires JVM flags: `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
- Python client uses `weakref.finalize` for automatic Java object cleanup
- Default socket path: `~/gatun.sock`, shared memory: `~/gatun.sock.shm`

## Supported Operations

### Object Creation
```python
client.create_object("java.util.ArrayList")           # No-arg constructor
client.create_object("java.util.ArrayList", 100)      # With initial capacity
client.create_object("java.lang.StringBuilder", "Hello")  # With string arg
```

### Method Invocation
```python
obj.method_name(arg1, arg2, ...)  # Via __getattr__ proxy
client.invoke_method(object_id, "methodName", arg1, arg2, ...)  # Direct
```

### Static Method Invocation
```python
client.invoke_static_method("java.lang.String", "valueOf", 42)  # "42"
client.invoke_static_method("java.lang.Integer", "parseInt", "123")  # 123
client.invoke_static_method("java.lang.Math", "max", 10, 20)  # 20
```

### Field Access
```python
client.get_field(obj, "fieldName")        # Get field value (works with private fields)
client.set_field(obj, "fieldName", value) # Set field value
```

### Supported Argument/Return Types
- Primitives: `int`, `long`, `double`, `boolean`
- `String`
- Object references (returned as `JavaObject` wrappers)
- `null`/`None`

## Security

### Class Allowlist
Only these classes can be instantiated or used for static methods (hardcoded in GatunServer.java):
- Collections: `java.util.ArrayList`, `LinkedList`, `HashMap`, `LinkedHashMap`, `TreeMap`, `HashSet`, `LinkedHashSet`, `TreeSet`, `Collections`
- Strings: `java.lang.String`, `StringBuilder`, `StringBuffer`
- Primitives: `java.lang.Integer`, `Long`, `Double`, `Boolean`, `Math`

Attempting to use non-allowlisted classes (e.g., `Runtime`, `ProcessBuilder`) raises `SecurityException`.

### Session Isolation
- Each client session tracks its own object IDs
- Objects are automatically cleaned up when session ends
- Double-free attempts are silently ignored

## Logging

- Java: `java.util.logging` (Logger for `GatunServer`)
- Python: `logging` module (loggers for `gatun.client` and `gatun.launcher`)
