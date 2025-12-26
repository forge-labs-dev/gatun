# Replacing Py4J in PySpark with Gatun

This document outlines the strategy for integrating Gatun as a replacement for Py4J in Apache Spark's Python interface.

## Current Status

**Phase 1 (API Compatibility Layer) is complete.** The `gatun.py4j_compat` module provides:

- ✅ `JavaGateway`, `ClientServer` - Py4J-compatible gateway wrappers
- ✅ `JavaObject`, `JVMView` - Object and JVM access wrappers
- ✅ `java_import` - Java class import functionality
- ✅ `Py4JJavaError` - Exception class compatible with PySpark's exception handlers
- ✅ Exception conversion to PySpark exceptions (`AnalysisException`, etc.)
- ✅ `is_instance_of` - Type checking for Java objects
- ✅ py4j module patching for transparent PySpark integration

### Tested PySpark Functionality

The following PySpark test categories pass with Gatun:
- Column operations (`test_column.py`)
- DataFrame operations (`test_dataframe.py`)
- Type handling (`test_types.py`)
- Basic SQL operations

## Current Py4J Architecture in PySpark

### Key Files in PySpark

1. **`python/pyspark/java_gateway.py`** - Gateway initialization
   - `launch_gateway()`: Spawns JVM process, establishes Py4J connection
   - `ensure_callback_server_started()`: Enables Java→Python callbacks

2. **`python/pyspark/context.py`** - SparkContext
   - `_gateway`: The Py4J JavaGateway instance (class variable)
   - `_jvm`: Access to Java packages/classes
   - `_jsc`: The JavaSparkContext instance

3. **`python/pyspark/sql/session.py`** - SparkSession
   - `_jsparkSession`: Java SparkSession object
   - Uses `self._jvm.SparkSession(...)` for construction

### Py4J Components Used

| Py4J Class | Purpose |
|------------|---------|
| `JavaGateway` | Main connection to JVM |
| `ClientServer` | Thread-pinned gateway (newer) |
| `JavaObject` | Proxy for Java object instances |
| `JVMView` | Access to static methods/packages |
| `java_import` | Import Java classes into namespace |

### How PySpark Uses Py4J

```python
# Gateway stored as class variable
SparkContext._gateway = launch_gateway(conf)
SparkContext._jvm = SparkContext._gateway.jvm

# Creating Java objects
self._jsc = self._jvm.JavaSparkContext(jconf)

# Calling instance methods
self._jsc.sc().addFile(path)

# Calling static methods
self._jvm.org.apache.spark.util.Utils.getLocalDir(jconf)

# Accessing Java collections
self._jvm.java.util.HashMap()
```

## Gatun API Comparison

| Py4J | Gatun | Status |
|------|-------|--------|
| `JavaGateway` | `GatunClient` | ✅ Equivalent |
| `gateway.jvm` | `client.jvm` | ✅ Equivalent |
| `JavaObject` | `JavaObject` | ✅ Equivalent |
| `JVMView` | `JVMView` | ✅ Equivalent |
| `java_import` | `java_import` | ✅ Equivalent |
| `is_instance_of` | `is_instance_of` | ✅ Equivalent |
| Callback server | `register_callback` | ✅ Equivalent |
| `gateway.close()` | `client.close()` | ✅ Equivalent |

### Key Differences

1. **Communication**: Py4J uses TCP sockets; Gatun uses Unix domain sockets + shared memory
2. **Serialization**: Py4J uses custom text protocol; Gatun uses FlatBuffers
3. **Bulk data**: Py4J serializes through socket; Gatun uses Apache Arrow zero-copy
4. **Process model**: Both spawn a JVM subprocess

## Integration Strategy

### Option A: Drop-in Py4J Replacement (Implemented ✅)

The `gatun.py4j_compat` module provides Py4J-compatible classes:

```python
# Usage in PySpark (via java_gateway.py modification)
from gatun.py4j_compat import JavaGateway, java_import, Py4JJavaError

# Create gateway (starts Gatun server)
gateway = JavaGateway(socket_path="/tmp/gatun.sock", start_server=True)

# Access JVM classes - identical to Py4J API
ArrayList = gateway.jvm.java.util.ArrayList
my_list = ArrayList()
my_list.add("hello")

# Exception handling works with PySpark's exception handlers
try:
    df.select("nonexistent_column")
except AnalysisException as e:
    print(f"Column not found: {e}")

gateway.close()
```

**Key Implementation Details:**

1. **Exception Handling Chain:**
   - Java throws exception → Gatun `JavaException`
   - `py4j_compat` converts to `Py4JJavaError`
   - PySpark's handlers convert to `AnalysisException`, `ParseException`, etc.

2. **py4j Module Patching:**
   - `sys.modules["py4j.protocol"].Py4JJavaError` is patched to Gatun's class
   - PySpark's `except Py4JJavaError:` blocks catch Gatun exceptions
   - Works even when real py4j package is installed

3. **Mock Exception Objects:**
   - `_MockJavaException` provides `getMessage()`, `getCause()`, `getStackTrace()`
   - Supports PySpark's introspection for building error messages

### Option B: Deep Integration (Future)

Modify PySpark to use Gatun natively, leveraging Arrow for DataFrame transfer.

#### Files to Modify in PySpark

1. **`python/pyspark/java_gateway.py`**
   - Replace Py4J imports with Gatun
   - Modify `launch_gateway()` to use Gatun's launcher
   - Return `GatunClient` instead of `JavaGateway`

2. **`python/pyspark/context.py`**
   - Update type hints
   - `_gateway` becomes `GatunClient`
   - `_jvm` stays the same (API compatible)

3. **`python/pyspark/sql/dataframe.py`**
   - Use `send_arrow_buffers()` for DataFrame→JVM transfer
   - Use `get_arrow_data()` for JVM→DataFrame transfer
   - This is where the real performance win is

4. **`core/src/main/scala/org/apache/spark/api/python/`**
   - Need Java-side Gatun server integration
   - Modify `PythonGatewayServer` to use Gatun protocol

#### Key Changes in `java_gateway.py`

```python
# Before (Py4J)
from py4j.java_gateway import JavaGateway, JavaObject, GatewayParameters
from py4j.clientserver import ClientServer, JavaParameters, PythonParameters

def launch_gateway(conf=None, popen_kwargs=None):
    # ... spawn JVM with spark-submit ...
    gateway = ClientServer(
        java_parameters=JavaParameters(port=gateway_port, ...),
        python_parameters=PythonParameters(port=0, ...),
    )
    return gateway

# After (Gatun)
from gatun import GatunClient, java_import
from gatun.launcher import GatunLauncher

def launch_gateway(conf=None, popen_kwargs=None):
    # ... configure from SparkConf ...
    launcher = GatunLauncher(
        socket_path=socket_path,
        memory=memory_size,
        jvm_args=jvm_args,
    )
    launcher.start()

    client = GatunClient(socket_path)
    client.connect()

    # Import Spark classes (same as before)
    java_import(client.jvm, "org.apache.spark.*")
    java_import(client.jvm, "org.apache.spark.api.python.*")

    return client
```

## Java-Side Changes

### Current: PythonGatewayServer

Spark includes `PythonGatewayServer.java` which starts the Py4J gateway on the JVM side.

### Required: GatunServer Integration

Options:

1. **Replace PythonGatewayServer**: New `GatunPythonServer` that extends or wraps `GatunServer`
2. **Extend GatunServer**: Add Spark-specific allowlisted classes and methods

Key additions to allowlist:
- `org.apache.spark.*`
- `org.apache.spark.sql.*`
- `org.apache.spark.api.python.*`
- Scala collections interop

## Arrow Data Transfer

The biggest win from Gatun is zero-copy Arrow transfer for DataFrames.

### Current PySpark Flow (Slow)

```
Python DataFrame → pickle/Arrow IPC → socket → Java deserialize → Spark RDD
```

### Gatun Flow (Fast)

```
Python DataFrame → Arrow buffers → shared memory (zero-copy) → Java ArrowBuf → Spark
```

### Integration Points for Arrow

1. **`toPandas()`**: Already uses Arrow internally, can leverage Gatun's `get_arrow_data()`
2. **`createDataFrame()`**: Can use `send_arrow_buffers()` for pandas→Spark
3. **UDF execution**: Arrow batches via shared memory

## Implementation Phases

### Phase 1: API Compatibility Layer ✅ COMPLETE
- ✅ Create `gatun.py4j_compat` module
- ✅ Verify all Py4J APIs are covered
- ✅ Exception handling compatible with PySpark
- ✅ Run PySpark test subsets with shim

### Phase 2: PySpark Gateway Replacement (In Progress)
- ✅ Modify `pyspark/java_gateway.py` to use Gatun
- ⬜ Add Gatun to Spark's Python dependencies
- ⬜ Comprehensive PySpark test coverage
- ⬜ Performance benchmarks

### Phase 3: Arrow DataFrame Transfer
- Modify `pyspark/sql/dataframe.py`
- Implement zero-copy `toPandas()` and `createDataFrame()`
- Benchmark improvements

### Phase 4: UDF Optimization
- Arrow-based UDF data transfer
- Potential for vectorized UDF improvements

## Security Considerations

Gatun's allowlist approach needs updating for Spark:
- Current allowlist is minimal (collections, primitives)
- Spark needs access to all `org.apache.spark.*` classes
- Consider dynamic allowlist based on SparkConf

## Testing Strategy

1. **Unit tests**: Verify API compatibility
2. **Integration tests**: Full Spark job execution
3. **Performance tests**: Compare with Py4J baseline
4. **Arrow tests**: DataFrame transfer benchmarks

## Expected Performance Gains

| Operation | Current (Py4J) | Expected (Gatun) |
|-----------|----------------|------------------|
| Method call overhead | ~100μs | ~10μs |
| Small object creation | ~200μs | ~20μs |
| DataFrame transfer (1MB) | ~50ms | ~5ms |
| DataFrame transfer (1GB) | ~50s | ~1s |

*Estimates based on shared memory vs socket transfer*

## Next Steps

1. ✅ ~~Create the Py4J compatibility shim~~
2. ✅ ~~Fork PySpark and test with shim~~
3. Expand PySpark test coverage
4. Implement Arrow DataFrame integration
5. Benchmark and optimize
6. Upstream changes to Apache Spark

## References

- [PySpark java_gateway.py](https://github.com/apache/spark/blob/master/python/pyspark/java_gateway.py)
- [Py4J Documentation](https://www.py4j.org/)
- [Py4J java_gateway.py](https://github.com/py4j/py4j/blob/master/py4j-python/src/py4j/java_gateway.py)
- [PySpark Internals](https://books.japila.pl/pyspark-internals/)
