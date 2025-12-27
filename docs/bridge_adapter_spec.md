# PySpark Bridge Adapter Specification

## Overview

This document defines the minimal bridge contract between PySpark and a JVM backend (Py4J or Gatun).
Instead of making Gatun emulate Py4J, we define a clean abstraction that both can implement.

## BridgeAdapter Protocol

```python
from typing import Protocol, Any, Callable, runtime_checkable

@runtime_checkable
class JVMRef(Protocol):
    """Opaque reference to a JVM object."""
    pass

@runtime_checkable
class BridgeAdapter(Protocol):
    """Minimal bridge contract for PySpark <-> JVM communication."""

    # === Object Lifecycle ===

    def new(self, class_name: str, *args: Any) -> JVMRef:
        """Create a new JVM object.

        Args:
            class_name: Fully qualified class name (e.g., "java.util.ArrayList")
            *args: Constructor arguments (Python types auto-converted)

        Returns:
            Reference to the created object
        """
        ...

    def close(self) -> None:
        """Close the bridge and release all resources."""
        ...

    def detach(self, ref: JVMRef) -> None:
        """Prevent automatic cleanup of this object reference.

        Used when passing objects to long-lived Java structures.
        """
        ...

    # === Method Calls ===

    def call(self, ref: JVMRef, method: str, *args: Any) -> Any:
        """Call an instance method on a JVM object.

        Args:
            ref: Object reference
            method: Method name
            *args: Method arguments

        Returns:
            Method result (JVMRef for objects, Python types for primitives)
        """
        ...

    def call_static(self, class_name: str, method: str, *args: Any) -> Any:
        """Call a static method on a JVM class.

        Args:
            class_name: Fully qualified class name
            method: Static method name
            *args: Method arguments

        Returns:
            Method result
        """
        ...

    # === Field Access ===

    def get_field(self, ref: JVMRef, name: str) -> Any:
        """Get an instance field value."""
        ...

    def set_field(self, ref: JVMRef, name: str, value: Any) -> None:
        """Set an instance field value."""
        ...

    def get_static_field(self, class_name: str, name: str) -> Any:
        """Get a static field value."""
        ...

    def set_static_field(self, class_name: str, name: str, value: Any) -> None:
        """Set a static field value."""
        ...

    # === Type Checking ===

    def is_instance_of(self, ref: JVMRef, class_name: str) -> bool:
        """Check if object is instance of class (supports interfaces)."""
        ...

    # === Arrays ===

    def new_array(self, element_class: str, length: int) -> JVMRef:
        """Create a new JVM array.

        Args:
            element_class: Element type (e.g., "java.lang.String", "int", "long")
            length: Array length

        Returns:
            Reference to the array (supports indexing via array_get/array_set)
        """
        ...

    def array_get(self, array_ref: JVMRef, index: int) -> Any:
        """Get element at index from JVM array."""
        ...

    def array_set(self, array_ref: JVMRef, index: int, value: Any) -> None:
        """Set element at index in JVM array."""
        ...

    def array_length(self, array_ref: JVMRef) -> int:
        """Get length of JVM array."""
        ...

    # === JVM View (Optional but Convenient) ===

    @property
    def jvm(self) -> "JVMView":
        """Get JVM view for navigating classes.

        Allows: bridge.jvm.java.util.ArrayList()
        Instead of: bridge.new("java.util.ArrayList")
        """
        ...

    def java_import(self, package: str) -> None:
        """Import package for shorter class names.

        Args:
            package: Package path with optional wildcard (e.g., "java.util.*")
        """
        ...
```

## Type Conversion Rules

### Python → JVM

| Python Type | JVM Type |
|-------------|----------|
| `int` (fits in 32 bits) | `int` |
| `int` (larger) | `long` |
| `float` | `double` |
| `bool` | `boolean` |
| `str` | `String` |
| `bytes` | `byte[]` |
| `None` | `null` |
| `list` | `ArrayList` |
| `dict` | `HashMap` |
| `JVMRef` | (pass through) |
| `datetime.date` | `java.sql.Date` |
| `datetime.datetime` | `java.sql.Timestamp` |

### JVM → Python

| JVM Type | Python Type |
|----------|-------------|
| `int`, `short`, `byte` | `int` |
| `long` | `int` |
| `float`, `double` | `float` |
| `boolean` | `bool` |
| `String` | `str` |
| `byte[]` | `bytes` |
| `null` | `None` |
| `List`, `ArrayList` | `list` |
| `Map`, `HashMap` | `dict` |
| Arrays (primitive) | `list` |
| Arrays (object) | `list` |
| Other objects | `JVMRef` |

## Exception Handling

```python
class JavaException(Exception):
    """Base class for Java exceptions raised in Python."""

    def __init__(self, class_name: str, message: str, stack_trace: str):
        self.java_class = class_name
        self.message = message
        self.stack_trace = stack_trace
        super().__init__(f"{class_name}: {message}")
```

Common subclasses:
- `JavaSecurityException`
- `JavaIllegalArgumentException`
- `JavaNoSuchMethodException`
- `JavaClassNotFoundException`
- `JavaNullPointerException`
- `JavaIndexOutOfBoundsException`

## Contract Tests

These tests define the required behavior. Both Py4JAdapter and GatunAdapter must pass.

```python
import pytest
from typing import Protocol

class BridgeContractTests:
    """Contract tests that any BridgeAdapter implementation must pass."""

    @pytest.fixture
    def bridge(self) -> BridgeAdapter:
        """Override in subclass to provide adapter instance."""
        raise NotImplementedError

    # === Object Creation ===

    def test_create_object_no_args(self, bridge):
        """Create object with no-arg constructor."""
        obj = bridge.new("java.util.ArrayList")
        assert obj is not None

    def test_create_object_with_args(self, bridge):
        """Create object with constructor arguments."""
        obj = bridge.new("java.util.ArrayList", 100)
        assert obj is not None

    def test_create_object_with_string_arg(self, bridge):
        """Create object with string argument."""
        obj = bridge.new("java.lang.StringBuilder", "hello")
        result = bridge.call(obj, "toString")
        assert result == "hello"

    # === Method Calls ===

    def test_call_instance_method_no_args(self, bridge):
        """Call instance method with no arguments."""
        obj = bridge.new("java.util.ArrayList")
        size = bridge.call(obj, "size")
        assert size == 0

    def test_call_instance_method_with_args(self, bridge):
        """Call instance method with arguments."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", "hello")
        size = bridge.call(obj, "size")
        assert size == 1

    def test_call_instance_method_returns_object(self, bridge):
        """Method returning object returns JVMRef."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", "hello")
        result = bridge.call(obj, "get", 0)
        assert result == "hello"

    def test_call_static_method(self, bridge):
        """Call static method."""
        result = bridge.call_static("java.lang.Integer", "parseInt", "42")
        assert result == 42

    def test_call_static_method_math(self, bridge):
        """Call Math static methods."""
        result = bridge.call_static("java.lang.Math", "max", 10, 20)
        assert result == 20

    def test_call_chained_methods(self, bridge):
        """Chain multiple method calls."""
        sb = bridge.new("java.lang.StringBuilder")
        bridge.call(sb, "append", "hello")
        bridge.call(sb, "append", " ")
        bridge.call(sb, "append", "world")
        result = bridge.call(sb, "toString")
        assert result == "hello world"

    # === Field Access ===

    def test_get_static_field(self, bridge):
        """Get static field value."""
        result = bridge.get_static_field("java.lang.Integer", "MAX_VALUE")
        assert result == 2147483647

    def test_get_static_field_object(self, bridge):
        """Get static field that is an object."""
        result = bridge.get_static_field("java.lang.System", "out")
        assert result is not None

    # === Type Checking ===

    def test_is_instance_of_true(self, bridge):
        """is_instance_of returns True for matching class."""
        obj = bridge.new("java.util.ArrayList")
        assert bridge.is_instance_of(obj, "java.util.ArrayList")

    def test_is_instance_of_interface(self, bridge):
        """is_instance_of works with interfaces."""
        obj = bridge.new("java.util.ArrayList")
        assert bridge.is_instance_of(obj, "java.util.List")
        assert bridge.is_instance_of(obj, "java.util.Collection")

    def test_is_instance_of_false(self, bridge):
        """is_instance_of returns False for non-matching class."""
        obj = bridge.new("java.util.ArrayList")
        assert not bridge.is_instance_of(obj, "java.util.HashMap")

    # === Arrays ===

    def test_new_array_object(self, bridge):
        """Create object array."""
        arr = bridge.new_array("java.lang.String", 3)
        assert bridge.array_length(arr) == 3

    def test_new_array_primitive(self, bridge):
        """Create primitive array."""
        arr = bridge.new_array("int", 5)
        assert bridge.array_length(arr) == 5

    def test_array_set_get(self, bridge):
        """Set and get array elements."""
        arr = bridge.new_array("java.lang.String", 3)
        bridge.array_set(arr, 0, "hello")
        bridge.array_set(arr, 1, "world")
        assert bridge.array_get(arr, 0) == "hello"
        assert bridge.array_get(arr, 1) == "world"
        assert bridge.array_get(arr, 2) is None

    def test_array_pass_to_method(self, bridge):
        """Pass array to Java method."""
        arr = bridge.new_array("java.lang.String", 2)
        bridge.array_set(arr, 0, "b")
        bridge.array_set(arr, 1, "a")
        # Arrays.sort(arr)
        bridge.call_static("java.util.Arrays", "sort", arr)
        assert bridge.array_get(arr, 0) == "a"
        assert bridge.array_get(arr, 1) == "b"

    # === Type Conversion ===

    def test_convert_int(self, bridge):
        """Integer conversion."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", 42)
        result = bridge.call(obj, "get", 0)
        assert result == 42

    def test_convert_long(self, bridge):
        """Long integer conversion."""
        obj = bridge.new("java.util.ArrayList")
        big_num = 9223372036854775807  # Long.MAX_VALUE
        bridge.call(obj, "add", big_num)
        result = bridge.call(obj, "get", 0)
        assert result == big_num

    def test_convert_float(self, bridge):
        """Float conversion."""
        result = bridge.call_static("java.lang.Math", "sqrt", 16.0)
        assert result == 4.0

    def test_convert_bool(self, bridge):
        """Boolean conversion."""
        obj = bridge.new("java.util.ArrayList")
        result = bridge.call(obj, "isEmpty")
        assert result is True
        bridge.call(obj, "add", "x")
        result = bridge.call(obj, "isEmpty")
        assert result is False

    def test_convert_string(self, bridge):
        """String conversion."""
        result = bridge.call_static("java.lang.String", "valueOf", 123)
        assert result == "123"

    def test_convert_none(self, bridge):
        """None/null conversion."""
        obj = bridge.new("java.util.HashMap")
        result = bridge.call(obj, "get", "nonexistent")
        assert result is None

    def test_convert_list(self, bridge):
        """List conversion."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "addAll", [1, 2, 3])
        size = bridge.call(obj, "size")
        assert size == 3

    def test_convert_dict(self, bridge):
        """Dict conversion."""
        obj = bridge.new("java.util.HashMap")
        bridge.call(obj, "putAll", {"a": 1, "b": 2})
        result = bridge.call(obj, "get", "a")
        assert result == 1

    def test_convert_bytes(self, bridge):
        """Bytes conversion."""
        data = b"hello"
        result = bridge.call_static("java.util.Arrays", "toString", data)
        # byte[] toString gives something like "[104, 101, 108, 108, 111]"
        assert "104" in result  # 'h' = 104

    # === Exception Handling ===

    def test_exception_class_not_found(self, bridge):
        """ClassNotFoundException raised properly."""
        with pytest.raises(JavaException) as exc_info:
            bridge.new("com.nonexistent.FakeClass")
        assert "ClassNotFoundException" in str(exc_info.value)

    def test_exception_no_such_method(self, bridge):
        """NoSuchMethodException raised properly."""
        obj = bridge.new("java.util.ArrayList")
        with pytest.raises(JavaException) as exc_info:
            bridge.call(obj, "nonExistentMethod")
        assert "NoSuchMethod" in str(exc_info.value) or "not found" in str(exc_info.value).lower()

    def test_exception_null_pointer(self, bridge):
        """NullPointerException raised properly."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", None)
        with pytest.raises(JavaException):
            # Calling method on null element
            elem = bridge.call(obj, "get", 0)
            bridge.call(elem, "toString")  # elem is None/null

    def test_exception_index_out_of_bounds(self, bridge):
        """IndexOutOfBoundsException raised properly."""
        obj = bridge.new("java.util.ArrayList")
        with pytest.raises(JavaException) as exc_info:
            bridge.call(obj, "get", 0)
        assert "IndexOutOfBounds" in str(exc_info.value) or "index" in str(exc_info.value).lower()

    def test_exception_has_stack_trace(self, bridge):
        """Exceptions include Java stack trace."""
        with pytest.raises(JavaException) as exc_info:
            bridge.new("com.nonexistent.FakeClass")
        assert exc_info.value.stack_trace is not None
        assert len(exc_info.value.stack_trace) > 0

    # === Object Lifecycle ===

    def test_detach_prevents_cleanup(self, bridge):
        """Detached objects remain valid."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", "hello")
        bridge.detach(obj)
        # Object should still be usable after detach
        result = bridge.call(obj, "get", 0)
        assert result == "hello"

    def test_close_releases_resources(self, bridge):
        """Close releases resources."""
        bridge.close()
        # After close, operations should fail
        with pytest.raises(Exception):
            bridge.new("java.util.ArrayList")

    # === JVM View ===

    def test_jvm_view_create_object(self, bridge):
        """JVM view can create objects."""
        arr = bridge.jvm.java.util.ArrayList()
        bridge.call(arr, "add", "test")
        assert bridge.call(arr, "size") == 1

    def test_jvm_view_static_method(self, bridge):
        """JVM view can call static methods."""
        result = bridge.jvm.java.lang.Integer.parseInt("42")
        assert result == 42

    def test_jvm_view_static_field(self, bridge):
        """JVM view can access static fields."""
        result = bridge.jvm.java.lang.Integer.MAX_VALUE
        assert result == 2147483647

    def test_java_import_wildcard(self, bridge):
        """java_import with wildcard."""
        bridge.java_import("java.util.*")
        arr = bridge.jvm.ArrayList()
        assert bridge.call(arr, "size") == 0

    def test_java_import_single_class(self, bridge):
        """java_import single class."""
        bridge.java_import("java.lang.StringBuilder")
        sb = bridge.jvm.StringBuilder("hello")
        assert bridge.call(sb, "toString") == "hello"

    # === Scala Interop (for Spark) ===

    def test_scala_object_access(self, bridge):
        """Access Scala singleton objects."""
        # scala.None$ is the singleton for scala.None
        none = bridge.get_static_field("scala.None$", "MODULE$")
        assert none is not None

    def test_scala_tuple(self, bridge):
        """Create and access Scala tuples."""
        t = bridge.new("scala.Tuple2", "key", "value")
        assert bridge.call(t, "_1") == "key"
        assert bridge.call(t, "_2") == "value"

    # === Iteration ===

    def test_iterate_collection(self, bridge):
        """Iterate over Java collection."""
        arr = bridge.new("java.util.ArrayList")
        bridge.call(arr, "add", "a")
        bridge.call(arr, "add", "b")
        bridge.call(arr, "add", "c")

        # Get iterator
        it = bridge.call(arr, "iterator")
        items = []
        while bridge.call(it, "hasNext"):
            items.append(bridge.call(it, "next"))

        assert items == ["a", "b", "c"]

    def test_iterate_array(self, bridge):
        """Iterate over Java array."""
        arr = bridge.new_array("java.lang.String", 3)
        bridge.array_set(arr, 0, "x")
        bridge.array_set(arr, 1, "y")
        bridge.array_set(arr, 2, "z")

        items = []
        for i in range(bridge.array_length(arr)):
            items.append(bridge.array_get(arr, i))

        assert items == ["x", "y", "z"]
```

## PySpark Integration Points

### Current Usage → Bridge Adapter Migration

| Current Pattern | Bridge Adapter Pattern |
|-----------------|----------------------|
| `sc._jvm.ClassName()` | `bridge.jvm.ClassName()` or `bridge.new("ClassName")` |
| `sc._jvm.Class.staticMethod()` | `bridge.jvm.Class.staticMethod()` or `bridge.call_static()` |
| `obj.method(args)` | `bridge.call(obj, "method", args)` |
| `gateway.new_array(cls, n)` | `bridge.new_array("cls", n)` |
| `is_instance_of(obj, cls)` | `bridge.is_instance_of(obj, "cls")` |
| `java_import(jvm, "pkg.*")` | `bridge.java_import("pkg.*")` |

### Key Files to Modify in PySpark

1. `pyspark/java_gateway.py` - Entry point, create adapter
2. `pyspark/core/context.py` - SparkContext uses gateway
3. `pyspark/sql/session.py` - SparkSession uses _jvm
4. `pyspark/sql/types.py` - Type conversions
5. `pyspark/sql/utils.py` - to_java_array function
6. `pyspark/ml/wrapper.py` - ML wrappers use new_array
7. `pyspark/ml/pipeline.py` - Pipeline stages use new_array
