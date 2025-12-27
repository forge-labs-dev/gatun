from __future__ import annotations

import array
import ctypes
import logging
import mmap
import os
import socket
import struct
from typing import TYPE_CHECKING
import weakref

import flatbuffers
import numpy as np
import pyarrow as pa

if TYPE_CHECKING:
    from gatun.arena import PayloadArena

from gatun.generated.org.gatun.protocol import Command as Cmd
from gatun.generated.org.gatun.protocol import Action as Act
from gatun.generated.org.gatun.protocol import (
    Response,
    Value,
    StringVal,
    IntVal,
    DoubleVal,
    BoolVal,
    ObjectRef,
    Argument,
    ListVal,
    MapVal,
    MapEntry,
    ArrayVal,
    ElementType,
    ArrowBatchDescriptor,
    BufferDescriptor,
    FieldNode,
)

logger = logging.getLogger(__name__)

# Protocol version - must match the server version
PROTOCOL_VERSION = 1

# Memory zone sizes - must match GatunServer.java constants
COMMAND_ZONE_SIZE = 65536  # 64KB for commands
RESPONSE_ZONE_SIZE = 65536  # 64KB for responses


class PayloadTooLargeError(Exception):
    """Raised when a payload exceeds the available shared memory space."""

    def __init__(self, payload_size: int, max_size: int, zone: str):
        self.payload_size = payload_size
        self.max_size = max_size
        self.zone = zone
        super().__init__(
            f"{zone} payload too large: {payload_size:,} bytes exceeds "
            f"maximum {max_size:,} bytes. Consider increasing memory size "
            f"when connecting (e.g., gatun.connect(memory='64MB'))"
        )


# --- Java Exception Hierarchy ---
# These exceptions mirror common Java exceptions for better error handling


class JavaException(Exception):
    """Base class for all Java exceptions.

    Attributes:
        java_class: The fully qualified Java exception class name
        message: The exception message
        stack_trace: The full Java stack trace as a string
    """

    def __init__(self, java_class: str, message: str, stack_trace: str):
        self.java_class = java_class
        self.message = message
        self.stack_trace = stack_trace
        super().__init__(stack_trace)


class JavaSecurityException(JavaException):
    """Raised when a security violation occurs (e.g., accessing blocked class)."""

    pass


class JavaIllegalArgumentException(JavaException):
    """Raised when an illegal argument is passed to a Java method."""

    pass


class JavaNoSuchMethodException(JavaException):
    """Raised when a method cannot be found."""

    pass


class JavaNoSuchFieldException(JavaException):
    """Raised when a field cannot be found."""

    pass


class JavaClassNotFoundException(JavaException):
    """Raised when a class cannot be found."""

    pass


class JavaNullPointerException(JavaException):
    """Raised when null is dereferenced in Java."""

    pass


class JavaIndexOutOfBoundsException(JavaException):
    """Raised when an index is out of bounds."""

    pass


class JavaNumberFormatException(JavaException):
    """Raised when a string cannot be parsed as a number."""

    pass


class JavaRuntimeException(JavaException):
    """Raised for generic Java runtime exceptions."""

    pass


class CancelledException(Exception):
    """Raised when a request is cancelled."""

    def __init__(self, request_id: int):
        self.request_id = request_id
        super().__init__(f"Request {request_id} was cancelled")


# Mapping from Java exception class names to Python exception classes
_JAVA_EXCEPTION_MAP: dict[str, type[JavaException]] = {
    "java.lang.SecurityException": JavaSecurityException,
    "java.lang.IllegalArgumentException": JavaIllegalArgumentException,
    "java.lang.NoSuchMethodException": JavaNoSuchMethodException,
    "java.lang.NoSuchFieldException": JavaNoSuchFieldException,
    "java.lang.ClassNotFoundException": JavaClassNotFoundException,
    "java.lang.NullPointerException": JavaNullPointerException,
    "java.lang.IndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.ArrayIndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.StringIndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.NumberFormatException": JavaNumberFormatException,
    "java.lang.RuntimeException": JavaRuntimeException,
    "org.gatun.PayloadTooLargeException": PayloadTooLargeError,
    "java.lang.InterruptedException": CancelledException,
}


def _raise_java_exception_impl(
    error_type: str,
    error_msg: str,
    exception_converter: callable | None = None,
) -> None:
    """Raise the appropriate Python exception for a Java error.

    This is the core implementation used by both sync and async clients.

    Args:
        error_type: The fully qualified Java exception class name
        error_msg: The full error message including stack trace
        exception_converter: Optional function to convert JavaException to another type
                           (e.g., Py4JJavaError for PySpark compatibility)
    """
    import re

    # Handle PayloadTooLargeException specially - parse sizes from message
    if error_type == "org.gatun.PayloadTooLargeException":
        # Message format: "Response too large: X bytes exceeds Y byte limit"
        match = re.search(r"(\d+) bytes exceeds (\d+) byte", error_msg)
        if match:
            payload_size = int(match.group(1))
            max_size = int(match.group(2))
        else:
            payload_size = 0
            max_size = 0
        raise PayloadTooLargeError(payload_size, max_size, "Response")

    # Handle InterruptedException specially - extract request ID
    if error_type == "java.lang.InterruptedException":
        # Message format: "Request X was cancelled"
        match = re.search(r"Request (\d+) was cancelled", error_msg)
        request_id = int(match.group(1)) if match else 0
        raise CancelledException(request_id)

    # Get the exception class, defaulting to JavaException
    exc_class = _JAVA_EXCEPTION_MAP.get(error_type, JavaException)

    # Extract just the message (first line before stack trace)
    message = error_msg.split("\n")[0] if error_msg else ""
    if ": " in message:
        message = message.split(": ", 1)[1]

    # Create the native Gatun exception
    native_exc = exc_class(error_type, message, error_msg)

    # If we have a converter, use it
    if exception_converter is not None:
        raise exception_converter(native_exc) from None

    raise native_exc


def _recv_exactly(sock, n):
    """Receive exactly n bytes from socket, handling partial reads."""
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise RuntimeError("Socket closed unexpectedly")
        data.extend(chunk)
    return bytes(data)


class StaleArenaError(RuntimeError):
    """Raised when accessing Arrow data from a stale arena epoch.

    This error occurs when trying to use a table returned from get_arrow_data()
    after reset_payload_arena() has been called. The underlying shared memory
    buffers may have been overwritten.
    """

    def __init__(self, table_epoch: int, current_epoch: int):
        super().__init__(
            f"Arrow table from epoch {table_epoch} is stale (current epoch: {current_epoch}). "
            f"Tables become invalid after reset_payload_arena() is called."
        )
        self.table_epoch = table_epoch
        self.current_epoch = current_epoch


class ArrowTableView:
    """A wrapper around pa.Table that validates arena epoch on access.

    Tables returned from get_arrow_data() are backed by shared memory that
    can be overwritten after reset_payload_arena(). This wrapper tracks the
    epoch when the table was created and raises StaleArenaError if accessed
    after the arena has been reset.

    For most use cases, immediately copy the data you need from the table
    or call table.to_pandas() / table.to_pydict() before resetting the arena.
    """

    def __init__(self, table: pa.Table, epoch: int, client: "GatunClient"):
        self._table = table
        self._epoch = epoch
        self._client = client

    def _check_epoch(self):
        """Raise StaleArenaError if epoch has changed."""
        current = self._client._arena_epoch
        if self._epoch != current:
            raise StaleArenaError(self._epoch, current)

    @property
    def table(self) -> pa.Table:
        """Get the underlying table, validating epoch first."""
        self._check_epoch()
        return self._table

    def to_pandas(self, **kwargs):
        """Convert to pandas DataFrame (copies data, safe after reset)."""
        self._check_epoch()
        return self._table.to_pandas(**kwargs)

    def to_pydict(self):
        """Convert to Python dict (copies data, safe after reset)."""
        self._check_epoch()
        return self._table.to_pydict()

    def to_pylist(self):
        """Convert to list of dicts (copies data, safe after reset)."""
        self._check_epoch()
        return self._table.to_pylist()

    @property
    def num_rows(self) -> int:
        """Get row count (safe, doesn't access buffer data)."""
        return self._table.num_rows

    @property
    def num_columns(self) -> int:
        """Get column count (safe, doesn't access buffer data)."""
        return self._table.num_columns

    @property
    def schema(self) -> pa.Schema:
        """Get schema (safe, doesn't access buffer data)."""
        return self._table.schema

    @property
    def column_names(self) -> list[str]:
        """Get column names (safe, doesn't access buffer data)."""
        return self._table.column_names

    def column(self, name: str):
        """Get a column by name."""
        self._check_epoch()
        return self._table.column(name)

    def __repr__(self):
        epoch_status = "valid" if self._epoch == self._client._arena_epoch else "STALE"
        return f"<ArrowTableView rows={self.num_rows} cols={self.num_columns} epoch={self._epoch} ({epoch_status})>"


class JavaList(list):
    """A Python list that also exposes Java List-like methods.

    This allows Gatun to auto-convert Java Lists to Python lists while
    still supporting code that expects Java method names like size(), isEmpty(), etc.
    """

    def size(self):
        """Java List.size() - returns the number of elements."""
        return len(self)

    def isEmpty(self):
        """Java List.isEmpty() - returns true if list is empty."""
        return len(self) == 0

    def get(self, index):
        """Java List.get(index) - returns element at index."""
        return self[index]

    def contains(self, item):
        """Java List.contains(item) - returns true if item is in list."""
        return item in self


class JavaArray(list):
    """A Python list that represents a Java array.

    When passed back to Java, this is serialized as an ArrayVal (Java array)
    rather than a ListVal (Java ArrayList). This preserves the array semantics
    when round-tripping through Gatun.

    The element_type attribute stores the original Java array element type.
    """

    def __init__(self, iterable=(), element_type: str = "Object"):
        super().__init__(iterable)
        self.element_type = element_type

    def size(self):
        """Java-style length access."""
        return len(self)

    @property
    def length(self):
        """Java array length property."""
        return len(self)


def _get_own_buffer_count(arrow_type: pa.DataType) -> int:
    """Get the number of buffers owned directly by this Arrow type (not children).

    Arrow types use different numbers of buffers for their own data:
    - Fixed-width primitives (int, float, bool): 2 (validity + data)
    - Variable-width (string, binary): 3 (validity + offsets + data)
    - Null type: 0 (no buffers)
    - List types: 2 (validity + offsets) - child buffers are separate
    - Struct types: 1 (validity only) - child buffers are separate
    - Map types: 2 (validity + offsets) - child buffers are separate

    This is needed to correctly partition buffers when reconstructing arrays.
    """
    if pa.types.is_null(arrow_type):
        return 0  # Null arrays have no buffers
    elif pa.types.is_boolean(arrow_type):
        return 2  # validity + packed bits
    elif pa.types.is_primitive(arrow_type):
        return 2  # validity + data
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return 3  # validity + offsets + data
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return 3  # validity + offsets + data
    elif pa.types.is_fixed_size_binary(arrow_type):
        return 2  # validity + data
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return 2  # validity + offsets (child buffers handled via recursion)
    elif pa.types.is_fixed_size_list(arrow_type):
        return 1  # validity only (child buffers handled via recursion)
    elif pa.types.is_struct(arrow_type):
        return 1  # validity only (child buffers handled via recursion)
    elif pa.types.is_map(arrow_type):
        return 2  # validity + offsets (child buffers handled via recursion)
    elif pa.types.is_union(arrow_type):
        if arrow_type.mode == "sparse":
            return 1  # type_ids only
        else:
            return 2  # type_ids + offsets
    elif pa.types.is_dictionary(arrow_type):
        return 2  # validity + indices (dictionary handled separately)
    else:
        # Default assumption for unknown types
        return 2


def _reconstruct_array_recursive(
    arrow_type: pa.DataType,
    buffers: list,
    field_nodes: list[tuple[int, int]],
    buffer_idx: int,
    node_idx: int,
) -> tuple[pa.Array, int, int]:
    """Recursively reconstruct an Arrow array from buffers and field nodes.

    This handles nested types by recursively building child arrays first,
    then constructing the parent array with the children.

    Args:
        arrow_type: The Arrow data type to reconstruct
        buffers: List of all PyArrow buffers (flattened)
        field_nodes: List of (length, null_count) tuples
        buffer_idx: Current position in buffers list
        node_idx: Current position in field_nodes list

    Returns:
        Tuple of (reconstructed array, next buffer index, next node index)
    """
    # Get this array's field node
    length, null_count = field_nodes[node_idx]
    node_idx += 1

    # Get this type's own buffers
    own_buffer_count = _get_own_buffer_count(arrow_type)
    own_buffers = buffers[buffer_idx : buffer_idx + own_buffer_count]
    buffer_idx += own_buffer_count

    # Handle nested types by recursively building children
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        # List has one child: the values array
        value_type = arrow_type.value_type
        child_array, buffer_idx, node_idx = _reconstruct_array_recursive(
            value_type, buffers, field_nodes, buffer_idx, node_idx
        )
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=[child_array]
        )

    elif pa.types.is_fixed_size_list(arrow_type):
        # Fixed-size list has one child: the values array
        value_type = arrow_type.value_type
        child_array, buffer_idx, node_idx = _reconstruct_array_recursive(
            value_type, buffers, field_nodes, buffer_idx, node_idx
        )
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=[child_array]
        )

    elif pa.types.is_struct(arrow_type):
        # Struct has multiple children: one per field
        children = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            child_array, buffer_idx, node_idx = _reconstruct_array_recursive(
                field.type, buffers, field_nodes, buffer_idx, node_idx
            )
            children.append(child_array)
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=children
        )

    elif pa.types.is_map(arrow_type):
        # Map is internally list<struct<key, value>>
        # We need to reconstruct the struct child which contains key and value
        # The struct type is struct<key: key_type, value: item_type>
        struct_type = pa.struct([
            pa.field("key", arrow_type.key_type, nullable=False),
            pa.field("value", arrow_type.item_type),
        ])
        struct_array, buffer_idx, node_idx = _reconstruct_array_recursive(
            struct_type, buffers, field_nodes, buffer_idx, node_idx
        )
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=[struct_array]
        )

    elif pa.types.is_union(arrow_type):
        # Union has multiple children: one per type code
        children = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            child_array, buffer_idx, node_idx = _reconstruct_array_recursive(
                field.type, buffers, field_nodes, buffer_idx, node_idx
            )
            children.append(child_array)
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=children
        )

    else:
        # Primitive type (no children)
        arr = pa.Array.from_buffers(arrow_type, length, own_buffers, null_count)

    return arr, buffer_idx, node_idx


class JavaObject:
    def __init__(self, client, object_id):
        self.client = client
        self.object_id = object_id
        # When this object is GC'd, tell Java to free the ID
        self._finalizer = weakref.finalize(self, client.free_object, object_id)

    def detach(self):
        """Prevents automatic freeing on GC (useful for manual testing)."""
        self._finalizer.detach()

    def __getattr__(self, name):
        """obj.method(args) -> client.invoke_method(id, method, args)"""

        def method_proxy(*args):
            return self.client.invoke_method(self.object_id, name, *args)

        return method_proxy

    def __str__(self):
        try:
            return self.client.invoke_method(self.object_id, "toString")
        except Exception:
            return f"<Dead JavaObject id={self.object_id}>"


def java_import(jvm_view: "JVMView", import_path: str) -> None:
    """Import Java classes into the JVM view's namespace.

    This is a convenience function similar to Py4J's java_import.
    It allows accessing classes without their full package path.

    Args:
        jvm_view: A JVMView instance (typically client.jvm)
        import_path: Package path with optional wildcard.
                    Examples: "java.util.*", "java.util.ArrayList"

    Example:
        from gatun import java_import

        java_import(client.jvm, "java.util.*")
        # Now you can access:
        arr = client.jvm.ArrayList()  # instead of client.jvm.java.util.ArrayList()

        java_import(client.jvm, "java.lang.Math")
        result = client.jvm.Math.max(1, 2)

    Note:
        Unlike Py4J, this doesn't actually import into a Python namespace.
        It registers shortcuts on the JVM view so that class names can be
        accessed directly without the full package path.
    """
    if not hasattr(jvm_view, "_imports"):
        jvm_view._imports = {}

    if import_path.endswith(".*"):
        # Wildcard import - store the package prefix
        package = import_path[:-2]  # Remove ".*"
        jvm_view._imports[package] = True
    else:
        # Single class import
        # Extract class name from full path
        last_dot = import_path.rfind(".")
        if last_dot != -1:
            class_name = import_path[last_dot + 1 :]
            jvm_view._imports[class_name] = import_path


class JavaClass:
    """Proxy for a Java class. Supports instantiation and static method calls."""

    def __init__(self, client, class_name):
        self._client = client
        self._class_name = class_name

    def __call__(self, *args):
        """Instantiate the class: ArrayList() -> client.create_object('java.util.ArrayList')"""
        return self._client.create_object(self._class_name, *args)

    def __getattr__(self, name):
        """Access static methods: Integer.parseInt(...) -> invoke_static_method(...)"""

        def static_method_proxy(*args):
            return self._client.invoke_static_method(self._class_name, name, *args)

        return static_method_proxy

    def __repr__(self):
        return f"<JavaClass {self._class_name}>"


class JVMView:
    """Navigate Java packages using attribute access.

    Example:
        jvm = client.jvm
        ArrayList = jvm.java.util.ArrayList
        my_list = ArrayList()  # creates instance
        result = jvm.java.lang.Integer.parseInt("123")  # static method

    With java_import:
        from gatun import java_import
        java_import(jvm, "java.util.*")
        my_list = jvm.ArrayList()  # shortcut access
    """

    def __init__(self, client, package_path=""):
        self._client = client
        self._package_path = package_path
        self._imports: dict[str, str | bool] = {}

    def __getattr__(self, name):
        """Navigate deeper into package hierarchy or return a JavaClass."""
        # Check for imported classes first (only at root level)
        if not self._package_path and hasattr(self, "_imports"):
            # Check for direct class import (e.g., java_import(jvm, "java.util.ArrayList"))
            if name in self._imports and isinstance(self._imports[name], str):
                return _JVMNode(self._client, self._imports[name])

            # Check for wildcard imports (e.g., java_import(jvm, "java.util.*"))
            # Only apply wildcards for uppercase names (class names per Java convention)
            # We need to check all wildcard packages and return the first one that
            # actually contains the class, since multiple packages may be imported.
            if name and name[0].isupper():
                for package, is_wildcard in self._imports.items():
                    if is_wildcard is True:
                        # Try the package.name path - verify it exists first
                        full_path = f"{package}.{name}"
                        # Use reflection to check if this class exists
                        if full_path not in _reflect_cache:
                            _reflect_cache[full_path] = self._client.reflect(full_path)
                        if _reflect_cache[full_path] in ("class", "method", "field"):
                            return _JVMNode(self._client, full_path)

        if self._package_path:
            new_path = f"{self._package_path}.{name}"
        else:
            new_path = name

        # Return a new JVMView that can be either a package or a class
        # We use a hybrid object that acts as both
        return _JVMNode(self._client, new_path)

    def __repr__(self):
        if self._package_path:
            return f"<JVMView {self._package_path}>"
        return "<JVMView>"


# Cache for reflection results to avoid repeated Java calls
# Maps fully qualified name -> type ("class", "method", "field", "none")
_reflect_cache: dict[str, str] = {}


class _JVMNode:
    """Hybrid node that can act as both a package path and a Java class.

    - Accessing attributes navigates deeper (jvm.java.util -> jvm.java.util.ArrayList)
    - Calling it instantiates a class (ArrayList() -> new ArrayList())
    - Accessing a method and calling it invokes static method (Integer.parseInt(...))

    This implementation uses Java reflection to correctly identify:
    - Regular Java classes (e.g., java.util.ArrayList)
    - Scala objects (e.g., org.apache.spark.sql.functions)
    - Static methods on classes
    - Static fields on classes

    Results are cached globally for performance.
    """

    def __init__(self, client, path):
        self._client = client
        self._path = path

    def _get_type(self) -> str:
        """Get the type of this path via reflection (cached)."""
        if self._path not in _reflect_cache:
            _reflect_cache[self._path] = self._client.reflect(self._path)
        return _reflect_cache[self._path]

    def __getattr__(self, name):
        """Navigate deeper or access static method/field."""
        new_path = f"{self._path}.{name}"

        # Get the last segment of the current path to check if it looks like a class
        last_segment = self._path.rsplit(".", 1)[-1] if "." in self._path else self._path
        # If current segment starts with uppercase, it likely is a class
        parent_looks_like_class = last_segment and last_segment[0].isupper()

        # Heuristic: if parent looks like a class AND name is ALL_UPPERCASE,
        # it's likely a static field constant - fetch it immediately
        # Examples: MAX_VALUE, EMPTY_LIST, FATAL, INFO, DEBUG
        if parent_looks_like_class and name.isupper():
            # This looks like a constant field - fetch it immediately
            return self._client.get_static_field(self._path, name)

        return _JVMNode(self._client, new_path)

    def __call__(self, *args):
        """Instantiate as a class or invoke as a static method."""
        # Use reflection to determine if this is a class or method
        node_type = self._get_type()

        if node_type == "class":
            # This is a constructor call: java.util.ArrayList()
            return self._client.create_object(self._path, *args)
        elif node_type == "method":
            # This is a static method call: java.lang.Integer.parseInt("42")
            # or Scala object method: functions.col("name")
            last_dot = self._path.rfind(".")
            if last_dot == -1:
                raise ValueError(f"Invalid method path: {self._path}")
            class_name = self._path[:last_dot]
            method_name = self._path[last_dot + 1:]
            return self._client.invoke_static_method(class_name, method_name, *args)
        elif node_type == "field":
            # This is a static field access (being called as function - error)
            raise TypeError(f"{self._path} is a static field, not a method")
        else:
            # Unknown type - use heuristics to decide whether this is a class or method
            # If the path has a dot and the parent segment looks like a class name
            # (starts with uppercase), treat the last segment as a method call.
            # Example: org.apache.spark.api.python.PythonSQLUtils.explainString
            #          -> PythonSQLUtils is the class, explainString is the method
            last_dot = self._path.rfind(".")
            if last_dot != -1:
                parent_path = self._path[:last_dot]
                method_name = self._path[last_dot + 1:]
                parent_last_segment = parent_path.rsplit(".", 1)[-1]

                # If parent looks like a class (starts with uppercase) and
                # method name looks like a method (starts with lowercase),
                # try as static method first
                if (parent_last_segment and parent_last_segment[0].isupper() and
                    method_name and method_name[0].islower()):
                    return self._client.invoke_static_method(
                        parent_path, method_name, *args
                    )

            # Fallback: try as class instantiation (backward compatibility)
            # This handles packages that haven't been loaded yet
            return self._client.create_object(self._path, *args)

    def __repr__(self):
        return f"<JVM {self._path}>"


class GatunClient:
    def __init__(self, socket_path=None, *, py4j_compat: bool = False):
        """Initialize a Gatun client.

        Args:
            socket_path: Path to the Unix domain socket for communication.
                        Defaults to ~/gatun.sock.
            py4j_compat: If True, raise Py4JJavaError instead of JavaException
                        for compatibility with PySpark's exception handling.
        """
        if socket_path is None:
            socket_path = os.path.expanduser("~/gatun.sock")

        self.socket_path = socket_path
        self.memory_path = socket_path + ".shm"

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.shm_file = None
        self.shm = None

        # These are set during connect()
        self.memory_size = 0
        self.command_offset = 0
        self.payload_offset = 65536  # 64KB - must match GatunServer.PAYLOAD_OFFSET
        self.response_offset = 0

        # JVM view for package-style access
        self._jvm = None

        # Callback registry: callback_id -> callable
        self._callbacks: dict[int, callable] = {}

        # Request ID counter for cancellation support
        self._next_request_id = 1

        # Arrow schema cache for Java -> Python transfers: hash -> Schema
        self._arrow_schema_cache: dict[int, pa.Schema] = {}

        # Arena epoch for lifetime safety - tracks current valid epoch
        self._arena_epoch: int = 0

        # Py4J compatibility mode - raise Py4JJavaError instead of JavaException
        self._py4j_compat = py4j_compat
        self._py4j_exception_converter = None

    @property
    def jvm(self):
        """Access Java classes via package navigation.

        Example:
            ArrayList = client.jvm.java.util.ArrayList
            my_list = ArrayList()
            result = client.jvm.java.lang.Integer.parseInt("123")
        """
        if self._jvm is None:
            self._jvm = JVMView(self)
        return self._jvm

    def connect(self):
        logger.debug("Connecting to %s", self.socket_path)
        try:
            self.sock.connect(self.socket_path)
            # Long timeout for Spark jobs which can take minutes
            # None = no timeout (blocking mode)
            self.sock.settimeout(None)

            # 1. Handshake - read version, epoch, and memory size
            # Format: [4 bytes: version] [4 bytes: arena_epoch] [8 bytes: memory size]
            handshake_data = _recv_exactly(self.sock, 16)
            server_version, arena_epoch, self.memory_size = struct.unpack(
                "<IIQ", handshake_data
            )

            # Verify protocol version
            if server_version != PROTOCOL_VERSION:
                raise RuntimeError(
                    f"Protocol version mismatch: client={PROTOCOL_VERSION}, "
                    f"server={server_version}. Please update your client or server."
                )

            # Synchronize arena epoch with server
            self._arena_epoch = arena_epoch

            # 2. Configure Offsets
            # Response zone size must match GatunServer.RESPONSE_ZONE_SIZE (64KB)
            self.response_offset = self.memory_size - 65536

            # 3. Map Memory
            self.shm_file = open(self.memory_path, "r+b")
            self.shm = mmap.mmap(self.shm_file.fileno(), self.memory_size)
            logger.info(
                "Connected to %s (shared memory: %.2f MB)",
                self.socket_path,
                self.memory_size / 1024 / 1024,
            )

            return True
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def get_payload_arena(self) -> "PayloadArena":
        """Get a PayloadArena view into the client's shared memory.

        The returned arena writes to the payload zone of the client's shared
        memory, which is the area that Java reads from. Use this instead of
        creating a separate arena file when using send_arrow_buffers.

        Returns:
            PayloadArena backed by the client's shared memory payload zone.

        Example:
            arena = client.get_payload_arena()
            schema_cache = {}
            for table in tables:
                arena.reset()
                client.send_arrow_buffers(table, arena, schema_cache)
            arena.close()  # Just clears reference, doesn't close the shm
        """
        from gatun.arena import PayloadArena

        # Payload zone: from payload_offset to response_offset
        payload_size = self.response_offset - self.payload_offset
        return PayloadArena.from_mmap(self.shm, self.payload_offset, payload_size)

    def _send_raw(self, data: bytes, wait_for_response=True):
        """
        Writes command to SHM and signals Java.
        If wait_for_response is False, returns immediately (Fire-and-Forget).
        """
        # 1. Validate command size
        max_command_size = self.payload_offset  # Command zone ends where payload starts
        if len(data) > max_command_size:
            raise PayloadTooLargeError(len(data), max_command_size, "Command")

        # 2. Write to Shared Memory
        self.shm.seek(self.command_offset)
        self.shm.write(data)

        # 2. Signal Java (Send Length)
        # Verify socket is open
        if self.sock.fileno() == -1:
            if wait_for_response:
                raise RuntimeError("Socket closed")
            return

        self.sock.sendall(struct.pack("<I", len(data)))

        # 3. Handle Response
        if wait_for_response:
            return self._read_response()

    def _unpack_value(self, val_type, val_table):
        """Unpack a FlatBuffer Value union to a Python object."""
        if val_type == Value.Value.NullVal:
            return None
        elif val_type == Value.Value.StringVal:
            union_obj = StringVal.StringVal()
            union_obj.Init(val_table.Bytes, val_table.Pos)
            return union_obj.V().decode("utf-8")
        elif val_type == Value.Value.IntVal:
            union_obj = IntVal.IntVal()
            union_obj.Init(val_table.Bytes, val_table.Pos)
            return union_obj.V()
        elif val_type == Value.Value.DoubleVal:
            union_obj = DoubleVal.DoubleVal()
            union_obj.Init(val_table.Bytes, val_table.Pos)
            return union_obj.V()
        elif val_type == Value.Value.BoolVal:
            union_obj = BoolVal.BoolVal()
            union_obj.Init(val_table.Bytes, val_table.Pos)
            return union_obj.V()
        elif val_type == Value.Value.ObjectRef:
            union_obj = ObjectRef.ObjectRef()
            union_obj.Init(val_table.Bytes, val_table.Pos)
            return JavaObject(self, union_obj.Id())
        elif val_type == Value.Value.ListVal:
            union_obj = ListVal.ListVal()
            union_obj.Init(val_table.Bytes, val_table.Pos)
            result = JavaList()
            for i in range(union_obj.ItemsLength()):
                item = union_obj.Items(i)
                result.append(self._unpack_value(item.ValType(), item.Val()))
            return result
        elif val_type == Value.Value.MapVal:
            union_obj = MapVal.MapVal()
            union_obj.Init(val_table.Bytes, val_table.Pos)
            result = {}
            for i in range(union_obj.EntriesLength()):
                entry = union_obj.Entries(i)
                key_arg = entry.Key()
                val_arg = entry.Value()
                key = self._unpack_value(key_arg.ValType(), key_arg.Val())
                val = self._unpack_value(val_arg.ValType(), val_arg.Val())
                result[key] = val
            return result
        elif val_type == Value.Value.ArrayVal:
            union_obj = ArrayVal.ArrayVal()
            union_obj.Init(val_table.Bytes, val_table.Pos)
            return self._unpack_array(union_obj)

        return None

    def _unpack_array(self, array_val):
        """Unpack an ArrayVal FlatBuffer to a JavaArray (preserves array semantics)."""
        elem_type = array_val.ElementType()

        if elem_type == ElementType.ElementType.Int:
            return JavaArray(
                (array_val.IntValues(i) for i in range(array_val.IntValuesLength())),
                element_type="Int",
            )
        elif elem_type == ElementType.ElementType.Long:
            return JavaArray(
                (array_val.LongValues(i) for i in range(array_val.LongValuesLength())),
                element_type="Long",
            )
        elif elem_type == ElementType.ElementType.Double:
            return JavaArray(
                (array_val.DoubleValues(i) for i in range(array_val.DoubleValuesLength())),
                element_type="Double",
            )
        elif elem_type == ElementType.ElementType.Float:
            # Float was widened to double
            return JavaArray(
                (array_val.DoubleValues(i) for i in range(array_val.DoubleValuesLength())),
                element_type="Float",
            )
        elif elem_type == ElementType.ElementType.Bool:
            return JavaArray(
                (array_val.BoolValues(i) for i in range(array_val.BoolValuesLength())),
                element_type="Bool",
            )
        elif elem_type == ElementType.ElementType.Byte:
            # Byte arrays are still returned as bytes for convenience
            return bytes(
                [array_val.ByteValues(i) for i in range(array_val.ByteValuesLength())]
            )
        elif elem_type == ElementType.ElementType.Short:
            # Short was widened to int
            return JavaArray(
                (array_val.IntValues(i) for i in range(array_val.IntValuesLength())),
                element_type="Short",
            )
        elif elem_type == ElementType.ElementType.String:
            result = JavaArray(element_type="String")
            for i in range(array_val.ObjectValuesLength()):
                item = array_val.ObjectValues(i)
                result.append(self._unpack_value(item.ValType(), item.Val()))
            return result
        else:
            # Object array
            result = JavaArray(element_type="Object")
            for i in range(array_val.ObjectValuesLength()):
                item = array_val.ObjectValues(i)
                result.append(self._unpack_value(item.ValType(), item.Val()))
            return result

    def _read_response(self):
        while True:
            # 1. Read Length
            sz_data = _recv_exactly(self.sock, 4)
            sz = struct.unpack("<I", sz_data)[0]

            # 2. Read Data from SHM
            self.shm.seek(self.response_offset)
            resp_buf = self.shm.read(sz)

            # 3. Parse FlatBuffer
            resp = Response.Response.GetRootAsResponse(resp_buf, 0)

            # Check if this is a callback request from Java
            if resp.IsCallback():
                self._handle_callback(resp)
                # After handling callback, continue reading for the actual response
                continue

            if resp.IsError():
                error_msg = resp.ErrorMsg().decode("utf-8")
                error_type_bytes = resp.ErrorType()
                error_type = (
                    error_type_bytes.decode("utf-8")
                    if error_type_bytes
                    else "java.lang.RuntimeException"
                )
                self._raise_java_exception(error_type, error_msg)

            # 4. Check for Arrow batch response
            arrow_batch = resp.ArrowBatch()
            if arrow_batch is not None and arrow_batch.NumRows() >= 0:
                return self._unpack_arrow_batch(arrow_batch)

            # 5. Unpack the return value
            return self._unpack_value(resp.ReturnValType(), resp.ReturnVal())

    def _raise_java_exception(self, error_type: str, error_msg: str) -> None:
        """Raise the appropriate Python exception for a Java error.

        If py4j_compat mode is enabled and a converter is set, this will
        raise Py4JJavaError instead of the native Gatun exceptions.
        """
        converter = self._py4j_exception_converter if self._py4j_compat else None
        _raise_java_exception_impl(error_type, error_msg, converter)

    def _handle_callback(self, resp):
        """Handle a callback invocation request from Java."""
        callback_id = resp.CallbackId()

        # Unpack callback arguments
        args = []
        for i in range(resp.CallbackArgsLength()):
            arg = resp.CallbackArgs(i)
            args.append(self._unpack_value(arg.ValType(), arg.Val()))

        # Look up the callback function
        callback_fn = self._callbacks.get(callback_id)
        if callback_fn is None:
            # Send error response
            self._send_callback_response(
                callback_id, None, True, f"Callback {callback_id} not found"
            )
            return

        # Execute the callback
        try:
            result = callback_fn(*args)
            self._send_callback_response(callback_id, result, False, None)
        except Exception as e:
            self._send_callback_response(callback_id, None, True, str(e))

    def _unpack_arrow_batch(self, arrow_batch) -> ArrowTableView:
        """Unpack an ArrowBatchDescriptor from Java into an ArrowTableView.

        This reconstructs an Arrow table from buffer descriptors in the payload
        shared memory zone. The buffers are wrapped as zero-copy PyArrow buffers.

        The returned ArrowTableView wraps the table and validates the arena epoch
        on access, preventing use of stale data after reset_payload_arena().

        Args:
            arrow_batch: ArrowBatchDescriptor FlatBuffer object

        Returns:
            ArrowTableView wrapping the reconstructed table with epoch validation
        """
        from gatun.arena import deserialize_schema, _validate_supported_schema

        import ctypes

        schema_hash = arrow_batch.SchemaHash()
        descriptor_epoch = arrow_batch.ArenaEpoch()

        # Validate epoch to prevent use-after-reset corruption
        # Python and Java epochs should be synchronized via reset_payload_arena()
        if descriptor_epoch != self._arena_epoch:
            raise StaleArenaError(descriptor_epoch, self._arena_epoch)

        # Get or deserialize schema
        schema = self._arrow_schema_cache.get(schema_hash)
        if schema is None:
            schema_bytes_len = arrow_batch.SchemaBytesLength()
            if schema_bytes_len == 0:
                raise RuntimeError(
                    f"No schema available for hash {schema_hash} and none cached"
                )
            schema_bytes = bytes(arrow_batch.SchemaBytesAsNumpy())
            schema = deserialize_schema(schema_bytes)
            # Validate schema doesn't contain unsupported types (e.g., dictionary)
            _validate_supported_schema(schema)
            self._arrow_schema_cache[schema_hash] = schema

        # Get base address of payload zone in shared memory
        base_address = ctypes.addressof(ctypes.c_char.from_buffer(self.shm))
        payload_base = base_address + self.payload_offset

        # Build PyArrow buffers from buffer descriptors
        buffers = []
        for i in range(arrow_batch.BuffersLength()):
            buf_desc = arrow_batch.Buffers(i)
            offset = buf_desc.Offset()
            length = buf_desc.Length()

            if length == 0:
                # Zero-length buffer (e.g., no validity bitmap when no nulls)
                buffers.append(None)
            else:
                # Create zero-copy buffer backed by shared memory
                buf_address = payload_base + offset
                buf = pa.foreign_buffer(buf_address, length, base=self.shm)
                buffers.append(buf)

        # Build field nodes as (length, null_count) tuples
        field_nodes = []
        for i in range(arrow_batch.NodesLength()):
            node = arrow_batch.Nodes(i)
            field_nodes.append((node.Length(), node.NullCount()))

        # Reconstruct arrays from buffers using schema (handles nested types recursively)
        arrays = []
        buffer_idx = 0
        node_idx = 0

        for field in schema:
            arr, buffer_idx, node_idx = _reconstruct_array_recursive(
                field.type, buffers, field_nodes, buffer_idx, node_idx
            )
            arrays.append(arr)

        # Build table from arrays and wrap in epoch-validating view
        table = pa.Table.from_arrays(arrays, schema=schema)
        return ArrowTableView(table, descriptor_epoch, self)

    def _send_callback_response(
        self, callback_id: int, result, is_error: bool, error_msg: str | None
    ):
        """Send the result of a callback execution back to Java."""
        builder = flatbuffers.Builder(1024)

        # Build arguments: [result, is_error]
        arg_offsets = []

        # First arg: the result value
        result_offset = self._build_argument(builder, result)
        arg_offsets.append(result_offset)

        # Second arg: is_error flag
        error_offset = self._build_argument(builder, is_error)
        arg_offsets.append(error_offset)

        # Build arguments vector
        Cmd.CommandStartArgsVector(builder, len(arg_offsets))
        for offset in reversed(arg_offsets):
            builder.PrependUOffsetTRelative(offset)
        args_vec = builder.EndVector()

        # Build target_name (error message if error)
        target_name_off = None
        if error_msg:
            target_name_off = builder.CreateString(error_msg)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CallbackResponse)
        Cmd.CommandAddTargetId(builder, callback_id)
        if target_name_off:
            Cmd.CommandAddTargetName(builder, target_name_off)
        Cmd.CommandAddArgs(builder, args_vec)
        cmd_offset = Cmd.CommandEnd(builder)
        builder.Finish(cmd_offset)

        # Send it (don't wait for response - Java is waiting for this)
        self._send_raw(builder.Output(), wait_for_response=False)

    def create_object(self, class_name, *args):
        builder = flatbuffers.Builder(1024)
        cls_off = builder.CreateString(class_name)

        # Build argument tables (must be done before Command)
        arg_offsets = []
        for arg in args:
            arg_offset = self._build_argument(builder, arg)
            arg_offsets.append(arg_offset)

        # Build arguments vector
        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObject)
        Cmd.CommandAddTargetName(builder, cls_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        cmd_offset = Cmd.CommandEnd(builder)
        builder.Finish(cmd_offset)

        # Pass bytes!
        return self._send_raw(builder.Output())

    def invoke_method(self, obj_id, method_name, *args):
        builder = flatbuffers.Builder(1024)
        meth_off = builder.CreateString(method_name)

        # Build argument tables (must be done before Command)
        arg_offsets = []
        for arg in args:
            arg_offset = self._build_argument(builder, arg)
            arg_offsets.append(arg_offset)

        # Build arguments vector
        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeMethod)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, meth_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)

        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def invoke_static_method(self, class_name, method_name, *args):
        """Invoke a static method on a class.

        Args:
            class_name: Fully qualified class name (e.g., "java.lang.Integer")
            method_name: Method name (e.g., "parseInt")
            *args: Arguments to pass to the method
        """
        builder = flatbuffers.Builder(1024)
        # Format: "fully.qualified.ClassName.methodName"
        full_name = f"{class_name}.{method_name}"
        name_off = builder.CreateString(full_name)

        # Build argument tables (must be done before Command)
        arg_offsets = []
        for arg in args:
            arg_offset = self._build_argument(builder, arg)
            arg_offsets.append(arg_offset)

        # Build arguments vector
        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeStaticMethod)
        Cmd.CommandAddTargetName(builder, name_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)

        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def get_field(self, obj_id, field_name):
        """Get a field value from a Java object.

        Args:
            obj_id: Object ID (or JavaObject instance)
            field_name: Name of the field to get
        """
        if isinstance(obj_id, JavaObject):
            obj_id = obj_id.object_id

        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(field_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetField)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def set_field(self, obj_id, field_name, value):
        """Set a field value on a Java object.

        Args:
            obj_id: Object ID (or JavaObject instance)
            field_name: Name of the field to set
            value: Value to set
        """
        if isinstance(obj_id, JavaObject):
            obj_id = obj_id.object_id

        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(field_name)

        # Build argument for the value
        arg_offset = self._build_argument(builder, value)
        Cmd.CommandStartArgsVector(builder, 1)
        builder.PrependUOffsetTRelative(arg_offset)
        args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SetField)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def get_static_field(self, class_name: str, field_name: str):
        """Get a static field value from a Java class.

        Args:
            class_name: Fully qualified class name (e.g., "java.lang.Integer")
            field_name: Name of the static field (e.g., "MAX_VALUE")

        Returns:
            The field value

        Example:
            >>> client.get_static_field("java.lang.Integer", "MAX_VALUE")
            2147483647
        """
        builder = flatbuffers.Builder(256)
        full_name = f"{class_name}.{field_name}"
        name_off = builder.CreateString(full_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetStaticField)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def set_static_field(self, class_name: str, field_name: str, value):
        """Set a static field value on a Java class.

        Args:
            class_name: Fully qualified class name
            field_name: Name of the static field
            value: Value to set
        """
        builder = flatbuffers.Builder(256)
        full_name = f"{class_name}.{field_name}"
        name_off = builder.CreateString(full_name)

        # Build argument for the value
        arg_offset = self._build_argument(builder, value)
        Cmd.CommandStartArgsVector(builder, 1)
        builder.PrependUOffsetTRelative(arg_offset)
        args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SetStaticField)
        Cmd.CommandAddTargetName(builder, name_off)
        Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def reflect(self, full_name: str) -> str:
        """Query Java to determine the type of a fully qualified name.

        This is used to distinguish between classes, methods, and fields
        when navigating the JVM via attribute access.

        Args:
            full_name: Fully qualified name to check
                      (e.g., "org.apache.spark.sql.functions" or
                       "org.apache.spark.sql.functions.col")

        Returns:
            One of:
            - "class" if the name refers to a loadable class (including Scala objects)
            - "method" if the last segment is a method on the parent class
            - "field" if the last segment is a field on the parent class
            - "none" if the name cannot be resolved

        Note:
            Results are typically cached by the caller for performance.
        """
        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(full_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Reflect)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def is_instance_of(self, obj, class_name: str) -> bool:
        """Check if a Java object is an instance of a class.

        This is equivalent to Java's `instanceof` operator. It checks if the
        object is an instance of the specified class or any of its subclasses.

        Args:
            obj: JavaObject instance or object ID
            class_name: Fully qualified Java class name
                       (e.g., "java.util.List", "java.util.ArrayList")

        Returns:
            True if the object is an instance of the specified class.

        Example:
            arr = client.create_object("java.util.ArrayList")
            client.is_instance_of(arr, "java.util.List")  # True
            client.is_instance_of(arr, "java.util.Map")   # False
        """
        if isinstance(obj, JavaObject):
            obj_id = obj.object_id
        else:
            obj_id = obj

        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(class_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.IsInstanceOf)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def register_callback(
        self, callback_fn: callable, interface_name: str
    ) -> "JavaObject":
        """Register a Python callable as a Java interface implementation.

        This creates a Java dynamic proxy that implements the specified interface.
        When Java code calls methods on this proxy, the calls are forwarded to
        the Python callback function.

        Args:
            callback_fn: A Python callable that will handle method invocations.
                        It receives the method arguments and should return a value.
            interface_name: Fully qualified Java interface name
                           (e.g., "java.util.function.Function")

        Returns:
            A JavaObject representing the proxy that implements the interface.

        Example:
            # Create a Comparator for sorting
            def my_compare(a, b):
                return a - b

            comparator = client.register_callback(my_compare, "java.util.Comparator")
            arr = client.create_object("java.util.ArrayList")
            arr.add(3)
            arr.add(1)
            arr.add(2)
            client.invoke_static_method("java.util.Collections", "sort", arr, comparator)
        """
        builder = flatbuffers.Builder(256)
        interface_off = builder.CreateString(interface_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.RegisterCallback)
        Cmd.CommandAddTargetName(builder, interface_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # Send command and get back the proxy object
        result = self._send_raw(builder.Output())

        if isinstance(result, JavaObject):
            # Get the callback_id from the response - it's stored as the object_id
            # We need to also store our callback function
            # The Java side assigns callback_id = object_id for simplicity
            callback_id = result.object_id
            self._callbacks[callback_id] = callback_fn

        return result

    def unregister_callback(self, callback_id: int):
        """Unregister a previously registered callback.

        Args:
            callback_id: The callback ID to unregister
        """
        # Remove from local registry
        self._callbacks.pop(callback_id, None)

        builder = flatbuffers.Builder(64)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.UnregisterCallback)
        Cmd.CommandAddTargetId(builder, callback_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        self._send_raw(builder.Output())

    def _build_argument(self, builder, value):
        """Convert a Python value to a FlatBuffer Argument."""
        val_type, val_off = self._build_value(builder, value)

        Argument.Start(builder)
        Argument.AddValType(builder, val_type)
        Argument.AddVal(builder, val_off)
        return Argument.End(builder)

    def _build_value(self, builder, value):
        """Build a Value union and return (type, offset)."""
        if isinstance(value, str):
            str_off = builder.CreateString(value)
            StringVal.Start(builder)
            StringVal.AddV(builder, str_off)
            return Value.Value.StringVal, StringVal.End(builder)
        elif isinstance(value, bool):  # Must check before int (bool is subclass of int)
            BoolVal.Start(builder)
            BoolVal.AddV(builder, value)
            return Value.Value.BoolVal, BoolVal.End(builder)
        elif isinstance(value, int):
            IntVal.Start(builder)
            IntVal.AddV(builder, value)
            return Value.Value.IntVal, IntVal.End(builder)
        elif isinstance(value, float):
            DoubleVal.Start(builder)
            DoubleVal.AddV(builder, value)
            return Value.Value.DoubleVal, DoubleVal.End(builder)
        elif isinstance(value, JavaObject):
            ObjectRef.Start(builder)
            ObjectRef.AddId(builder, value.object_id)
            return Value.Value.ObjectRef, ObjectRef.End(builder)
        elif isinstance(value, JavaArray):
            # JavaArray came from Java and should go back as ArrayVal (Java array)
            return self._build_java_array(builder, value)
        elif isinstance(value, (list, tuple)):
            # Auto-convert Python list/tuple to ListVal (Java ArrayList)
            # Tuples are treated the same as lists for Java interop
            item_offsets = []
            for item in value:
                item_offsets.append(self._build_argument(builder, item))
            ListVal.StartItemsVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            items_vec = builder.EndVector()
            ListVal.Start(builder)
            ListVal.AddItems(builder, items_vec)
            return Value.Value.ListVal, ListVal.End(builder)
        elif isinstance(value, dict):
            # Auto-convert Python dict to MapVal
            entry_offsets = []
            for k, v in value.items():
                key_arg = self._build_argument(builder, k)
                val_arg = self._build_argument(builder, v)
                MapEntry.Start(builder)
                MapEntry.AddKey(builder, key_arg)
                MapEntry.AddValue(builder, val_arg)
                entry_offsets.append(MapEntry.End(builder))
            MapVal.StartEntriesVector(builder, len(entry_offsets))
            for offset in reversed(entry_offsets):
                builder.PrependUOffsetTRelative(offset)
            entries_vec = builder.EndVector()
            MapVal.Start(builder)
            MapVal.AddEntries(builder, entries_vec)
            return Value.Value.MapVal, MapVal.End(builder)
        elif isinstance(value, np.ndarray):
            # Convert numpy array to ArrayVal
            return self._build_array(builder, value)
        elif isinstance(value, (bytes, bytearray)):
            # Convert bytes to byte array
            return self._build_byte_array(builder, value)
        elif isinstance(value, array.array):
            # Convert Python array.array to ArrayVal
            return self._build_typed_array(builder, value)
        elif value is None:
            from gatun.generated.org.gatun.protocol import NullVal

            NullVal.Start(builder)
            return Value.Value.NullVal, NullVal.End(builder)
        else:
            # Check for datetime types (import here to avoid circular imports)
            import datetime
            import time as time_module
            import calendar

            if isinstance(value, datetime.datetime):
                # Convert datetime to java.sql.Timestamp
                # First calculate epoch milliseconds
                if value.tzinfo is not None:
                    # Timezone-aware datetime
                    epoch_secs = calendar.timegm(value.utctimetuple())
                else:
                    # Naive datetime - treat as local time
                    epoch_secs = time_module.mktime(value.timetuple())
                epoch_millis = int(epoch_secs * 1000) + value.microsecond // 1000
                nanos = value.microsecond * 1000

                # Create java.sql.Timestamp object
                timestamp = self.create_object("java.sql.Timestamp", epoch_millis)
                if nanos > 0:
                    timestamp.setNanos(nanos)
                # Detach the finalizer to prevent premature GC from freeing
                # the Java object before the outer command uses it
                timestamp.detach()
                ObjectRef.Start(builder)
                ObjectRef.AddId(builder, timestamp.object_id)
                return Value.Value.ObjectRef, ObjectRef.End(builder)
            elif isinstance(value, datetime.date):
                # Convert date to java.sql.Date using valueOf
                # java.sql.Date.valueOf expects "YYYY-MM-DD" format
                date_str = value.isoformat()
                date_obj = self.invoke_static_method(
                    "java.sql.Date", "valueOf", date_str
                )
                # Detach the finalizer to prevent premature GC from freeing
                # the Java object before the outer command uses it
                date_obj.detach()
                ObjectRef.Start(builder)
                ObjectRef.AddId(builder, date_obj.object_id)
                return Value.Value.ObjectRef, ObjectRef.End(builder)
            elif isinstance(value, datetime.time):
                # Convert time to java.sql.Time using valueOf
                # java.sql.Time.valueOf expects "HH:MM:SS" format
                time_str = value.strftime("%H:%M:%S")
                time_obj = self.invoke_static_method(
                    "java.sql.Time", "valueOf", time_str
                )
                # Detach the finalizer to prevent premature GC from freeing
                # the Java object before the outer command uses it
                time_obj.detach()
                ObjectRef.Start(builder)
                ObjectRef.AddId(builder, time_obj.object_id)
                return Value.Value.ObjectRef, ObjectRef.End(builder)
            else:
                raise TypeError(f"Unsupported argument type: {type(value)}")

    def _build_array(self, builder, arr):
        """Build an ArrayVal from a numpy array."""
        dtype = arr.dtype

        # Mapping of numpy dtypes to (element_type, add_values_fn, optional_cast)
        primitive_types = {
            np.dtype(np.int32): (
                ElementType.ElementType.Int,
                ArrayVal.AddIntValues,
                None,
            ),
            np.dtype(np.int64): (
                ElementType.ElementType.Long,
                ArrayVal.AddLongValues,
                None,
            ),
            np.dtype(np.float64): (
                ElementType.ElementType.Double,
                ArrayVal.AddDoubleValues,
                None,
            ),
            np.dtype(np.float32): (
                ElementType.ElementType.Float,
                ArrayVal.AddDoubleValues,
                np.float64,  # Widen float32 to float64 for transmission
            ),
            np.dtype(np.bool_): (
                ElementType.ElementType.Bool,
                ArrayVal.AddBoolValues,
                np.uint8,
            ),
            np.dtype(np.int8): (
                ElementType.ElementType.Byte,
                ArrayVal.AddByteValues,
                np.int8,
            ),
            np.dtype(np.uint8): (
                ElementType.ElementType.Byte,
                ArrayVal.AddByteValues,
                np.int8,
            ),
            np.dtype(np.int16): (
                ElementType.ElementType.Short,
                ArrayVal.AddIntValues,
                np.int32,  # Widen short to int for transmission
            ),
        }

        if dtype in primitive_types:
            elem_type, add_values_fn, cast_type = primitive_types[dtype]
            data = arr if cast_type is None else arr.astype(cast_type)
            vec_off = builder.CreateNumpyVector(data)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, elem_type)
            add_values_fn(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)

        # String or object arrays - serialize each element
        if dtype.kind in ("U", "O"):
            elem_type = (
                ElementType.ElementType.String
                if dtype.kind == "U"
                else ElementType.ElementType.Object
            )
        else:
            # Fallback: treat as object array
            elem_type = ElementType.ElementType.Object

        item_offsets = [self._build_argument(builder, item) for item in arr]
        ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
        for offset in reversed(item_offsets):
            builder.PrependUOffsetTRelative(offset)
        vec_off = builder.EndVector()
        ArrayVal.Start(builder)
        ArrayVal.AddElementType(builder, elem_type)
        ArrayVal.AddObjectValues(builder, vec_off)
        return Value.Value.ArrayVal, ArrayVal.End(builder)

    def _build_byte_array(self, builder, data):
        """Build an ArrayVal from bytes or bytearray."""
        vec_off = builder.CreateByteVector(data)
        ArrayVal.Start(builder)
        ArrayVal.AddElementType(builder, ElementType.ElementType.Byte)
        ArrayVal.AddByteValues(builder, vec_off)
        return Value.Value.ArrayVal, ArrayVal.End(builder)

    def _build_typed_array(self, builder, arr):
        """Build an ArrayVal from Python array.array."""
        typecode = arr.typecode
        # Convert array.array to numpy for CreateNumpyVector
        if typecode == "i":  # int (usually 32-bit)
            np_arr = np.array(arr, dtype=np.int32)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Int)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "l" or typecode == "q":  # long
            np_arr = np.array(arr, dtype=np.int64)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Long)
            ArrayVal.AddLongValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "d":  # double
            np_arr = np.array(arr, dtype=np.float64)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Double)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "f":  # float -> widen to double
            np_arr = np.array(arr, dtype=np.float64)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Float)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "b" or typecode == "B":  # byte
            vec_off = builder.CreateByteVector(arr.tobytes())
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Byte)
            ArrayVal.AddByteValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "h" or typecode == "H":  # short -> widen to int
            np_arr = np.array(arr, dtype=np.int32)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Short)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        else:
            raise TypeError(f"Unsupported array.array typecode: {typecode}")

    def _build_java_array(self, builder, java_array: JavaArray):
        """Build an ArrayVal from a JavaArray (preserving array semantics for round-trip)."""
        elem_type = java_array.element_type

        if elem_type == "Int":
            np_arr = np.array(list(java_array), dtype=np.int32)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Int)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Long":
            np_arr = np.array(list(java_array), dtype=np.int64)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Long)
            ArrayVal.AddLongValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Double":
            np_arr = np.array(list(java_array), dtype=np.float64)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Double)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Float":
            np_arr = np.array(list(java_array), dtype=np.float64)  # widened to double
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Float)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Bool":
            np_arr = np.array(list(java_array), dtype=np.bool_)
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Bool)
            ArrayVal.AddBoolValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Byte":
            vec_off = builder.CreateByteVector(bytes(java_array))
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Byte)
            ArrayVal.AddByteValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Short":
            np_arr = np.array(list(java_array), dtype=np.int32)  # widened to int
            vec_off = builder.CreateNumpyVector(np_arr)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Short)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "String":
            # String array - need to build each as Argument
            item_offsets = []
            for item in java_array:
                item_offsets.append(self._build_argument(builder, item))
            ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            vec_off = builder.EndVector()
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.String)
            ArrayVal.AddObjectValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        else:
            # Object array - need to build each as Argument
            item_offsets = []
            for item in java_array:
                item_offsets.append(self._build_argument(builder, item))
            ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            vec_off = builder.EndVector()
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Object)
            ArrayVal.AddObjectValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)

    def free_object(self, object_id):
        """Sends FreeObject to release a Java object."""
        if self.sock is None or self.sock.fileno() == -1:
            return

        builder = flatbuffers.Builder(64)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.FreeObject)
        Cmd.CommandAddTargetId(builder, object_id)
        cmd_offset = Cmd.CommandEnd(builder)
        builder.Finish(cmd_offset)

        try:
            self._send_raw(builder.Output())
        except OSError:
            pass

    def send_arrow_table(self, table):
        """Send an Arrow table to Java via shared memory.

        This writes Arrow IPC data to the shared memory mmap using a single
        memmove, avoiding the overhead of opening a new memory-mapped file.

        The data flow is:
        1. Python serializes Arrow IPC to a buffer (Arrow's efficient internal format)
        2. Buffer is copied to shared memory via ctypes.memmove (single copy)
        3. Java reads Arrow IPC from shared memory (zero-copy read via mmap)

        Note: Arrow IPC serialization still occurs (table → IPC format).
        For true zero-copy, use send_arrow_buffers() instead.
        """
        max_payload_size = self.response_offset - self.payload_offset

        # Serialize to an in-memory buffer first
        # Arrow's IPC serialization is already optimized and this is fast
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)

        ipc_buffer = sink.getvalue()
        bytes_written = ipc_buffer.size

        if bytes_written > max_payload_size:
            raise PayloadTooLargeError(bytes_written, max_payload_size, "Arrow batch")

        # Copy IPC data to shared memory payload zone using single memmove
        payload_base = ctypes.addressof(ctypes.c_char.from_buffer(self.shm))
        payload_addr = payload_base + self.payload_offset
        ctypes.memmove(payload_addr, ipc_buffer.address, bytes_written)

        # Build Command
        builder = flatbuffers.Builder(256)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SendArrowBatch)
        command = Cmd.CommandEnd(builder)
        builder.Finish(command)

        return self._send_raw(builder.Output())

    def send_arrow_table_batched(self, table, batch_size=None):
        """Send a large Arrow table in batches that fit in the payload zone.

        Args:
            table: PyArrow Table to send
            batch_size: Number of rows per batch. If None, automatically
                       calculated to fit within the payload zone.

        Returns:
            List of responses from each batch sent.

        Example:
            # Send a large table in batches
            responses = client.send_arrow_table_batched(large_table)

            # Or specify a custom batch size
            responses = client.send_arrow_table_batched(large_table, batch_size=10000)
        """
        max_payload_size = self.response_offset - self.payload_offset
        total_rows = table.num_rows

        if total_rows == 0:
            return [self.send_arrow_table(table)]

        # If batch_size not specified, try to estimate optimal size
        if batch_size is None:
            # Estimate bytes per row by sampling first 100 rows
            sample_size = min(100, total_rows)
            sample = table.slice(0, sample_size)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, sample.schema) as writer:
                writer.write_table(sample)
            sample_bytes = len(sink.getvalue())

            # Estimate bytes per row (subtract ~200 bytes for schema overhead)
            schema_overhead = 200
            bytes_per_row = max(1, (sample_bytes - schema_overhead) / sample_size)

            # Target 80% of max payload to leave margin
            target_size = int(max_payload_size * 0.8)
            batch_size = max(1, int((target_size - schema_overhead) / bytes_per_row))

        responses = []
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = table.slice(start, end - start)
            response = self.send_arrow_table(batch)
            responses.append(response)

        return responses

    def send_arrow_buffers(
        self,
        table: pa.Table,
        arena: "PayloadArena",
        schema_cache: dict[int, bool] | None = None,
    ):
        """Send an Arrow table via true zero-copy buffer transfer.

        This method copies Arrow buffers directly into the payload shared memory
        arena and sends buffer descriptors to Java, enabling Java to read the
        data in-place without any additional copies.

        Data flow:
        1. Python copies Arrow buffers into payload shm (one copy)
        2. Python sends buffer descriptors (offsets/lengths) to Java
        3. Java wraps the shm buffers directly as ArrowBuf (zero-copy read)

        Args:
            table: PyArrow Table to send
            arena: PayloadArena for buffer allocation
            schema_cache: Optional dict to track which schemas Java has seen.
                         If provided and schema hash is in cache, schema bytes
                         are omitted from the message.

        Returns:
            Response from Java after processing the batch.

        Example:
            from gatun.arena import PayloadArena

            arena = PayloadArena(Path("~/gatun_payload.shm"), 64 * 1024 * 1024)
            schema_cache = {}

            # Send multiple tables, reusing the arena
            for table in tables:
                arena.reset()  # Reset for each batch
                client.send_arrow_buffers(table, arena, schema_cache)

            arena.close()
        """
        from gatun.arena import (
            copy_arrow_table_to_arena,
            compute_schema_hash,
            serialize_schema,
        )

        # 1. Copy table buffers into the arena
        buffer_infos, field_nodes = copy_arrow_table_to_arena(table, arena)

        # 2. Compute schema hash and check cache
        schema_hash = compute_schema_hash(table.schema)
        include_schema = schema_cache is None or schema_hash not in schema_cache

        # 3. Build FlatBuffers command
        builder = flatbuffers.Builder(1024)

        # Build schema bytes vector if needed
        schema_bytes_vec = None
        if include_schema:
            schema_bytes = serialize_schema(table.schema)
            schema_bytes_vec = builder.CreateByteVector(schema_bytes)
            if schema_cache is not None:
                schema_cache[schema_hash] = True

        # Build buffer descriptors
        buffer_offsets = []
        for info in buffer_infos:
            BufferDescriptor.Start(builder)
            BufferDescriptor.AddOffset(builder, info.offset)
            BufferDescriptor.AddLength(builder, info.length)
            buffer_offsets.append(BufferDescriptor.End(builder))

        # Build buffers vector
        ArrowBatchDescriptor.StartBuffersVector(builder, len(buffer_offsets))
        for offset in reversed(buffer_offsets):
            builder.PrependUOffsetTRelative(offset)
        buffers_vec = builder.EndVector()

        # Build field nodes
        node_offsets = []
        for length, null_count in field_nodes:
            FieldNode.Start(builder)
            FieldNode.AddLength(builder, length)
            FieldNode.AddNullCount(builder, null_count)
            node_offsets.append(FieldNode.End(builder))

        # Build nodes vector
        ArrowBatchDescriptor.StartNodesVector(builder, len(node_offsets))
        for offset in reversed(node_offsets):
            builder.PrependUOffsetTRelative(offset)
        nodes_vec = builder.EndVector()

        # Build ArrowBatchDescriptor
        ArrowBatchDescriptor.Start(builder)
        ArrowBatchDescriptor.AddSchemaHash(builder, schema_hash)
        if schema_bytes_vec is not None:
            ArrowBatchDescriptor.AddSchemaBytes(builder, schema_bytes_vec)
        ArrowBatchDescriptor.AddNumRows(builder, table.num_rows)
        ArrowBatchDescriptor.AddNodes(builder, nodes_vec)
        ArrowBatchDescriptor.AddBuffers(builder, buffers_vec)
        ArrowBatchDescriptor.AddArenaEpoch(builder, self._arena_epoch)
        batch_descriptor = ArrowBatchDescriptor.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SendArrowBuffers)
        Cmd.CommandAddArrowBatch(builder, batch_descriptor)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # 4. Send via standard path
        return self._send_raw(builder.Output())

    def reset_payload_arena(self):
        """Signal Java to reset its view of the payload arena.

        Call this after resetting the Python PayloadArena to ensure Java
        releases any references to the old buffer contents.

        WARNING: After calling this, any ArrowTableView objects from previous
        get_arrow_data() calls become invalid. Accessing them will raise
        StaleArenaError. Copy the data you need before resetting.
        """
        # Increment epoch to invalidate any outstanding ArrowTableView objects
        self._arena_epoch += 1

        builder = flatbuffers.Builder(64)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.ResetPayloadArena)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def get_arrow_data(self) -> ArrowTableView:
        """Retrieve Arrow data from Java via zero-copy transfer.

        This method requests the current Arrow data held by the Java server
        (e.g., data previously sent via send_arrow_buffers). Java writes the
        Arrow buffers to the payload shared memory zone and sends buffer
        descriptors back, which are used to reconstruct the table in Python.

        Returns:
            ArrowTableView wrapping the reconstructed table. The view validates
            the arena epoch on access - if reset_payload_arena() is called after
            receiving this table, accessing data will raise StaleArenaError.

            To safely use data after reset, copy it first:
            - table_view.to_pandas() -> pandas DataFrame
            - table_view.to_pydict() -> Python dict
            - table_view.to_pylist() -> list of dicts

        Raises:
            RuntimeError: If no Arrow data is available on the Java side.
            StaleArenaError: If accessing data after reset_payload_arena().

        Example:
            # Send data to Java
            client.send_arrow_buffers(table, arena, schema_cache)

            # Get it back (useful for testing or round-trip operations)
            table_view = client.get_arrow_data()

            # Safe: copy data before resetting
            data = table_view.to_pydict()
            arena.reset()
            client.reset_payload_arena()

            # Unsafe: would raise StaleArenaError after reset
            # table_view.column("x")  # Don't do this!
        """
        builder = flatbuffers.Builder(64)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetArrowData)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def _get_request_id(self) -> int:
        """Get the next request ID for cancellation support."""
        request_id = self._next_request_id
        self._next_request_id += 1
        return request_id

    def cancel(self, request_id: int) -> bool:
        """Cancel a running request by its ID.

        This sends a cancellation signal to the Java server. The server will
        attempt to interrupt the running operation, which will raise a
        CancelledException on the next response.

        Note: Cancellation is cooperative - the operation must check for
        cancellation to actually stop. Long-running Java operations that
        don't check Thread.interrupted() may not respond to cancellation.

        Args:
            request_id: The ID of the request to cancel (returned by operations
                       when using request IDs).

        Returns:
            True if the cancel request was acknowledged by the server.

        Example:
            # In async context or with threading:
            request_id = client._get_request_id()
            # ... start operation with request_id in another thread ...
            client.cancel(request_id)  # Cancel it
        """
        builder = flatbuffers.Builder(64)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Cancel)
        Cmd.CommandAddRequestId(builder, request_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        result = self._send_raw(builder.Output())
        return result is True

    def close(self):
        try:
            if self.shm:
                self.shm.close()
                self.shm = None
            if self.shm_file:
                self.shm_file.close()
                self.shm_file = None
            if self.sock:
                self.sock.close()
                self.sock = None
        except Exception:
            pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()
        return False
