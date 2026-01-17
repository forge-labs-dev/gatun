from __future__ import annotations

import array
from collections import OrderedDict
import ctypes
import logging
import mmap
import os
import socket
import struct
import time
from typing import TYPE_CHECKING, Any, Callable, cast
import weakref

import flatbuffers
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
    CharVal,
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
    BatchCommand,
    GetFieldsRequest,
    InvokeMethodsRequest,
    MethodCall,
    CreateObjectsRequest,
    ObjectSpec,
)

logger = logging.getLogger(__name__)


def _create_typed_vector(builder, data: bytes, itemsize: int, num_elements: int):
    """Create a typed FlatBuffers vector from raw bytes.

    This mimics flatbuffers.Builder.CreateNumpyVector but without numpy.
    It creates a properly-typed vector that can be read with typed accessors
    like IntValues(i), DoubleValues(i), etc.

    Args:
        builder: FlatBuffers Builder instance
        data: Raw bytes in little-endian format
        itemsize: Size of each element in bytes (4 for int32, 8 for int64/double, etc.)
        num_elements: Number of elements in the vector

    Returns:
        Vector offset for use with FlatBuffers Add*Values methods
    """
    from flatbuffers.builder import UOffsetTFlags

    builder.StartVector(itemsize, num_elements, itemsize)  # alignment = itemsize

    # Calculate total length
    length = UOffsetTFlags.py_type(itemsize * num_elements)
    builder.head = UOffsetTFlags.py_type(builder.Head() - length)

    # Copy bytes directly into buffer
    builder.Bytes[builder.Head() : builder.Head() + length] = data

    builder.vectorNumElems = num_elements
    return builder.EndVector()


# Protocol version - must match the server version
# Version 2: Per-client shared memory (handshake includes SHM path)
PROTOCOL_VERSION = 2

# Memory zone sizes - must match GatunServer.java constants
COMMAND_ZONE_SIZE = 65536  # 64KB for commands
RESPONSE_ZONE_SIZE = 65536  # 64KB for responses

# Maximum size for callback error messages to prevent exceeding command zone.
# Leave headroom for FlatBuffers overhead and other command fields.
MAX_CALLBACK_ERROR_SIZE = COMMAND_ZONE_SIZE // 16  # 4KB

# Default timeout for callback execution in seconds.
# Prevents hung callbacks from deadlocking the protocol.
# Can be overridden per-client via callback_timeout parameter.
DEFAULT_CALLBACK_TIMEOUT = 30.0

# Default timeout for socket read operations in seconds.
# Used for handshake, response reads, etc. to prevent indefinite hangs.
# None means no timeout (blocking) - only used when explicitly needed.
DEFAULT_SOCKET_TIMEOUT = 30.0

# Handshake timeout - shorter since handshake should be fast
HANDSHAKE_TIMEOUT = 10.0

# FlatBuffer field offsets for ArrayVal - derived from generated code (ArrayVal.py)
# These correspond to vtable slot numbers: (slot_index + 2) * 2
# See: https://flatbuffers.dev/flatbuffers_internals.html
_ARRAYVAL_INT_VALUES_OFFSET = 6  # slot 1: int_values
_ARRAYVAL_LONG_VALUES_OFFSET = 8  # slot 2: long_values
_ARRAYVAL_DOUBLE_VALUES_OFFSET = 10  # slot 3: double_values
_ARRAYVAL_BOOL_VALUES_OFFSET = 12  # slot 4: bool_values
_ARRAYVAL_BYTE_VALUES_OFFSET = 14  # slot 5: byte_values

# FlatBuffer field offset for ArrowBatchDescriptor.schema_bytes
_ARROW_BATCH_SCHEMA_BYTES_OFFSET = 6  # slot 1: schema_bytes


def _unpack_flatbuffer_vector(tab, field_offset, length, fmt_char, elem_size):
    """Unpack a primitive vector directly from FlatBuffer bytes.

    This is a fast alternative to element-by-element access, using direct
    buffer slicing and struct.unpack for O(1) bulk unpacking.

    Args:
        tab: FlatBuffer table object (has Offset, Vector, Bytes)
        field_offset: VTable offset for the field (e.g., 6, 8, 10)
        length: Number of elements in the vector
        fmt_char: struct format character ('i' for int32, 'q' for int64, etc.)
        elem_size: Size of each element in bytes

    Returns:
        List of unpacked values, or empty list if vector is absent
    """
    o = flatbuffers.number_types.UOffsetTFlags.py_type(tab.Offset(field_offset))
    if o == 0 or length == 0:
        return []
    start = tab.Vector(o)
    buf = tab.Bytes[start : start + length * elem_size]
    return list(struct.unpack(f"<{length}{fmt_char}", buf))


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


class ReentrancyError(Exception):
    """Raised when a nested Java call is attempted from within a callback.

    Gatun does not support reentrant calls - you cannot call Java from Python
    while inside a callback that Java invoked. This would deadlock because:
    1. The original request is waiting for a response
    2. The callback tries to send a new request on the same socket
    3. Both sides would wait forever for the other

    To work around this:
    - Queue work for later instead of calling Java immediately
    - Use the async client with proper concurrency patterns
    - Restructure code to avoid nested calls
    """

    pass


class CallbackTimeoutError(Exception):
    """Raised when a callback execution exceeds the timeout.

    This prevents hung callbacks from deadlocking the protocol. When a callback
    times out, an error response is sent to Java so the original request can
    complete (with an error) rather than hanging indefinitely.
    """

    pass


class ProtocolDesyncError(Exception):
    """Raised when the client and server are out of sync.

    This typically indicates:
    - Corrupted data on the socket
    - Server crashed mid-response
    - Response size exceeds valid bounds

    The connection should be closed and re-established.
    """

    def __init__(self, message: str, response_size: int | None = None):
        self.response_size = response_size
        super().__init__(message)


class DeadConnectionError(Exception):
    """Raised when attempting to use a dead connection.

    After a ProtocolDesyncError or SocketTimeoutError, the client is marked
    as dead and cannot be used. Create a new client to continue.
    """

    pass


class TypeHint:
    """Wrapper to provide an explicit Java type hint for overload resolution.

    Use this when Python's automatic type inference doesn't select the correct
    Java method overload. This is common with Spark/Scala APIs that have many
    overloaded methods.

    Examples:
        # Force long instead of int
        client.invoke_method(obj, "method", TypeHint(42, "long"))

        # Use interface type instead of implementation
        client.invoke_method(obj, "method", TypeHint(my_list, "java.util.List"))

        # Scala collection types
        client.invoke_method(obj, "method", TypeHint(my_list, "scala.collection.Seq"))
    """

    __slots__ = ("value", "java_type")

    def __init__(self, value, java_type: str):
        """Create a type-hinted value.

        Args:
            value: The Python value to pass
            java_type: The Java type to use for overload resolution.
                       Can be a primitive ("int", "long", "double"), a boxed type
                       ("Integer", "Long"), a full class name ("java.util.List"),
                       or a Scala type ("scala.collection.Seq").
        """
        self.value = value
        self.java_type = java_type

    def __repr__(self):
        return f"TypeHint({self.value!r}, {self.java_type!r})"


# Mapping from Java exception class names to Python exception classes
_JAVA_EXCEPTION_MAP: dict[str, type[Exception]] = {
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


def _raise_java_exception_impl(error_type: str, error_msg: str) -> None:
    """Raise the appropriate Python exception for a Java error.

    This is the core implementation used by both sync and async clients.

    Args:
        error_type: The fully qualified Java exception class name
        error_msg: The full error message including stack trace
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

    # Create and raise the exception
    raise exc_class(error_type, message, error_msg)


class SocketTimeoutError(Exception):
    """Raised when a socket read times out."""

    pass


def _recv_exactly(sock, n, timeout: float | None = None):
    """Receive exactly n bytes from socket, handling partial reads.

    Args:
        sock: The socket to read from
        n: Number of bytes to receive
        timeout: Optional timeout in seconds. If None, uses the socket's
                 current timeout setting without modification.

    Raises:
        SocketTimeoutError: If the timeout expires before receiving all bytes
        RuntimeError: If the socket closes unexpectedly
    """
    # Fast path: no timeout specified, use socket's existing timeout
    # This avoids gettimeout/settimeout overhead in the common case
    if timeout is None:
        data = bytearray()
        while len(data) < n:
            try:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise RuntimeError("Socket closed unexpectedly")
                data.extend(chunk)
            except socket.timeout:
                raise SocketTimeoutError(
                    f"Timeout after receiving {len(data)}/{n} bytes"
                )
        return bytes(data)

    # Slow path: caller specified explicit timeout
    # Use deadline-based approach to handle partial reads correctly
    deadline = time.monotonic() + timeout
    original_timeout = sock.gettimeout()

    try:
        data = bytearray()
        while len(data) < n:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SocketTimeoutError(
                    f"Timeout after receiving {len(data)}/{n} bytes"
                )
            # Set timeout for this recv to remaining time
            sock.settimeout(remaining)
            try:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise RuntimeError("Socket closed unexpectedly")
                data.extend(chunk)
            except socket.timeout:
                raise SocketTimeoutError(
                    f"Timeout after receiving {len(data)}/{n} bytes"
                )
        return bytes(data)
    finally:
        # Restore original timeout
        sock.settimeout(original_timeout)


class StaleArenaError(RuntimeError):
    """Raised when accessing Arrow data from a stale arena epoch.

    This error occurs when trying to use a table returned from get_arrow_data()
    after reset_payload_arena() has been called or the client has been closed.
    The underlying shared memory buffers may have been overwritten or unmapped.
    """

    def __init__(self, table_epoch: int, current_epoch: int):
        if current_epoch == -1:
            # Client was closed - shm is unmapped
            msg = (
                f"Arrow table from epoch {table_epoch} is invalid: client was closed. "
                f"The shared memory buffer has been unmapped."
            )
        else:
            msg = (
                f"Arrow table from epoch {table_epoch} is stale (current epoch: {current_epoch}). "
                f"Tables become invalid after reset_payload_arena() is called."
            )
        super().__init__(msg)
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
        """Raise StaleArenaError if epoch changed or client closed."""
        # Check if client's shm was closed - buffer pointers would be invalid
        if self._client.shm is None:
            raise StaleArenaError(
                self._epoch,
                -1,  # Sentinel indicating client closed
            )
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
        return cast(int, self._table.num_rows)

    @property
    def num_columns(self) -> int:
        """Get column count (safe, doesn't access buffer data)."""
        return cast(int, self._table.num_columns)

    @property
    def schema(self) -> pa.Schema:
        """Get schema (safe, doesn't access buffer data)."""
        return self._table.schema

    @property
    def column_names(self) -> list[str]:
        """Get column names (safe, doesn't access buffer data)."""
        return cast(list[str], self._table.column_names)

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
        struct_type = pa.struct(
            [
                pa.field("key", arrow_type.key_type, nullable=False),
                pa.field("value", arrow_type.item_type),
            ]
        )
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

    def __eq__(self, other):
        """Compare with another JavaObject using Java's equals() method."""
        if not isinstance(other, JavaObject):
            return False
        # If same object_id, they're definitely equal
        if self.object_id == other.object_id:
            return True
        # Otherwise, call Java's equals() method
        try:
            return self.client.invoke_method(self.object_id, "equals", other)
        except Exception:
            return False

    def __hash__(self):
        """Return Java's hashCode() for this object."""
        try:
            return self.client.invoke_method(self.object_id, "hashCode")
        except Exception:
            # Fallback to object_id if we can't get hashCode
            return hash(self.object_id)

    def __len__(self):
        """Return length of Java array or collection.

        For arrays, uses Array.getLength().
        For collections, calls size().
        Raises TypeError if object doesn't support length.
        """
        # Try array length first
        try:
            return self.client.invoke_static_method(
                "java.lang.reflect.Array", "getLength", self
            )
        except Exception:
            pass

        # Try collection size()
        try:
            return self.client.invoke_method(self.object_id, "size")
        except Exception:
            pass

        # Object doesn't support length
        raise TypeError("object has no len()")

    def __bool__(self):
        """Return True if this is a valid Java object reference.

        This prevents bool() from calling __len__() on objects that
        don't support length (like JavaSparkContext).
        """
        # A JavaObject with a valid object_id is truthy
        return self.object_id is not None

    def __iter__(self):
        """Iterate over Java array or collection.

        For arrays, uses Array.get() with index.
        For Iterable objects, calls iterator().
        """
        try:
            # Try array iteration first
            length = self.client.invoke_static_method(
                "java.lang.reflect.Array", "getLength", self
            )
            for i in range(length):
                yield self.client.invoke_static_method(
                    "java.lang.reflect.Array", "get", self, i
                )
        except Exception:
            # Fall back to Iterable.iterator()
            iterator = self.client.invoke_method(self.object_id, "iterator")
            while self.client.invoke_method(iterator.object_id, "hasNext"):
                yield self.client.invoke_method(iterator.object_id, "next")

    def __getitem__(self, index):
        """Get element at index from Java array or list.

        For arrays, uses Array.get().
        For lists, calls get(index).
        """
        try:
            # Try array access first
            return self.client.invoke_static_method(
                "java.lang.reflect.Array", "get", self, index
            )
        except Exception:
            # Fall back to list.get(index)
            return self.client.invoke_method(self.object_id, "get", index)

    def __setitem__(self, index, value):
        """Set element at index in Java array or list.

        For arrays, uses Array.set().
        For lists, calls set(index, value).
        """
        try:
            # Try array access first
            self.client.invoke_static_method(
                "java.lang.reflect.Array", "set", self, index, value
            )
        except Exception:
            # Fall back to list.set(index, value)
            self.client.invoke_method(self.object_id, "set", index, value)


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
                        cached_type = _reflect_cache.get(full_path)
                        if cached_type is None:
                            cached_type = self._client.reflect(full_path)
                            _reflect_cache.set(full_path, cached_type)
                        if cached_type in ("class", "method", "field"):
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


# Maximum size for LRU caches to prevent unbounded memory growth
_CACHE_MAX_SIZE = 10000


class _LRUCache[V]:
    """Simple LRU cache with bounded size.

    Uses OrderedDict to maintain insertion order. When the cache is full,
    the oldest entry is evicted. Access (get) moves the entry to the end.
    """

    def __init__(self, maxsize: int = _CACHE_MAX_SIZE):
        self._cache: OrderedDict[str, V] = OrderedDict()
        self._maxsize = maxsize

    def get(self, key: str) -> V | None:
        """Get a value from the cache, moving it to the end (most recent)."""
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def set(self, key: str, value: V) -> None:
        """Set a value in the cache, evicting oldest if full."""
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self._maxsize:
                self._cache.popitem(last=False)  # Remove oldest
        self._cache[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __getitem__(self, key: str) -> V:
        """Get a value from the cache (without moving it to the end)."""
        return self._cache[key]

    def pop(self, key: str, default: V | None = None) -> V | None:
        """Remove and return a value from the cache."""
        return self._cache.pop(key, default)

    def __len__(self) -> int:
        return len(self._cache)


# Cache for reflection results to avoid repeated Java calls
# Maps fully qualified name -> type ("class", "method", "field", "none")
# Bounded to prevent memory bloat in long-running applications
_reflect_cache: _LRUCache[str] = _LRUCache(_CACHE_MAX_SIZE)


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
        cached = _reflect_cache.get(self._path)
        if cached is None:
            cached = self._client.reflect(self._path)
            _reflect_cache.set(self._path, cached)
        return cached

    def __getattr__(self, name):
        """Navigate deeper or access static method/field."""
        new_path = f"{self._path}.{name}"

        # Get the last segment of the current path to check if it looks like a class
        last_segment = (
            self._path.rsplit(".", 1)[-1] if "." in self._path else self._path
        )
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
            method_name = self._path[last_dot + 1 :]
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
                method_name = self._path[last_dot + 1 :]
                parent_last_segment = parent_path.rsplit(".", 1)[-1]

                # If parent looks like a class (starts with uppercase) and
                # method name looks like a method (starts with lowercase),
                # try as static method first
                if (
                    parent_last_segment
                    and parent_last_segment[0].isupper()
                    and method_name
                    and method_name[0].islower()
                ):
                    return self._client.invoke_static_method(
                        parent_path, method_name, *args
                    )

            # Fallback: try as class instantiation (backward compatibility)
            # This handles packages that haven't been loaded yet
            return self._client.create_object(self._path, *args)

    def __repr__(self):
        return f"<JVM {self._path}>"


class GatunClient:
    def __init__(
        self,
        socket_path=None,
        callback_timeout: float | None = None,
        socket_timeout: float | None = None,
        handshake_timeout: float | None = None,
    ):
        """Initialize a Gatun client.

        Args:
            socket_path: Path to the Unix domain socket for communication.
                        Defaults to ~/gatun.sock.
            callback_timeout: Timeout in seconds for callback execution.
                             Prevents hung callbacks from deadlocking the protocol.
                             Defaults to DEFAULT_CALLBACK_TIMEOUT (30s).
                             Set to None to disable timeout (not recommended).
            socket_timeout: Timeout in seconds for socket read operations.
                           Used for response reads to prevent indefinite hangs.
                           Defaults to DEFAULT_SOCKET_TIMEOUT (30s).
                           Set to None to disable timeout (not recommended).
            handshake_timeout: Timeout in seconds for the initial handshake.
                              Shorter than socket_timeout since handshake should be fast.
                              Defaults to HANDSHAKE_TIMEOUT (10s).
                              Set to None to disable timeout (not recommended).
        """
        if socket_path is None:
            socket_path = os.path.expanduser("~/gatun.sock")

        self.socket_path = socket_path
        self.memory_path: str | None = (
            None  # Set during connect() from server handshake
        )
        self.callback_timeout = (
            callback_timeout
            if callback_timeout is not None
            else DEFAULT_CALLBACK_TIMEOUT
        )
        self.socket_timeout = (
            socket_timeout if socket_timeout is not None else DEFAULT_SOCKET_TIMEOUT
        )
        self.handshake_timeout = (
            handshake_timeout if handshake_timeout is not None else HANDSHAKE_TIMEOUT
        )

        self.sock: socket.socket | None = socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        self.shm_file: Any = None  # BufferedRandom when connected
        self.shm: mmap.mmap | None = None

        # These are set during connect()
        self.memory_size = 0
        self.command_offset = 0
        self.payload_offset = 65536  # 64KB - must match GatunServer.PAYLOAD_OFFSET
        self.response_offset = 0

        # JVM view for package-style access
        self._jvm: JVMView | None = None

        # Callback registry: callback_id -> callable
        self._callbacks: dict[int, Callable[..., Any]] = {}

        # Request ID counter for cancellation support
        self._next_request_id = 1

        # Arrow schema cache for Java -> Python transfers: hash -> Schema
        self._arrow_schema_cache: dict[int, pa.Schema] = {}

        # Arena epoch for lifetime safety - tracks current valid epoch
        self._arena_epoch: int = 0

        # FlatBuffers builder pool - reuse builders to reduce allocation overhead
        # Small builder for simple commands (256 bytes initial)
        self._builder_small = flatbuffers.Builder(256)
        # Large builder for commands with arguments/data (1024 bytes initial)
        self._builder_large = flatbuffers.Builder(1024)

        # String encoding cache - cache UTF-8 encoded bytes for commonly used strings
        # This avoids repeated str.encode() calls for class/method names
        # Bounded to prevent memory bloat in long-running applications
        self._string_cache: _LRUCache[bytes] = _LRUCache(_CACHE_MAX_SIZE)

        # Count of fire-and-forget commands sent without reading responses
        # These responses must be drained before the next synchronous command
        self._pending_responses: int = 0

        # Reentrancy detection - tracks if we're inside a callback.
        # Nested Java calls from callbacks would deadlock the single-stream protocol.
        # Note: Java serializes callbacks per session via synchronized(ctx.callbackLock),
        # so we're guaranteed only one callback is in-flight at a time per connection.
        self._in_callback: bool = False

        # Dead connection flag - set after protocol desync or timeout
        # Once dead, all operations will fail fast with DeadConnectionError
        self._dead: bool = False

    def _create_string(self, builder: flatbuffers.Builder, s: str) -> int:
        """Create a FlatBuffers string with caching of encoded bytes.

        Caches the UTF-8 encoded bytes of strings to avoid repeated encoding.
        Bounded to 10,000 entries to prevent memory bloat.
        """
        encoded = self._string_cache.get(s)
        if encoded is None:
            encoded = s.encode("utf-8")
            self._string_cache.set(s, encoded)
        return cast(int, builder.CreateString(encoded))

    def _get_builder(self, large: bool = False) -> flatbuffers.Builder:
        """Get a reusable FlatBuffers builder.

        Clears and returns an existing builder from the pool to avoid allocation.
        Use large=True for commands with arguments or data.

        Note: We cannot preserve vtables across clears because vtable entries
        contain absolute buffer offsets that become invalid after Clear().
        """
        builder = self._builder_large if large else self._builder_small
        builder.Clear()
        return builder

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

            # 1. Handshake - read version, epoch, memory size, and SHM path
            # Format: [4 bytes: version] [4 bytes: arena_epoch] [8 bytes: memory size]
            #         [2 bytes: shm_path_length] [N bytes: shm_path (UTF-8)]
            # Use timeout to prevent indefinite hang if server doesn't respond
            handshake_header = _recv_exactly(
                self.sock, 18, timeout=self.handshake_timeout
            )
            server_version, arena_epoch, self.memory_size, shm_path_len = struct.unpack(
                "<IIQH", handshake_header
            )

            # Verify protocol version
            if server_version != PROTOCOL_VERSION:
                raise RuntimeError(
                    f"Protocol version mismatch: client={PROTOCOL_VERSION}, "
                    f"server={server_version}. Please update your client or server."
                )

            # Read SHM path
            shm_path_bytes = _recv_exactly(
                self.sock, shm_path_len, timeout=self.handshake_timeout
            )
            self.memory_path = shm_path_bytes.decode("utf-8")

            # Synchronize arena epoch with server
            self._arena_epoch = arena_epoch

            # 2. Configure Offsets
            # Response zone size must match GatunServer.RESPONSE_ZONE_SIZE (64KB)
            self.response_offset = self.memory_size - 65536

            # 3. Map Memory (session-specific SHM file created by server)
            assert self.memory_path is not None, "Memory path not set"
            self.shm_file = open(self.memory_path, "r+b")
            self.shm = mmap.mmap(self.shm_file.fileno(), self.memory_size)
            logger.info(
                "Connected to %s (shared memory: %s, %.2f MB)",
                self.socket_path,
                self.memory_path,
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
        assert self.shm is not None, "Not connected"
        return PayloadArena.from_mmap(self.shm, self.payload_offset, payload_size)

    def _send_raw(self, data: bytes, wait_for_response=True, expects_response=True):
        """
        Writes command to SHM and signals Java.
        If wait_for_response is False, returns immediately (Fire-and-Forget).
        If expects_response is False, Java won't send a response (used for CallbackResponse).
        """
        # 0a. Check if connection is dead - fail fast
        if self._dead:
            raise DeadConnectionError(
                "Connection is dead after protocol error. Create a new client."
            )

        # 0b. Check for reentrancy - nested calls from callbacks would deadlock
        # Exception: CallbackResponse is sent during callback handling
        if self._in_callback and expects_response:
            raise ReentrancyError(
                "Cannot call Java from within a callback. "
                "Nested calls would deadlock because the original request is still waiting. "
                "Queue work for later or restructure code to avoid nested calls."
            )

        # 1. Validate command size
        max_command_size = self.payload_offset  # Command zone ends where payload starts
        if len(data) > max_command_size:
            raise PayloadTooLargeError(len(data), max_command_size, "Command")

        # 2. Drain any pending responses from fire-and-forget commands
        # This must happen before writing to shared memory to avoid race conditions
        if wait_for_response:
            self._drain_pending_responses()

        # 3. Write to Shared Memory
        self.shm.seek(self.command_offset)
        self.shm.write(data)
        # Flush to ensure writes are visible to Java before signaling.
        # On most platforms mmap writes are immediately visible, but flush
        # provides a memory barrier that guarantees ordering.
        self.shm.flush()

        # 4. Signal Java (Send Length)
        # Verify socket is open (check None first to avoid AttributeError)
        if not self.sock or self.sock.fileno() == -1:
            self._mark_dead()
            raise DeadConnectionError("Socket already closed")

        try:
            self.sock.sendall(struct.pack("<I", len(data)))
        except (OSError, BrokenPipeError, ConnectionResetError) as e:
            self._mark_dead()
            raise ProtocolDesyncError(f"Socket error sending command: {e}")

        # 5. Handle Response
        if wait_for_response:
            return self._read_response()
        elif expects_response:
            # Track that we have a pending response to drain later
            # (fire-and-forget commands like FreeObject still get responses)
            self._pending_responses += 1
        # If not wait_for_response and not expects_response: truly one-way (CallbackResponse)

    def _mark_dead(self):
        """Mark client as dead after protocol desync.

        After a timeout or invalid response, the connection is in an unknown state.
        We must close the socket to prevent cascading failures from reading garbage.
        The SHM mapping is left intact so any existing JavaObject references don't
        crash, but the socket is closed so subsequent operations will fail cleanly.

        Once marked dead:
        - All future operations will immediately raise DeadConnectionError
        - The socket is closed (shutdown + close)
        - The client cannot be reused - create a new one
        """
        if self._dead:
            return  # Already dead

        self._dead = True
        self._pending_responses = 0

        # Shutdown socket to unblock any pending reads, then close
        try:
            if self.sock:
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass  # May fail if already disconnected
                self.sock.close()
                self.sock = None
        except Exception:
            pass

    def _drain_pending_responses(self, timeout: float = 5.0, max_drain: int = 100):
        """Drain any pending responses from fire-and-forget commands.

        Fire-and-forget commands (like FreeObject) still get responses from Java.
        We must read and discard these responses before the next synchronous
        command to keep the response stream in sync.

        IMPORTANT: This must handle callback requests that may arrive during drain.
        If Java sends a callback request, we must handle it (execute the callback
        and send CallbackResponse) before continuing to drain. Otherwise Java will
        block waiting for our callback response and we'll deadlock.

        Args:
            timeout: Timeout in seconds for each individual response read.
                    If a single drain times out, we treat it as a desync.
            max_drain: Maximum number of responses to drain in one call.
                      Prevents infinite loops if _pending_responses is wrong.

        Raises:
            ProtocolDesyncError: If drain times out or response size is invalid.
                                Connection is closed before raising.
        """
        drained = 0
        errors_discarded = 0
        # Track total reads to prevent infinite callback loops
        total_reads = 0
        max_total_reads = max_drain * 10  # Allow callbacks but not infinite

        while self._pending_responses > 0 and drained < max_drain:
            if total_reads >= max_total_reads:
                self._mark_dead()
                raise ProtocolDesyncError(
                    f"Too many reads during drain ({total_reads}), possible callback loop"
                )
            total_reads += 1

            try:
                # Read response length with timeout
                sz_data = _recv_exactly(self.sock, 4, timeout=timeout)
                sz = struct.unpack("<I", sz_data)[0]

                # Validate response size (same checks as _read_response)
                max_response_size = self.memory_size - self.response_offset
                if sz == 0 or sz > RESPONSE_ZONE_SIZE or sz > max_response_size:
                    self._mark_dead()
                    raise ProtocolDesyncError(
                        f"Invalid response size during drain: {sz}", response_size=sz
                    )

                # Read response data from SHM
                self.shm.seek(self.response_offset)
                resp_buf = self.shm.read(sz)

                # Validate we got all the bytes (mmap.read can return fewer)
                if len(resp_buf) != sz:
                    self._mark_dead()
                    raise ProtocolDesyncError(
                        f"SHM read returned {len(resp_buf)} bytes, expected {sz}",
                        response_size=sz,
                    )

                # Parse the response to check if it's a callback
                resp = Response.Response.GetRootAsResponse(resp_buf, 0)

                if resp.IsCallback():
                    # Must handle callback - Java is blocked waiting for our response
                    # This does NOT count as draining a pending response
                    self._handle_callback(resp)
                    continue

                # It's a real response - drain complete for this one
                # Log discarded errors for diagnosability (fire-and-forget commands
                # that failed may indicate issues even if we don't raise here)
                if resp.IsError():
                    errors_discarded += 1
                    error_msg = resp.ErrorMsg()
                    if error_msg:
                        error_text = error_msg.decode("utf-8", errors="replace")
                        # Truncate long error messages for logging
                        if len(error_text) > 200:
                            error_text = error_text[:200] + "..."
                        logger.debug("Discarded error during drain: %s", error_text)

                self._pending_responses -= 1
                drained += 1

            except SocketTimeoutError:
                # Timeout during drain means protocol desync - close connection
                self._mark_dead()
                raise ProtocolDesyncError(
                    f"Timeout draining pending responses (drained {drained})"
                )
            except (RuntimeError, OSError, BrokenPipeError, ConnectionResetError) as e:
                # Socket error during drain (e.g., from _handle_callback sending response,
                # or "Socket closed unexpectedly" from _recv_exactly)
                self._mark_dead()
                raise ProtocolDesyncError(f"Socket error during drain: {e}")

        if self._pending_responses > 0:
            # Hit max_drain limit - likely a bug or desync
            remaining = self._pending_responses
            self._mark_dead()
            raise ProtocolDesyncError(
                f"Too many pending responses to drain: {remaining + drained} "
                f"(max_drain={max_drain})"
            )

        # Log summary if we discarded any errors
        if errors_discarded > 0:
            logger.debug(
                "Drain completed: %d responses drained, %d errors discarded",
                drained,
                errors_discarded,
            )

    def _unpack_value(self, val_type: int, val_table: Any) -> Any:
        """Unpack a FlatBuffer Value union to a Python object."""
        if val_type == Value.Value.NullVal:
            return None
        elif val_type == Value.Value.StringVal:
            str_obj = StringVal.StringVal()
            str_obj.Init(val_table.Bytes, val_table.Pos)
            return str_obj.V().decode("utf-8")
        elif val_type == Value.Value.IntVal:
            int_obj = IntVal.IntVal()
            int_obj.Init(val_table.Bytes, val_table.Pos)
            return int_obj.V()
        elif val_type == Value.Value.DoubleVal:
            double_obj = DoubleVal.DoubleVal()
            double_obj.Init(val_table.Bytes, val_table.Pos)
            return double_obj.V()
        elif val_type == Value.Value.BoolVal:
            bool_obj = BoolVal.BoolVal()
            bool_obj.Init(val_table.Bytes, val_table.Pos)
            return bool_obj.V()
        elif val_type == Value.Value.CharVal:
            char_obj = CharVal.CharVal()
            char_obj.Init(val_table.Bytes, val_table.Pos)
            return chr(char_obj.V())  # Convert to Python str (single char)
        elif val_type == Value.Value.ObjectRef:
            ref_obj = ObjectRef.ObjectRef()
            ref_obj.Init(val_table.Bytes, val_table.Pos)
            return JavaObject(self, ref_obj.Id())
        elif val_type == Value.Value.ListVal:
            list_obj = ListVal.ListVal()
            list_obj.Init(val_table.Bytes, val_table.Pos)
            list_result = JavaList()
            for i in range(list_obj.ItemsLength()):
                item = list_obj.Items(i)
                list_result.append(self._unpack_value(item.ValType(), item.Val()))
            return list_result
        elif val_type == Value.Value.MapVal:
            map_obj = MapVal.MapVal()
            map_obj.Init(val_table.Bytes, val_table.Pos)
            map_result: dict[Any, Any] = {}
            for i in range(map_obj.EntriesLength()):
                entry = map_obj.Entries(i)
                key_arg = entry.Key()
                val_arg = entry.Value()
                key = self._unpack_value(key_arg.ValType(), key_arg.Val())
                val = self._unpack_value(val_arg.ValType(), val_arg.Val())
                map_result[key] = val
            return map_result
        elif val_type == Value.Value.ArrayVal:
            arr_obj = ArrayVal.ArrayVal()
            arr_obj.Init(val_table.Bytes, val_table.Pos)
            return self._unpack_array(arr_obj)

        return None

    def _unpack_array(self, array_val):
        """Unpack an ArrayVal FlatBuffer to a JavaArray (preserves array semantics).

        Uses direct buffer slicing with struct.unpack for primitive types,
        which is much faster than element-by-element access.
        """
        elem_type = array_val.ElementType()
        tab = array_val._tab

        if elem_type == ElementType.ElementType.Int:
            length = array_val.IntValuesLength()
            values = _unpack_flatbuffer_vector(
                tab, _ARRAYVAL_INT_VALUES_OFFSET, length, "i", 4
            )
            return JavaArray(values, element_type="Int")
        elif elem_type == ElementType.ElementType.Long:
            length = array_val.LongValuesLength()
            values = _unpack_flatbuffer_vector(
                tab, _ARRAYVAL_LONG_VALUES_OFFSET, length, "q", 8
            )
            return JavaArray(values, element_type="Long")
        elif elem_type == ElementType.ElementType.Double:
            length = array_val.DoubleValuesLength()
            values = _unpack_flatbuffer_vector(
                tab, _ARRAYVAL_DOUBLE_VALUES_OFFSET, length, "d", 8
            )
            return JavaArray(values, element_type="Double")
        elif elem_type == ElementType.ElementType.Float:
            # Float was widened to double
            length = array_val.DoubleValuesLength()
            values = _unpack_flatbuffer_vector(
                tab, _ARRAYVAL_DOUBLE_VALUES_OFFSET, length, "d", 8
            )
            return JavaArray(values, element_type="Float")
        elif elem_type == ElementType.ElementType.Bool:
            length = array_val.BoolValuesLength()
            values = _unpack_flatbuffer_vector(
                tab, _ARRAYVAL_BOOL_VALUES_OFFSET, length, "?", 1
            )
            return JavaArray(values, element_type="Bool")
        elif elem_type == ElementType.ElementType.Byte:
            length = array_val.ByteValuesLength()
            # Direct buffer slice for bytes
            o = flatbuffers.number_types.UOffsetTFlags.py_type(
                tab.Offset(_ARRAYVAL_BYTE_VALUES_OFFSET)
            )
            if o == 0 or length == 0:
                return b""
            start = tab.Vector(o)
            return bytes(tab.Bytes[start : start + length])
        elif elem_type == ElementType.ElementType.Short:
            # Short was widened to int
            length = array_val.IntValuesLength()
            values = _unpack_flatbuffer_vector(
                tab, _ARRAYVAL_INT_VALUES_OFFSET, length, "i", 4
            )
            return JavaArray(values, element_type="Short")
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

    def _read_response(self, timeout: float | None = None, _use_default: bool = True):
        """Read and parse a response from the server.

        Args:
            timeout: Socket read timeout in seconds. None for no timeout.
                    Default uses self.socket_timeout (30 seconds by default).
            _use_default: If True and timeout is None, use self.socket_timeout.

        Raises:
            SocketTimeoutError: If the socket read times out.
                               Connection is closed before raising.
            ProtocolDesyncError: If the response size is invalid.
                                Connection is closed before raising.
        """
        # Resolve to instance default if requested
        if _use_default and timeout is None:
            timeout = self.socket_timeout

        while True:
            # 1. Read Length with timeout
            try:
                sz_data = _recv_exactly(self.sock, 4, timeout=timeout)
            except SocketTimeoutError:
                # Timeout during read means connection is in unknown state
                self._mark_dead()
                raise
            except (RuntimeError, OSError, BrokenPipeError, ConnectionResetError) as e:
                # Socket closed or error - mark dead and convert to ProtocolDesyncError
                self._mark_dead()
                raise ProtocolDesyncError(f"Socket error reading response: {e}")
            sz = struct.unpack("<I", sz_data)[0]

            # 2. Validate response size before reading from SHM
            # This catches protocol desync (garbage data) early
            # On invalid size, close the connection to prevent cascading failures
            max_response_size = self.memory_size - self.response_offset
            if sz == 0:
                self._mark_dead()
                raise ProtocolDesyncError(
                    "Invalid response size: 0 bytes (empty response)", response_size=sz
                )
            if sz > RESPONSE_ZONE_SIZE:
                self._mark_dead()
                raise ProtocolDesyncError(
                    f"Response size {sz} exceeds RESPONSE_ZONE_SIZE ({RESPONSE_ZONE_SIZE})",
                    response_size=sz,
                )
            if sz > max_response_size:
                self._mark_dead()
                raise ProtocolDesyncError(
                    f"Response size {sz} exceeds available SHM space ({max_response_size})",
                    response_size=sz,
                )

            # 3. Read Data from SHM
            self.shm.seek(self.response_offset)
            resp_buf = self.shm.read(sz)

            # Validate we got all the bytes (mmap.read can return fewer)
            if len(resp_buf) != sz:
                self._mark_dead()
                raise ProtocolDesyncError(
                    f"SHM read returned {len(resp_buf)} bytes, expected {sz}",
                    response_size=sz,
                )

            # 4. Parse FlatBuffer
            resp = Response.Response.GetRootAsResponse(resp_buf, 0)

            # Check if this is a callback request from Java
            if resp.IsCallback():
                try:
                    self._handle_callback(resp)
                except (OSError, BrokenPipeError, ConnectionResetError) as e:
                    # Socket error while sending callback response
                    self._mark_dead()
                    raise ProtocolDesyncError(f"Socket error during callback: {e}")
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
        """Raise the appropriate Python exception for a Java error."""
        _raise_java_exception_impl(error_type, error_msg)

    def _handle_callback(self, resp):
        """Handle a callback invocation request from Java.

        Sets _in_callback flag to detect and prevent reentrant calls, which would
        deadlock. The flag is always cleared in finally to ensure cleanup even if
        the callback raises an exception.

        If callback_timeout is set, the callback is executed in a separate thread
        with a timeout to prevent hung callbacks from deadlocking the protocol.
        """
        import threading

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

        # Execute the callback with reentrancy guard
        self._in_callback = True
        try:
            if self.callback_timeout is None or self.callback_timeout <= 0:
                # No timeout - execute directly (not recommended but supported)
                result = callback_fn(*args)
                self._send_callback_response(callback_id, result, False, None)
            else:
                # Execute with timeout using a thread
                result_holder: dict[str, Any] = {
                    "result": None,
                    "error": None,
                    "done": False,
                }

                def run_callback() -> None:
                    try:
                        result_holder["result"] = callback_fn(*args)
                    except Exception as e:
                        result_holder["error"] = e
                    finally:
                        result_holder["done"] = True

                thread = threading.Thread(target=run_callback, daemon=True)
                thread.start()
                thread.join(timeout=self.callback_timeout)

                if not result_holder["done"]:
                    # Timeout - callback is still running (possibly hung)
                    # We can't kill the thread, but we must respond to Java
                    self._send_callback_response(
                        callback_id,
                        None,
                        True,
                        f"CallbackTimeoutError: Callback execution exceeded {self.callback_timeout}s timeout",
                    )
                    return
                elif result_holder["error"] is not None:
                    raise cast(Exception, result_holder["error"])
                else:
                    self._send_callback_response(
                        callback_id, result_holder["result"], False, None
                    )
        except ReentrancyError:
            # Propagate reentrancy error with clear message
            self._send_callback_response(
                callback_id,
                None,
                True,
                "ReentrancyError: Cannot call Java from within a callback",
            )
        except Exception as e:
            self._send_callback_response(callback_id, None, True, str(e))
        finally:
            self._in_callback = False

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
            # Direct buffer slice - much faster than element-by-element access
            # Access the underlying FlatBuffer and slice the vector directly
            o = flatbuffers.number_types.UOffsetTFlags.py_type(
                arrow_batch._tab.Offset(_ARROW_BATCH_SCHEMA_BYTES_OFFSET)
            )
            start = arrow_batch._tab.Vector(o)
            schema_bytes = bytes(
                arrow_batch._tab.Bytes[start : start + schema_bytes_len]
            )
            schema = deserialize_schema(schema_bytes)
            # Validate schema doesn't contain unsupported types (e.g., dictionary)
            _validate_supported_schema(schema)
            self._arrow_schema_cache[schema_hash] = schema

        # Get base address of payload zone in shared memory
        assert self.shm is not None, "Not connected"
        base_address = ctypes.addressof(ctypes.c_char.from_buffer(self.shm))
        payload_base = base_address + self.payload_offset

        # Build PyArrow buffers from buffer descriptors
        buffers: list[pa.Buffer | None] = []
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
        builder = self._get_builder(large=True)

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
        # Truncate large error messages to prevent exceeding command zone size.
        # During fuzzing, callback exceptions can have huge repr() or stack traces.
        # If we don't truncate, _send_raw will raise PayloadTooLargeError and
        # Java will be stuck waiting for a CallbackResponse that never arrives.
        target_name_off = None
        if error_msg:
            if len(error_msg) > MAX_CALLBACK_ERROR_SIZE:
                error_msg = error_msg[:MAX_CALLBACK_ERROR_SIZE] + "...<truncated>"
            target_name_off = self._create_string(builder, error_msg)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CallbackResponse)
        Cmd.CommandAddTargetId(builder, callback_id)
        if target_name_off:
            Cmd.CommandAddTargetName(builder, target_name_off)
        Cmd.CommandAddArgs(builder, args_vec)
        cmd_offset = Cmd.CommandEnd(builder)
        builder.Finish(cmd_offset)

        # Send it (don't wait for response - Java is waiting for this)
        # Java doesn't send a response for CallbackResponse, so expects_response=False
        self._send_raw(
            builder.Output(), wait_for_response=False, expects_response=False
        )

    def create_object(self, class_name, *args):
        builder = self._get_builder(large=True)
        cls_off = self._create_string(builder, class_name)

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
        builder = self._get_builder(large=True)
        meth_off = self._create_string(builder, method_name)

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

    def invoke_static_method(
        self, class_name, method_name, *args, return_object_ref: bool = False
    ):
        """Invoke a static method on a class.

        Args:
            class_name: Fully qualified class name (e.g., "java.lang.Integer")
            method_name: Method name (e.g., "parseInt")
            *args: Arguments to pass to the method
            return_object_ref: If True, return result as ObjectRef (no auto-conversion)
        """
        builder = self._get_builder(large=True)
        # Format: "fully.qualified.ClassName.methodName"
        full_name = f"{class_name}.{method_name}"
        name_off = self._create_string(builder, full_name)

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
        if return_object_ref:
            Cmd.CommandAddReturnObjectRef(builder, True)

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

        builder = self._get_builder()
        name_off = self._create_string(builder, field_name)

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

        builder = self._get_builder()
        name_off = self._create_string(builder, field_name)

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
        builder = self._get_builder()
        full_name = f"{class_name}.{field_name}"
        name_off = self._create_string(builder, full_name)

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
        builder = self._get_builder()
        full_name = f"{class_name}.{field_name}"
        name_off = self._create_string(builder, full_name)

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
        builder = self._get_builder()
        name_off = self._create_string(builder, full_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Reflect)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(str, self._send_raw(builder.Output()))

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

        builder = self._get_builder()
        name_off = self._create_string(builder, class_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.IsInstanceOf)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(bool, self._send_raw(builder.Output()))

    def get_metrics(self) -> str:
        """Get server metrics report.

        Returns a human-readable report of server performance metrics including:
        - Total request counts and error rates
        - Requests per second
        - Current and peak object counts
        - Arrow data transfer statistics
        - Callback statistics
        - Per-action latency percentiles (p50, p99)

        Returns:
            String containing the metrics report.

        Example:
            metrics = client.get_metrics()
            print(metrics)
        """
        builder = self._get_builder()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetMetrics)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(str, self._send_raw(builder.Output()))

    def get_fields(self, obj, field_names: list[str]) -> list[Any]:
        """Get multiple field values from a Java object in a single round-trip.

        This is more efficient than calling get_field() multiple times when
        you need to read several fields from the same object.

        Args:
            obj: JavaObject instance or object ID
            field_names: List of field names to read

        Returns:
            List of field values in the same order as field_names.

        Example:
            point = client.create_object("java.awt.Point", 10, 20)
            x, y = client.get_fields(point, ["x", "y"])  # Single round-trip
        """
        if isinstance(obj, JavaObject):
            obj_id = obj.object_id
        else:
            obj_id = obj

        builder = self._get_builder(large=True)

        # Build field names vector (strings must be created before the table)
        field_name_offsets = [
            self._create_string(builder, name) for name in field_names
        ]
        GetFieldsRequest.StartFieldNamesVector(builder, len(field_name_offsets))
        for offset in reversed(field_name_offsets):
            builder.PrependUOffsetTRelative(offset)
        field_names_vec = builder.EndVector()

        # Build GetFieldsRequest
        GetFieldsRequest.Start(builder)
        GetFieldsRequest.AddFieldNames(builder, field_names_vec)
        get_fields_off = GetFieldsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetFields)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddGetFields(builder, get_fields_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list[Any], self._send_raw(builder.Output()))

    def invoke_methods(
        self,
        obj,
        calls: list[tuple[str, tuple]],
        return_object_refs: bool | list[bool] = False,
    ) -> list[Any]:
        """Invoke multiple methods on the same Java object in a single round-trip.

        This is more efficient than calling invoke_method() multiple times when
        you need to call several methods on the same object.

        Args:
            obj: JavaObject instance or object ID
            calls: List of (method_name, args_tuple) pairs.
                   Each tuple is (method_name, (arg1, arg2, ...))
            return_object_refs: If True (or list of bools), return results as
                               ObjectRefs instead of auto-converting to Python.

        Returns:
            List of results in the same order as calls.

        Example:
            arr = client.create_object("java.util.ArrayList")
            # Add 3 items and get size in one round-trip
            results = client.invoke_methods(arr, [
                ("add", ("a",)),
                ("add", ("b",)),
                ("add", ("c",)),
                ("size", ()),
            ])
            # results = [True, True, True, 3]
        """
        if isinstance(obj, JavaObject):
            obj_id = obj.object_id
        else:
            obj_id = obj

        # Normalize return_object_refs to a list
        if isinstance(return_object_refs, bool):
            return_object_refs = [return_object_refs] * len(calls)

        builder = self._get_builder(large=True)

        # Build each MethodCall (must be done in reverse order for FlatBuffers)
        method_call_offsets = []
        for i, (method_name, args) in enumerate(calls):
            # Build method name string
            method_name_off = self._create_string(builder, method_name)

            # Build arguments
            arg_offsets = []
            for arg in args:
                arg_offset = self._build_argument(builder, arg)
                arg_offsets.append(arg_offset)

            args_vec = None
            if arg_offsets:
                MethodCall.StartArgsVector(builder, len(arg_offsets))
                for offset in reversed(arg_offsets):
                    builder.PrependUOffsetTRelative(offset)
                args_vec = builder.EndVector()

            # Build MethodCall
            MethodCall.Start(builder)
            MethodCall.AddMethodName(builder, method_name_off)
            if args_vec:
                MethodCall.AddArgs(builder, args_vec)
            if return_object_refs[i]:
                MethodCall.AddReturnObjectRef(builder, True)
            method_call_offsets.append(MethodCall.End(builder))

        # Build method calls vector
        InvokeMethodsRequest.StartMethodCallsVector(builder, len(method_call_offsets))
        for offset in reversed(method_call_offsets):
            builder.PrependUOffsetTRelative(offset)
        method_calls_vec = builder.EndVector()

        # Build InvokeMethodsRequest
        InvokeMethodsRequest.Start(builder)
        InvokeMethodsRequest.AddMethodCalls(builder, method_calls_vec)
        invoke_methods_off = InvokeMethodsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeMethods)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddInvokeMethods(builder, invoke_methods_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list[Any], self._send_raw(builder.Output()))

    def create_objects(self, specs: list[tuple[str, tuple]]) -> list["JavaObject"]:
        """Create multiple Java objects in a single round-trip.

        This is more efficient than calling create_object() multiple times when
        you need to create several objects.

        Args:
            specs: List of (class_name, args_tuple) pairs.
                   Each tuple is (fully_qualified_class_name, (arg1, arg2, ...))

        Returns:
            List of JavaObject instances in the same order as specs.

        Example:
            # Create 3 ArrayLists in one round-trip
            lists = client.create_objects([
                ("java.util.ArrayList", ()),
                ("java.util.ArrayList", (100,)),  # with initial capacity
                ("java.util.HashMap", ()),
            ])
        """
        builder = self._get_builder(large=True)

        # Build each ObjectSpec
        object_spec_offsets = []
        for class_name, args in specs:
            # Build class name string
            class_name_off = self._create_string(builder, class_name)

            # Build arguments
            arg_offsets = []
            for arg in args:
                arg_offset = self._build_argument(builder, arg)
                arg_offsets.append(arg_offset)

            args_vec = None
            if arg_offsets:
                ObjectSpec.StartArgsVector(builder, len(arg_offsets))
                for offset in reversed(arg_offsets):
                    builder.PrependUOffsetTRelative(offset)
                args_vec = builder.EndVector()

            # Build ObjectSpec
            ObjectSpec.Start(builder)
            ObjectSpec.AddClassName(builder, class_name_off)
            if args_vec:
                ObjectSpec.AddArgs(builder, args_vec)
            object_spec_offsets.append(ObjectSpec.End(builder))

        # Build objects vector
        CreateObjectsRequest.StartObjectsVector(builder, len(object_spec_offsets))
        for offset in reversed(object_spec_offsets):
            builder.PrependUOffsetTRelative(offset)
        objects_vec = builder.EndVector()

        # Build CreateObjectsRequest
        CreateObjectsRequest.Start(builder)
        CreateObjectsRequest.AddObjects(builder, objects_vec)
        create_objects_off = CreateObjectsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObjects)
        Cmd.CommandAddCreateObjects(builder, create_objects_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list["JavaObject"], self._send_raw(builder.Output()))

    def register_callback(
        self, callback_fn: Callable[..., Any], interface_name: str
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
        builder = self._get_builder()
        interface_off = self._create_string(builder, interface_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.RegisterCallback)
        Cmd.CommandAddTargetName(builder, interface_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # Send command and get back the proxy object
        result: JavaObject = cast("JavaObject", self._send_raw(builder.Output()))

        # Get the callback_id from the response - it's stored as the object_id
        # We need to also store our callback function
        # The Java side assigns callback_id = object_id for simplicity
        callback_id = result.object_id
        self._callbacks[callback_id] = callback_fn

        return result

    def unregister_callback(self, callback_id: int) -> None:
        """Unregister a previously registered callback.

        Args:
            callback_id: The callback ID to unregister
        """
        # Remove from local registry
        self._callbacks.pop(callback_id, None)

        builder = self._get_builder()
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.UnregisterCallback)
        Cmd.CommandAddTargetId(builder, callback_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        self._send_raw(builder.Output())

    def _build_argument(self, builder, value):
        """Convert a Python value to a FlatBuffer Argument.

        If value is a TypeHint wrapper, extracts the actual value and
        includes the type hint for Java overload resolution.
        """
        # Extract type hint if present
        type_hint = None
        if isinstance(value, TypeHint):
            type_hint = value.java_type
            value = value.value

        # Special case: char type hint with single-character string
        if type_hint in ("char", "Character", "java.lang.Character"):
            if isinstance(value, str) and len(value) == 1:
                CharVal.Start(builder)
                CharVal.AddV(builder, ord(value[0]))
                val_type, val_off = Value.Value.CharVal, CharVal.End(builder)
            elif isinstance(value, int):
                CharVal.Start(builder)
                CharVal.AddV(builder, value)
                val_type, val_off = Value.Value.CharVal, CharVal.End(builder)
            else:
                raise ValueError(
                    f"char type hint requires single-character string or int, got {type(value)}"
                )
        else:
            val_type, val_off = self._build_value(builder, value)

        # Build type hint string offset if present (must be before Start)
        type_hint_off = 0
        if type_hint:
            type_hint_off = self._create_string(builder, type_hint)

        Argument.Start(builder)
        Argument.AddValType(builder, val_type)
        Argument.AddVal(builder, val_off)
        if type_hint_off:
            Argument.AddTypeHint(builder, type_hint_off)
        return Argument.End(builder)

    def _build_value(self, builder, value):
        """Build a Value union and return (type, offset)."""
        if isinstance(value, str):
            str_off = self._create_string(builder, value)
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
        elif isinstance(value, (list, tuple, range)):
            # Auto-convert Python list/tuple/range to ListVal (Java ArrayList)
            # These are treated the same as lists for Java interop
            items = list(value) if isinstance(value, range) else value
            item_offsets = []
            for item in items:
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
        elif isinstance(value, (pa.Array, pa.ChunkedArray)):
            # Convert PyArrow array to ArrayVal
            if isinstance(value, pa.ChunkedArray):
                value = value.combine_chunks()
            return self._build_arrow_array(builder, value)
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
                epoch_secs: float
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

    def _build_arrow_array(self, builder, arr: pa.Array):
        """Build an ArrayVal from a PyArrow Array.

        Uses Arrow's buffer directly for efficient serialization of primitive types.
        """
        # Mapping of Arrow types to (element_type, add_values_fn, itemsize, target_arrow_type)
        # itemsize is needed for creating properly-typed FlatBuffers vectors
        # target_arrow_type is used for casting when needed (e.g., float32 -> float64)
        primitive_types = {
            pa.int32(): (ElementType.ElementType.Int, ArrayVal.AddIntValues, 4, None),
            pa.int64(): (ElementType.ElementType.Long, ArrayVal.AddLongValues, 8, None),
            pa.float64(): (
                ElementType.ElementType.Double,
                ArrayVal.AddDoubleValues,
                8,
                None,
            ),
            pa.float32(): (
                ElementType.ElementType.Float,
                ArrayVal.AddDoubleValues,
                8,  # After widening to float64
                pa.float64(),  # Widen float32 to float64 for transmission
            ),
            # Note: bool handled specially below due to Arrow's bit-packing
            pa.int8(): (ElementType.ElementType.Byte, ArrayVal.AddByteValues, 1, None),
            pa.uint8(): (
                ElementType.ElementType.Byte,
                ArrayVal.AddByteValues,
                1,
                pa.int8(),
            ),
            pa.int16(): (
                ElementType.ElementType.Short,
                ArrayVal.AddIntValues,
                4,  # After widening to int32
                pa.int32(),  # Widen short to int for transmission
            ),
        }

        arrow_type = arr.type

        # Special handling for bool arrays - Arrow uses bit-packing, Java expects byte-per-bool
        if arrow_type == pa.bool_():
            # Convert to byte array (1 byte per boolean)
            packed = struct.pack(f"<{len(arr)}B", *(1 if b.as_py() else 0 for b in arr))
            vec_off = _create_typed_vector(builder, packed, 1, len(arr))
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Bool)
            ArrayVal.AddBoolValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)

        if arrow_type in primitive_types:
            elem_type, add_values_fn, itemsize, cast_type = primitive_types[arrow_type]
            if cast_type is not None:
                arr = arr.cast(cast_type)
            # Get the data buffer (index 1, index 0 is validity bitmap)
            # For primitive arrays without nulls, we can use the buffer directly
            buf = arr.buffers()[1].to_pybytes()
            vec_off = _create_typed_vector(builder, buf, itemsize, len(arr))
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, elem_type)
            add_values_fn(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)

        # String array - serialize each element
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            item_offsets = [
                self._build_argument(builder, arr[i].as_py()) for i in range(len(arr))
            ]
            ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            vec_off = builder.EndVector()
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.String)
            ArrayVal.AddObjectValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)

        # Fallback: treat as object array, serialize each element
        item_offsets = [
            self._build_argument(builder, arr[i].as_py()) for i in range(len(arr))
        ]
        ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
        for offset in reversed(item_offsets):
            builder.PrependUOffsetTRelative(offset)
        vec_off = builder.EndVector()
        ArrayVal.Start(builder)
        ArrayVal.AddElementType(builder, ElementType.ElementType.Object)
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
        """Build an ArrayVal from Python array.array.

        Uses array.tobytes() for direct byte conversion and _create_typed_vector
        for proper FlatBuffers vector creation.
        """
        typecode = arr.typecode
        n = len(arr)
        # array.array.tobytes() gives us the raw bytes directly
        if typecode == "i":  # int (usually 32-bit)
            vec_off = _create_typed_vector(builder, arr.tobytes(), 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Int)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "l" or typecode == "q":  # long
            vec_off = _create_typed_vector(builder, arr.tobytes(), 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Long)
            ArrayVal.AddLongValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "d":  # double
            vec_off = _create_typed_vector(builder, arr.tobytes(), 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Double)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "f":  # float -> widen to double
            # Need to widen float32 to float64
            widened = struct.pack(f"<{n}d", *arr)
            vec_off = _create_typed_vector(builder, widened, 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Float)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "b" or typecode == "B":  # byte
            vec_off = _create_typed_vector(builder, arr.tobytes(), 1, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Byte)
            ArrayVal.AddByteValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "h" or typecode == "H":  # short -> widen to int
            # Need to widen int16 to int32
            widened = struct.pack(f"<{n}i", *arr)
            vec_off = _create_typed_vector(builder, widened, 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Short)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        else:
            raise TypeError(f"Unsupported array.array typecode: {typecode}")

    def _build_java_array(self, builder, java_array: JavaArray):
        """Build an ArrayVal from a JavaArray (preserving array semantics for round-trip).

        Uses struct.pack for efficient serialization and _create_typed_vector
        for proper FlatBuffers vector creation.
        """
        elem_type = java_array.element_type
        items = list(java_array)
        n = len(items)

        if elem_type == "Int":
            packed = struct.pack(f"<{n}i", *items)
            vec_off = _create_typed_vector(builder, packed, 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Int)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Long":
            packed = struct.pack(f"<{n}q", *items)
            vec_off = _create_typed_vector(builder, packed, 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Long)
            ArrayVal.AddLongValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Double":
            packed = struct.pack(f"<{n}d", *items)
            vec_off = _create_typed_vector(builder, packed, 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Double)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Float":
            # Widen float to double for transmission
            packed = struct.pack(f"<{n}d", *items)
            vec_off = _create_typed_vector(builder, packed, 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Float)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Bool":
            # Pack bools as bytes (0 or 1)
            packed = struct.pack(f"<{n}B", *(1 if b else 0 for b in items))
            vec_off = _create_typed_vector(builder, packed, 1, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Bool)
            ArrayVal.AddBoolValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Byte":
            vec_off = _create_typed_vector(builder, bytes(items), 1, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Byte)
            ArrayVal.AddByteValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Short":
            # Widen short to int for transmission
            packed = struct.pack(f"<{n}i", *items)
            vec_off = _create_typed_vector(builder, packed, 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Short)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "String":
            # String array - need to build each as Argument
            item_offsets = []
            for item in items:
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
            for item in items:
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
        """Sends FreeObject to release a Java object (fire-and-forget).

        This is called from the weak reference finalizer when JavaObjects are
        garbage collected. It uses fire-and-forget mode to avoid blocking GC
        with a round-trip, which significantly improves throughput.
        """
        if self.sock is None or self.sock.fileno() == -1:
            return

        builder = self._get_builder()
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.FreeObject)
        Cmd.CommandAddTargetId(builder, object_id)
        cmd_offset = Cmd.CommandEnd(builder)
        builder.Finish(cmd_offset)

        try:
            # Fire-and-forget: don't wait for response since FreeObject
            # just returns null and we don't care about the result.
            # This avoids blocking GC with a socket round-trip.
            self._send_raw(builder.Output(), wait_for_response=False)
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
        assert self.shm is not None, "Not connected"
        payload_base = ctypes.addressof(ctypes.c_char.from_buffer(self.shm))
        payload_addr = payload_base + self.payload_offset
        ctypes.memmove(payload_addr, ipc_buffer.address, bytes_written)

        # Build Command
        builder = self._get_builder()
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
        builder = self._get_builder(large=True)

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

        builder = self._get_builder()

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
        builder = self._get_builder()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetArrowData)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(ArrowTableView, self._send_raw(builder.Output()))

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
        builder = self._get_builder()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Cancel)
        Cmd.CommandAddRequestId(builder, request_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        result = self._send_raw(builder.Output())
        return result is True

    def batch(self, stop_on_error: bool = False) -> "BatchContext":
        """Create a batch context for executing multiple commands in one round-trip.

        Batching amortizes the per-call overhead (~100-150μs) across multiple
        operations. Commands are queued locally and sent together when the
        context exits or execute() is called.

        Args:
            stop_on_error: If True, stop executing on first error.
                          If False (default), continue and collect all results/errors.

        Returns:
            BatchContext that can be used as a context manager.

        Example:
            # Context manager (auto-execute on exit):
            with client.batch() as b:
                r1 = b.call(arr, "add", 1)
                r2 = b.call(arr, "add", 2)
                r3 = b.call(arr, "size")
            print(r3.get())  # 2

            # Manual execute:
            batch = client.batch()
            r1 = batch.call(arr, "add", 1)
            r2 = batch.call_static("java.lang.Math", "max", 10, 20)
            batch.execute()
            print(r2.get())  # 20

            # Stop on error:
            with client.batch(stop_on_error=True) as b:
                r1 = b.call(arr, "add", 1)
                r2 = b.call(invalid_obj, "bad_method")  # Will error
                r3 = b.call(arr, "size")  # Skipped if stop_on_error=True
        """
        return BatchContext(self, stop_on_error)

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
        # Only connect if not already connected
        if self.shm is None:
            self.connect()
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()
        return False


class BatchResult:
    """Lazy result placeholder that resolves after batch.execute().

    BatchResult objects are returned when queueing commands in a BatchContext.
    The actual result is not available until execute() is called.
    """

    __slots__ = ("_batch", "_index")

    def __init__(self, batch: "BatchContext", index: int):
        self._batch = batch
        self._index = index

    def get(self):
        """Get the result value.

        Returns:
            The result of the command (same as calling the method directly).

        Raises:
            RuntimeError: If execute() has not been called yet.
            JavaException: If the command raised a Java exception.
        """
        if not self._batch._executed:
            raise RuntimeError("Must call batch.execute() before getting results")
        result = self._batch._results[self._index]
        if isinstance(result, Exception):
            raise result
        return result

    @property
    def is_error(self) -> bool:
        """Check if this result is an error.

        Raises:
            RuntimeError: If execute() has not been called yet.
        """
        if not self._batch._executed:
            raise RuntimeError("Must call batch.execute() before checking errors")
        return isinstance(self._batch._results[self._index], Exception)


class BatchContext:
    """Context manager for batching multiple commands into a single round-trip.

    Commands queued in a batch are sent together when execute() is called
    (or automatically when exiting a context manager). This reduces per-call
    overhead from ~100-150μs to ~10μs per operation.

    Attributes:
        stop_on_error: If True, Java stops executing after first error.
    """

    def __init__(self, client: GatunClient, stop_on_error: bool = False):
        self._client = client
        self._stop_on_error = stop_on_error
        self._command_data: list[tuple[int, int, str | None, tuple]] = []
        self._results: list = []
        self._executed = False

    def __enter__(self) -> "BatchContext":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and not self._executed:
            self.execute()
        return False

    def create(self, class_name: str, *args) -> BatchResult:
        """Queue object creation.

        Args:
            class_name: Fully qualified Java class name.
            *args: Constructor arguments.

        Returns:
            BatchResult that will contain the JavaObject after execute().
        """
        idx = len(self._command_data)
        self._command_data.append((Act.Action.CreateObject, 0, class_name, args))
        return BatchResult(self, idx)

    def call(self, obj: "JavaObject", method_name: str, *args) -> BatchResult:
        """Queue an instance method call.

        Args:
            obj: JavaObject to call method on.
            method_name: Name of the method to call.
            *args: Method arguments.

        Returns:
            BatchResult that will contain the return value after execute().
        """
        idx = len(self._command_data)
        self._command_data.append(
            (Act.Action.InvokeMethod, obj.object_id, method_name, args)
        )
        return BatchResult(self, idx)

    def call_static(self, class_name: str, method_name: str, *args) -> BatchResult:
        """Queue a static method call.

        Args:
            class_name: Fully qualified Java class name.
            method_name: Name of the static method to call.
            *args: Method arguments.

        Returns:
            BatchResult that will contain the return value after execute().
        """
        idx = len(self._command_data)
        full_name = f"{class_name}.{method_name}"
        self._command_data.append((Act.Action.InvokeStaticMethod, 0, full_name, args))
        return BatchResult(self, idx)

    def get_field(self, obj: "JavaObject", field_name: str) -> BatchResult:
        """Queue a field get operation.

        Args:
            obj: JavaObject to get field from.
            field_name: Name of the field.

        Returns:
            BatchResult that will contain the field value after execute().
        """
        idx = len(self._command_data)
        self._command_data.append((Act.Action.GetField, obj.object_id, field_name, ()))
        return BatchResult(self, idx)

    def set_field(self, obj: "JavaObject", field_name: str, value) -> BatchResult:
        """Queue a field set operation.

        Args:
            obj: JavaObject to set field on.
            field_name: Name of the field.
            value: Value to set.

        Returns:
            BatchResult (value will be None after execute()).
        """
        idx = len(self._command_data)
        self._command_data.append(
            (Act.Action.SetField, obj.object_id, field_name, (value,))
        )
        return BatchResult(self, idx)

    def execute(self) -> list:
        """Execute all queued commands in a single round-trip.

        Returns:
            List of results in the same order as commands were queued.
            Errors are stored as Exception objects in the list.

        Raises:
            RuntimeError: If execute() was already called.
        """
        if self._executed:
            return self._results
        self._executed = True

        if not self._command_data:
            return []

        # Build the batch command
        builder = self._client._get_builder(large=True)

        # Build all sub-commands first (must be done before BatchCommand)
        sub_cmd_offsets = []
        for action, target_id, target_name, args in self._command_data:
            # Build argument tables
            arg_offsets = []
            for arg in args:
                arg_offsets.append(self._client._build_argument(builder, arg))

            # Build arguments vector
            args_vec = None
            if arg_offsets:
                Cmd.CommandStartArgsVector(builder, len(arg_offsets))
                for offset in reversed(arg_offsets):
                    builder.PrependUOffsetTRelative(offset)
                args_vec = builder.EndVector()

            # Build target name string
            name_off = None
            if target_name:
                name_off = self._client._create_string(builder, target_name)

            # Build sub-command
            Cmd.CommandStart(builder)
            Cmd.CommandAddAction(builder, action)
            if target_id:
                Cmd.CommandAddTargetId(builder, target_id)
            if name_off:
                Cmd.CommandAddTargetName(builder, name_off)
            if args_vec:
                Cmd.CommandAddArgs(builder, args_vec)
            sub_cmd_offsets.append(Cmd.CommandEnd(builder))

        # Build commands vector for BatchCommand
        BatchCommand.StartCommandsVector(builder, len(sub_cmd_offsets))
        for offset in reversed(sub_cmd_offsets):
            builder.PrependUOffsetTRelative(offset)
        commands_vec = builder.EndVector()

        # Build BatchCommand
        BatchCommand.Start(builder)
        BatchCommand.AddCommands(builder, commands_vec)
        BatchCommand.AddStopOnError(builder, self._stop_on_error)
        batch_cmd_off = BatchCommand.End(builder)

        # Build outer Command with Action.Batch
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Batch)
        Cmd.CommandAddBatch(builder, batch_cmd_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # Send and get response
        # We need to handle the batch response specially
        data = builder.Output()

        # Check if connection is dead - fail fast
        if self._client._dead:
            raise DeadConnectionError(
                "Connection is dead after protocol error. Create a new client."
            )

        # Validate command size
        max_command_size = self._client.payload_offset
        if len(data) > max_command_size:
            raise PayloadTooLargeError(len(data), max_command_size, "Command")

        # Drain pending responses
        self._client._drain_pending_responses()

        # Write to shared memory
        self._client.shm.seek(self._client.command_offset)
        self._client.shm.write(data)
        self._client.shm.flush()

        # Signal Java
        if not self._client.sock or self._client.sock.fileno() == -1:
            self._client._mark_dead()
            raise DeadConnectionError("Socket already closed")

        try:
            self._client.sock.sendall(struct.pack("<I", len(data)))
        except (OSError, BrokenPipeError, ConnectionResetError) as e:
            self._client._mark_dead()
            raise ProtocolDesyncError(f"Socket error sending batch command: {e}")

        # Read response with timeout to prevent indefinite hang
        try:
            sz_data = _recv_exactly(
                self._client.sock, 4, timeout=self._client.socket_timeout
            )
        except SocketTimeoutError:
            self._client._mark_dead()
            raise
        except (RuntimeError, OSError, BrokenPipeError, ConnectionResetError) as e:
            self._client._mark_dead()
            raise ProtocolDesyncError(f"Socket error reading batch response: {e}")

        sz = struct.unpack("<I", sz_data)[0]

        # Validate response size
        max_response_size = self._client.memory_size - self._client.response_offset
        if sz == 0 or sz > RESPONSE_ZONE_SIZE or sz > max_response_size:
            self._client._mark_dead()
            raise ProtocolDesyncError(
                f"Invalid batch response size: {sz}", response_size=sz
            )

        self._client.shm.seek(self._client.response_offset)
        resp_buf = self._client.shm.read(sz)

        # Parse response
        resp = Response.Response.GetRootAsResponse(resp_buf, 0)

        if resp.IsError():
            # Top-level error (shouldn't happen for batch, but handle it)
            error_msg = resp.ErrorMsg().decode("utf-8")
            error_type_bytes = resp.ErrorType()
            error_type = (
                error_type_bytes.decode("utf-8")
                if error_type_bytes
                else "java.lang.RuntimeException"
            )
            self._client._raise_java_exception(error_type, error_msg)

        # Unpack BatchResponse
        batch_resp = resp.Batch()
        if batch_resp is None:
            raise RuntimeError("Expected BatchResponse but got None")

        _ = batch_resp.ErrorIndex()  # Available for debugging
        response_count = batch_resp.ResponsesLength()

        # Unpack each sub-response
        for i in range(response_count):
            sub_resp = batch_resp.Responses(i)
            if sub_resp.IsError():
                error_msg = sub_resp.ErrorMsg().decode("utf-8")
                error_type_bytes = sub_resp.ErrorType()
                error_type = (
                    error_type_bytes.decode("utf-8")
                    if error_type_bytes
                    else "java.lang.RuntimeException"
                )
                # Store exception object instead of raising
                exc = self._make_exception(error_type, error_msg)
                self._results.append(exc)
            else:
                value = self._client._unpack_value(
                    sub_resp.ReturnValType(), sub_resp.ReturnVal()
                )
                self._results.append(value)

        # Fill remaining slots with None if stopped early
        while len(self._results) < len(self._command_data):
            self._results.append(None)

        return self._results

    def _make_exception(self, error_type: str, error_msg: str) -> Exception:
        """Create an exception object without raising it."""
        import re

        # Handle special exception types
        if error_type == "org.gatun.PayloadTooLargeException":
            match = re.search(r"(\d+) bytes exceeds (\d+) byte", error_msg)
            if match:
                return PayloadTooLargeError(
                    int(match.group(1)), int(match.group(2)), "Response"
                )
            return PayloadTooLargeError(0, 0, "Response")

        if error_type == "java.lang.InterruptedException":
            match = re.search(r"Request (\d+) was cancelled", error_msg)
            request_id = int(match.group(1)) if match else 0
            return CancelledException(request_id)

        # Get exception class
        exc_class = _JAVA_EXCEPTION_MAP.get(error_type, JavaException)

        # Extract message
        lines = error_msg.split("\n")
        message = lines[0] if lines else error_msg

        return exc_class(error_type, message, error_msg)
