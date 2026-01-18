"""Payload arena for true zero-copy Arrow transfer.

This module provides a bump allocator for Arrow buffers in shared memory,
enabling true zero-copy data transfer between Python and Java.

The arena uses a simple reset strategy: allocate buffers sequentially,
then reset the entire arena when done processing. This works well for
batch processing patterns.

Example:
    arena = PayloadArena.create(Path("~/gatun_payload.shm"), 64 * 1024 * 1024)

    # Allocate buffers for Arrow data
    info1 = arena.allocate(1024)
    info2 = arena.allocate(2048)

    # When done processing, reset for next batch
    arena.reset()

    arena.close()
"""

from __future__ import annotations

import ctypes
import io
import mmap
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa


class BufferInfo(NamedTuple):
    """Information about an allocated buffer."""

    offset: int  # Offset from start of arena
    length: int  # Buffer size in bytes
    buffer: (
        pa.Buffer | None
    )  # PyArrow buffer backed by shared memory (None for zero-length)


class PayloadArena:
    """Manages Arrow buffer allocation in payload shared memory.

    Uses a simple bump allocator with arena-wide reset. Buffers are allocated
    sequentially with 64-byte alignment (Arrow's requirement).

    This arena is separate from the control shared memory used for commands
    and responses, allowing large Arrow data to be sized independently.
    """

    # Arrow requires 64-byte alignment for SIMD operations
    ALIGNMENT = 64

    # Instance attributes with optional types for from_mmap factory
    path: Path | None
    _file: "io.BufferedRandom | None"
    _mmap: mmap.mmap | None
    _base_offset: int

    def __init__(self, path: Path | str, size: int):
        """Initialize the payload arena from an existing file.

        Args:
            path: Path to the shared memory file (must exist)
            size: Size of the shared memory region in bytes

        Use PayloadArena.create() to create a new arena file.
        """
        self.path = Path(path).expanduser()
        self.size = size
        self.offset = 0  # Current allocation offset (bump pointer)
        self._base_offset = 0  # No offset for direct file access

        # Open and map the shared memory file
        self._file = open(self.path, "r+b")
        self._mmap = mmap.mmap(self._file.fileno(), size)

        # Get the base address for foreign_buffer
        # We need to use ctypes to get the actual memory address
        self._base_address = ctypes.addressof(ctypes.c_char.from_buffer(self._mmap))

    @classmethod
    def create(cls, path: Path | str, size: int) -> PayloadArena:
        """Create a new payload arena file.

        Args:
            path: Path for the shared memory file (will be created/truncated)
            size: Size of the shared memory region in bytes

        Returns:
            PayloadArena instance backed by the new file
        """
        path = Path(path).expanduser()

        # Create the file with the specified size
        with open(path, "wb") as f:
            f.truncate(size)

        return cls(path, size)

    @classmethod
    def from_mmap(
        cls, mmap_obj: mmap.mmap, base_offset: int, size: int
    ) -> "PayloadArena":
        """Create a PayloadArena from an existing mmap.

        This is used to create an arena view into a slice of an existing
        shared memory region (e.g., the payload zone of the control shm).

        Args:
            mmap_obj: Existing mmap object
            base_offset: Offset within the mmap where the arena starts
            size: Size of the arena region in bytes

        Returns:
            PayloadArena instance backed by the mmap slice
        """
        arena = object.__new__(cls)
        arena.path = None
        arena.size = size
        arena.offset = 0
        arena._file = None  # No separate file - using existing mmap
        arena._mmap = mmap_obj
        arena._base_offset = base_offset  # Track offset within parent mmap

        # Get the base address for foreign_buffer
        base_address = ctypes.addressof(ctypes.c_char.from_buffer(mmap_obj))
        arena._base_address = base_address + base_offset

        return arena

    def allocate(self, size: int, alignment: int | None = None) -> BufferInfo:
        """Allocate a buffer in the arena.

        Args:
            size: Size of the buffer in bytes
            alignment: Alignment requirement (default: 64 bytes for Arrow)

        Returns:
            BufferInfo with offset, length, and PyArrow buffer

        Raises:
            MemoryError: If arena doesn't have enough space
        """
        if alignment is None:
            alignment = self.ALIGNMENT

        # Align the offset
        aligned_offset = (self.offset + alignment - 1) & ~(alignment - 1)

        if aligned_offset + size > self.size:
            raise MemoryError(
                f"Payload arena full: need {aligned_offset + size} bytes, "
                f"have {self.size} bytes (current offset: {self.offset})"
            )

        # Create an Arrow buffer backed by the shared memory region
        # foreign_buffer creates a zero-copy view into existing memory
        address = self._base_address + aligned_offset
        buffer = pa.foreign_buffer(address, size, base=self._mmap)

        # Update the bump pointer
        self.offset = aligned_offset + size

        return BufferInfo(offset=aligned_offset, length=size, buffer=buffer)

    def allocate_and_copy(self, data: bytes | pa.Buffer) -> BufferInfo:
        """Allocate a buffer and copy data into it.

        Uses a single memcpy/memmove to copy data directly into shared memory,
        avoiding intermediate Python bytes objects for Arrow buffers.

        Args:
            data: Data to copy (bytes or PyArrow Buffer)

        Returns:
            BufferInfo with the data copied into shared memory
        """
        if isinstance(data, pa.Buffer):
            size = data.size
        else:
            size = len(data)

        if size == 0:
            return BufferInfo(offset=0, length=0, buffer=None)

        info = self.allocate(size)

        # Copy data into the shared memory buffer using a single memcpy
        base_offset = getattr(self, "_base_offset", 0)
        dst_addr = self._base_address + info.offset

        if isinstance(data, pa.Buffer):
            # Direct copy from Arrow buffer's native memory - single memcpy
            # Arrow buffers expose their address via .address property
            ctypes.memmove(dst_addr, data.address, size)
        else:
            # For Python bytes, use mmap.write (still efficient)
            self._mmap.seek(base_offset + info.offset)
            self._mmap.write(data)

        return info

    def reset(self):
        """Reset the arena for the next batch.

        This simply resets the bump pointer to the beginning.
        All previously allocated buffers become invalid.
        """
        self.offset = 0

    def bytes_used(self) -> int:
        """Return the number of bytes currently allocated."""
        return self.offset

    def bytes_available(self) -> int:
        """Return the number of bytes available for allocation."""
        return self.size - self.offset

    def close(self):
        """Close the arena and release resources.

        If this arena is a view into an existing mmap (created via from_mmap),
        this only clears the reference without closing the underlying mmap.
        """
        # Only close mmap if we own it (created via __init__ or create)
        if self._file is not None:
            if self._mmap:
                self._mmap.close()
            self._file.close()
            self._file = None
        self._mmap = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# Nested Arrow types that require recursive field node extraction
_NESTED_TYPES = (
    pa.ListType,
    pa.LargeListType,
    pa.FixedSizeListType,
    pa.StructType,
    pa.MapType,
    pa.UnionType,
)

# Dictionary type needs special handling (not yet supported)
_UNSUPPORTED_TYPES = (pa.DictionaryType,)


class UnsupportedArrowTypeError(TypeError):
    """Raised when an Arrow schema contains unsupported types."""

    pass


def _validate_supported_schema(schema: pa.Schema) -> None:
    """Validate that schema contains only supported types.

    Currently supports:
    - Primitive types: int, float, bool, string, binary, etc.
    - Date/time types: date32, date64, timestamp, time32, time64, duration
    - Nested types: list, large_list, fixed_size_list, struct, map, union

    Not yet supported:
    - Dictionary (requires separate dictionary/indices handling)

    Args:
        schema: Arrow schema to validate

    Raises:
        UnsupportedArrowTypeError: If schema contains unsupported types
    """
    unsupported: list[str] = []
    _check_type_recursive(schema, unsupported)

    if unsupported:
        raise UnsupportedArrowTypeError(
            f"Arrow zero-copy transfer does not support these types: "
            f"{', '.join(unsupported)}. "
            f"Use send_arrow_table() for tables with unsupported types."
        )


def _check_type_recursive(
    schema_or_type: pa.Schema | pa.DataType, unsupported: list[str], path: str = ""
) -> None:
    """Recursively check types for unsupported ones."""
    if isinstance(schema_or_type, pa.Schema):
        for field in schema_or_type:
            field_path = field.name if not path else f"{path}.{field.name}"
            _check_type_recursive(field.type, unsupported, field_path)
    elif isinstance(schema_or_type, _UNSUPPORTED_TYPES):
        unsupported.append(f"{path}: {schema_or_type}")
    elif isinstance(schema_or_type, pa.ListType):
        _check_type_recursive(schema_or_type.value_type, unsupported, f"{path}[]")
    elif isinstance(schema_or_type, pa.LargeListType):
        _check_type_recursive(schema_or_type.value_type, unsupported, f"{path}[]")
    elif isinstance(schema_or_type, pa.FixedSizeListType):
        _check_type_recursive(schema_or_type.value_type, unsupported, f"{path}[]")
    elif isinstance(schema_or_type, pa.StructType):
        for i in range(schema_or_type.num_fields):
            field = schema_or_type.field(i)
            field_path = f"{path}.{field.name}" if path else field.name
            _check_type_recursive(field.type, unsupported, field_path)
    elif isinstance(schema_or_type, pa.MapType):
        _check_type_recursive(schema_or_type.key_type, unsupported, f"{path}.key")
        _check_type_recursive(schema_or_type.item_type, unsupported, f"{path}.value")
    elif isinstance(schema_or_type, pa.UnionType):
        for i in range(schema_or_type.num_fields):
            field = schema_or_type.field(i)
            _check_type_recursive(field.type, unsupported, f"{path}.{field.name}")


def _get_own_buffers(array: pa.Array) -> list[pa.Buffer | None]:
    """Get only this array's own buffers (not children's buffers).

    For nested types, array.buffers() includes children's buffers, but Arrow IPC
    format requires buffers in DFS order. We extract only the parent's buffers here
    and handle children recursively.

    Buffer layout per array type (from Arrow spec):
    - Primitive (int, float, bool, etc.): [validity, data]
    - String/Binary: [validity, offsets, data]
    - LargeString/LargeBinary: [validity, offsets (64-bit), data]
    - List: [validity, offsets]
    - LargeList: [validity, offsets (64-bit)]
    - FixedSizeList: [validity]
    - Struct: [validity]
    - Map: [validity, offsets] (like List)
    - DenseUnion: [type_ids, offsets]
    - SparseUnion: [type_ids]
    """
    arr_type = array.type
    all_buffers: list[pa.Buffer | None] = array.buffers()

    # For nested types, we only want the parent's buffers, not children's
    if isinstance(arr_type, (pa.ListType, pa.LargeListType)):
        # List: validity + offsets (2 buffers), rest are child's
        return all_buffers[:2]
    elif isinstance(arr_type, pa.FixedSizeListType):
        # FixedSizeList: validity only (1 buffer)
        return all_buffers[:1]
    elif isinstance(arr_type, pa.StructType):
        # Struct: validity only (1 buffer)
        return all_buffers[:1]
    elif isinstance(arr_type, pa.MapType):
        # Map: validity + offsets (2 buffers)
        return all_buffers[:2]
    elif isinstance(arr_type, pa.UnionType):
        if arr_type.mode == "dense":
            # DenseUnion: type_ids + offsets (2 buffers)
            return all_buffers[:2]
        else:
            # SparseUnion: type_ids only (1 buffer)
            return all_buffers[:1]
    else:
        # Primitive types: all buffers belong to this array
        return all_buffers


def _extract_field_nodes_and_buffers_recursive(
    array: pa.Array,
    field_nodes: list[tuple[int, int]],
    buffers: list[pa.Buffer | None],
) -> None:
    """Extract field nodes and buffers from an array in depth-first pre-order.

    Arrow IPC format requires both field nodes and buffers in depth-first pre-order
    traversal. For nested types, we emit the parent's data first, then recurse into
    children.

    This function ensures that:
    1. Field nodes are in DFS order: parent before children
    2. Buffers are in DFS order: parent's buffers before children's buffers
    3. Buffer counts match what VectorLoader expects

    Args:
        array: The Arrow array to extract from
        field_nodes: List to append (length, null_count) tuples to
        buffers: List to append buffers to
    """
    # Emit this array's field node
    field_nodes.append((len(array), array.null_count))

    # Emit this array's own buffers (not children's)
    own_buffers = _get_own_buffers(array)
    buffers.extend(own_buffers)

    # Recurse into children based on type
    arr_type = array.type

    if isinstance(arr_type, (pa.ListType, pa.LargeListType, pa.FixedSizeListType)):
        # List types have a single child: the values array
        _extract_field_nodes_and_buffers_recursive(array.values, field_nodes, buffers)

    elif isinstance(arr_type, pa.StructType):
        # Struct types have multiple children: one per field
        for i in range(arr_type.num_fields):
            _extract_field_nodes_and_buffers_recursive(
                array.field(i), field_nodes, buffers
            )

    elif isinstance(arr_type, pa.MapType):
        # Map is internally list<struct<key, value>>
        # The .values property gives us the struct array (entries)
        # which contains the key and value child arrays
        _extract_field_nodes_and_buffers_recursive(array.values, field_nodes, buffers)

    elif isinstance(arr_type, pa.UnionType):
        # Union types have multiple children: one per type code
        for i in range(arr_type.num_fields):
            _extract_field_nodes_and_buffers_recursive(
                array.field(i), field_nodes, buffers
            )


def _extract_field_nodes_recursive(
    array: pa.Array, field_nodes: list[tuple[int, int]]
) -> None:
    """Extract field nodes from an array in depth-first pre-order.

    Arrow IPC format requires field nodes in depth-first pre-order traversal.
    For nested types, we emit the parent node first, then recurse into children.

    Note: This function is kept for backwards compatibility. Prefer using
    _extract_field_nodes_and_buffers_recursive which extracts both in lockstep.

    Args:
        array: The Arrow array to extract nodes from
        field_nodes: List to append (length, null_count) tuples to
    """
    # Emit this array's field node
    field_nodes.append((len(array), array.null_count))

    # Recurse into children based on type
    arr_type = array.type

    if isinstance(arr_type, (pa.ListType, pa.LargeListType, pa.FixedSizeListType)):
        # List types have a single child: the values array
        _extract_field_nodes_recursive(array.values, field_nodes)

    elif isinstance(arr_type, pa.StructType):
        # Struct types have multiple children: one per field
        for i in range(arr_type.num_fields):
            _extract_field_nodes_recursive(array.field(i), field_nodes)

    elif isinstance(arr_type, pa.MapType):
        # Map is internally list<struct<key, value>>
        # The .values property gives us the struct array (entries)
        # which contains the key and value child arrays
        # Field nodes: map -> struct (entries) -> key, value
        _extract_field_nodes_recursive(array.values, field_nodes)

    elif isinstance(arr_type, pa.UnionType):
        # Union types have multiple children: one per type code
        for i in range(arr_type.num_fields):
            _extract_field_nodes_recursive(array.field(i), field_nodes)


def copy_arrow_table_to_arena(
    table: pa.Table, arena: PayloadArena
) -> tuple[list[BufferInfo], list[tuple[int, int]]]:
    """Copy an Arrow table's buffers into the payload arena.

    This is the key function for zero-copy transfer. It:
    1. Validates schema contains only supported types
    2. Combines chunked columns into single arrays (required for protocol)
    3. Extracts field nodes and buffers in DFS order (matching Arrow IPC format)
    4. Copies each buffer into the arena
    5. Returns buffer descriptors for the protocol message

    IMPORTANT: Both field nodes and buffers MUST be in depth-first pre-order
    traversal to match Arrow IPC/VectorLoader expectations. For nested types:
    - Field nodes: parent before children
    - Buffers: parent's buffers before children's buffers

    Note: Tables with chunked columns are combined into single chunks before
    copying. This ensures a 1:1 mapping between schema fields and field nodes,
    which simplifies the protocol and reconstruction logic.

    Args:
        table: PyArrow Table to copy
        arena: PayloadArena to allocate into

    Returns:
        Tuple of:
        - List of BufferInfo for each buffer
        - List of (length, null_count) tuples for each field node

    Raises:
        UnsupportedArrowTypeError: If table contains unsupported types (e.g., dictionary)
    """
    # Validate schema before processing
    _validate_supported_schema(table.schema)

    field_nodes: list[tuple[int, int]] = []
    raw_buffers: list[pa.Buffer | None] = []

    # Combine chunks to ensure one chunk per column
    # This is required because our protocol assumes 1:1 field-to-node mapping
    table = table.combine_chunks()

    for column in table.columns:
        # After combine_chunks(), each column has exactly one chunk
        chunk = column.chunks[0]

        # Normalize sliced arrays to offset 0
        # Sliced arrays have non-zero offsets, which means their buffers contain
        # data before the slice that shouldn't be accessed. We use concat_arrays
        # to create a new array with offset 0 and compacted buffers.
        if chunk.offset != 0:
            chunk = pa.concat_arrays([chunk])

        # Extract field nodes AND buffers together in DFS order
        # This ensures correct ordering for nested types (list, struct, map, union)
        _extract_field_nodes_and_buffers_recursive(chunk, field_nodes, raw_buffers)

    # Copy buffers to arena
    # Note: Absent buffers (e.g., no validity bitmap when no nulls) are represented
    # as zero-length buffers (offset=0, length=0). This is required because Arrow's
    # VectorLoader expects the exact buffer count from TypeLayout and doesn't handle
    # null/absent buffers well. See: https://issues.apache.org/jira/browse/ARROW-8803
    buffer_infos: list[BufferInfo] = []
    for buf in raw_buffers:
        if buf is None:
            # Absent buffer - use zero-length placeholder
            info = BufferInfo(offset=0, length=0, buffer=None)
        else:
            info = arena.allocate_and_copy(buf)
        buffer_infos.append(info)

    return buffer_infos, field_nodes


def compute_schema_hash(schema: pa.Schema) -> int:
    """Compute a hash of an Arrow schema for caching.

    The hash is based on the serialized schema bytes.
    """
    schema_bytes = schema.serialize().to_pybytes()

    # Use a simple hash (Python's hash is not stable across runs,
    # so we use a simple FNV-1a style hash)
    h = 0xCBF29CE484222325  # FNV offset basis
    for b in schema_bytes:
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF  # FNV prime, 64-bit
    return h


def serialize_schema(schema: pa.Schema) -> bytes:
    """Serialize an Arrow schema for transmission."""
    result: bytes = schema.serialize().to_pybytes()
    return result


def deserialize_schema(schema_bytes: bytes) -> pa.Schema:
    """Deserialize an Arrow schema from IPC format bytes."""
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))
