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
import mmap
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa


class BufferInfo(NamedTuple):
    """Information about an allocated buffer."""
    offset: int      # Offset from start of arena
    length: int      # Buffer size in bytes
    buffer: pa.Buffer | None  # PyArrow buffer backed by shared memory (None for zero-length)


class PayloadArena:
    """Manages Arrow buffer allocation in payload shared memory.

    Uses a simple bump allocator with arena-wide reset. Buffers are allocated
    sequentially with 64-byte alignment (Arrow's requirement).

    This arena is separate from the control shared memory used for commands
    and responses, allowing large Arrow data to be sized independently.
    """

    # Arrow requires 64-byte alignment for SIMD operations
    ALIGNMENT = 64

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

        # Open and map the shared memory file
        self._file = open(self.path, "r+b")
        self._mmap = mmap.mmap(self._file.fileno(), size)

        # Get the base address for foreign_buffer
        # We need to use ctypes to get the actual memory address
        self._base_address = ctypes.addressof(
            ctypes.c_char.from_buffer(self._mmap)
        )

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
    def from_mmap(cls, mmap_obj: mmap.mmap, base_offset: int, size: int) -> "PayloadArena":
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

        Args:
            data: Data to copy (bytes or PyArrow Buffer)

        Returns:
            BufferInfo with the data copied into shared memory
        """
        if isinstance(data, pa.Buffer):
            size = data.size
            data_bytes = data.to_pybytes()
        else:
            size = len(data)
            data_bytes = data

        info = self.allocate(size)

        # Copy data into the shared memory buffer
        # We use the mmap directly for the copy
        # Account for base offset when using a slice of a larger mmap
        base_offset = getattr(self, "_base_offset", 0)
        self._mmap.seek(base_offset + info.offset)
        self._mmap.write(data_bytes)

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


def copy_arrow_table_to_arena(
    table: pa.Table, arena: PayloadArena
) -> tuple[list[BufferInfo], list[tuple[int, int]]]:
    """Copy an Arrow table's buffers into the payload arena.

    This is the key function for zero-copy transfer. It:
    1. Extracts all buffers from the table's columns
    2. Copies each buffer into the arena
    3. Returns buffer descriptors for the protocol message

    Args:
        table: PyArrow Table to copy
        arena: PayloadArena to allocate into

    Returns:
        Tuple of:
        - List of BufferInfo for each buffer
        - List of (length, null_count) tuples for each field node
    """
    buffer_infos: list[BufferInfo] = []
    field_nodes: list[tuple[int, int]] = []

    for column in table.columns:
        for chunk in column.chunks:
            # Record field node info
            field_nodes.append((len(chunk), chunk.null_count))

            # Copy each buffer
            for buf in chunk.buffers():
                if buf is None:
                    # Null buffer (e.g., no validity bitmap when no nulls)
                    # We still need a placeholder - use zero-length buffer
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
    h = 0xcbf29ce484222325  # FNV offset basis
    for b in schema_bytes:
        h ^= b
        h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF  # FNV prime, 64-bit
    return h


def serialize_schema(schema: pa.Schema) -> bytes:
    """Serialize an Arrow schema for transmission."""
    return schema.serialize().to_pybytes()


def deserialize_schema(schema_bytes: bytes) -> pa.Schema:
    """Deserialize an Arrow schema from IPC format bytes."""
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))
