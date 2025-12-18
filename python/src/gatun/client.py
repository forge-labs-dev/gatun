import logging
import mmap
import os
import socket
import struct
import weakref

import flatbuffers
import pyarrow as pa

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
)

logger = logging.getLogger(__name__)

# Protocol version - must match the server version
PROTOCOL_VERSION = 1

# Memory zone sizes
COMMAND_ZONE_SIZE = 4096  # 4KB for commands
RESPONSE_ZONE_SIZE = 4096  # 4KB for responses


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


def _recv_exactly(sock, n):
    """Receive exactly n bytes from socket, handling partial reads."""
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise RuntimeError("Socket closed unexpectedly")
        data.extend(chunk)
    return bytes(data)


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
    """

    def __init__(self, client, package_path=""):
        self._client = client
        self._package_path = package_path

    def __getattr__(self, name):
        """Navigate deeper into package hierarchy or return a JavaClass."""
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


class _JVMNode:
    """Hybrid node that can act as both a package path and a Java class.

    - Accessing attributes navigates deeper (jvm.java.util -> jvm.java.util.ArrayList)
    - Calling it instantiates a class (ArrayList() -> new ArrayList())
    - Accessing a method and calling it invokes static method (Integer.parseInt(...))

    The trick: we don't know if an attribute access is a package/class or a method
    until we see what happens next. So we return a _JVMNode that can be:
    - Called with no args or object args -> constructor
    - Called after method-like access -> static method

    We detect method calls by checking if the name looks like a method (starts lowercase)
    vs a class (starts uppercase). This is a heuristic based on Java naming conventions.
    """

    def __init__(self, client, path, is_method=False):
        self._client = client
        self._path = path
        self._is_method = is_method

    def __getattr__(self, name):
        """Navigate deeper or access static method."""
        new_path = f"{self._path}.{name}"
        # Heuristic: if name starts with lowercase, it's likely a method
        is_method = name[0].islower() if name else False
        return _JVMNode(self._client, new_path, is_method=is_method)

    def __call__(self, *args):
        """Instantiate as a class or invoke as a static method."""
        if self._is_method:
            # This is a static method call: java.lang.Integer.parseInt("42")
            # Split path into class and method
            last_dot = self._path.rfind(".")
            if last_dot == -1:
                raise ValueError(f"Invalid method path: {self._path}")
            class_name = self._path[:last_dot]
            method_name = self._path[last_dot + 1:]
            return self._client.invoke_static_method(class_name, method_name, *args)
        else:
            # This is a constructor call: java.util.ArrayList()
            return self._client.create_object(self._path, *args)

    def __repr__(self):
        return f"<JVM {self._path}>"


class GatunClient:
    def __init__(self, socket_path=None):
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
        self.payload_offset = 4096
        self.response_offset = 0

        # JVM view for package-style access
        self._jvm = None

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
            self.sock.settimeout(5.0)

            # 1. Handshake - read version and memory size
            # Format: [4 bytes: version] [4 bytes: reserved] [8 bytes: memory size]
            handshake_data = _recv_exactly(self.sock, 16)
            server_version, _, self.memory_size = struct.unpack("<IIQ", handshake_data)

            # Verify protocol version
            if server_version != PROTOCOL_VERSION:
                raise RuntimeError(
                    f"Protocol version mismatch: client={PROTOCOL_VERSION}, "
                    f"server={server_version}. Please update your client or server."
                )

            # 2. Configure Offsets
            self.response_offset = self.memory_size - 4096

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
            result = []
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

        return None

    def _read_response(self):
        # 1. Read Length
        sz_data = _recv_exactly(self.sock, 4)
        sz = struct.unpack("<I", sz_data)[0]

        # 2. Read Data from SHM
        self.shm.seek(self.response_offset)
        resp_buf = self.shm.read(sz)

        # 3. Parse FlatBuffer
        resp = Response.Response.GetRootAsResponse(resp_buf, 0)

        if resp.IsError():
            error_msg = resp.ErrorMsg().decode("utf-8")
            raise RuntimeError(f"Java Error: {error_msg}")

        # 4. Unpack the return value
        return self._unpack_value(resp.ReturnValType(), resp.ReturnVal())

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
        elif isinstance(value, list):
            # Auto-convert Python list to ListVal
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
        elif value is None:
            from gatun.generated.org.gatun.protocol import NullVal
            NullVal.Start(builder)
            return Value.Value.NullVal, NullVal.End(builder)
        else:
            raise TypeError(f"Unsupported argument type: {type(value)}")

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
        # 1. Calculate payload zone size and validate
        max_payload_size = self.response_offset - self.payload_offset

        # Estimate Arrow IPC size by writing to a sink first
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        arrow_data = sink.getvalue()

        if len(arrow_data) > max_payload_size:
            raise PayloadTooLargeError(len(arrow_data), max_payload_size, "Arrow batch")

        # 2. Write Arrow Data to SHM
        self.shm.seek(self.payload_offset)
        self.shm.write(arrow_data)

        # 3. Build Command
        builder = flatbuffers.Builder(256)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SendArrowBatch)
        command = Cmd.CommandEnd(builder)
        builder.Finish(command)

        # 4. Send via standard path
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
