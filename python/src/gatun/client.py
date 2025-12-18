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
)

logger = logging.getLogger(__name__)

# Protocol version - must match the server version
PROTOCOL_VERSION = 1


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
        # 1. Write to Shared Memory
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

        val_type = resp.ReturnValType()

        # 4. Unpack Union
        if val_type == Value.Value.NullVal:
            return None
        elif val_type == Value.Value.StringVal:
            union_obj = StringVal.StringVal()
            union_obj.Init(resp.ReturnVal().Bytes, resp.ReturnVal().Pos)
            return union_obj.V().decode("utf-8")
        elif val_type == Value.Value.IntVal:
            union_obj = IntVal.IntVal()
            union_obj.Init(resp.ReturnVal().Bytes, resp.ReturnVal().Pos)
            return union_obj.V()
        elif val_type == Value.Value.DoubleVal:
            union_obj = DoubleVal.DoubleVal()
            union_obj.Init(resp.ReturnVal().Bytes, resp.ReturnVal().Pos)
            return union_obj.V()
        elif val_type == Value.Value.BoolVal:
            union_obj = BoolVal.BoolVal()
            union_obj.Init(resp.ReturnVal().Bytes, resp.ReturnVal().Pos)
            return union_obj.V()
        elif val_type == Value.Value.ObjectRef:
            union_obj = ObjectRef.ObjectRef()
            union_obj.Init(resp.ReturnVal().Bytes, resp.ReturnVal().Pos)
            return JavaObject(self, union_obj.Id())

        return None

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
        if isinstance(value, str):
            str_off = builder.CreateString(value)
            StringVal.Start(builder)
            StringVal.AddV(builder, str_off)
            val_off = StringVal.End(builder)
            val_type = Value.Value.StringVal
        elif isinstance(value, bool):  # Must check before int (bool is subclass of int)
            BoolVal.Start(builder)
            BoolVal.AddV(builder, value)
            val_off = BoolVal.End(builder)
            val_type = Value.Value.BoolVal
        elif isinstance(value, int):
            IntVal.Start(builder)
            IntVal.AddV(builder, value)
            val_off = IntVal.End(builder)
            val_type = Value.Value.IntVal
        elif isinstance(value, float):
            DoubleVal.Start(builder)
            DoubleVal.AddV(builder, value)
            val_off = DoubleVal.End(builder)
            val_type = Value.Value.DoubleVal
        elif isinstance(value, JavaObject):
            ObjectRef.Start(builder)
            ObjectRef.AddId(builder, value.object_id)
            val_off = ObjectRef.End(builder)
            val_type = Value.Value.ObjectRef
        else:
            raise TypeError(f"Unsupported argument type: {type(value)}")

        Argument.Start(builder)
        Argument.AddValType(builder, val_type)
        Argument.AddVal(builder, val_off)
        return Argument.End(builder)

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
        # 1. Write Arrow Data to SHM
        self.shm.seek(self.payload_offset)
        with pa.ipc.new_stream(self.shm, table.schema) as writer:
            writer.write_table(table)

        # 2. Build Command
        builder = flatbuffers.Builder(256)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SendArrowBatch)
        command = Cmd.CommandEnd(builder)
        builder.Finish(command)

        # 3. Send via standard path
        return self._send_raw(builder.Output())

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
