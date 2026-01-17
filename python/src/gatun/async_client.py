"""Async support for Gatun client.

This module provides:
- AsyncGatunClient: Full async/await client using asyncio
- run_sync: Utility to run sync GatunClient methods in a thread pool

Example using AsyncGatunClient:
    async with AsyncGatunClient(socket_path) as client:
        arr = await client.create_object("java.util.ArrayList")
        await arr.add("hello")
        size = await arr.size()

Example using run_sync with existing sync client:
    client = GatunClient(socket_path)
    client.connect()

    # In async context:
    result = await run_sync(client.create_object, "java.util.ArrayList")
"""

import asyncio
import ctypes
import mmap
import os
import struct
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, Optional, TypeVar, cast

import flatbuffers
import pyarrow as pa

from gatun.client import (
    CallbackTimeoutError,
    DEFAULT_CALLBACK_TIMEOUT,
    DEFAULT_SOCKET_TIMEOUT,
    HANDSHAKE_TIMEOUT,
    JavaObject,
    PayloadTooLargeError,
    ProtocolDesyncError,
    PROTOCOL_VERSION,
    RESPONSE_ZONE_SIZE,
    SocketTimeoutError,
    _raise_java_exception_impl,
)
from gatun.generated.org.gatun.protocol import Command as Cmd
from gatun.generated.org.gatun.protocol import Action as Act
from gatun.generated.org.gatun.protocol import (
    Response,
    Value,
    GetFieldsRequest,
    InvokeMethodsRequest,
    MethodCall,
    CreateObjectsRequest,
    ObjectSpec,
)

# Default thread pool for run_sync
_default_executor: Optional[ThreadPoolExecutor] = None

T = TypeVar("T")


def get_default_executor() -> ThreadPoolExecutor:
    """Get or create the default thread pool executor."""
    global _default_executor
    if _default_executor is None:
        _default_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="gatun-"
        )
    return _default_executor


async def run_sync(
    func: Callable[..., T],
    *args,
    executor: Optional[ThreadPoolExecutor] = None,
    **kwargs,
) -> T:
    """Run a synchronous function in a thread pool executor.

    This is useful for running blocking GatunClient methods in an async context.

    Args:
        func: The synchronous function to run
        *args: Positional arguments to pass to the function
        executor: Optional custom executor. Uses default if not provided.
        **kwargs: Keyword arguments to pass to the function

    Returns:
        The result of the function call

    Example:
        client = GatunClient(socket_path)
        client.connect()

        # Run sync method in thread pool
        result = await run_sync(client.create_object, "java.util.ArrayList")
        await run_sync(result.add, "hello")
        size = await run_sync(result.size)
    """
    loop = asyncio.get_running_loop()
    if executor is None:
        executor = get_default_executor()

    if kwargs:
        func = partial(func, **kwargs)

    return await loop.run_in_executor(executor, func, *args)


class AsyncJavaObject:
    """Async wrapper for Java objects returned from the JVM."""

    def __init__(self, client: "AsyncGatunClient", object_id: int):
        self._client = client
        self.object_id = object_id

    def __getattr__(self, name: str):
        """Return an async method invoker for the given name."""
        if name.startswith("_"):
            raise AttributeError(name)
        return _AsyncMethodInvoker(self._client, self.object_id, name)

    def __repr__(self):
        return f"<AsyncJavaObject id={self.object_id}>"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.free_object(self.object_id)
        return False


class _AsyncMethodInvoker:
    """Callable that invokes a method asynchronously on a Java object."""

    def __init__(self, client: "AsyncGatunClient", object_id: int, method_name: str):
        self._client = client
        self._object_id = object_id
        self._method_name = method_name

    async def __call__(self, *args):
        return await self._client.invoke_method(
            self._object_id, self._method_name, *args
        )


class AsyncJVMView:
    """Async package-style navigation for Java classes."""

    def __init__(self, client: "AsyncGatunClient", path: str = ""):
        self._client = client
        self._path = path

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        new_path = f"{self._path}.{name}" if self._path else name
        return AsyncJavaClass(self._client, new_path)


class AsyncJavaClass:
    """Async callable that creates objects or invokes static methods."""

    def __init__(self, client: "AsyncGatunClient", path: str):
        self._client = client
        self._path = path

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return AsyncJavaClass(self._client, f"{self._path}.{name}")

    async def __call__(self, *args):
        # Determine if this is a constructor or static method call
        parts = self._path.split(".")
        if parts[-1][0].islower():
            # Static method call
            last_dot = self._path.rfind(".")
            if last_dot == -1:
                raise ValueError(f"Invalid method path: {self._path}")
            class_name = self._path[:last_dot]
            method_name = self._path[last_dot + 1 :]
            return await self._client.invoke_static_method(
                class_name, method_name, *args
            )
        else:
            # Constructor call
            return await self._client.create_object(self._path, *args)

    def __repr__(self):
        return f"<AsyncJVM {self._path}>"


class AsyncGatunClient:
    """Async client for communicating with the Gatun Java server.

    This client uses asyncio for non-blocking I/O operations. All Java
    interactions are performed asynchronously.

    Example:
        async with AsyncGatunClient(socket_path) as client:
            # Create Java objects
            arr = await client.create_object("java.util.ArrayList")
            await arr.add("hello")
            await arr.add("world")
            size = await arr.size()  # Returns 2

            # Use JVM view for package navigation
            Integer = client.jvm.java.lang.Integer
            result = await Integer.parseInt("42")
    """

    def __init__(
        self,
        socket_path: Optional[str] = None,
        callback_timeout: float | None = None,
        socket_timeout: float | None = None,
        handshake_timeout: float | None = None,
    ):
        """Initialize an async Gatun client.

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
        self.callback_timeout: float | None = (
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

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

        self.shm_file: Any = None  # BufferedRandom when connected
        self.shm: mmap.mmap | None = None

        self.memory_size = 0
        self.command_offset = 0
        self.payload_offset = 65536  # 64KB - must match GatunServer.PAYLOAD_OFFSET
        self.response_offset = 0

        self._jvm: Optional[AsyncJVMView] = None
        self._callbacks: dict[int, Callable[..., Any]] = {}

        # Reentrancy detection - tracks if we're inside a callback
        # Nested Java calls from callbacks would deadlock
        self._in_callback: bool = False

    @property
    def jvm(self) -> AsyncJVMView:
        """Access Java classes via package navigation."""
        if self._jvm is None:
            self._jvm = AsyncJVMView(self)
        return self._jvm

    async def _read_with_timeout(self, n: int, timeout: float | None) -> bytes:
        """Read exactly n bytes with optional timeout.

        Args:
            n: Number of bytes to read
            timeout: Timeout in seconds, or None for no timeout

        Returns:
            The bytes read

        Raises:
            SocketTimeoutError: If the read times out
            asyncio.IncompleteReadError: If connection closes before n bytes
        """
        if timeout is None:
            return await self._reader.readexactly(n)
        try:
            return await asyncio.wait_for(self._reader.readexactly(n), timeout=timeout)
        except asyncio.TimeoutError:
            raise SocketTimeoutError(f"Socket read timed out after {timeout}s")

    async def connect(self) -> bool:
        """Connect to the Gatun server asynchronously."""
        try:
            self._reader, self._writer = await asyncio.open_unix_connection(
                self.socket_path
            )

            # Read handshake header with timeout
            # Format: [4 bytes: version] [4 bytes: arena_epoch] [8 bytes: memory size]
            #         [2 bytes: shm_path_length] [N bytes: shm_path (UTF-8)]
            handshake_header = await self._read_with_timeout(18, self.handshake_timeout)
            server_version, _, self.memory_size, shm_path_len = struct.unpack(
                "<IIQH", handshake_header
            )

            if server_version != PROTOCOL_VERSION:
                raise RuntimeError(
                    f"Protocol version mismatch: client={PROTOCOL_VERSION}, "
                    f"server={server_version}"
                )

            # Read SHM path with timeout
            shm_path_bytes = await self._read_with_timeout(
                shm_path_len, self.handshake_timeout
            )
            self.memory_path = shm_path_bytes.decode("utf-8")

            self.response_offset = self.memory_size - 65536  # 64KB response zone

            # Map shared memory (still sync - mmap doesn't have async API)
            self.shm_file = open(self.memory_path, "r+b")
            self.shm = mmap.mmap(self.shm_file.fileno(), self.memory_size)

            return True
        except Exception:
            return False

    async def _send_raw(
        self, data: bytes, wait_for_response: bool = True, expects_response: bool = True
    ):
        """Write command to SHM and signal Java asynchronously."""
        # Check for reentrancy - nested calls from callbacks would deadlock
        # Exception: CallbackResponse is sent during callback handling
        if self._in_callback and expects_response:
            from gatun.client import ReentrancyError

            raise ReentrancyError(
                "Cannot call Java from within a callback. "
                "Nested calls would deadlock because the original request is still waiting. "
                "Queue work for later or restructure code to avoid nested calls."
            )

        async with self._lock:
            max_command_size = self.payload_offset
            if len(data) > max_command_size:
                raise PayloadTooLargeError(len(data), max_command_size, "Command")

            # Write to shared memory (sync - mmap doesn't have async API)
            self.shm.seek(self.command_offset)
            self.shm.write(data)

            # Signal Java
            self._writer.write(struct.pack("<I", len(data)))
            await self._writer.drain()

            if wait_for_response:
                return await self._read_response()

    async def _read_response(self):
        """Read and parse response from the server.

        Raises:
            SocketTimeoutError: If the socket read times out
            asyncio.IncompleteReadError: If socket closes unexpectedly
            ProtocolDesyncError: If the response size is invalid
        """
        while True:
            # Read response length with timeout
            length_data = await self._read_with_timeout(4, self.socket_timeout)
            response_len = struct.unpack("<I", length_data)[0]

            # Validate response size before reading from SHM
            # This catches protocol desync (garbage data) early
            max_response_size = self.memory_size - self.response_offset
            if response_len == 0:
                raise ProtocolDesyncError(
                    "Invalid response size: 0 bytes (empty response)",
                    response_size=response_len,
                )
            if response_len > RESPONSE_ZONE_SIZE:
                raise ProtocolDesyncError(
                    f"Response size {response_len} exceeds RESPONSE_ZONE_SIZE ({RESPONSE_ZONE_SIZE})",
                    response_size=response_len,
                )
            if response_len > max_response_size:
                raise ProtocolDesyncError(
                    f"Response size {response_len} exceeds available SHM space ({max_response_size})",
                    response_size=response_len,
                )

            # Read response from shared memory
            self.shm.seek(self.response_offset)
            response_data = self.shm.read(response_len)

            # Parse response
            resp = Response.Response.GetRootAsResponse(response_data, 0)

            # Check if this is a callback request from Java
            if resp.IsCallback():
                await self._handle_callback(resp)
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
                _raise_java_exception_impl(error_type, error_msg)

            # Unpack the return value
            return self._unpack_value(resp.ReturnValType(), resp.ReturnVal())

    async def _handle_callback(self, resp):
        """Handle a callback invocation request from Java.

        Sets _in_callback flag to detect and prevent reentrant calls, which would
        deadlock. The flag is always cleared in finally to ensure cleanup even if
        the callback raises an exception.
        """
        from gatun.client import ReentrancyError

        callback_id = resp.CallbackId()

        # Unpack callback arguments
        args = []
        for i in range(resp.CallbackArgsLength()):
            arg = resp.CallbackArgs(i)
            args.append(self._unpack_value(arg.ValType(), arg.Val()))

        # Look up the callback function
        callback_fn = self._callbacks.get(callback_id)
        if callback_fn is None:
            await self._send_callback_response(
                callback_id, None, True, f"Callback {callback_id} not found"
            )
            return

        # Execute the callback with reentrancy guard and timeout
        self._in_callback = True
        try:
            if asyncio.iscoroutinefunction(callback_fn):
                # Async callback - use wait_for for timeout
                if self.callback_timeout is not None:
                    try:
                        result = await asyncio.wait_for(
                            callback_fn(*args), timeout=self.callback_timeout
                        )
                    except asyncio.TimeoutError:
                        raise CallbackTimeoutError(
                            f"Callback execution timed out after {self.callback_timeout}s"
                        )
                else:
                    result = await callback_fn(*args)
            else:
                # Sync callback - run in executor with timeout
                loop = asyncio.get_running_loop()
                if self.callback_timeout is not None:
                    try:
                        result = await asyncio.wait_for(
                            loop.run_in_executor(None, callback_fn, *args),
                            timeout=self.callback_timeout,
                        )
                    except asyncio.TimeoutError:
                        raise CallbackTimeoutError(
                            f"Callback execution timed out after {self.callback_timeout}s"
                        )
                else:
                    result = await loop.run_in_executor(None, callback_fn, *args)
            await self._send_callback_response(callback_id, result, False, None)
        except ReentrancyError:
            # Propagate reentrancy error with clear message
            await self._send_callback_response(
                callback_id,
                None,
                True,
                "ReentrancyError: Cannot call Java from within a callback",
            )
        except CallbackTimeoutError as e:
            # Propagate timeout error
            await self._send_callback_response(callback_id, None, True, str(e))
        except Exception as e:
            await self._send_callback_response(callback_id, None, True, str(e))
        finally:
            self._in_callback = False

    async def _send_callback_response(
        self, callback_id: int, result, is_error: bool, error_msg: Optional[str]
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
        error_name_off = None
        if error_msg:
            error_name_off = builder.CreateString(error_msg)

        # Build command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CallbackResponse)
        Cmd.CommandAddArgs(builder, args_vec)
        if error_name_off:
            Cmd.CommandAddTargetName(builder, error_name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        data = builder.Output()

        # Write to command zone
        self.shm.seek(self.command_offset)
        self.shm.write(bytes(data))

        # Signal Java
        self._writer.write(struct.pack("<I", len(data)))
        await self._writer.drain()

    def _unpack_value(self, val_type: int, val_table: Any) -> Any:
        """Unpack a FlatBuffer Value union to a Python object."""
        from gatun.generated.org.gatun.protocol import (
            StringVal,
            IntVal,
            DoubleVal,
            BoolVal,
            ObjectRef,
            ListVal,
            MapVal,
            ArrayVal,
            ElementType,
        )

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
        elif val_type == Value.Value.ObjectRef:
            ref_obj = ObjectRef.ObjectRef()
            ref_obj.Init(val_table.Bytes, val_table.Pos)
            return AsyncJavaObject(self, ref_obj.Id())
        elif val_type == Value.Value.ListVal:
            list_obj = ListVal.ListVal()
            list_obj.Init(val_table.Bytes, val_table.Pos)
            list_result: list[Any] = []
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
                key = self._unpack_value(entry.KeyType(), entry.Key())
                value = self._unpack_value(entry.ValueType(), entry.Value())
                map_result[key] = value
            return map_result
        elif val_type == Value.Value.ArrayVal:
            arr_obj = ArrayVal.ArrayVal()
            arr_obj.Init(val_table.Bytes, val_table.Pos)
            elem_type = arr_obj.ElementType()

            if elem_type == ElementType.ElementType.Int:
                return list(arr_obj.IntValuesAsNumpy())
            elif elem_type == ElementType.ElementType.Long:
                return list(arr_obj.LongValuesAsNumpy())
            elif elem_type == ElementType.ElementType.Double:
                return list(arr_obj.DoubleValuesAsNumpy())
            elif elem_type == ElementType.ElementType.Float:
                return list(arr_obj.DoubleValuesAsNumpy())
            elif elem_type == ElementType.ElementType.Byte:
                return bytes(arr_obj.ByteValuesAsNumpy())
            elif elem_type == ElementType.ElementType.String:
                return [
                    arr_obj.StringValues(i).decode("utf-8")
                    for i in range(arr_obj.StringValuesLength())
                ]
            else:
                return []
        else:
            return None

    def _build_value(self, builder, value):
        """Build a Value union and return (type, offset)."""
        from gatun.generated.org.gatun.protocol import (
            StringVal,
            IntVal,
            DoubleVal,
            BoolVal,
            ObjectRef,
            ListVal,
            Argument,
        )

        if isinstance(value, str):
            str_off = builder.CreateString(value)
            StringVal.Start(builder)
            StringVal.AddV(builder, str_off)
            return Value.Value.StringVal, StringVal.End(builder)
        elif isinstance(value, bool):
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
        elif isinstance(value, (JavaObject, AsyncJavaObject)):
            ObjectRef.Start(builder)
            ObjectRef.AddId(builder, value.object_id)
            return Value.Value.ObjectRef, ObjectRef.End(builder)
        elif isinstance(value, list):
            item_offsets = []
            for item in value:
                item_type, item_off = self._build_value(builder, item)
                Argument.Start(builder)
                Argument.AddValType(builder, item_type)
                Argument.AddVal(builder, item_off)
                item_offsets.append(Argument.End(builder))

            ListVal.StartItemsVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            items_vec = builder.EndVector()

            ListVal.Start(builder)
            ListVal.AddItems(builder, items_vec)
            return Value.Value.ListVal, ListVal.End(builder)
        elif value is None:
            return Value.Value.NullVal, 0
        else:
            raise TypeError(f"Unsupported type: {type(value)}")

    def _build_argument(self, builder, value):
        """Convert a Python value to a FlatBuffer Argument."""
        from gatun.generated.org.gatun.protocol import Argument

        val_type, val_off = self._build_value(builder, value)
        Argument.Start(builder)
        Argument.AddValType(builder, val_type)
        Argument.AddVal(builder, val_off)
        return Argument.End(builder)

    async def create_object(self, class_name: str, *args) -> AsyncJavaObject:
        """Create a new Java object asynchronously."""
        builder = flatbuffers.Builder(1024)
        name_off = builder.CreateString(class_name)

        arg_offsets = []
        for arg in args:
            arg_offsets.append(self._build_argument(builder, arg))

        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObject)
        Cmd.CommandAddTargetName(builder, name_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(AsyncJavaObject, await self._send_raw(builder.Output()))

    async def invoke_method(self, obj_id, method_name: str, *args):
        """Invoke a method on a Java object asynchronously."""
        if isinstance(obj_id, (JavaObject, AsyncJavaObject)):
            obj_id = obj_id.object_id

        builder = flatbuffers.Builder(1024)
        name_off = builder.CreateString(method_name)

        arg_offsets = []
        for arg in args:
            arg_offsets.append(self._build_argument(builder, arg))

        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeMethod)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return await self._send_raw(builder.Output())

    async def invoke_static_method(self, class_name: str, method_name: str, *args):
        """Invoke a static method on a class asynchronously."""
        builder = flatbuffers.Builder(1024)
        full_name = f"{class_name}.{method_name}"
        name_off = builder.CreateString(full_name)

        arg_offsets = []
        for arg in args:
            arg_offsets.append(self._build_argument(builder, arg))

        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeStaticMethod)
        Cmd.CommandAddTargetName(builder, name_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return await self._send_raw(builder.Output())

    async def get_field(self, obj_id, field_name: str):
        """Get a field value from a Java object asynchronously."""
        if isinstance(obj_id, (JavaObject, AsyncJavaObject)):
            obj_id = obj_id.object_id

        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(field_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetField)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return await self._send_raw(builder.Output())

    async def set_field(self, obj_id, field_name: str, value):
        """Set a field value on a Java object asynchronously."""
        if isinstance(obj_id, (JavaObject, AsyncJavaObject)):
            obj_id = obj_id.object_id

        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(field_name)

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

        return await self._send_raw(builder.Output())

    async def is_instance_of(self, obj, class_name: str) -> bool:
        """Check if a Java object is an instance of a class asynchronously.

        This is equivalent to Java's `instanceof` operator.

        Args:
            obj: AsyncJavaObject instance or object ID
            class_name: Fully qualified Java class name
                       (e.g., "java.util.List", "java.util.ArrayList")

        Returns:
            True if the object is an instance of the specified class.
        """
        if isinstance(obj, (JavaObject, AsyncJavaObject)):
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

        return cast(bool, await self._send_raw(builder.Output()))

    async def get_metrics(self) -> str:
        """Get server metrics report asynchronously.

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
            metrics = await client.get_metrics()
            print(metrics)
        """
        builder = flatbuffers.Builder(256)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetMetrics)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(str, await self._send_raw(builder.Output()))

    async def get_fields(self, obj, field_names: list[str]) -> list[Any]:
        """Get multiple field values from a Java object in a single round-trip.

        This is more efficient than calling get_field() multiple times when
        you need to read several fields from the same object.

        Args:
            obj: AsyncJavaObject instance or object ID
            field_names: List of field names to read

        Returns:
            List of field values in the same order as field_names.

        Example:
            sb = await client.create_object("java.lang.StringBuilder", "hello")
            values = await client.get_fields(sb, ["count"])  # [5]
        """
        if isinstance(obj, (JavaObject, AsyncJavaObject)):
            obj_id = obj.object_id
        else:
            obj_id = obj

        builder = flatbuffers.Builder(1024)

        # Build field names vector (strings must be created before the table)
        field_name_offsets = [builder.CreateString(name) for name in field_names]
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

        return cast(list[Any], await self._send_raw(builder.Output()))

    async def invoke_methods(
        self,
        obj,
        calls: list[tuple[str, tuple]],
        return_object_refs: bool | list[bool] = False,
    ) -> list[Any]:
        """Invoke multiple methods on the same Java object in a single round-trip.

        This is more efficient than calling invoke_method() multiple times when
        you need to call several methods on the same object.

        Args:
            obj: AsyncJavaObject instance or object ID
            calls: List of (method_name, args_tuple) pairs.
                   Each tuple is (method_name, (arg1, arg2, ...))
            return_object_refs: If True (or list of bools), return results as
                               ObjectRefs instead of auto-converting to Python.

        Returns:
            List of results in the same order as calls.

        Example:
            arr = await client.create_object("java.util.ArrayList")
            results = await client.invoke_methods(arr, [
                ("add", ("a",)),
                ("add", ("b",)),
                ("size", ()),
            ])
            # results = [True, True, 2]
        """
        if isinstance(obj, (JavaObject, AsyncJavaObject)):
            obj_id = obj.object_id
        else:
            obj_id = obj

        # Normalize return_object_refs to a list
        if isinstance(return_object_refs, bool):
            return_object_refs = [return_object_refs] * len(calls)

        builder = flatbuffers.Builder(4096)

        # Build each MethodCall
        method_call_offsets = []
        for i, (method_name, args) in enumerate(calls):
            # Build method name string
            method_name_off = builder.CreateString(method_name)

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

        return cast(list[Any], await self._send_raw(builder.Output()))

    async def create_objects(
        self, specs: list[tuple[str, tuple]]
    ) -> list["AsyncJavaObject"]:
        """Create multiple Java objects in a single round-trip.

        This is more efficient than calling create_object() multiple times when
        you need to create several objects.

        Args:
            specs: List of (class_name, args_tuple) pairs.
                   Each tuple is (fully_qualified_class_name, (arg1, arg2, ...))

        Returns:
            List of AsyncJavaObject instances in the same order as specs.

        Example:
            lists = await client.create_objects([
                ("java.util.ArrayList", ()),
                ("java.util.ArrayList", (100,)),
                ("java.util.HashMap", ()),
            ])
        """
        builder = flatbuffers.Builder(4096)

        # Build each ObjectSpec
        object_spec_offsets = []
        for class_name, args in specs:
            # Build class name string
            class_name_off = builder.CreateString(class_name)

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

        return cast(list["AsyncJavaObject"], await self._send_raw(builder.Output()))

    async def register_callback(
        self, callback_fn: Callable[..., Any], interface_name: str
    ) -> AsyncJavaObject:
        """Register a Python callable as a Java interface implementation.

        The callback can be either a sync function or an async coroutine.
        """
        builder = flatbuffers.Builder(256)
        interface_off = builder.CreateString(interface_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.RegisterCallback)
        Cmd.CommandAddTargetName(builder, interface_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        result: AsyncJavaObject = cast(
            AsyncJavaObject, await self._send_raw(builder.Output())
        )

        # Store the callback function
        callback_id = result.object_id
        self._callbacks[callback_id] = callback_fn

        return result

    async def unregister_callback(self, callback_id: int) -> None:
        """Unregister a previously registered callback."""
        self._callbacks.pop(callback_id, None)

        builder = flatbuffers.Builder(64)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.UnregisterCallback)
        Cmd.CommandAddTargetId(builder, callback_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        await self._send_raw(builder.Output())

    async def free_object(self, object_id: int):
        """Free a Java object asynchronously."""
        if self._writer is None:
            return

        builder = flatbuffers.Builder(64)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.FreeObject)
        Cmd.CommandAddTargetId(builder, object_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        try:
            await self._send_raw(builder.Output())
        except Exception:
            pass

    async def send_arrow_table(self, table: pa.Table):
        """Send an Arrow table to Java via shared memory asynchronously.

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

        builder = flatbuffers.Builder(256)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SendArrowBatch)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return await self._send_raw(builder.Output())

    async def close(self):
        """Close the connection."""
        try:
            if self._writer:
                self._writer.close()
                await self._writer.wait_closed()
                self._writer = None
                self._reader = None
            if self.shm:
                self.shm.close()
                self.shm = None
            if self.shm_file:
                self.shm_file.close()
                self.shm_file = None
        except Exception:
            pass

    async def __aenter__(self):
        # Only connect if not already connected
        if self.shm is None:
            await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False
