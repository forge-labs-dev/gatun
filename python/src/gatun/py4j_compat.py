"""Py4J compatibility layer for PySpark integration.

This module provides Py4J-compatible classes that wrap Gatun's implementation,
allowing PySpark to use Gatun as a drop-in replacement for Py4J.

Usage:
    # Instead of:
    from py4j.java_gateway import JavaGateway, java_import, JavaObject
    from py4j.clientserver import ClientServer

    # Use:
    from gatun.py4j_compat import JavaGateway, java_import, JavaObject
    from gatun.py4j_compat import ClientServer

The API is designed to be compatible with py4j 0.10.9+.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Callable

from gatun.client import (
    GatunClient,
    JavaObject as GatunJavaObject,
    JVMView as GatunJVMView,
    java_import as gatun_java_import,
    JavaException as GatunJavaException,
)
from gatun.launcher import launch_gateway as gatun_launch_gateway, GatunSession


# Py4J-compatible exception classes
class Py4JJavaError(Exception):
    """Py4J-compatible Java exception wrapper.

    This class mimics py4j.protocol.Py4JJavaError to allow PySpark's
    exception handling to work with Gatun.
    """

    def __init__(self, msg: str, java_exception: "Py4JJavaError | None" = None):
        super().__init__(msg)
        self.java_exception = java_exception
        self._error_type = ""
        self._error_msg = msg

    @classmethod
    def from_gatun_exception(cls, exc: GatunJavaException) -> "Py4JJavaError":
        """Create a Py4JJavaError from a Gatun JavaException."""
        # Create a mock Java exception object that PySpark can introspect
        java_class = getattr(exc, "java_class", type(exc).__name__)
        message = getattr(exc, "message", str(exc))
        stack_trace = getattr(exc, "stack_trace", str(exc))
        java_exc = _MockJavaException(java_class, message, stack_trace)
        error = cls(str(exc), java_exc)
        error._error_type = java_class
        error._error_msg = str(exc)
        return error

    def __str__(self):
        return self._error_msg


class _MockJavaException:
    """Mock Java exception object for PySpark compatibility.

    PySpark's exception handling calls methods like getCause(), getMessage(),
    getStackTrace() on the java_exception. This class provides those methods.

    This class also supports PySpark's is_instance_of checks by maintaining
    a _gatun_class_name attribute that can be used to check type hierarchy.
    """

    def __init__(self, class_name: str, message: str, stack_trace: str):
        self._class_name = class_name
        self._message = message
        self._stack_trace = stack_trace
        self._cause = None
        # Special attribute for Gatun's is_instance_of to use
        self._gatun_class_name = class_name

    def toString(self) -> str:
        return f"{self._class_name}: {self._message}"

    def getMessage(self) -> str:
        return self._message

    def getCause(self) -> "_MockJavaException | None":
        return self._cause

    def getStackTrace(self) -> list:
        """Return a list of mock stack trace elements."""
        return [_MockStackTraceElement(line) for line in self._stack_trace.split("\n")]

    def getClass(self) -> "_MockJavaClass":
        return _MockJavaClass(self._class_name)


class _MockStackTraceElement:
    """Mock stack trace element for PySpark compatibility."""

    def __init__(self, line: str):
        self._line = line

    def toString(self) -> str:
        return self._line


class _MockJavaClass:
    """Mock Java class for PySpark compatibility."""

    def __init__(self, name: str):
        self._name = name

    def getName(self) -> str:
        return self._name


def _convert_exception(func: Callable) -> Callable:
    """Decorator to convert Gatun exceptions to Py4JJavaError/PySpark exceptions.

    This allows PySpark's exception handling to work with Gatun.
    """
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except GatunJavaException as e:
            # First convert to Py4JJavaError
            py4j_error = Py4JJavaError.from_gatun_exception(e)
            # Then try to convert to appropriate PySpark exception
            # Import here to avoid circular import at module load
            converted = _convert_py4j_error_to_pyspark_lazy(py4j_error)
            raise converted from None

    return wrapper


def _convert_py4j_error_to_pyspark_lazy(error: "Py4JJavaError") -> Exception:
    """Lazy conversion to avoid import issues at module load time."""
    # This function is called from _convert_exception decorator
    # We can't import _convert_py4j_error_to_pyspark at decorator definition time
    # because it might not exist yet
    return _convert_py4j_error_to_pyspark(error)


# Re-export java_import directly - API is compatible
java_import = gatun_java_import


@dataclass
class GatewayParameters:
    """Parameters for configuring the Java gateway connection.

    This is a Py4J-compatible configuration class. Most parameters are
    ignored since Gatun uses Unix domain sockets instead of TCP.
    """

    port: int = 25333
    address: str = "127.0.0.1"
    auto_field: bool = False
    auto_close: bool = True
    auto_convert: bool = False
    eager_load: bool = False
    ssl_context: Any = None
    auth_token: str | None = None
    # Gatun-specific
    socket_path: str | None = None
    memory: str | None = None


@dataclass
class JavaParameters:
    """Java-side parameters for ClientServer mode.

    Most parameters are ignored in Gatun.
    """

    port: int = 25333
    address: str = "127.0.0.1"
    auto_field: bool = False
    auto_close: bool = True
    auto_convert: bool = False
    eager_load: bool = False
    ssl_context: Any = None
    auth_token: str | None = None
    # Gatun-specific
    socket_path: str | None = None


@dataclass
class PythonParameters:
    """Python-side callback parameters for ClientServer mode.

    Most parameters are ignored in Gatun since callbacks use
    a different mechanism.
    """

    port: int = 25334
    address: str = "127.0.0.1"
    eager_load: bool = True
    ssl_context: Any = None
    auth_token: str | None = None
    auto_gc: bool = True
    daemonize: bool = True
    daemonize_connections: bool = True
    propagate_java_exceptions: bool = False


class JavaObject(GatunJavaObject):
    """Py4J-compatible JavaObject wrapper.

    Extends Gatun's JavaObject with Py4J-specific attributes.
    Converts Gatun exceptions to Py4JJavaError for PySpark compatibility.
    """

    def __init__(self, target_id: str | int, gateway: "JavaGateway"):
        # Py4J uses string target IDs like "o123"
        if isinstance(target_id, str) and target_id.startswith("o"):
            object_id = int(target_id[1:])
        else:
            object_id = int(target_id)

        super().__init__(gateway._client, object_id)
        self._gateway = gateway
        self._target_id = target_id

    @property
    def _gateway_client(self):
        """Py4J compatibility: return the gateway client."""
        return self._gateway

    def __getattr__(self, name: str):
        """Get attribute with exception conversion."""
        # Get the underlying callable from GatunJavaObject
        attr = super().__getattr__(name)

        # Wrap callables to convert exceptions and wrap returned objects
        if callable(attr):
            return _ExceptionConvertingCallable(attr, self._gateway)
        return attr


class _ExceptionConvertingCallable:
    """Wrapper that converts Gatun exceptions to Py4JJavaError/PySpark exceptions."""

    def __init__(self, func: Callable, gateway: "JavaGateway | None" = None):
        self._func = func
        self._gateway = gateway

    def __call__(self, *args, **kwargs):
        try:
            result = self._func(*args, **kwargs)
            # If result is a GatunJavaObject and we have a gateway, wrap it
            if isinstance(result, GatunJavaObject) and not isinstance(result, JavaObject):
                if self._gateway is not None:
                    # Detach the original object to prevent double-free
                    result.detach()
                    return JavaObject(result.object_id, self._gateway)
            return result
        except GatunJavaException as e:
            # First convert to Py4JJavaError
            py4j_error = Py4JJavaError.from_gatun_exception(e)
            # Then try to convert to appropriate PySpark exception
            converted = _convert_py4j_error_to_pyspark_lazy(py4j_error)
            raise converted from None
        except Py4JJavaError as e:
            # Client already raised Py4JJavaError - try to convert to PySpark exception
            converted = _convert_py4j_error_to_pyspark_lazy(e)
            if converted is not e:
                raise converted from None
            raise


class JVMView(GatunJVMView):
    """Py4J-compatible JVMView wrapper.

    Provides the same interface as py4j.java_gateway.JVMView.
    Converts Gatun exceptions to Py4JJavaError for PySpark compatibility.
    """

    def __init__(self, gateway: "JavaGateway", jvm_name: str = "default"):
        super().__init__(gateway._client)
        self._gateway = gateway
        self._id = jvm_name

    @property
    def _gateway_client(self):
        """Py4J compatibility: return the gateway client."""
        return self._gateway

    def __getattr__(self, name: str):
        """Get attribute with exception conversion wrapper."""
        attr = super().__getattr__(name)
        # Wrap the _JVMNode to convert exceptions
        if hasattr(attr, "__call__"):
            return _ExceptionConvertingJVMNode(attr, self._gateway)
        return attr


def _convert_py4j_error_to_pyspark(error: "Py4JJavaError") -> Exception:
    """Convert Py4JJavaError to appropriate PySpark exception.

    This function converts Gatun's Py4JJavaError to the appropriate PySpark
    exception (AnalysisException, IllegalArgumentException, etc.) so that
    PySpark code that catches these specific exceptions will work correctly.

    Args:
        error: The Py4JJavaError to convert

    Returns:
        The appropriate PySpark exception, or the original error if no
        conversion is needed or possible
    """
    # Get the java_exception which is our _MockJavaException
    java_exc = error.java_exception
    if java_exc is None or not hasattr(java_exc, "_gatun_class_name"):
        return error

    class_name = java_exc._gatun_class_name
    message = java_exc.getMessage() or ""
    stack_trace = java_exc._stack_trace if hasattr(java_exc, "_stack_trace") else str(error)

    # Try to import PySpark exception classes
    try:
        from pyspark.errors.exceptions.captured import (
            AnalysisException,
            ParseException,
            IllegalArgumentException,
            StreamingQueryException,
            QueryExecutionException,
            PythonException,
            ArithmeticException,
            UnsupportedOperationException,
            ArrayIndexOutOfBoundsException,
            DateTimeException,
            NumberFormatException,
            SparkRuntimeException,
            SparkUpgradeException,
            SparkNoSuchElementException,
        )
    except ImportError:
        # PySpark not available or exceptions not available
        return error

    # Map Java exception class to PySpark exception
    # Order matters - more specific exceptions should come first
    if "catalyst.parser.ParseException" in class_name:
        return ParseException(desc=message, stackTrace=stack_trace)
    elif "AnalysisException" in class_name:
        return AnalysisException(desc=message, stackTrace=stack_trace)
    elif "StreamingQueryException" in class_name:
        return StreamingQueryException(desc=message, stackTrace=stack_trace)
    elif "QueryExecutionException" in class_name:
        return QueryExecutionException(desc=message, stackTrace=stack_trace)
    elif "NumberFormatException" in class_name:
        return NumberFormatException(desc=message, stackTrace=stack_trace)
    elif "IllegalArgumentException" in class_name:
        return IllegalArgumentException(desc=message, stackTrace=stack_trace)
    elif "ArithmeticException" in class_name:
        return ArithmeticException(desc=message, stackTrace=stack_trace)
    elif "UnsupportedOperationException" in class_name:
        return UnsupportedOperationException(desc=message, stackTrace=stack_trace)
    elif "ArrayIndexOutOfBoundsException" in class_name:
        return ArrayIndexOutOfBoundsException(desc=message, stackTrace=stack_trace)
    elif "DateTimeException" in class_name:
        return DateTimeException(desc=message, stackTrace=stack_trace)
    elif "SparkRuntimeException" in class_name:
        return SparkRuntimeException(desc=message, stackTrace=stack_trace)
    elif "SparkUpgradeException" in class_name:
        return SparkUpgradeException(desc=message, stackTrace=stack_trace)
    elif "SparkNoSuchElementException" in class_name:
        return SparkNoSuchElementException(desc=message, stackTrace=stack_trace)
    elif "PythonException" in class_name:
        return PythonException(desc=message, stackTrace=stack_trace)

    # Return the original error for unknown exception types
    return error


class _ExceptionConvertingJVMNode:
    """Wrapper for JVM nodes that converts exceptions and wraps results."""

    def __init__(self, node, gateway: "JavaGateway"):
        self._node = node
        self._gateway = gateway

    def __getattr__(self, name: str):
        attr = getattr(self._node, name)
        if hasattr(attr, "__call__") or hasattr(attr, "__getattr__"):
            return _ExceptionConvertingJVMNode(attr, self._gateway)
        return attr

    def __call__(self, *args, **kwargs):
        try:
            result = self._node(*args, **kwargs)
            # Wrap returned JavaObjects
            if isinstance(result, GatunJavaObject) and not isinstance(result, JavaObject):
                # Detach the original object to prevent double-free
                # The wrapper will manage the object lifecycle
                result.detach()
                return JavaObject(result.object_id, self._gateway)
            return result
        except GatunJavaException as e:
            # First convert to Py4JJavaError
            py4j_error = Py4JJavaError.from_gatun_exception(e)
            # Then try to convert to appropriate PySpark exception
            converted = _convert_py4j_error_to_pyspark(py4j_error)
            raise converted from None

    def __repr__(self):
        return repr(self._node)


class JavaGateway:
    """Py4J-compatible gateway wrapper around GatunClient.

    This class provides the same interface as py4j.java_gateway.JavaGateway,
    but uses Gatun's shared memory communication instead of TCP sockets.

    Example:
        # Py4J style usage
        gateway = JavaGateway()
        java_list = gateway.jvm.java.util.ArrayList()
        java_list.add("hello")
        gateway.close()

        # With parameters
        gateway = JavaGateway(
            gateway_parameters=GatewayParameters(socket_path="/tmp/gatun.sock")
        )
    """

    def __init__(
        self,
        gateway_parameters: GatewayParameters | None = None,
        callback_server_parameters: PythonParameters | None = None,
        java_process: Any = None,
        python_proxy_port: int | None = None,
        python_server_entry_point: Any = None,
        eager_load: bool = False,
        auto_field: bool = False,
        auto_convert: bool = False,
        auto_close: bool = True,
        # Gatun-specific
        socket_path: str | None = None,
        memory: str | None = None,
        start_server: bool = True,
    ):
        """Initialize the JavaGateway.

        Args:
            gateway_parameters: Connection parameters (Py4J compatible)
            callback_server_parameters: Callback server parameters (Py4J compatible)
            java_process: External Java process (ignored, Gatun manages its own)
            python_proxy_port: Callback port (ignored, Gatun uses different mechanism)
            python_server_entry_point: Entry point for callbacks (ignored)
            eager_load: Whether to eagerly load JVM classes (ignored)
            auto_field: Whether to enable auto field access (ignored)
            auto_convert: Whether to auto-convert Python types (ignored)
            auto_close: Whether to close gateway on finalization
            socket_path: Gatun-specific: Unix socket path
            memory: Gatun-specific: Shared memory size (e.g., "64MB")
            start_server: Whether to start the Gatun server (default True)
        """
        self._auto_close = auto_close
        self._callback_server = None
        self._python_proxy_port = None
        self._session: GatunSession | None = None
        self._java_process = java_process

        # Extract socket_path from parameters if provided
        if gateway_parameters is not None:
            socket_path = socket_path or gateway_parameters.socket_path
            memory = memory or gateway_parameters.memory

        # Start server if requested
        if start_server and java_process is None:
            self._session = gatun_launch_gateway(
                memory=memory,
                socket_path=socket_path,
            )
            socket_path = self._session.socket_path

        # Default socket path (only if not starting server)
        if socket_path is None:
            socket_path = os.path.expanduser("~/gatun.sock")

        self._socket_path = socket_path

        # Store gateway parameters for Py4J compatibility
        self._gateway_parameters = gateway_parameters or GatewayParameters()

        # Create and connect client with py4j_compat mode enabled
        self._client = GatunClient(socket_path, py4j_compat=True)
        # Set the exception converter so client raises Py4JJavaError
        self._client._py4j_exception_converter = Py4JJavaError.from_gatun_exception
        if not self._client.connect():
            if self._session:
                self._session.stop()
            raise RuntimeError("Failed to connect to Gatun server")

        # Create JVM view
        self._jvm = JVMView(self)

    @property
    def jvm(self) -> JVMView:
        """Access JVM classes and packages.

        Example:
            gateway.jvm.java.util.ArrayList()
            gateway.jvm.java.lang.Math.max(1, 2)
        """
        return self._jvm

    @property
    def gateway_parameters(self) -> GatewayParameters:
        """Get the gateway connection parameters.

        Returns:
            GatewayParameters instance with connection settings
        """
        return self._gateway_parameters

    @property
    def java_gateway_server(self):
        """Py4J compatibility: return None (Gatun doesn't expose server)."""
        return None

    def get_callback_server(self):
        """Get the callback server (if started)."""
        return self._callback_server

    def start_callback_server(
        self, callback_server_parameters: PythonParameters | None = None
    ) -> bool:
        """Start the callback server for Java->Python calls.

        In Gatun, callbacks are handled differently, so this is a no-op
        that returns True for compatibility.
        """
        # Gatun handles callbacks through register_callback()
        self._callback_server = _CallbackServer(self)
        return True

    def shutdown_callback_server(self, raise_exception: bool = False) -> bool:
        """Shutdown the callback server.

        In Gatun, this is a no-op since callbacks are managed differently.
        """
        self._callback_server = None
        return True

    def new_jvm_view(self, name: str = "custom") -> JVMView:
        """Create a new JVM view with its own imports namespace.

        Args:
            name: Name for the new JVM view

        Returns:
            A new JVMView instance
        """
        return JVMView(self, name)

    def new_array(self, java_class, dimensions: list[int]) -> JavaObject:
        """Create a new Java array.

        Note: This is a simplified implementation that creates an ArrayList
        instead of a true array. Full array support may be added later.
        """
        # TODO: Implement proper array creation in Gatun protocol
        arr = self._client.create_object("java.util.ArrayList")
        return JavaObject(arr.object_id, self)

    def close(self, keep_callback_server: bool = False, close_callback_server_connections: bool = False):
        """Close the gateway and release resources.

        Args:
            keep_callback_server: Ignored (Py4J compatibility)
            close_callback_server_connections: Ignored (Py4J compatibility)
        """
        if self._callback_server:
            self._callback_server = None

        if self._client:
            self._client.close()
            self._client = None

        if self._session:
            self._session.stop()
            self._session = None

    def shutdown(self, raise_exception: bool = False):
        """Shutdown the gateway (alias for close)."""
        self.close()

    def detach(self, java_object: JavaObject):
        """Detach a Java object from automatic cleanup.

        This prevents the object from being freed when the Python
        reference is garbage collected.
        """
        if hasattr(java_object, "detach"):
            java_object.detach()

    def is_instance_of(self, java_object: JavaObject, java_class: str) -> bool:
        """Check if a Java object is an instance of a class.

        Args:
            java_object: The Java object to check
            java_class: Fully qualified class name

        Returns:
            True if java_object is an instance of java_class
        """
        return self._client.is_instance_of(java_object, java_class)

    def help(self, var: Any, pattern: str | None = None, short_name: bool = True, display: bool = True) -> str | None:
        """Display help about a Java object.

        Note: This is a stub for Py4J compatibility. Gatun doesn't
        currently support reflection-based help.
        """
        if display:
            print(f"Help not available for {var}")
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        if self._auto_close:
            self.close()


class _CallbackServer:
    """Minimal callback server for Py4J compatibility.

    Gatun handles callbacks through register_callback(), so this is
    primarily for API compatibility.
    """

    def __init__(self, gateway: JavaGateway):
        self._gateway = gateway
        self._listening_address = "127.0.0.1"
        self._listening_port = 0

    def get_listening_port(self) -> int:
        """Get the port the callback server is listening on."""
        return self._listening_port

    def close(self):
        """Close the callback server."""
        pass


class ClientServer(JavaGateway):
    """Py4J ClientServer-compatible gateway.

    ClientServer is Py4J's thread-pinning mode where each Python thread
    has a dedicated connection to Java. In Gatun, all communication goes
    through the same shared memory region, so this is functionally
    identical to JavaGateway.

    Example:
        server = ClientServer(
            java_parameters=JavaParameters(port=25333),
            python_parameters=PythonParameters(port=25334)
        )
        server.jvm.java.util.ArrayList()
        server.shutdown()
    """

    def __init__(
        self,
        java_parameters: JavaParameters | None = None,
        python_parameters: PythonParameters | None = None,
        python_server_entry_point: Any = None,
        # Gatun-specific
        socket_path: str | None = None,
        memory: str | None = None,
        start_server: bool = True,
    ):
        """Initialize the ClientServer.

        Args:
            java_parameters: Java-side connection parameters
            python_parameters: Python-side callback parameters
            python_server_entry_point: Entry point for callbacks (ignored)
            socket_path: Gatun-specific: Unix socket path
            memory: Gatun-specific: Shared memory size
            start_server: Whether to start the Gatun server
        """
        # Extract socket_path from java_parameters if provided
        if java_parameters is not None:
            socket_path = socket_path or java_parameters.socket_path

        super().__init__(
            socket_path=socket_path,
            memory=memory,
            start_server=start_server,
        )

        # Auto-start callback server for ClientServer mode
        if python_parameters is not None:
            self.start_callback_server(python_parameters)

    def shutdown(self, raise_exception: bool = False):
        """Shutdown the ClientServer."""
        self.close()


# Utility functions for Py4J compatibility


def get_field(java_object: JavaObject, field_name: str) -> Any:
    """Get a field value from a Java object.

    Args:
        java_object: The Java object
        field_name: Name of the field

    Returns:
        The field value
    """
    return java_object.client.get_field(java_object, field_name)


def set_field(java_object: JavaObject, field_name: str, value: Any) -> None:
    """Set a field value on a Java object.

    Args:
        java_object: The Java object
        field_name: Name of the field
        value: Value to set
    """
    java_object.client.set_field(java_object, field_name, value)


def is_instance_of(gateway: JavaGateway, java_object: JavaObject, java_class: str) -> bool:
    """Check if a Java object is an instance of a class.

    This is the function form of gateway.is_instance_of().

    Args:
        gateway: The JavaGateway instance
        java_object: The Java object to check
        java_class: Fully qualified class name (e.g., "java.util.List")

    Returns:
        True if java_object is an instance of java_class
    """
    return gateway.is_instance_of(java_object, java_class)


def get_java_class(java_object: JavaObject) -> str:
    """Get the Java class name of an object.

    Args:
        java_object: The Java object

    Returns:
        Fully qualified class name
    """
    return java_object.client.invoke_method(
        java_object.object_id, "getClass"
    ).invoke_method("getName")


def launch_gateway(
    port: int = 0,
    javaopts: list[str] | None = None,
    die_on_exit: bool = False,
    classpath: str | None = None,
    redirect_stdout: Any = None,
    redirect_stderr: Any = None,
    # Gatun-specific
    socket_path: str | None = None,
    memory: str | None = None,
) -> JavaGateway:
    """Launch a new gateway and return a connected JavaGateway.

    This is a Py4J-compatible convenience function.

    Args:
        port: Ignored (Gatun uses Unix sockets)
        javaopts: JVM options (passed to Gatun launcher)
        die_on_exit: Whether JVM should die when Python exits (always True in Gatun)
        classpath: Additional classpath entries
        redirect_stdout: Where to redirect JVM stdout
        redirect_stderr: Where to redirect JVM stderr
        socket_path: Gatun-specific: Unix socket path
        memory: Gatun-specific: Shared memory size

    Returns:
        Connected JavaGateway instance
    """
    return JavaGateway(
        socket_path=socket_path,
        memory=memory,
        start_server=True,
    )


# Global exception handler that can be set by PySpark's install_exception_handler()
# When set, this function is called with Py4JJavaError before re-raising
_exception_handler: callable | None = None


def _set_exception_handler(handler: callable) -> None:
    """Set a global exception handler for Py4JJavaError.

    This is called by PySpark's install_exception_handler() to install a handler
    that converts Py4JJavaError to PySpark-specific exceptions like AnalysisException.

    The handler takes a function as input and returns a wrapped function that
    catches Py4JJavaError and converts it to the appropriate exception.
    """
    global _exception_handler
    _exception_handler = handler


def _get_exception_handler() -> callable | None:
    """Get the currently registered exception handler."""
    return _exception_handler


# Known exception inheritance patterns for Spark/Java
# This is used by _gatun_is_instance_of to check exception types
_EXCEPTION_INHERITANCE = {
    # Spark SQL exceptions
    "org.apache.spark.sql.catalyst.parser.ParseException": [
        "org.apache.spark.sql.AnalysisException",
        "java.lang.Exception",
    ],
    "org.apache.spark.sql.AnalysisException": [
        "java.lang.Exception",
    ],
    "org.apache.spark.sql.streaming.StreamingQueryException": [
        "java.lang.Exception",
    ],
    "org.apache.spark.sql.execution.QueryExecutionException": [
        "java.lang.Exception",
    ],
    # Java exceptions
    "java.lang.NumberFormatException": [
        "java.lang.IllegalArgumentException",
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
    "java.lang.IllegalArgumentException": [
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
    "java.lang.ArithmeticException": [
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
    "java.lang.UnsupportedOperationException": [
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
    "java.lang.ArrayIndexOutOfBoundsException": [
        "java.lang.IndexOutOfBoundsException",
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
    "java.lang.IndexOutOfBoundsException": [
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
    "java.time.DateTimeException": [
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
    # Spark runtime exceptions
    "org.apache.spark.SparkRuntimeException": [
        "org.apache.spark.SparkException",
        "java.lang.Exception",
    ],
    "org.apache.spark.SparkUpgradeException": [
        "java.lang.Exception",
    ],
    "org.apache.spark.SparkNoSuchElementException": [
        "java.util.NoSuchElementException",
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
    "org.apache.spark.api.python.PythonException": [
        "java.lang.RuntimeException",
        "java.lang.Exception",
    ],
}


def _check_exception_inheritance(obj_class: str, parent_class: str) -> bool:
    """Check if obj_class inherits from parent_class.

    This handles the common exception inheritance patterns in Spark/Java.
    """
    parents = _EXCEPTION_INHERITANCE.get(obj_class, [])
    return parent_class in parents


# Monkey-patch py4j.protocol to use our Py4JJavaError
# This allows PySpark's exception handlers (which import from py4j.protocol)
# to catch Gatun exceptions correctly.
def _patch_py4j_protocol():
    """Patch py4j.protocol.Py4JJavaError to be Gatun's Py4JJavaError.

    This is necessary because PySpark imports Py4JJavaError from py4j.protocol
    in many places and uses `except Py4JJavaError:` to catch Java exceptions.
    By patching the py4j.protocol module, those catch blocks will also catch
    our Gatun Py4JJavaError exceptions.

    This function patches py4j modules regardless of whether the real py4j
    package is installed, ensuring Gatun's exception handling is used.
    """
    import sys
    import types

    # Create or get the py4j module
    if "py4j" not in sys.modules:
        py4j_module = types.ModuleType("py4j")
        sys.modules["py4j"] = py4j_module
    else:
        py4j_module = sys.modules["py4j"]

    # Create or get the py4j.protocol module
    if "py4j.protocol" not in sys.modules:
        protocol_module = types.ModuleType("py4j.protocol")
        sys.modules["py4j.protocol"] = protocol_module
        py4j_module.protocol = protocol_module
    else:
        protocol_module = sys.modules["py4j.protocol"]

    # ALWAYS patch Py4JJavaError to be our class (even if real py4j is installed)
    protocol_module.Py4JJavaError = Py4JJavaError

    # Add a dummy get_return_value that PySpark's install_exception_handler can patch.
    # This is only used to make install_exception_handler not fail - the actual
    # exception conversion happens in _ExceptionConvertingJVMNode.
    def _dummy_get_return_value(*args, **kwargs):
        """Dummy function for PySpark compatibility - not actually used."""
        pass

    protocol_module.get_return_value = _dummy_get_return_value

    # Custom is_instance_of that handles both real Java objects and mock exceptions
    def _gatun_is_instance_of(gw, obj, cls_name):
        """Check if obj is an instance of cls_name.

        For real JavaObject instances, delegates to gateway.is_instance_of.
        For _MockJavaException objects (from Gatun's exception handling),
        checks based on the class name using a known inheritance hierarchy.
        """
        # Handle mock exceptions specially
        if hasattr(obj, "_gatun_class_name"):
            obj_class = obj._gatun_class_name
            # Check exact match first
            if obj_class == cls_name:
                return True
            # Check known inheritance patterns for Spark exceptions
            return _check_exception_inheritance(obj_class, cls_name)
        # Fall back to gateway's is_instance_of for real Java objects
        if gw is not None:
            return gw.is_instance_of(obj, cls_name)
        return False

    # Create or get the py4j.java_gateway module
    if "py4j.java_gateway" not in sys.modules:
        gateway_module = types.ModuleType("py4j.java_gateway")
        sys.modules["py4j.java_gateway"] = gateway_module
        py4j_module.java_gateway = gateway_module
    else:
        gateway_module = sys.modules["py4j.java_gateway"]

    # ALWAYS patch is_instance_of (even if real py4j is installed)
    gateway_module.is_instance_of = _gatun_is_instance_of
    gateway_module.java_import = java_import
    gateway_module.get_return_value = _dummy_get_return_value


# Apply the patch when this module is imported
_patch_py4j_protocol()


# Export all public symbols
__all__ = [
    # Main classes
    "JavaGateway",
    "ClientServer",
    "JavaObject",
    "JVMView",
    # Parameter classes
    "GatewayParameters",
    "JavaParameters",
    "PythonParameters",
    # Exceptions (Py4J compatible)
    "Py4JJavaError",
    # Functions
    "java_import",
    "get_field",
    "set_field",
    "is_instance_of",
    "get_java_class",
    "launch_gateway",
]
