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
)
from gatun.launcher import launch_gateway as gatun_launch_gateway, GatunSession


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


class JVMView(GatunJVMView):
    """Py4J-compatible JVMView wrapper.

    Provides the same interface as py4j.java_gateway.JVMView.
    """

    def __init__(self, gateway: "JavaGateway", jvm_name: str = "default"):
        super().__init__(gateway._client)
        self._gateway = gateway
        self._id = jvm_name

    @property
    def _gateway_client(self):
        """Py4J compatibility: return the gateway client."""
        return self._gateway


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

        # Create and connect client
        self._client = GatunClient(socket_path)
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
    # Functions
    "java_import",
    "get_field",
    "set_field",
    "is_instance_of",
    "get_java_class",
    "launch_gateway",
]
