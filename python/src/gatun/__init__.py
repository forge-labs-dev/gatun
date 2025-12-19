from typing import Optional

from gatun.client import (
    GatunClient,
    JavaObject,
    JVMView,
    JavaClass,
    java_import,
    PROTOCOL_VERSION,
    PayloadTooLargeError,
    JavaException,
    JavaSecurityException,
    JavaIllegalArgumentException,
    JavaNoSuchMethodException,
    JavaNoSuchFieldException,
    JavaClassNotFoundException,
    JavaNullPointerException,
    JavaIndexOutOfBoundsException,
    JavaNumberFormatException,
    JavaRuntimeException,
    CancelledException,
)
from gatun.async_client import (
    AsyncGatunClient,
    AsyncJavaObject,
    AsyncJVMView,
    AsyncJavaClass,
    run_sync,
)
from gatun.config import GatunConfig, get_config, load_config, reset_config
from gatun.launcher import launch_gateway, GatunSession

__all__ = [
    # Sync client
    "GatunClient",
    "JavaObject",
    "JVMView",
    "JavaClass",
    "java_import",
    # Async client
    "AsyncGatunClient",
    "AsyncJavaObject",
    "AsyncJVMView",
    "AsyncJavaClass",
    "run_sync",
    # Utilities
    "launch_gateway",
    "GatunSession",
    "connect",
    "aconnect",
    "PROTOCOL_VERSION",
    "PayloadTooLargeError",
    # Exceptions
    "JavaException",
    "JavaSecurityException",
    "JavaIllegalArgumentException",
    "JavaNoSuchMethodException",
    "JavaNoSuchFieldException",
    "JavaClassNotFoundException",
    "JavaNullPointerException",
    "JavaIndexOutOfBoundsException",
    "JavaNumberFormatException",
    "JavaRuntimeException",
    "CancelledException",
    # Config
    "GatunConfig",
    "get_config",
    "load_config",
    "reset_config",
]


def connect(memory: Optional[str] = None, socket_path: Optional[str] = None):
    """Convenience: Launches server and returns connected client.

    Args:
        memory: Memory size (e.g., "512MB", "1GB"). Defaults to config value.
        socket_path: Path to Unix socket. Defaults to config value or ~/gatun.sock.

    Configuration can be set in pyproject.toml:
        [tool.gatun]
        memory = "64MB"
        socket_path = "/tmp/gatun.sock"

    Or via environment variables:
        GATUN_MEMORY=64MB
        GATUN_SOCKET_PATH=/tmp/gatun.sock
    """
    session = launch_gateway(memory=memory, socket_path=socket_path)

    client = GatunClient(session.socket_path)
    if not client.connect():
        session.stop()
        raise RuntimeError("Failed to connect to Gatun Server")

    # Attach session to client so it doesn't get GC'd
    client._server_session = session
    return client


async def aconnect(
    memory: Optional[str] = None, socket_path: Optional[str] = None
) -> AsyncGatunClient:
    """Async convenience: Launches server and returns connected async client.

    Args:
        memory: Memory size (e.g., "512MB", "1GB"). Defaults to config value.
        socket_path: Path to Unix socket. Defaults to config value or ~/gatun.sock.

    Example:
        async with await aconnect() as client:
            arr = await client.create_object("java.util.ArrayList")
            await arr.add("hello")

    Configuration can be set in pyproject.toml:
        [tool.gatun]
        memory = "64MB"
        socket_path = "/tmp/gatun.sock"

    Or via environment variables:
        GATUN_MEMORY=64MB
        GATUN_SOCKET_PATH=/tmp/gatun.sock
    """
    session = launch_gateway(memory=memory, socket_path=socket_path)

    client = AsyncGatunClient(session.socket_path)
    if not await client.connect():
        session.stop()
        raise RuntimeError("Failed to connect to Gatun Server")

    # Attach session to client so it doesn't get GC'd
    client._server_session = session
    return client
