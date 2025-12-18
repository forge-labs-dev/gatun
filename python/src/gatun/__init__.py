from typing import Optional

from gatun.client import (
    GatunClient,
    JavaObject,
    JVMView,
    JavaClass,
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
)
from gatun.config import GatunConfig, get_config, load_config, reset_config
from gatun.launcher import launch_gateway, GatunSession

__all__ = [
    "GatunClient",
    "JavaObject",
    "JVMView",
    "JavaClass",
    "launch_gateway",
    "GatunSession",
    "connect",
    "PROTOCOL_VERSION",
    "PayloadTooLargeError",
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
