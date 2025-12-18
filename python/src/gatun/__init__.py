from gatun.client import GatunClient, JavaObject, PROTOCOL_VERSION
from gatun.launcher import launch_gateway, GatunSession

__all__ = [
    "GatunClient",
    "JavaObject",
    "launch_gateway",
    "GatunSession",
    "connect",
    "PROTOCOL_VERSION",
]


def connect(memory: str = "16MB"):
    """Convenience: Launches server and returns connected client."""
    session = launch_gateway(memory=memory)

    client = GatunClient(session.socket_path)
    if not client.connect():
        session.stop()
        raise RuntimeError("Failed to connect to Gatun Server")

    # Attach session to client so it doesn't get GC'd
    client._server_session = session
    return client
