import logging
import tempfile
import time
from pathlib import Path

import pytest

from gatun.launcher import launch_gateway
from gatun.client import GatunClient

# --- Logging Setup ---
# Pytest captures this automatically. Run with -o log_cli=true to see it live.
logger = logging.getLogger("gatun.tests")


@pytest.fixture(scope="session")
def java_gateway():
    """
    Launches the Java Server once for the entire test session.
    Uses the packed JAR via the new 'launch_gateway' API.
    """
    # 1. Setup specific test paths (use tempfile for cross-platform compatibility)
    socket_path = Path(tempfile.gettempdir()) / "gatun_test.sock"

    # Cleanup stale socket from previous runs
    # (unlink(missing_ok=True) requires Python 3.8+)
    if socket_path.exists():
        socket_path.unlink()

    logger.info(f"Launching Gatun Server (64MB) at {socket_path}...")

    # 2. Launch (Configuration Phase)
    # We pass the path as a string because the Popen/Java args expect strings
    session = launch_gateway(memory="64MB", socket_path=str(socket_path))

    yield session

    # 3. Teardown
    logger.info("Stopping Gatun Server...")
    session.stop()

    # Final cleanup
    if socket_path.exists():
        socket_path.unlink()


def _create_client(socket_path: str) -> GatunClient:
    """Helper to create and connect a client with retry logic."""
    c = GatunClient(socket_path)
    connected = False
    for _ in range(10):
        connected = c.connect()
        if connected:
            break
        time.sleep(0.1)

    if not connected:
        raise RuntimeError(f"Client failed to connect to Gateway at {socket_path}")

    # Verify Handshake (Sanity Check)
    # 64MB = 67,108,864 bytes
    expected_size = 64 * 1024 * 1024
    if c.memory_size != expected_size:
        raise RuntimeError(
            f"Memory size mismatch! Expected {expected_size}, got {c.memory_size}"
        )

    return c


@pytest.fixture(scope="function")
def client(java_gateway):
    """
    Provides a connected GatunClient for each test function.
    """
    socket_str = str(java_gateway.socket_path)
    logger.debug(f"Connecting client to {socket_str}")

    c = _create_client(socket_str)
    yield c

    # Cleanup
    try:
        if c.sock:
            c.sock.close()
    except Exception:
        pass


@pytest.fixture(scope="function")
def make_client(java_gateway):
    """
    Factory fixture for creating fresh clients.

    Use this for Hypothesis tests where each example should get a fresh
    connection to avoid protocol state corruption affecting subsequent examples.

    Usage:
        def test_something(make_client):
            client = make_client()
            try:
                # ... test code ...
            finally:
                client.close()
    """
    socket_str = str(java_gateway.socket_path)
    created_clients = []

    def _make():
        c = _create_client(socket_str)
        created_clients.append(c)
        return c

    yield _make

    # Cleanup all created clients
    for c in created_clients:
        try:
            if c.sock:
                c.sock.close()
        except Exception:
            pass
