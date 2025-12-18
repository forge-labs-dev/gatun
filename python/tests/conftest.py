import logging
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
    # 1. Setup specific test paths
    socket_path = Path("/tmp/gatun_test.sock")

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


@pytest.fixture(scope="function")
def client(java_gateway):
    """
    Provides a connected GatunClient for each test function.
    """
    socket_str = str(java_gateway.socket_path)
    logger.debug(f"Connecting client to {socket_str}")

    c = GatunClient(socket_str)
    connected = c.connect()

    if not connected:
        pytest.fail(f"Client failed to connect to Gateway at {socket_str}")

    # 2. Verify Handshake (Sanity Check)
    # 64MB = 67,108,864 bytes
    expected_size = 64 * 1024 * 1024
    if c.memory_size != expected_size:
        pytest.fail(
            f"Memory size mismatch! Expected {expected_size}, got {c.memory_size}"
        )

    yield c

    # 3. Cleanup
    c.sock.close()
