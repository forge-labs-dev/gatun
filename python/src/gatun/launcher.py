import atexit
import logging
import os
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# --- Configuration ---
MODULE_DIR = Path(__file__).parent.resolve()
JAR_PATH = MODULE_DIR / "jars" / "gatun-server-all.jar"

# JVM Flags required for Apache Arrow & Netty (Java 21+)
JVM_FLAGS = [
    "--enable-preview",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Darrow.memory.debug.allocator=true",
]


class GatunSession:
    def __init__(self, process, socket_path, memory_bytes):
        self.process = process
        self.socket_path = socket_path
        self.memory_bytes = memory_bytes

    def stop(self):
        if self.process:
            logger.debug("Stopping Java server...")
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None


def launch_gateway(memory="16MB", socket_path=None):
    """
    Launches the embedded Java server.

    Args:
        memory (str): Memory size (e.g., "512MB", "1GB").
        socket_path (str): Path to the Unix socket. Defaults to ~/gatun.sock.
    """
    if not JAR_PATH.exists():
        raise RuntimeError(f"Gatun JAR not found at {JAR_PATH}. Did you run 'uv sync'?")

    # 1. Parse Memory
    size_str = memory.upper()
    if size_str.endswith("GB"):
        mem_bytes = int(float(size_str[:-2]) * 1024 * 1024 * 1024)
    elif size_str.endswith("MB"):
        mem_bytes = int(float(size_str[:-2]) * 1024 * 1024)
    else:
        mem_bytes = int(size_str)  # Assume bytes

    # 2. Setup Paths
    if socket_path is None:
        socket_path = os.path.expanduser("~/gatun.sock")

    # 3. Construct Command
    # java [FLAGS] -jar [JAR] [MEM_SIZE] [SOCKET_PATH]
    cmd = ["java"] + JVM_FLAGS + ["-jar", str(JAR_PATH), str(mem_bytes), socket_path]

    logger.info("Launching Java server: %s @ %s", memory, socket_path)

    process = subprocess.Popen(cmd, stdout=None, stderr=None, text=True)

    # 4. Wait for Socket (Handshake)
    retries = 50
    while retries > 0:
        if os.path.exists(socket_path):
            break

        if process.poll() is not None:
            # Process died
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Java Server failed to start:\n{stdout}\n{stderr}")

        time.sleep(0.1)
        retries -= 1

    if retries == 0:
        process.terminate()
        raise RuntimeError("Timed out waiting for Java Server socket.")

    # 5. Register Cleanup
    session = GatunSession(process, socket_path, mem_bytes)
    atexit.register(session.stop)

    return session
