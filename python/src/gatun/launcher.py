import atexit
import logging
import os
import secrets
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from gatun.config import get_config

logger = logging.getLogger(__name__)

# --- Configuration ---
MODULE_DIR = Path(__file__).parent.resolve()
JAR_PATH = MODULE_DIR / "jars" / "gatun-server-all.jar"

# JVM Flags required for Apache Arrow & Netty (Java 21+)
DEFAULT_JVM_FLAGS = [
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

        # Clean up socket and shared memory files
        for path in [self.socket_path, f"{self.socket_path}.shm"]:
            try:
                if os.path.exists(path):
                    os.unlink(path)
            except OSError:
                pass  # Ignore cleanup errors


def launch_gateway(
    memory: str | None = None,
    socket_path: str | None = None,
    classpath: list[str] | None = None,
):
    """
    Launches the embedded Java server.

    Args:
        memory: Memory size (e.g., "512MB", "1GB"). Defaults to config value.
        socket_path: Path to the Unix socket. If not specified, generates a unique
                     path in the system temp directory to allow concurrent sessions.
        classpath: Additional JAR files or directories to add to the classpath.
                   This allows loading external classes (e.g., Spark JARs).

    Configuration can be set in pyproject.toml:
        [tool.gatun]
        memory = "64MB"
        socket_path = "/tmp/gatun.sock"  # Optional: fixed path for single session
        jvm_flags = ["-Xmx512m"]
    """
    config = get_config()

    if not JAR_PATH.exists():
        raise RuntimeError(f"Gatun JAR not found at {JAR_PATH}. Did you run 'uv sync'?")

    # 1. Apply config defaults, then parse memory
    if memory is None:
        memory = config.memory

    size_str = memory.upper()
    if size_str.endswith("GB"):
        mem_bytes = int(float(size_str[:-2]) * 1024 * 1024 * 1024)
    elif size_str.endswith("MB"):
        mem_bytes = int(float(size_str[:-2]) * 1024 * 1024)
    else:
        mem_bytes = int(size_str)  # Assume bytes

    # 2. Setup Paths - use random temp file if not specified
    if socket_path is None:
        socket_path = config.socket_path
    if socket_path is None:
        # Generate unique socket path to allow multiple concurrent sessions
        random_suffix = secrets.token_hex(8)
        socket_path = os.path.join(
            tempfile.gettempdir(), f"gatun_{os.getpid()}_{random_suffix}.sock"
        )

    # 3. Construct Command with config JVM flags
    jvm_flags = DEFAULT_JVM_FLAGS + config.jvm_flags

    if classpath:
        # Use -cp with main class to allow additional classpath entries
        # java [FLAGS] -cp [CLASSPATH] org.gatun.server.GatunServer [MEM_SIZE] [SOCKET_PATH]
        all_jars = [str(JAR_PATH)] + classpath
        cp = os.pathsep.join(all_jars)
        cmd = (
            ["java"]
            + jvm_flags
            + ["-cp", cp, "org.gatun.server.GatunServer", str(mem_bytes), socket_path]
        )
    else:
        # Use -jar for simplicity when no extra classpath needed
        # java [FLAGS] -jar [JAR] [MEM_SIZE] [SOCKET_PATH]
        cmd = ["java"] + jvm_flags + ["-jar", str(JAR_PATH), str(mem_bytes), socket_path]

    logger.info("Launching Java server: %s @ %s", memory, socket_path)

    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # 4. Wait for Socket to be ready (not just file existence, but connectable)
    max_retries = int(config.startup_timeout / 0.1)
    retries = max_retries
    while retries > 0:
        if process.poll() is not None:
            # Process died
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"Java Server failed to start:\nstdout: {stdout}\nstderr: {stderr}"
            )

        # Check if socket file exists AND is connectable
        if os.path.exists(socket_path):
            try:
                # Try to connect to verify server is ready
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(0.1)
                sock.connect(socket_path)
                sock.close()
                break  # Server is ready
            except (ConnectionRefusedError, OSError):
                pass  # Socket exists but server not ready yet

        time.sleep(0.1)
        retries -= 1

    if retries == 0:
        process.terminate()
        raise RuntimeError("Timed out waiting for Java Server socket.")

    # 5. Register Cleanup
    session = GatunSession(process, socket_path, mem_bytes)
    atexit.register(session.stop)

    return session
