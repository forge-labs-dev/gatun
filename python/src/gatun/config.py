"""Configuration system for Gatun.

Reads configuration from pyproject.toml under the [tool.gatun] section.

Example pyproject.toml:
    [tool.gatun]
    memory = "64MB"
    socket_path = "/tmp/gatun.sock"
    jvm_flags = ["-Xmx512m"]

Configuration precedence (highest to lowest):
    1. Function arguments (e.g., connect(memory="32MB"))
    2. Environment variables (e.g., GATUN_MEMORY=32MB)
    3. pyproject.toml [tool.gatun] section
    4. Built-in defaults
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

import tomllib


@dataclass
class GatunConfig:
    """Configuration for Gatun client and server."""

    # Server settings
    memory: str = "16MB"
    socket_path: str | None = None

    # JVM settings
    jvm_flags: list[str] = field(default_factory=list)

    # Timeouts
    connect_timeout: float = 5.0  # seconds
    startup_timeout: float = 5.0  # seconds

    def __post_init__(self) -> None:
        # Expand ~ in socket_path
        if self.socket_path:
            self.socket_path = os.path.expanduser(self.socket_path)


def find_pyproject_toml(start_path: Path | None = None) -> Path | None:
    """Find pyproject.toml by searching up from start_path.

    Args:
        start_path: Directory to start searching from. Defaults to cwd.

    Returns:
        Path to pyproject.toml if found, None otherwise.
    """
    if start_path is None:
        start_path = Path.cwd()

    current = start_path.resolve()
    while current != current.parent:
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            return pyproject
        current = current.parent

    # Check root as well
    pyproject = current / "pyproject.toml"
    if pyproject.exists():
        return pyproject

    return None


def load_config(pyproject_path: Path | None = None) -> GatunConfig:
    """Load configuration from pyproject.toml.

    Args:
        pyproject_path: Explicit path to pyproject.toml. If None, searches
                       up from current directory.

    Returns:
        GatunConfig with values from pyproject.toml merged with defaults.
    """
    config = GatunConfig()

    # 1. Load from pyproject.toml
    if pyproject_path is None:
        pyproject_path = find_pyproject_toml()

    if pyproject_path and pyproject_path.exists():
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)

        gatun_config = data.get("tool", {}).get("gatun", {})

        if "memory" in gatun_config:
            config.memory = gatun_config["memory"]
        if "socket_path" in gatun_config:
            config.socket_path = gatun_config["socket_path"]
        if "jvm_flags" in gatun_config:
            config.jvm_flags = gatun_config["jvm_flags"]
        if "connect_timeout" in gatun_config:
            config.connect_timeout = gatun_config["connect_timeout"]
        if "startup_timeout" in gatun_config:
            config.startup_timeout = gatun_config["startup_timeout"]

    # 2. Override with environment variables
    if env_memory := os.environ.get("GATUN_MEMORY"):
        config.memory = env_memory
    if env_socket := os.environ.get("GATUN_SOCKET_PATH"):
        config.socket_path = env_socket
    if env_timeout := os.environ.get("GATUN_CONNECT_TIMEOUT"):
        config.connect_timeout = float(env_timeout)
    if env_startup := os.environ.get("GATUN_STARTUP_TIMEOUT"):
        config.startup_timeout = float(env_startup)

    # Reprocess paths after env override
    config.__post_init__()

    return config


# Global config instance, lazily loaded
_config: GatunConfig | None = None


def get_config() -> GatunConfig:
    """Get the global configuration, loading it if necessary."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Reset the global configuration. Useful for testing."""
    global _config
    _config = None
