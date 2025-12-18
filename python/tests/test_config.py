"""Tests for the configuration system."""

import os
import tempfile
from pathlib import Path

import pytest

from gatun.config import (
    GatunConfig,
    find_pyproject_toml,
    load_config,
    reset_config,
)


@pytest.fixture(autouse=True)
def reset_global_config():
    """Reset global config before and after each test."""
    reset_config()
    yield
    reset_config()


def test_default_config():
    """Test default configuration values."""
    config = GatunConfig()
    assert config.memory == "16MB"
    assert config.socket_path is None
    assert config.jvm_flags == []
    assert config.connect_timeout == 5.0
    assert config.startup_timeout == 5.0


def test_config_expands_home():
    """Test that socket_path expands ~."""
    config = GatunConfig(socket_path="~/test.sock")
    assert config.socket_path == os.path.expanduser("~/test.sock")
    assert "~" not in config.socket_path


def test_find_pyproject_toml_in_current_dir():
    """Test finding pyproject.toml in current directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pyproject = Path(tmpdir).resolve() / "pyproject.toml"
        pyproject.write_text("[project]\nname = 'test'\n")

        found = find_pyproject_toml(Path(tmpdir).resolve())
        assert found == pyproject


def test_find_pyproject_toml_in_parent():
    """Test finding pyproject.toml in parent directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pyproject = Path(tmpdir).resolve() / "pyproject.toml"
        pyproject.write_text("[project]\nname = 'test'\n")

        subdir = Path(tmpdir).resolve() / "src" / "package"
        subdir.mkdir(parents=True)

        found = find_pyproject_toml(subdir)
        assert found == pyproject


def test_find_pyproject_toml_not_found():
    """Test when pyproject.toml doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        found = find_pyproject_toml(Path(tmpdir))
        assert found is None


def test_load_config_from_pyproject():
    """Test loading config from pyproject.toml."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pyproject = Path(tmpdir) / "pyproject.toml"
        pyproject.write_text("""
[tool.gatun]
memory = "64MB"
socket_path = "/tmp/test.sock"
jvm_flags = ["-Xmx512m", "-Xms256m"]
connect_timeout = 10.0
startup_timeout = 15.0
""")

        config = load_config(pyproject)
        assert config.memory == "64MB"
        assert config.socket_path == "/tmp/test.sock"
        assert config.jvm_flags == ["-Xmx512m", "-Xms256m"]
        assert config.connect_timeout == 10.0
        assert config.startup_timeout == 15.0


def test_load_config_partial_settings():
    """Test loading config with only some settings."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pyproject = Path(tmpdir) / "pyproject.toml"
        pyproject.write_text("""
[tool.gatun]
memory = "32MB"
""")

        config = load_config(pyproject)
        assert config.memory == "32MB"
        # Defaults should be preserved
        assert config.socket_path is None
        assert config.jvm_flags == []
        assert config.connect_timeout == 5.0


def test_load_config_no_gatun_section():
    """Test loading config when [tool.gatun] section is missing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pyproject = Path(tmpdir) / "pyproject.toml"
        pyproject.write_text("""
[project]
name = "test"
""")

        config = load_config(pyproject)
        # Should use all defaults
        assert config.memory == "16MB"
        assert config.socket_path is None


def test_load_config_env_override(monkeypatch):
    """Test that environment variables override pyproject.toml."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pyproject = Path(tmpdir) / "pyproject.toml"
        pyproject.write_text("""
[tool.gatun]
memory = "64MB"
socket_path = "/tmp/from_config.sock"
""")

        monkeypatch.setenv("GATUN_MEMORY", "128MB")
        monkeypatch.setenv("GATUN_SOCKET_PATH", "/tmp/from_env.sock")

        config = load_config(pyproject)
        assert config.memory == "128MB"
        assert config.socket_path == "/tmp/from_env.sock"


def test_load_config_env_only(monkeypatch):
    """Test loading config from environment only."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # No pyproject.toml

        monkeypatch.setenv("GATUN_MEMORY", "256MB")
        monkeypatch.setenv("GATUN_CONNECT_TIMEOUT", "20.0")

        config = load_config(Path(tmpdir) / "nonexistent.toml")
        assert config.memory == "256MB"
        assert config.connect_timeout == 20.0


def test_config_jvm_flags_list():
    """Test that JVM flags are properly handled as a list."""
    config = GatunConfig(jvm_flags=["-Xmx1g", "-XX:+UseG1GC"])
    assert len(config.jvm_flags) == 2
    assert "-Xmx1g" in config.jvm_flags
    assert "-XX:+UseG1GC" in config.jvm_flags


def test_load_config_nonexistent_file():
    """Test loading config when file doesn't exist uses defaults."""
    config = load_config(Path("/nonexistent/path/pyproject.toml"))
    assert config.memory == "16MB"
    assert config.socket_path is None
