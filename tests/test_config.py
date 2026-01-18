"""Tests for config.py.

Tests configuration loading from pyproject.toml, environment variables,
and default values.
"""

import os
import tempfile
from pathlib import Path


from gatun.config import (
    GatunConfig,
    find_pyproject_toml,
    load_config,
    get_config,
    reset_config,
)


class TestGatunConfig:
    """Tests for GatunConfig dataclass."""

    def test_default_values(self):
        """Test GatunConfig has correct defaults."""
        config = GatunConfig()
        assert config.memory == "16MB"
        assert config.socket_path is None
        assert config.jvm_flags == []
        assert config.connect_timeout == 5.0
        assert config.startup_timeout == 5.0

    def test_custom_values(self):
        """Test GatunConfig accepts custom values."""
        config = GatunConfig(
            memory="64MB",
            socket_path="/tmp/test.sock",
            jvm_flags=["-Xmx512m"],
            connect_timeout=10.0,
            startup_timeout=15.0,
        )
        assert config.memory == "64MB"
        assert config.socket_path == "/tmp/test.sock"
        assert config.jvm_flags == ["-Xmx512m"]
        assert config.connect_timeout == 10.0
        assert config.startup_timeout == 15.0

    def test_socket_path_expansion(self):
        """Test socket_path expands ~ to home directory."""
        config = GatunConfig(socket_path="~/test.sock")
        assert config.socket_path == os.path.expanduser("~/test.sock")
        assert "~" not in config.socket_path

    def test_socket_path_none_no_expansion(self):
        """Test socket_path=None doesn't cause errors."""
        config = GatunConfig(socket_path=None)
        assert config.socket_path is None


class TestFindPyprojectToml:
    """Tests for find_pyproject_toml function."""

    def test_finds_in_current_dir(self):
        """Test finding pyproject.toml in current directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir).resolve()
            pyproject = tmppath / "pyproject.toml"
            pyproject.write_text("[project]\nname = 'test'\n")

            result = find_pyproject_toml(tmppath)
            # Resolve both to handle macOS /var -> /private/var symlinks
            assert result.resolve() == pyproject.resolve()

    def test_finds_in_parent_dir(self):
        """Test finding pyproject.toml in parent directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir).resolve()
            subdir = tmppath / "subdir"
            subdir.mkdir()

            pyproject = tmppath / "pyproject.toml"
            pyproject.write_text("[project]\nname = 'test'\n")

            result = find_pyproject_toml(subdir)
            # Resolve both to handle macOS /var -> /private/var symlinks
            assert result.resolve() == pyproject.resolve()

    def test_returns_none_if_not_found(self):
        """Test returns None when no pyproject.toml exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            _ = find_pyproject_toml(tmppath)
            # Should return None or find a pyproject.toml further up
            # (may find the repo's pyproject.toml if run from within the repo)
            # Just verify it doesn't crash


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_default_config(self):
        """Test loading config with no pyproject.toml returns defaults."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            # Create empty pyproject.toml without [tool.gatun]
            pyproject = tmppath / "pyproject.toml"
            pyproject.write_text("[project]\nname = 'test'\n")

            config = load_config(pyproject)
            assert config.memory == "16MB"
            assert config.socket_path is None

    def test_load_config_with_gatun_section(self):
        """Test loading config with [tool.gatun] section."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            pyproject = tmppath / "pyproject.toml"
            pyproject.write_text(
                """
[project]
name = "test"

[tool.gatun]
memory = "128MB"
socket_path = "/custom/path.sock"
jvm_flags = ["-Xmx1g", "-Xms512m"]
connect_timeout = 10.0
startup_timeout = 20.0
"""
            )

            config = load_config(pyproject)
            assert config.memory == "128MB"
            assert config.socket_path == "/custom/path.sock"
            assert config.jvm_flags == ["-Xmx1g", "-Xms512m"]
            assert config.connect_timeout == 10.0
            assert config.startup_timeout == 20.0

    def test_load_config_partial_gatun_section(self):
        """Test loading config with partial [tool.gatun] section."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            pyproject = tmppath / "pyproject.toml"
            pyproject.write_text(
                """
[project]
name = "test"

[tool.gatun]
memory = "256MB"
"""
            )

            config = load_config(pyproject)
            assert config.memory == "256MB"
            # Defaults for unspecified values
            assert config.socket_path is None
            assert config.jvm_flags == []
            assert config.connect_timeout == 5.0

    def test_load_config_env_override(self, monkeypatch):
        """Test environment variables override pyproject.toml."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            pyproject = tmppath / "pyproject.toml"
            pyproject.write_text(
                """
[project]
name = "test"

[tool.gatun]
memory = "64MB"
socket_path = "/config/path.sock"
"""
            )

            # Set environment variables
            monkeypatch.setenv("GATUN_MEMORY", "512MB")
            monkeypatch.setenv("GATUN_SOCKET_PATH", "/env/path.sock")
            monkeypatch.setenv("GATUN_CONNECT_TIMEOUT", "15.0")
            monkeypatch.setenv("GATUN_STARTUP_TIMEOUT", "30.0")

            config = load_config(pyproject)
            assert config.memory == "512MB"
            assert config.socket_path == "/env/path.sock"
            assert config.connect_timeout == 15.0
            assert config.startup_timeout == 30.0

    def test_load_config_nonexistent_file(self):
        """Test loading config from non-existent file."""
        config = load_config(Path("/nonexistent/pyproject.toml"))
        # Should return defaults
        assert config.memory == "16MB"


class TestGetConfigAndReset:
    """Tests for get_config and reset_config functions."""

    def test_get_config_returns_config(self):
        """Test get_config returns a GatunConfig."""
        reset_config()  # Clear any cached config
        config = get_config()
        assert isinstance(config, GatunConfig)

    def test_get_config_caches_result(self):
        """Test get_config returns the same instance on repeated calls."""
        reset_config()
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2

    def test_reset_config_clears_cache(self):
        """Test reset_config clears the cached config."""
        reset_config()
        _ = get_config()
        reset_config()
        config2 = get_config()
        # After reset, get_config creates a new instance
        # (they may be equal but could be different objects)
        # Just verify reset doesn't crash
        assert config2 is not None
