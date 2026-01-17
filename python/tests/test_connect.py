"""Tests for connect() and aconnect() convenience functions."""

import pytest

from gatun import connect, aconnect, GatunClient
from gatun.async_client import AsyncGatunClient


class TestConnect:
    """Tests for the sync connect() function."""

    def test_connect_returns_client(self):
        """Test connect() returns a connected GatunClient."""
        client = connect()
        try:
            assert isinstance(client, GatunClient)
            assert client.shm is not None  # Connected
            # Verify we can use it
            arr = client.jvm.java.util.ArrayList()
            arr.add("test")
            assert arr.size() == 1
        finally:
            client.close()

    def test_connect_as_context_manager(self):
        """Test connect() works as a context manager."""
        with connect() as client:
            assert isinstance(client, GatunClient)
            arr = client.jvm.java.util.ArrayList()
            arr.add("hello")
            assert arr.size() == 1

    def test_connect_with_memory(self):
        """Test connect() accepts memory parameter."""
        with connect(memory="32MB") as client:
            arr = client.jvm.java.util.ArrayList()
            assert arr is not None

    def test_connect_client_has_session(self):
        """Test connected client has server session attached."""
        with connect() as client:
            assert hasattr(client, "_server_session")
            assert client._server_session is not None


class TestAconnect:
    """Tests for the async aconnect() function."""

    @pytest.mark.asyncio
    async def test_aconnect_returns_client(self):
        """Test aconnect() returns a connected AsyncGatunClient."""
        client = await aconnect()
        try:
            assert isinstance(client, AsyncGatunClient)
            assert client.shm is not None  # Connected
            # Verify we can use it
            arr = await client.jvm.java.util.ArrayList()
            await arr.add("test")
            size = await arr.size()
            assert size == 1
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_aconnect_as_context_manager(self):
        """Test aconnect() works as an async context manager."""
        async with await aconnect() as client:
            assert isinstance(client, AsyncGatunClient)
            arr = await client.jvm.java.util.ArrayList()
            await arr.add("hello")
            size = await arr.size()
            assert size == 1

    @pytest.mark.asyncio
    async def test_aconnect_with_memory(self):
        """Test aconnect() accepts memory parameter."""
        async with await aconnect(memory="32MB") as client:
            arr = await client.jvm.java.util.ArrayList()
            assert arr is not None

    @pytest.mark.asyncio
    async def test_aconnect_client_has_session(self):
        """Test async connected client has server session attached."""
        async with await aconnect() as client:
            assert hasattr(client, "_server_session")
            assert client._server_session is not None
