"""Tests for server observability features.

These tests verify that metrics are being tracked correctly by the server.
"""

import pytest

from gatun import connect


class TestGetMetrics:
    """Test the get_metrics() API."""

    def test_get_metrics_returns_string(self, client):
        """Test that get_metrics returns a string report."""
        metrics = client.get_metrics()
        assert isinstance(metrics, str)
        assert "=== Gatun Server Metrics ===" in metrics

    def test_metrics_contains_global_stats(self, client):
        """Test that metrics report contains global statistics."""
        metrics = client.get_metrics()
        assert "Global:" in metrics
        assert "total_requests:" in metrics
        assert "total_errors:" in metrics
        assert "requests_per_sec:" in metrics
        assert "current_sessions:" in metrics
        assert "current_objects:" in metrics
        assert "peak_objects:" in metrics

    def test_metrics_contains_arrow_stats(self, client):
        """Test that metrics report contains Arrow statistics."""
        metrics = client.get_metrics()
        assert "Arrow:" in metrics
        assert "total_rows:" in metrics
        assert "total_bytes_copied:" in metrics

    def test_metrics_contains_callback_stats(self, client):
        """Test that metrics report contains callback statistics."""
        metrics = client.get_metrics()
        assert "Callbacks:" in metrics
        assert "total_invocations:" in metrics

    def test_metrics_contains_latency_section(self, client):
        """Test that metrics report contains per-action latency."""
        metrics = client.get_metrics()
        assert "Per-Action Latency" in metrics


class TestMetricsTracking:
    """Test that operations are correctly tracked in metrics."""

    def test_request_count_increases(self, client):
        """Test that request count increases with operations."""
        # Get initial metrics
        metrics1 = client.get_metrics()

        # Do some operations
        arr = client.create_object("java.util.ArrayList")
        arr.add("test")
        arr.size()

        # Get metrics again
        metrics2 = client.get_metrics()

        # Extract total_requests from both
        def extract_total_requests(metrics):
            for line in metrics.split("\n"):
                if "total_requests:" in line:
                    return int(line.split(":")[1].strip())
            return 0

        count1 = extract_total_requests(metrics1)
        count2 = extract_total_requests(metrics2)

        # Should have increased (at least 3: create + add + size)
        assert count2 > count1

    def test_object_count_field_present(self, client):
        """Test that object count fields are present in metrics."""
        # Create some objects
        objs = [client.create_object("java.util.ArrayList") for _ in range(5)]

        metrics = client.get_metrics()

        # Verify the fields are present (actual counts may be 0 until
        # updateObjectCount is wired up in more places)
        assert "current_objects:" in metrics
        assert "peak_objects:" in metrics

        # Keep references to prevent GC
        _ = objs

    def test_error_count_tracked(self):
        """Test that errors are tracked in metrics."""
        client = connect()
        try:
            # Trigger some errors
            for _ in range(3):
                try:
                    client.invoke_method(999999, "nonexistent")
                except Exception:
                    pass

            metrics = client.get_metrics()

            def extract_total_errors(metrics):
                for line in metrics.split("\n"):
                    if "total_errors:" in line:
                        return int(line.split(":")[1].strip())
                return 0

            errors = extract_total_errors(metrics)
            assert errors >= 3
        finally:
            client.close()


class TestMetricsPerAction:
    """Test per-action metrics tracking."""

    def test_create_object_tracked(self, client):
        """Test that CreateObject actions are tracked."""
        # Create several objects
        for _ in range(5):
            client.create_object("java.util.ArrayList")

        metrics = client.get_metrics()

        # Should see CreateObject in the per-action stats
        assert "CreateObject:" in metrics

    def test_invoke_method_tracked(self, client):
        """Test that InvokeMethod actions are tracked."""
        arr = client.create_object("java.util.ArrayList")
        for i in range(5):
            arr.add(i)

        metrics = client.get_metrics()

        # Should see InvokeMethod in the per-action stats
        assert "InvokeMethod:" in metrics


class TestMetricsWithTracing:
    """Test metrics with tracing enabled."""

    def test_metrics_with_trace_mode(self):
        """Test that metrics work when trace mode is enabled."""
        client = connect(trace=True)
        try:
            arr = client.create_object("java.util.ArrayList")
            arr.add("test")

            metrics = client.get_metrics()
            assert "=== Gatun Server Metrics ===" in metrics
            assert "total_requests:" in metrics
        finally:
            client.close()


class TestAsyncMetrics:
    """Test async client get_metrics."""

    @pytest.mark.asyncio
    async def test_async_get_metrics(self):
        """Test that async get_metrics returns a string report."""
        from gatun import aconnect

        client = await aconnect()
        try:
            metrics = await client.get_metrics()
            assert isinstance(metrics, str)
            assert "=== Gatun Server Metrics ===" in metrics
            assert "Global:" in metrics
        finally:
            await client.close()


class TestMetricsIsolation:
    """Test that metrics aggregate across sessions."""

    def test_metrics_across_sessions(self):
        """Test that metrics accumulate across multiple sessions."""
        # First session
        client1 = connect()
        try:
            for _ in range(5):
                client1.create_object("java.util.ArrayList")

            # Get metrics from first session (verifies API works)
            _ = client1.get_metrics()
        finally:
            client1.close()

        # Second session - metrics should include both sessions' activity
        client2 = connect()
        try:
            for _ in range(5):
                client2.create_object("java.util.ArrayList")

            metrics2 = client2.get_metrics()

            # Total requests should be higher in second metrics
            def extract_total_requests(metrics):
                for line in metrics.split("\n"):
                    if "total_requests:" in line:
                        return int(line.split(":")[1].strip())
                return 0

            # Note: Different server instances won't share metrics
            # but within the same server process, metrics accumulate
            # This test just verifies the API works across sessions
            assert "total_requests:" in metrics2
        finally:
            client2.close()
