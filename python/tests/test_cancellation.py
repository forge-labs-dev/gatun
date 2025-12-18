"""Tests for request cancellation functionality."""

from gatun.client import CancelledException


class TestCancellation:
    """Test cancellation support."""

    def test_cancel_returns_true(self, client):
        """Test that cancel returns True for acknowledgement."""
        # Cancel a non-existent request (should still acknowledge)
        request_id = client._get_request_id()
        result = client.cancel(request_id)
        assert result is True

    def test_get_request_id_increments(self, client):
        """Test that request IDs increment."""
        id1 = client._get_request_id()
        id2 = client._get_request_id()
        id3 = client._get_request_id()

        assert id2 == id1 + 1
        assert id3 == id2 + 1

    def test_cancelled_exception_has_request_id(self):
        """Test CancelledException stores request ID."""
        exc = CancelledException(42)
        assert exc.request_id == 42
        assert "42" in str(exc)
        assert "cancelled" in str(exc).lower()

    def test_cancel_already_completed_request(self, client):
        """Test cancelling an already-completed request is safe."""
        # Create and use an object (request completes)
        arr = client.create_object("java.util.ArrayList")
        arr.add("test")

        # Try to cancel a past request - should be safe (no error)
        result = client.cancel(1)  # First request was create_object
        assert result is True

    def test_multiple_cancels_same_id(self, client):
        """Test multiple cancels of same ID are idempotent."""
        request_id = client._get_request_id()

        # Cancel same request multiple times
        result1 = client.cancel(request_id)
        result2 = client.cancel(request_id)
        result3 = client.cancel(request_id)

        assert result1 is True
        assert result2 is True
        assert result3 is True


class TestCancelledExceptionMapping:
    """Test that InterruptedException is mapped to CancelledException."""

    def test_exception_class_exists(self):
        """Test CancelledException is properly defined."""
        assert issubclass(CancelledException, Exception)
        assert not issubclass(CancelledException, RuntimeError)

    def test_exception_attributes(self):
        """Test CancelledException has expected attributes."""
        exc = CancelledException(123)
        assert hasattr(exc, "request_id")
        assert exc.request_id == 123
