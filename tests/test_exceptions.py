"""Tests for Java exception handling."""

import pytest

from gatun import (
    JavaException,
    JavaSecurityException,
    JavaNoSuchFieldException,
    JavaNumberFormatException,
    JavaIndexOutOfBoundsException,
)


# Uses the shared `client` fixture from conftest.py


class TestExceptionTypes:
    """Test that Java exceptions are raised as the correct Python exception type."""

    def test_number_format_exception(self, client):
        """Test NumberFormatException when parsing invalid number."""
        with pytest.raises(JavaNumberFormatException) as excinfo:
            client.invoke_static_method("java.lang.Integer", "parseInt", "not_a_number")

        exc = excinfo.value
        assert exc.java_class == "java.lang.NumberFormatException"
        assert "not_a_number" in exc.message
        assert "NumberFormatException" in exc.stack_trace

    def test_security_exception_blocked_class(self, client):
        """Test SecurityException when accessing blocked class."""
        with pytest.raises(JavaSecurityException) as excinfo:
            client.create_object("java.lang.Runtime")

        exc = excinfo.value
        assert exc.java_class == "java.lang.SecurityException"
        assert "not allowed" in exc.message.lower()

    def test_security_exception_static_method_blocked(self, client):
        """Test SecurityException when calling static method on blocked class."""
        with pytest.raises(JavaSecurityException) as excinfo:
            client.invoke_static_method("java.lang.Runtime", "getRuntime")

        exc = excinfo.value
        assert exc.java_class == "java.lang.SecurityException"

    def test_no_such_field_exception(self, client):
        """Test NoSuchFieldException when accessing non-existent field."""
        arr = client.create_object("java.util.ArrayList")
        with pytest.raises(JavaNoSuchFieldException) as excinfo:
            client.get_field(arr, "nonExistentField")

        exc = excinfo.value
        assert exc.java_class == "java.lang.NoSuchFieldException"
        assert "nonExistentField" in exc.message

    def test_index_out_of_bounds_exception(self, client):
        """Test IndexOutOfBoundsException when accessing invalid index."""
        arr = client.create_object("java.util.ArrayList")
        with pytest.raises(JavaIndexOutOfBoundsException) as excinfo:
            arr.get(0)  # Empty list, index 0 is out of bounds

        exc = excinfo.value
        assert "IndexOutOfBoundsException" in exc.java_class


class TestExceptionAttributes:
    """Test that exception attributes are properly populated."""

    def test_exception_has_java_class(self, client):
        """Test that java_class attribute is set."""
        with pytest.raises(JavaException) as excinfo:
            client.invoke_static_method("java.lang.Integer", "parseInt", "bad")

        assert excinfo.value.java_class is not None
        assert "Exception" in excinfo.value.java_class

    def test_exception_has_message(self, client):
        """Test that message attribute is set."""
        with pytest.raises(JavaException) as excinfo:
            client.invoke_static_method("java.lang.Integer", "parseInt", "xyz")

        assert excinfo.value.message is not None
        assert len(excinfo.value.message) > 0

    def test_exception_has_stack_trace(self, client):
        """Test that stack_trace attribute contains full trace."""
        with pytest.raises(JavaException) as excinfo:
            client.invoke_static_method("java.lang.Integer", "parseInt", "abc")

        trace = excinfo.value.stack_trace
        assert trace is not None
        # Stack trace should contain 'at ' markers
        assert "\tat " in trace or "at " in trace


class TestExceptionHierarchy:
    """Test exception inheritance hierarchy."""

    def test_all_exceptions_inherit_from_java_exception(self, client):
        """Test that specific exceptions inherit from JavaException."""
        with pytest.raises(JavaException):
            client.invoke_static_method("java.lang.Integer", "parseInt", "x")

    def test_security_exception_is_java_exception(self, client):
        """Test JavaSecurityException is also a JavaException."""
        with pytest.raises(JavaException):
            client.create_object("java.lang.Runtime")

    def test_can_catch_specific_or_generic(self, client):
        """Test that both specific and generic catches work."""
        # Catch specific
        caught_specific = False
        try:
            client.invoke_static_method("java.lang.Integer", "parseInt", "y")
        except JavaNumberFormatException:
            caught_specific = True
        assert caught_specific

        # Catch generic
        caught_generic = False
        try:
            client.invoke_static_method("java.lang.Integer", "parseInt", "z")
        except JavaException:
            caught_generic = True
        assert caught_generic


class TestExceptionStr:
    """Test exception string representation."""

    def test_str_contains_stack_trace(self, client):
        """Test that str(exception) contains the stack trace."""
        with pytest.raises(JavaException) as excinfo:
            client.invoke_static_method("java.lang.Integer", "parseInt", "bad")

        exc_str = str(excinfo.value)
        # Should contain the exception class name
        assert "NumberFormatException" in exc_str
        # Should contain stack trace
        assert "\tat " in exc_str or "at " in exc_str
