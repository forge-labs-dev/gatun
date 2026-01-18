"""Tests for bridge.py and bridge_adapters.py.

These tests cover the BridgeAdapter protocol and GatunAdapter implementation.
"""

import pytest

from gatun.bridge import (
    JavaException,
    JavaSecurityException,
    JavaIllegalArgumentException,
    JavaNoSuchMethodException,
    JavaNoSuchFieldException,
    JavaClassNotFoundException,
    JavaNullPointerException,
    JavaIndexOutOfBoundsException,
    JavaNumberFormatException,
    JVMRef,
    JAVA_EXCEPTION_MAP,
)
from gatun.bridge_adapters import GatunAdapter


@pytest.fixture(scope="module")
def bridge():
    """Create a GatunAdapter for the test module."""
    adapter = GatunAdapter(memory="16MB")
    yield adapter
    adapter.close()


class TestJavaExceptions:
    """Tests for Java exception classes in bridge.py."""

    def test_java_exception_basic(self):
        """Test JavaException creation and attributes."""
        exc = JavaException("java.lang.RuntimeException", "Something went wrong")
        assert exc.java_class == "java.lang.RuntimeException"
        assert exc.message == "Something went wrong"
        assert exc.stack_trace == ""
        assert "RuntimeException" in str(exc)
        assert "Something went wrong" in str(exc)

    def test_java_exception_with_stack_trace(self):
        """Test JavaException with stack trace."""
        stack = "at com.example.Foo.bar(Foo.java:42)"
        exc = JavaException("java.lang.Exception", "Error", stack)
        assert exc.stack_trace == stack

    def test_java_security_exception(self):
        """Test JavaSecurityException is a JavaException."""
        exc = JavaSecurityException("java.lang.SecurityException", "Access denied")
        assert isinstance(exc, JavaException)
        assert exc.java_class == "java.lang.SecurityException"

    def test_java_illegal_argument_exception(self):
        """Test JavaIllegalArgumentException."""
        exc = JavaIllegalArgumentException(
            "java.lang.IllegalArgumentException", "Invalid arg"
        )
        assert isinstance(exc, JavaException)

    def test_java_no_such_method_exception(self):
        """Test JavaNoSuchMethodException."""
        exc = JavaNoSuchMethodException(
            "java.lang.NoSuchMethodException", "Method not found"
        )
        assert isinstance(exc, JavaException)

    def test_java_no_such_field_exception(self):
        """Test JavaNoSuchFieldException."""
        exc = JavaNoSuchFieldException(
            "java.lang.NoSuchFieldException", "Field not found"
        )
        assert isinstance(exc, JavaException)

    def test_java_class_not_found_exception(self):
        """Test JavaClassNotFoundException."""
        exc = JavaClassNotFoundException(
            "java.lang.ClassNotFoundException", "Class not found"
        )
        assert isinstance(exc, JavaException)

    def test_java_null_pointer_exception(self):
        """Test JavaNullPointerException."""
        exc = JavaNullPointerException("java.lang.NullPointerException", "Null value")
        assert isinstance(exc, JavaException)

    def test_java_index_out_of_bounds_exception(self):
        """Test JavaIndexOutOfBoundsException."""
        exc = JavaIndexOutOfBoundsException(
            "java.lang.IndexOutOfBoundsException", "Index 5"
        )
        assert isinstance(exc, JavaException)

    def test_java_number_format_exception(self):
        """Test JavaNumberFormatException."""
        exc = JavaNumberFormatException(
            "java.lang.NumberFormatException", "Invalid number"
        )
        assert isinstance(exc, JavaException)

    def test_exception_map_contains_all_types(self):
        """Test JAVA_EXCEPTION_MAP has expected mappings."""
        assert "java.lang.SecurityException" in JAVA_EXCEPTION_MAP
        assert "java.lang.IllegalArgumentException" in JAVA_EXCEPTION_MAP
        assert "java.lang.NoSuchMethodException" in JAVA_EXCEPTION_MAP
        assert "java.lang.NoSuchFieldException" in JAVA_EXCEPTION_MAP
        assert "java.lang.ClassNotFoundException" in JAVA_EXCEPTION_MAP
        assert "java.lang.NullPointerException" in JAVA_EXCEPTION_MAP
        assert "java.lang.IndexOutOfBoundsException" in JAVA_EXCEPTION_MAP
        assert "java.lang.ArrayIndexOutOfBoundsException" in JAVA_EXCEPTION_MAP
        assert "java.lang.StringIndexOutOfBoundsException" in JAVA_EXCEPTION_MAP
        assert "java.lang.NumberFormatException" in JAVA_EXCEPTION_MAP

    def test_exception_map_values_are_correct_types(self):
        """Test JAVA_EXCEPTION_MAP values are correct exception classes."""
        assert (
            JAVA_EXCEPTION_MAP["java.lang.SecurityException"] is JavaSecurityException
        )
        assert (
            JAVA_EXCEPTION_MAP["java.lang.IllegalArgumentException"]
            is JavaIllegalArgumentException
        )
        assert (
            JAVA_EXCEPTION_MAP["java.lang.NumberFormatException"]
            is JavaNumberFormatException
        )


class TestJVMRefProtocol:
    """Tests for JVMRef protocol."""

    def test_jvm_ref_protocol_check(self):
        """Test that JVMRef is a runtime checkable protocol."""

        class MockRef:
            @property
            def object_id(self) -> int:
                return 42

        ref = MockRef()
        assert isinstance(ref, JVMRef)


class TestGatunAdapterObjectLifecycle:
    """Tests for GatunAdapter object lifecycle methods."""

    def test_new_creates_object(self, bridge):
        """Test new() creates a JVM object."""
        obj = bridge.new("java.util.ArrayList")
        assert obj is not None
        assert hasattr(obj, "object_id")

    def test_new_with_args(self, bridge):
        """Test new() with constructor arguments."""
        obj = bridge.new("java.util.ArrayList", 100)
        assert obj is not None

    def test_new_invalid_class_raises(self, bridge):
        """Test new() with invalid class raises exception."""
        with pytest.raises(JavaException):
            bridge.new("com.invalid.NonexistentClass")

    def test_detach_prevents_cleanup(self, bridge):
        """Test detach() marks object to prevent auto-cleanup."""
        obj = bridge.new("java.util.ArrayList")
        bridge.detach(obj)
        # Should not raise - object is detached


class TestGatunAdapterMethodCalls:
    """Tests for GatunAdapter method call methods."""

    def test_call_instance_method(self, bridge):
        """Test call() invokes instance method."""
        arr = bridge.new("java.util.ArrayList")
        result = bridge.call(arr, "add", "hello")
        assert result is True
        size = bridge.call(arr, "size")
        assert size == 1

    def test_call_static_method(self, bridge):
        """Test call_static() invokes static method."""
        result = bridge.call_static("java.lang.Integer", "parseInt", "42")
        assert result == 42

    def test_call_static_method_math(self, bridge):
        """Test call_static() with Math methods."""
        result = bridge.call_static("java.lang.Math", "max", 10, 20)
        assert result == 20

    def test_call_invalid_method_raises(self, bridge):
        """Test call() with invalid method raises exception."""
        arr = bridge.new("java.util.ArrayList")
        with pytest.raises(JavaException):
            bridge.call(arr, "nonexistentMethod")


class TestGatunAdapterFieldAccess:
    """Tests for GatunAdapter field access methods."""

    def test_get_static_field(self, bridge):
        """Test get_static_field() reads static field."""
        max_value = bridge.get_static_field("java.lang.Integer", "MAX_VALUE")
        assert max_value == 2147483647

    def test_get_static_field_min_value(self, bridge):
        """Test get_static_field() with MIN_VALUE."""
        min_value = bridge.get_static_field("java.lang.Integer", "MIN_VALUE")
        assert min_value == -2147483648


class TestGatunAdapterTypeChecking:
    """Tests for GatunAdapter type checking methods."""

    def test_is_instance_of_same_class(self, bridge):
        """Test is_instance_of() with same class."""
        arr = bridge.new("java.util.ArrayList")
        assert bridge.is_instance_of(arr, "java.util.ArrayList") is True

    def test_is_instance_of_interface(self, bridge):
        """Test is_instance_of() with interface."""
        arr = bridge.new("java.util.ArrayList")
        assert bridge.is_instance_of(arr, "java.util.List") is True
        assert bridge.is_instance_of(arr, "java.util.Collection") is True

    def test_is_instance_of_unrelated_class(self, bridge):
        """Test is_instance_of() with unrelated class."""
        arr = bridge.new("java.util.ArrayList")
        assert bridge.is_instance_of(arr, "java.util.HashMap") is False


class TestGatunAdapterArrays:
    """Tests for GatunAdapter array methods."""

    def test_new_array_object_type(self, bridge):
        """Test new_array() with object type."""
        arr = bridge.new_array("java.lang.String", 3)
        assert arr is not None
        length = bridge.array_length(arr)
        assert length == 3

    def test_new_array_primitive_int(self, bridge):
        """Test new_array() with primitive int."""
        arr = bridge.new_array("int", 5)
        assert arr is not None
        length = bridge.array_length(arr)
        assert length == 5

    def test_new_array_primitive_double(self, bridge):
        """Test new_array() with primitive double."""
        arr = bridge.new_array("double", 3)
        length = bridge.array_length(arr)
        assert length == 3

    def test_new_array_primitive_boolean(self, bridge):
        """Test new_array() with primitive boolean."""
        arr = bridge.new_array("boolean", 2)
        length = bridge.array_length(arr)
        assert length == 2

    def test_array_set_and_get(self, bridge):
        """Test array_set() and array_get()."""
        arr = bridge.new_array("java.lang.String", 3)
        bridge.array_set(arr, 0, "hello")
        bridge.array_set(arr, 1, "world")
        bridge.array_set(arr, 2, "!")

        assert bridge.array_get(arr, 0) == "hello"
        assert bridge.array_get(arr, 1) == "world"
        assert bridge.array_get(arr, 2) == "!"

    def test_array_get_out_of_bounds_raises(self, bridge):
        """Test array_get() with out of bounds index raises."""
        arr = bridge.new_array("java.lang.String", 2)
        with pytest.raises(JavaException):
            bridge.array_get(arr, 10)


class TestGatunAdapterJVMView:
    """Tests for GatunAdapter JVM view."""

    def test_jvm_property_exists(self, bridge):
        """Test jvm property returns a view."""
        jvm = bridge.jvm
        assert jvm is not None

    def test_jvm_create_object(self, bridge):
        """Test creating object via JVM view."""
        arr = bridge.jvm.java.util.ArrayList()
        assert arr is not None

    def test_jvm_static_field(self, bridge):
        """Test accessing static field via JVM view."""
        max_val = bridge.jvm.java.lang.Integer.MAX_VALUE
        assert max_val == 2147483647

    def test_jvm_static_method(self, bridge):
        """Test calling static method via JVM view."""
        result = bridge.jvm.java.lang.Integer.parseInt("123")
        assert result == 123

    def test_jvm_navigation(self, bridge):
        """Test navigating package hierarchy."""
        java = bridge.jvm.java
        util = java.util
        ArrayList = util.ArrayList
        arr = ArrayList()
        assert arr is not None

    def test_java_import(self, bridge):
        """Test java_import() for shorter paths."""
        bridge.java_import("java.util.*")
        # After import, can use shorter path
        arr = bridge.jvm.ArrayList()
        assert arr is not None


class TestGatunAdapterExceptionConversion:
    """Tests for exception conversion in GatunAdapter."""

    def test_number_format_exception_converted(self, bridge):
        """Test NumberFormatException is properly converted."""
        with pytest.raises(JavaNumberFormatException):
            bridge.call_static("java.lang.Integer", "parseInt", "not_a_number")

    def test_security_exception_converted(self, bridge):
        """Test SecurityException is properly converted."""
        with pytest.raises(JavaSecurityException):
            # Try to create a disallowed class
            bridge.new("java.lang.Runtime")


class TestGatunAdapterClose:
    """Tests for GatunAdapter close behavior."""

    def test_close_releases_resources(self):
        """Test close() releases resources."""
        adapter = GatunAdapter(memory="16MB")
        arr = adapter.new("java.util.ArrayList")
        assert arr is not None
        adapter.close()
        # Client should be None after close
        assert adapter._client is None

    def test_close_idempotent(self):
        """Test close() can be called multiple times."""
        adapter = GatunAdapter(memory="16MB")
        adapter.close()
        adapter.close()  # Should not raise
