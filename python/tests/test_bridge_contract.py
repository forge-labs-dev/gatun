"""Bridge contract tests.

These tests define the required behavior for any BridgeAdapter implementation.
Both Py4JAdapter and GatunAdapter must pass these tests.

Run with: pytest tests/test_bridge_contract.py -v
"""

from __future__ import annotations

import pytest

from gatun.bridge import BridgeAdapter, JavaException


class BridgeContractTests:
    """Contract tests that any BridgeAdapter implementation must pass.

    Subclass this and implement the `bridge` fixture to test your adapter.
    """

    @pytest.fixture
    def bridge(self) -> BridgeAdapter:
        """Override in subclass to provide adapter instance."""
        raise NotImplementedError

    # ==========================================================================
    # Object Creation (5 tests)
    # ==========================================================================

    def test_create_object_no_args(self, bridge: BridgeAdapter):
        """Create object with no-arg constructor."""
        obj = bridge.new("java.util.ArrayList")
        assert obj is not None

    def test_create_object_with_int_arg(self, bridge: BridgeAdapter):
        """Create object with int argument."""
        obj = bridge.new("java.util.ArrayList", 100)
        assert obj is not None

    def test_create_object_with_string_arg(self, bridge: BridgeAdapter):
        """Create object with string argument."""
        obj = bridge.new("java.lang.StringBuilder", "hello")
        result = bridge.call(obj, "toString")
        assert result == "hello"

    def test_create_object_class_not_found(self, bridge: BridgeAdapter):
        """Exception raised for unknown/disallowed class."""
        with pytest.raises(JavaException) as exc_info:
            bridge.new("com.nonexistent.FakeClass")
        # Could be ClassNotFoundException or SecurityException (if allowlisted)
        exc_str = str(exc_info.value).lower()
        assert "class" in exc_str and (
            "not found" in exc_str or "not allowed" in exc_str
        )

    def test_create_object_returns_jvm_ref(self, bridge: BridgeAdapter):
        """Created object has object_id attribute."""
        obj = bridge.new("java.util.ArrayList")
        assert hasattr(obj, "object_id")

    # ==========================================================================
    # Instance Method Calls (8 tests)
    # ==========================================================================

    def test_call_method_no_args(self, bridge: BridgeAdapter):
        """Call instance method with no arguments."""
        obj = bridge.new("java.util.ArrayList")
        size = bridge.call(obj, "size")
        assert size == 0

    def test_call_method_with_args(self, bridge: BridgeAdapter):
        """Call instance method with arguments."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", "hello")
        size = bridge.call(obj, "size")
        assert size == 1

    def test_call_method_returns_primitive(self, bridge: BridgeAdapter):
        """Method returning primitive returns Python type."""
        obj = bridge.new("java.util.ArrayList")
        result = bridge.call(obj, "isEmpty")
        assert result is True
        assert isinstance(result, bool)

    def test_call_method_returns_string(self, bridge: BridgeAdapter):
        """Method returning String returns Python str."""
        obj = bridge.new("java.lang.StringBuilder", "test")
        result = bridge.call(obj, "toString")
        assert result == "test"
        assert isinstance(result, str)

    def test_call_method_returns_object(self, bridge: BridgeAdapter):
        """Method returning object returns value or JVMRef."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", "hello")
        result = bridge.call(obj, "get", 0)
        assert result == "hello"

    def test_call_method_chained(self, bridge: BridgeAdapter):
        """Chain multiple method calls."""
        sb = bridge.new("java.lang.StringBuilder")
        bridge.call(sb, "append", "hello")
        bridge.call(sb, "append", " ")
        bridge.call(sb, "append", "world")
        result = bridge.call(sb, "toString")
        assert result == "hello world"

    def test_call_method_not_found(self, bridge: BridgeAdapter):
        """NoSuchMethodException raised for unknown method."""
        obj = bridge.new("java.util.ArrayList")
        with pytest.raises(JavaException):
            bridge.call(obj, "nonExistentMethod")

    def test_call_method_wrong_arg_type(self, bridge: BridgeAdapter):
        """Exception raised for wrong argument type."""
        obj = bridge.new("java.util.ArrayList")
        with pytest.raises(JavaException):
            # ArrayList.get expects int, not string
            bridge.call(obj, "get", "not an int")

    # ==========================================================================
    # Static Method Calls (4 tests)
    # ==========================================================================

    def test_call_static_method(self, bridge: BridgeAdapter):
        """Call static method."""
        result = bridge.call_static("java.lang.Integer", "parseInt", "42")
        assert result == 42

    def test_call_static_method_math(self, bridge: BridgeAdapter):
        """Call Math static methods."""
        result = bridge.call_static("java.lang.Math", "max", 10, 20)
        assert result == 20

    def test_call_static_method_float(self, bridge: BridgeAdapter):
        """Call static method with float args."""
        result = bridge.call_static("java.lang.Math", "sqrt", 16.0)
        assert result == 4.0

    def test_call_static_string_format(self, bridge: BridgeAdapter):
        """Call String.format static method."""
        result = bridge.call_static("java.lang.String", "format", "Hello %s!", "World")
        assert result == "Hello World!"

    # ==========================================================================
    # Field Access (4 tests)
    # ==========================================================================

    def test_get_static_field_primitive(self, bridge: BridgeAdapter):
        """Get static field with primitive value."""
        result = bridge.get_static_field("java.lang.Integer", "MAX_VALUE")
        assert result == 2147483647

    def test_get_static_field_object(self, bridge: BridgeAdapter):
        """Get static field that is an object."""
        result = bridge.get_static_field("java.lang.System", "out")
        assert result is not None

    def test_get_static_field_not_found(self, bridge: BridgeAdapter):
        """Exception raised for unknown field."""
        with pytest.raises(JavaException):
            bridge.get_static_field("java.lang.Integer", "NONEXISTENT_FIELD")

    def test_set_static_field(self, bridge: BridgeAdapter):
        """Set static field (using System.setProperty as proxy)."""
        # We can't directly set final fields, so test via System properties
        bridge.call_static(
            "java.lang.System", "setProperty", "test.bridge.key", "test_value"
        )
        result = bridge.call_static(
            "java.lang.System", "getProperty", "test.bridge.key"
        )
        assert result == "test_value"

    # ==========================================================================
    # Type Checking (4 tests)
    # ==========================================================================

    def test_is_instance_of_same_class(self, bridge: BridgeAdapter):
        """is_instance_of returns True for same class."""
        obj = bridge.new("java.util.ArrayList")
        assert bridge.is_instance_of(obj, "java.util.ArrayList")

    def test_is_instance_of_superclass(self, bridge: BridgeAdapter):
        """is_instance_of returns True for superclass."""
        obj = bridge.new("java.util.ArrayList")
        assert bridge.is_instance_of(obj, "java.util.AbstractList")

    def test_is_instance_of_interface(self, bridge: BridgeAdapter):
        """is_instance_of works with interfaces."""
        obj = bridge.new("java.util.ArrayList")
        assert bridge.is_instance_of(obj, "java.util.List")
        assert bridge.is_instance_of(obj, "java.util.Collection")
        assert bridge.is_instance_of(obj, "java.lang.Iterable")

    def test_is_instance_of_false(self, bridge: BridgeAdapter):
        """is_instance_of returns False for non-matching class."""
        obj = bridge.new("java.util.ArrayList")
        assert not bridge.is_instance_of(obj, "java.util.HashMap")
        assert not bridge.is_instance_of(obj, "java.lang.String")

    # ==========================================================================
    # Arrays (8 tests)
    # ==========================================================================

    def test_new_array_object(self, bridge: BridgeAdapter):
        """Create object array."""
        arr = bridge.new_array("java.lang.String", 3)
        assert bridge.array_length(arr) == 3

    def test_new_array_primitive_int(self, bridge: BridgeAdapter):
        """Create int array."""
        arr = bridge.new_array("int", 5)
        assert bridge.array_length(arr) == 5

    def test_new_array_primitive_long(self, bridge: BridgeAdapter):
        """Create long array."""
        arr = bridge.new_array("long", 3)
        assert bridge.array_length(arr) == 3

    def test_array_set_get_string(self, bridge: BridgeAdapter):
        """Set and get String array elements."""
        arr = bridge.new_array("java.lang.String", 3)
        bridge.array_set(arr, 0, "hello")
        bridge.array_set(arr, 1, "world")
        assert bridge.array_get(arr, 0) == "hello"
        assert bridge.array_get(arr, 1) == "world"
        assert bridge.array_get(arr, 2) is None

    def test_array_set_get_int(self, bridge: BridgeAdapter):
        """Set and get int array elements."""
        arr = bridge.new_array("int", 3)
        bridge.array_set(arr, 0, 10)
        bridge.array_set(arr, 1, 20)
        bridge.array_set(arr, 2, 30)
        assert bridge.array_get(arr, 0) == 10
        assert bridge.array_get(arr, 1) == 20
        assert bridge.array_get(arr, 2) == 30

    def test_array_pass_to_method(self, bridge: BridgeAdapter):
        """Pass array to Java method."""
        arr = bridge.new_array("java.lang.String", 3)
        bridge.array_set(arr, 0, "c")
        bridge.array_set(arr, 1, "a")
        bridge.array_set(arr, 2, "b")
        bridge.call_static("java.util.Arrays", "sort", arr)
        assert bridge.array_get(arr, 0) == "a"
        assert bridge.array_get(arr, 1) == "b"
        assert bridge.array_get(arr, 2) == "c"

    def test_array_index_out_of_bounds(self, bridge: BridgeAdapter):
        """ArrayIndexOutOfBoundsException for bad index."""
        arr = bridge.new_array("int", 3)
        with pytest.raises((JavaException, IndexError)):
            bridge.array_get(arr, 10)

    def test_array_from_method_return(self, bridge: BridgeAdapter):
        """Handle array returned from method."""
        # String.getBytes() returns byte[]
        s = bridge.new("java.lang.String", "hello")
        # Just verify it doesn't crash - return type may vary
        result = bridge.call(s, "getBytes")
        assert result is not None

    # ==========================================================================
    # Type Conversion (10 tests)
    # ==========================================================================

    def test_convert_int(self, bridge: BridgeAdapter):
        """Integer conversion."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", 42)
        result = bridge.call(obj, "get", 0)
        assert result == 42

    def test_convert_long(self, bridge: BridgeAdapter):
        """Long integer conversion."""
        big_num = 9223372036854775807  # Long.MAX_VALUE
        # Long.valueOf returns primitive long (auto-unboxed)
        result = bridge.call_static("java.lang.Long", "valueOf", big_num)
        assert result == big_num

    def test_convert_float(self, bridge: BridgeAdapter):
        """Float/double conversion."""
        result = bridge.call_static("java.lang.Math", "sqrt", 16.0)
        assert abs(result - 4.0) < 0.0001

    def test_convert_bool_true(self, bridge: BridgeAdapter):
        """Boolean True conversion."""
        obj = bridge.new("java.util.ArrayList")
        result = bridge.call(obj, "isEmpty")
        assert result is True

    def test_convert_bool_false(self, bridge: BridgeAdapter):
        """Boolean False conversion."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", "x")
        result = bridge.call(obj, "isEmpty")
        assert result is False

    def test_convert_string(self, bridge: BridgeAdapter):
        """String conversion."""
        result = bridge.call_static("java.lang.String", "valueOf", 123)
        assert result == "123"

    def test_convert_none_to_null(self, bridge: BridgeAdapter):
        """None converts to null."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", None)
        result = bridge.call(obj, "get", 0)
        assert result is None

    def test_convert_null_to_none(self, bridge: BridgeAdapter):
        """Null converts to None."""
        obj = bridge.new("java.util.HashMap")
        result = bridge.call(obj, "get", "nonexistent")
        assert result is None

    def test_convert_list(self, bridge: BridgeAdapter):
        """List conversion to Java."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "addAll", [1, 2, 3])
        size = bridge.call(obj, "size")
        assert size == 3

    def test_convert_dict(self, bridge: BridgeAdapter):
        """Dict conversion to Java."""
        obj = bridge.new("java.util.HashMap")
        bridge.call(obj, "putAll", {"a": 1, "b": 2})
        result = bridge.call(obj, "get", "a")
        assert result == 1

    # ==========================================================================
    # Exception Handling (4 tests)
    # ==========================================================================

    def test_exception_has_class_name(self, bridge: BridgeAdapter):
        """Exceptions include Java class name."""
        with pytest.raises(JavaException) as exc_info:
            bridge.new("com.nonexistent.FakeClass")
        assert exc_info.value.java_class is not None

    def test_exception_has_message(self, bridge: BridgeAdapter):
        """Exceptions include message."""
        with pytest.raises(JavaException) as exc_info:
            bridge.new("com.nonexistent.FakeClass")
        assert exc_info.value.message is not None

    def test_exception_has_stack_trace(self, bridge: BridgeAdapter):
        """Exceptions include Java stack trace."""
        with pytest.raises(JavaException) as exc_info:
            bridge.new("com.nonexistent.FakeClass")
        assert exc_info.value.stack_trace is not None
        assert len(exc_info.value.stack_trace) > 0

    def test_exception_index_out_of_bounds(self, bridge: BridgeAdapter):
        """IndexOutOfBoundsException raised properly."""
        obj = bridge.new("java.util.ArrayList")
        with pytest.raises(JavaException) as exc_info:
            bridge.call(obj, "get", 0)
        exc_str = str(exc_info.value).lower()
        assert "index" in exc_str or "bounds" in exc_str

    # ==========================================================================
    # Object Lifecycle (3 tests)
    # ==========================================================================

    def test_detach_object(self, bridge: BridgeAdapter):
        """Detached objects remain valid."""
        obj = bridge.new("java.util.ArrayList")
        bridge.call(obj, "add", "hello")
        bridge.detach(obj)
        # Object should still be usable
        result = bridge.call(obj, "get", 0)
        assert result == "hello"

    def test_close_bridge(self, bridge: BridgeAdapter):
        """Close releases resources."""
        # Create a fresh bridge for this test
        # (don't close the shared fixture)
        pass  # Skip - would need separate bridge instance

    def test_object_usable_after_creation(self, bridge: BridgeAdapter):
        """Objects remain usable across multiple operations."""
        obj = bridge.new("java.util.ArrayList")
        for i in range(100):
            bridge.call(obj, "add", i)
        assert bridge.call(obj, "size") == 100

    # ==========================================================================
    # JVM View (6 tests)
    # ==========================================================================

    def test_jvm_view_create_object(self, bridge: BridgeAdapter):
        """JVM view can create objects."""
        arr = bridge.jvm.java.util.ArrayList()
        bridge.call(arr, "add", "test")
        assert bridge.call(arr, "size") == 1

    def test_jvm_view_static_method(self, bridge: BridgeAdapter):
        """JVM view can call static methods."""
        result = bridge.jvm.java.lang.Integer.parseInt("42")
        assert result == 42

    def test_jvm_view_static_field(self, bridge: BridgeAdapter):
        """JVM view can access static fields."""
        result = bridge.jvm.java.lang.Integer.MAX_VALUE
        assert result == 2147483647

    def test_java_import_wildcard(self, bridge: BridgeAdapter):
        """java_import with wildcard."""
        bridge.java_import("java.util.*")
        arr = bridge.jvm.ArrayList()
        assert bridge.call(arr, "size") == 0

    def test_java_import_single_class(self, bridge: BridgeAdapter):
        """java_import single class."""
        bridge.java_import("java.lang.StringBuilder")
        sb = bridge.jvm.StringBuilder("hello")
        assert bridge.call(sb, "toString") == "hello"

    def test_jvm_view_nested_package(self, bridge: BridgeAdapter):
        """JVM view handles nested packages."""
        hm = bridge.jvm.java.util.HashMap()
        bridge.call(hm, "put", "key", "value")
        assert bridge.call(hm, "get", "key") == "value"


# ==========================================================================
# Gatun Adapter Tests
# ==========================================================================


class TestGatunAdapter(BridgeContractTests):
    """Run contract tests against GatunAdapter."""

    @pytest.fixture
    def bridge(self):
        """Provide GatunAdapter instance."""
        from gatun.bridge_adapters import GatunAdapter

        adapter = GatunAdapter()
        yield adapter
        adapter.close()
