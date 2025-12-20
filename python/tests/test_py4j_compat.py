"""Tests for Py4J compatibility layer.

These tests verify that the gatun.py4j_compat module provides the same
API as py4j.java_gateway, allowing PySpark to use Gatun as a drop-in
replacement.
"""

import pytest

from gatun.py4j_compat import (
    JavaGateway,
    ClientServer,
    JavaObject,
    JVMView,
    GatewayParameters,
    JavaParameters,
    PythonParameters,
    java_import,
    is_instance_of,
    launch_gateway,
)


@pytest.fixture
def gateway():
    """Create a JavaGateway for testing."""
    gw = JavaGateway()
    yield gw
    gw.close()


class TestJavaGateway:
    """Test JavaGateway Py4J compatibility."""

    def test_basic_creation(self, gateway):
        """Test creating a JavaGateway."""
        assert gateway is not None
        assert gateway.jvm is not None

    def test_jvm_attribute_access(self, gateway):
        """Test accessing JVM packages via attribute access."""
        # Should return a JVMView-like object
        java = gateway.jvm.java
        assert java is not None

        util = gateway.jvm.java.util
        assert util is not None

    def test_create_arraylist(self, gateway):
        """Test creating a Java ArrayList (Py4J style)."""
        ArrayList = gateway.jvm.java.util.ArrayList
        arr = ArrayList()
        assert arr is not None

        # Add elements
        arr.add("hello")
        arr.add("world")

        # Check size
        assert arr.size() == 2

    def test_static_method_call(self, gateway):
        """Test calling static methods (Py4J style)."""
        result = gateway.jvm.java.lang.Integer.parseInt("42")
        assert result == 42

        result = gateway.jvm.java.lang.Math.max(10, 20)
        assert result == 20

    def test_java_import_wildcard(self, gateway):
        """Test java_import with wildcard."""
        java_import(gateway.jvm, "java.util.*")

        # Should now be accessible without full path
        arr = gateway.jvm.ArrayList()
        assert arr is not None
        arr.add("test")
        assert arr.size() == 1

    def test_java_import_single_class(self, gateway):
        """Test java_import with single class."""
        java_import(gateway.jvm, "java.lang.StringBuilder")

        sb = gateway.jvm.StringBuilder("hello")
        sb.append(" world")
        assert "hello world" in str(sb)

    def test_context_manager(self):
        """Test using gateway as context manager."""
        with JavaGateway() as gw:
            arr = gw.jvm.java.util.ArrayList()
            arr.add("test")
            assert arr.size() == 1
        # Gateway should be closed after exiting context

    def test_is_instance_of(self, gateway):
        """Test is_instance_of method."""
        arr = gateway.jvm.java.util.ArrayList()

        assert gateway.is_instance_of(arr, "java.util.List")
        assert gateway.is_instance_of(arr, "java.util.Collection")
        assert gateway.is_instance_of(arr, "java.lang.Object")
        assert not gateway.is_instance_of(arr, "java.util.Map")

    def test_is_instance_of_function(self, gateway):
        """Test is_instance_of as standalone function."""
        arr = gateway.jvm.java.util.ArrayList()

        assert is_instance_of(gateway, arr, "java.util.List")
        assert not is_instance_of(gateway, arr, "java.util.Map")

    def test_new_jvm_view(self, gateway):
        """Test creating new JVM views."""
        view1 = gateway.new_jvm_view("view1")
        view2 = gateway.new_jvm_view("view2")

        # Each view has independent imports
        java_import(view1, "java.util.ArrayList")
        java_import(view2, "java.util.HashMap")

        # view1 should have ArrayList
        arr = view1.ArrayList()
        assert arr is not None

        # view2 should have HashMap
        hm = view2.HashMap()
        assert hm is not None


class TestClientServer:
    """Test ClientServer Py4J compatibility."""

    def test_basic_creation(self):
        """Test creating a ClientServer."""
        server = ClientServer(
            java_parameters=JavaParameters(),
            python_parameters=PythonParameters(),
        )
        try:
            assert server is not None
            assert server.jvm is not None

            # Basic operation
            arr = server.jvm.java.util.ArrayList()
            arr.add("test")
            assert arr.size() == 1
        finally:
            server.shutdown()

    def test_context_manager(self):
        """Test using ClientServer as context manager."""
        with ClientServer() as server:
            arr = server.jvm.java.util.ArrayList()
            arr.add("test")
            assert arr.size() == 1


class TestGatewayParameters:
    """Test GatewayParameters configuration."""

    def test_default_values(self):
        """Test default parameter values."""
        params = GatewayParameters()
        assert params.port == 25333
        assert params.address == "127.0.0.1"
        assert params.auto_close is True

    def test_custom_socket_path(self):
        """Test custom socket path parameter."""
        params = GatewayParameters(socket_path="/tmp/custom.sock")
        assert params.socket_path == "/tmp/custom.sock"

    def test_gateway_with_parameters(self):
        """Test creating gateway with parameters."""
        params = GatewayParameters(memory="32MB")
        gw = JavaGateway(gateway_parameters=params)
        try:
            arr = gw.jvm.java.util.ArrayList()
            assert arr is not None
        finally:
            gw.close()


class TestCallbackServer:
    """Test callback server compatibility."""

    def test_start_callback_server(self, gateway):
        """Test starting callback server."""
        result = gateway.start_callback_server()
        assert result is True

        server = gateway.get_callback_server()
        assert server is not None

    def test_shutdown_callback_server(self, gateway):
        """Test shutting down callback server."""
        gateway.start_callback_server()
        result = gateway.shutdown_callback_server()
        assert result is True

        server = gateway.get_callback_server()
        assert server is None


class TestJavaObject:
    """Test JavaObject compatibility."""

    def test_method_chaining(self, gateway):
        """Test method chaining on Java objects."""
        sb = gateway.jvm.java.lang.StringBuilder()
        result = sb.append("hello").append(" ").append("world")
        assert "hello world" in str(result)

    def test_nested_method_calls(self, gateway):
        """Test nested method calls."""
        # Use StringBuilder which returns JavaObject, not auto-converted String
        sb = gateway.jvm.java.lang.StringBuilder("hello")
        sb.append(" world")

        # Get result - calling toString returns a String which gets auto-converted
        result = str(sb)
        assert "hello world" in result


class TestLaunchGateway:
    """Test launch_gateway function."""

    def test_launch_gateway_function(self):
        """Test launching gateway via function."""
        gw = launch_gateway()
        try:
            assert gw is not None
            arr = gw.jvm.java.util.ArrayList()
            arr.add("launched")
            assert arr.size() == 1
        finally:
            gw.close()

    def test_launch_gateway_with_memory(self):
        """Test launching with custom memory size."""
        gw = launch_gateway(memory="32MB")
        try:
            arr = gw.jvm.java.util.ArrayList()
            assert arr is not None
        finally:
            gw.close()


class TestPySparkPatterns:
    """Test patterns commonly used in PySpark."""

    def test_spark_context_pattern(self, gateway):
        """Test pattern used in SparkContext initialization."""
        # PySpark does: self._jvm = gateway.jvm
        jvm = gateway.jvm
        assert jvm is not None

        # Then accesses classes via: self._jvm.java.util.HashMap()
        hashmap = jvm.java.util.HashMap()
        hashmap.put("key", "value")
        assert hashmap.get("key") == "value"

    def test_collections_import_pattern(self, gateway):
        """Test pattern used for importing Spark collections."""
        # PySpark imports: java_import(gateway.jvm, "org.apache.spark.*")
        java_import(gateway.jvm, "java.util.*")

        # Then uses: gateway.jvm.ArrayList()
        arr = gateway.jvm.ArrayList()
        arr.add(1)
        arr.add(2)
        assert arr.size() == 2

    def test_static_util_calls(self, gateway):
        """Test pattern for calling static utility methods."""
        # PySpark calls: gateway.jvm.org.apache.spark.util.Utils.someMethod()
        # We test with allowlisted Java classes
        result = gateway.jvm.java.lang.Math.abs(-42)
        assert result == 42

        result = gateway.jvm.java.lang.Integer.parseInt("123")
        assert result == 123

    def test_object_iteration(self, gateway):
        """Test iterating over Java collections."""
        arr = gateway.jvm.java.util.ArrayList()
        arr.add("a")
        arr.add("b")
        arr.add("c")

        # PySpark often converts to Python list
        size = arr.size()
        items = [arr.get(i) for i in range(size)]
        assert items == ["a", "b", "c"]
