"""Bridge adapter implementations.

This module provides BridgeAdapter implementations for different backends.
"""

from __future__ import annotations

from typing import Any, cast

from gatun.bridge import (
    BridgeAdapter,
    JavaException,
    JVMRef,
    JVMView,
    JAVA_EXCEPTION_MAP,
)
from gatun.client import (
    GatunClient,
    JavaObject,
    JVMView as GatunJVMView,
    java_import as gatun_java_import,
    JavaException as GatunJavaException,
)
from gatun import connect as gatun_connect


class GatunAdapter(BridgeAdapter):
    """BridgeAdapter implementation backed by Gatun.

    This adapter wraps Gatun's native client and exposes it through
    the standard BridgeAdapter interface.
    """

    def __init__(
        self,
        memory: str = "64MB",
        classpath: list[str] | None = None,
    ):
        """Create a new GatunAdapter.

        Args:
            memory: Shared memory size (e.g., "64MB", "256MB")
            classpath: Additional JAR files for the classpath
        """
        # connect() launches the server and returns a connected client
        self._client: GatunClient | None = gatun_connect(memory=memory)

    # === Object Lifecycle ===

    def new(self, class_name: str, *args: Any) -> JVMRef:
        """Create a new JVM object."""
        try:
            assert self._client is not None, "Bridge is closed"
            result: JVMRef = self._client.create_object(class_name, *args)
            return result
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    def close(self) -> None:
        """Close the bridge and release all resources."""
        if self._client:
            self._client.close()
            self._client = None

    def detach(self, ref: JVMRef) -> None:
        """Prevent automatic cleanup of this object reference."""
        if isinstance(ref, JavaObject):
            ref.detach()

    # === Method Calls ===

    def call(self, ref: JVMRef, method: str, *args: Any) -> Any:
        """Call an instance method on a JVM object."""
        try:
            obj_id = ref.object_id if hasattr(ref, "object_id") else ref
            return self._client.invoke_method(obj_id, method, *args)
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    def call_static(self, class_name: str, method: str, *args: Any) -> Any:
        """Call a static method on a JVM class."""
        try:
            return self._client.invoke_static_method(class_name, method, *args)
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    # === Field Access ===

    def get_field(self, ref: JVMRef, name: str) -> Any:
        """Get an instance field value."""
        try:
            obj_id = ref.object_id if hasattr(ref, "object_id") else ref
            return self._client.get_field(obj_id, name)
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    def set_field(self, ref: JVMRef, name: str, value: Any) -> None:
        """Set an instance field value."""
        try:
            obj_id = ref.object_id if hasattr(ref, "object_id") else ref
            self._client.set_field(obj_id, name, value)
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    def get_static_field(self, class_name: str, name: str) -> Any:
        """Get a static field value."""
        try:
            return self._client.get_static_field(class_name, name)
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    def set_static_field(self, class_name: str, name: str, value: Any) -> None:
        """Set a static field value."""
        try:
            self._client.set_static_field(class_name, name, value)
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    # === Type Checking ===

    def is_instance_of(self, ref: JVMRef, class_name: str) -> bool:
        """Check if object is instance of class."""
        try:
            return self._client.is_instance_of(ref, class_name)
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    # === Arrays ===

    # Mapping of primitive type names to their wrapper class TYPE field
    _PRIMITIVE_TYPES = {
        "int": ("java.lang.Integer", "TYPE"),
        "long": ("java.lang.Long", "TYPE"),
        "double": ("java.lang.Double", "TYPE"),
        "float": ("java.lang.Float", "TYPE"),
        "boolean": ("java.lang.Boolean", "TYPE"),
        "byte": ("java.lang.Byte", "TYPE"),
        "short": ("java.lang.Short", "TYPE"),
        "char": ("java.lang.Character", "TYPE"),
    }

    def new_array(self, element_class: str, length: int) -> JVMRef:
        """Create a new JVM array."""
        try:
            # Get the Class object for the element type
            if element_class in self._PRIMITIVE_TYPES:
                wrapper_class, field = self._PRIMITIVE_TYPES[element_class]
                class_obj = self._client.get_static_field(wrapper_class, field)
            else:
                class_obj = self._client.invoke_static_method(
                    "java.lang.Class", "forName", element_class
                )

            # Create the array using reflection
            # Use return_object_ref=True to get an ObjectRef instead of auto-converted array
            return cast(
                JVMRef,
                self._client.invoke_static_method(
                    "java.lang.reflect.Array",
                    "newInstance",
                    class_obj,
                    length,
                    return_object_ref=True,
                ),
            )
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    def array_get(self, array_ref: JVMRef, index: int) -> Any:
        """Get element at index from JVM array."""
        try:
            return self._client.invoke_static_method(
                "java.lang.reflect.Array", "get", array_ref, index
            )
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    def array_set(self, array_ref: JVMRef, index: int, value: Any) -> None:
        """Set element at index in JVM array."""
        try:
            self._client.invoke_static_method(
                "java.lang.reflect.Array", "set", array_ref, index, value
            )
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    def array_length(self, array_ref: JVMRef) -> int:
        """Get length of JVM array."""
        try:
            return cast(
                int,
                self._client.invoke_static_method(
                    "java.lang.reflect.Array", "getLength", array_ref
                ),
            )
        except GatunJavaException as e:
            raise self._convert_exception(e) from None

    # === JVM View ===

    @property
    def jvm(self) -> JVMView:
        """Get JVM view for navigating classes."""
        assert self._client is not None, "Bridge is closed"
        return _GatunJVMViewAdapter(self._client.jvm, self._client)

    def java_import(self, package: str) -> None:
        """Import package for shorter class names."""
        gatun_java_import(self._client.jvm, package)

    # === Exception Conversion ===

    def _convert_exception(self, exc: GatunJavaException) -> JavaException:
        """Convert Gatun exception to bridge JavaException."""
        java_class = getattr(exc, "java_class", type(exc).__name__)
        message = getattr(exc, "message", str(exc))
        stack_trace = getattr(exc, "stack_trace", "")

        # Find the appropriate exception class
        exc_class = JAVA_EXCEPTION_MAP.get(java_class, JavaException)
        return exc_class(java_class, message, stack_trace)


class _GatunJVMViewAdapter(JVMView):
    """Adapter that wraps Gatun's JVMView to implement the bridge JVMView."""

    def __init__(self, jvm_view: GatunJVMView, client: GatunClient):
        self._jvm_view = jvm_view
        self._client = client

    def __getattr__(self, name: str) -> "_GatunJVMViewAdapter | JVMRef | Any":
        """Navigate to package, class, or access static member."""
        result = getattr(self._jvm_view, name)

        # If it's a JVMView node, wrap it
        if hasattr(result, "_path"):
            return _GatunJVMViewAdapter(result, self._client)

        # If it's a JavaObject, return as-is (it's a JVMRef)
        if isinstance(result, JavaObject):
            return result

        # Otherwise return the value (primitives, strings, etc.)
        return result

    def __call__(self, *args: Any) -> JVMRef:
        """Create instance of this class."""
        return cast(JVMRef, self._jvm_view(*args))  # type: ignore[operator]
