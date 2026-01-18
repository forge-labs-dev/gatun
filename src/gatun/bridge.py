"""Bridge Adapter Protocol for PySpark <-> JVM communication.

This module defines the minimal contract between PySpark and a JVM backend.
Both Py4J and Gatun can implement this protocol.

Usage in PySpark:
    from gatun.bridge import BridgeAdapter, GatunAdapter

    # Create adapter
    bridge = GatunAdapter()

    # Use it
    arr = bridge.new("java.util.ArrayList")
    bridge.call(arr, "add", "hello")
    size = bridge.call(arr, "size")  # 1

    # Or use JVM view
    arr = bridge.jvm.java.util.ArrayList()
    arr_ref = bridge.call(arr, "add", "hello")
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class JVMRef(Protocol):
    """Opaque reference to a JVM object.

    This is a marker protocol - any object that represents a JVM reference
    should satisfy this. Both Py4J's JavaObject and Gatun's JavaObject work.
    """

    @property
    def object_id(self) -> int:
        """Unique identifier for this object in the JVM."""
        ...


class BridgeAdapter(ABC):
    """Abstract base class defining the bridge contract.

    This is the minimal API that PySpark needs to communicate with a JVM.
    Implementations can be backed by Py4J, Gatun, or any other bridge.
    """

    # === Object Lifecycle ===

    @abstractmethod
    def new(self, class_name: str, *args: Any) -> JVMRef:
        """Create a new JVM object.

        Args:
            class_name: Fully qualified class name (e.g., "java.util.ArrayList")
            *args: Constructor arguments (Python types auto-converted)

        Returns:
            Reference to the created object

        Raises:
            JavaException: If class not found or constructor fails
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Close the bridge and release all resources."""
        ...

    @abstractmethod
    def detach(self, ref: JVMRef) -> None:
        """Prevent automatic cleanup of this object reference.

        Used when passing objects to long-lived Java structures that will
        manage the object's lifecycle.
        """
        ...

    # === Method Calls ===

    @abstractmethod
    def call(self, ref: JVMRef, method: str, *args: Any) -> Any:
        """Call an instance method on a JVM object.

        Args:
            ref: Object reference
            method: Method name
            *args: Method arguments (auto-converted)

        Returns:
            Method result (JVMRef for objects, Python types for primitives)

        Raises:
            JavaException: If method not found or invocation fails
        """
        ...

    @abstractmethod
    def call_static(self, class_name: str, method: str, *args: Any) -> Any:
        """Call a static method on a JVM class.

        Args:
            class_name: Fully qualified class name
            method: Static method name
            *args: Method arguments

        Returns:
            Method result
        """
        ...

    # === Field Access ===

    @abstractmethod
    def get_field(self, ref: JVMRef, name: str) -> Any:
        """Get an instance field value."""
        ...

    @abstractmethod
    def set_field(self, ref: JVMRef, name: str, value: Any) -> None:
        """Set an instance field value."""
        ...

    @abstractmethod
    def get_static_field(self, class_name: str, name: str) -> Any:
        """Get a static field value."""
        ...

    @abstractmethod
    def set_static_field(self, class_name: str, name: str, value: Any) -> None:
        """Set a static field value."""
        ...

    # === Type Checking ===

    @abstractmethod
    def is_instance_of(self, ref: JVMRef, class_name: str) -> bool:
        """Check if object is instance of class (supports interfaces)."""
        ...

    # === Arrays ===

    @abstractmethod
    def new_array(self, element_class: str, length: int) -> JVMRef:
        """Create a new JVM array.

        Args:
            element_class: Element type. For primitives use lowercase
                          ("int", "long", "double", etc.). For objects
                          use fully qualified name ("java.lang.String").
            length: Array length

        Returns:
            Reference to the array
        """
        ...

    @abstractmethod
    def array_get(self, array_ref: JVMRef, index: int) -> Any:
        """Get element at index from JVM array."""
        ...

    @abstractmethod
    def array_set(self, array_ref: JVMRef, index: int, value: Any) -> None:
        """Set element at index in JVM array."""
        ...

    @abstractmethod
    def array_length(self, array_ref: JVMRef) -> int:
        """Get length of JVM array."""
        ...

    # === JVM View ===

    @property
    @abstractmethod
    def jvm(self) -> "JVMView":
        """Get JVM view for navigating classes.

        Allows: bridge.jvm.java.util.ArrayList()
        Instead of: bridge.new("java.util.ArrayList")
        """
        ...

    @abstractmethod
    def java_import(self, package: str) -> None:
        """Import package for shorter class names.

        Args:
            package: Package path with optional wildcard (e.g., "java.util.*")

        After calling java_import("java.util.*"), you can use:
            bridge.jvm.ArrayList() instead of bridge.jvm.java.util.ArrayList()
        """
        ...


class JVMView(ABC):
    """View into JVM class hierarchy.

    Supports attribute-style navigation:
        jvm.java.util.ArrayList  -> class reference
        jvm.java.util.ArrayList() -> new instance
        jvm.java.lang.Integer.MAX_VALUE -> static field
        jvm.java.lang.Integer.parseInt("42") -> static method call
    """

    @abstractmethod
    def __getattr__(self, name: str) -> "JVMView | JVMRef | Any":
        """Navigate to package, class, or access static member."""
        ...

    @abstractmethod
    def __call__(self, *args: Any) -> JVMRef:
        """Create instance of this class."""
        ...


# === Exception Hierarchy ===


class JavaException(Exception):
    """Base class for Java exceptions raised in Python."""

    def __init__(self, java_class: str, message: str, stack_trace: str = ""):
        self.java_class = java_class
        self.message = message
        self.stack_trace = stack_trace
        super().__init__(f"{java_class}: {message}")


class JavaSecurityException(JavaException):
    """java.lang.SecurityException"""

    pass


class JavaIllegalArgumentException(JavaException):
    """java.lang.IllegalArgumentException"""

    pass


class JavaNoSuchMethodException(JavaException):
    """java.lang.NoSuchMethodException"""

    pass


class JavaNoSuchFieldException(JavaException):
    """java.lang.NoSuchFieldException"""

    pass


class JavaClassNotFoundException(JavaException):
    """java.lang.ClassNotFoundException"""

    pass


class JavaNullPointerException(JavaException):
    """java.lang.NullPointerException"""

    pass


class JavaIndexOutOfBoundsException(JavaException):
    """java.lang.IndexOutOfBoundsException"""

    pass


class JavaNumberFormatException(JavaException):
    """java.lang.NumberFormatException"""

    pass


# Mapping from Java exception class names to Python exception classes
JAVA_EXCEPTION_MAP = {
    "java.lang.SecurityException": JavaSecurityException,
    "java.lang.IllegalArgumentException": JavaIllegalArgumentException,
    "java.lang.NoSuchMethodException": JavaNoSuchMethodException,
    "java.lang.NoSuchFieldException": JavaNoSuchFieldException,
    "java.lang.ClassNotFoundException": JavaClassNotFoundException,
    "java.lang.NullPointerException": JavaNullPointerException,
    "java.lang.IndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.ArrayIndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.StringIndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.NumberFormatException": JavaNumberFormatException,
}
