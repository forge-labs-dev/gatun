package org.gatun.server;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Centralized cache for reflection lookups to avoid repeated reflection overhead.
 *
 * <p>This class caches:
 * <ul>
 *   <li>Method lookups (with MethodHandle for fast invocation)</li>
 *   <li>Constructor lookups</li>
 *   <li>Field lookups (static and instance)</li>
 *   <li>Class lookups by name</li>
 *   <li>No-arg methods as MethodHandles for the common case</li>
 * </ul>
 *
 * <p>All caches are thread-safe using ConcurrentHashMap.
 */
public final class ReflectionCache {
  private static final Logger LOG = Logger.getLogger(ReflectionCache.class.getName());

  // --- METHOD CACHE ---
  // Key: (class, methodName, argTypes) -> CachedMethod (method + MethodHandle + varargs info)
  private static final Map<MethodCacheKey, CachedMethod> methodCache = new ConcurrentHashMap<>();

  // --- CONSTRUCTOR CACHE ---
  // Key: (class, argTypes) -> CachedConstructor (constructor + varargs info)
  private static final Map<ConstructorCacheKey, CachedConstructor> constructorCache =
      new ConcurrentHashMap<>();

  // --- NO-ARG CONSTRUCTOR CACHE ---
  // Key: class -> no-arg Constructor
  private static final Map<Class<?>, java.lang.reflect.Constructor<?>> noArgConstructorCache =
      new ConcurrentHashMap<>();

  // --- NO-ARG METHOD HANDLE CACHE ---
  // Uses a simple two-level map: Class -> methodName -> MethodHandle
  private static final Map<Class<?>, Map<String, MethodHandle>> noArgMethodHandleCache =
      new ConcurrentHashMap<>();

  // --- STATIC FIELD CACHE ---
  // Key: "className.fieldName" -> Field object
  private static final Map<String, java.lang.reflect.Field> staticFieldCache =
      new ConcurrentHashMap<>();

  // --- INSTANCE FIELD CACHE ---
  // Key: "className.fieldName" -> Field object
  private static final Map<String, java.lang.reflect.Field> instanceFieldCache =
      new ConcurrentHashMap<>();

  // --- METHODS CACHE ---
  // Caches getMethods() results per class
  private static final Map<Class<?>, java.lang.reflect.Method[]> methodsCache =
      new ConcurrentHashMap<>();

  // --- CONSTRUCTORS CACHE ---
  // Caches getConstructors() results per class
  private static final Map<Class<?>, java.lang.reflect.Constructor<?>[]> constructorsCache =
      new ConcurrentHashMap<>();

  // --- CLASS CACHE ---
  // Caches Class.forName() results
  private static final Map<String, Class<?>> classCache = new ConcurrentHashMap<>();

  /** Lookup for converting Methods to MethodHandles. */
  public static final MethodHandles.Lookup METHOD_LOOKUP = MethodHandles.publicLookup();

  /** Empty Class array constant to avoid allocation. */
  public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];

  /** Empty Object array constant to avoid allocation. */
  public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  // Common JDK classes with no-arg constructors to pre-warm the cache
  private static final String[] PREWARM_CLASSES = {
      "java.util.ArrayList",
      "java.util.LinkedList",
      "java.util.HashMap",
      "java.util.LinkedHashMap",
      "java.util.HashSet",
      "java.util.LinkedHashSet",
      "java.util.TreeMap",
      "java.util.TreeSet",
      "java.lang.StringBuilder",
      "java.lang.StringBuffer"
  };

  // Static initializer to pre-warm constructor and method caches
  static {
    for (String className : PREWARM_CLASSES) {
      try {
        Class<?> clazz = Class.forName(className);
        // Pre-cache the no-arg constructor
        java.lang.reflect.Constructor<?> ctor = clazz.getDeclaredConstructor();
        noArgConstructorCache.put(clazz, ctor);
        // Pre-cache common no-arg methods as MethodHandles
        for (String methodName : new String[] {"size", "isEmpty", "toString", "hashCode", "clear"}) {
          try {
            java.lang.reflect.Method method = clazz.getMethod(methodName);
            MethodHandle handle = METHOD_LOOKUP.unreflect(method);
            noArgMethodHandleCache.computeIfAbsent(clazz, k -> new ConcurrentHashMap<>()).put(methodName, handle);
          } catch (NoSuchMethodException | IllegalAccessException e) {
            // Method doesn't exist on this class - skip
          }
        }
      } catch (ClassNotFoundException | NoSuchMethodException e) {
        LOG.fine("Failed to pre-warm cache for " + className + ": " + e.getMessage());
      }
    }
    LOG.fine("Pre-warmed constructor cache with " + noArgConstructorCache.size() + " entries");
  }

  private ReflectionCache() {} // Prevent instantiation

  // ========== NO-ARG CONSTRUCTOR ==========

  /**
   * Get no-arg constructor for a class, using cache.
   */
  public static java.lang.reflect.Constructor<?> getNoArgConstructor(Class<?> clazz)
      throws NoSuchMethodException {
    java.lang.reflect.Constructor<?> ctor = noArgConstructorCache.get(clazz);
    if (ctor != null) {
      return ctor;
    }
    ctor = clazz.getDeclaredConstructor();
    noArgConstructorCache.put(clazz, ctor);
    return ctor;
  }

  // ========== NO-ARG METHOD HANDLE ==========

  /**
   * Get no-arg method handle for a class, using fast cache lookup.
   *
   * @return the MethodHandle, or null if not found (caller should fall back to normal resolution)
   */
  public static MethodHandle getNoArgMethodHandle(Class<?> clazz, String methodName) {
    Map<String, MethodHandle> classMethods = noArgMethodHandleCache.get(clazz);
    if (classMethods != null) {
      MethodHandle cached = classMethods.get(methodName);
      if (cached != null) {
        return cached;
      }
    }

    // Try to find the no-arg method and convert to MethodHandle
    try {
      java.lang.reflect.Method method = clazz.getMethod(methodName);
      MethodHandle handle = METHOD_LOOKUP.unreflect(method);
      // Cache it
      noArgMethodHandleCache.computeIfAbsent(clazz, k -> new ConcurrentHashMap<>()).put(methodName, handle);
      return handle;
    } catch (NoSuchMethodException | IllegalAccessException e) {
      // Not a simple no-arg method or not accessible - return null so caller uses normal resolution
      return null;
    }
  }

  // ========== STATIC FIELD ==========

  /**
   * Get a static field by class and name, using cache.
   */
  public static java.lang.reflect.Field getStaticField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    String key = clazz.getName() + "." + fieldName;
    java.lang.reflect.Field field = staticFieldCache.get(key);
    if (field != null) {
      return field;
    }
    field = clazz.getField(fieldName);
    staticFieldCache.put(key, field);
    return field;
  }

  // ========== INSTANCE FIELD ==========

  /**
   * Get an instance field by class and name, using cache. Searches up the class hierarchy.
   */
  public static java.lang.reflect.Field getInstanceField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    String key = clazz.getName() + "." + fieldName;
    java.lang.reflect.Field field = instanceFieldCache.get(key);
    if (field != null) {
      return field;
    }
    // Search up the class hierarchy
    Class<?> current = clazz;
    while (current != null) {
      try {
        field = current.getDeclaredField(fieldName);
        instanceFieldCache.put(key, field);
        return field;
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException("No field named '" + fieldName + "' in " + clazz.getName());
  }

  // ========== METHODS CACHE ==========

  /**
   * Get all public methods for a class, using cache.
   */
  public static java.lang.reflect.Method[] getCachedMethods(Class<?> clazz) {
    java.lang.reflect.Method[] methods = methodsCache.get(clazz);
    if (methods != null) {
      return methods;
    }
    methods = clazz.getMethods();
    methodsCache.put(clazz, methods);
    return methods;
  }

  // ========== CONSTRUCTORS CACHE ==========

  /**
   * Get all public constructors for a class, using cache.
   */
  public static java.lang.reflect.Constructor<?>[] getCachedConstructors(Class<?> clazz) {
    java.lang.reflect.Constructor<?>[] ctors = constructorsCache.get(clazz);
    if (ctors != null) {
      return ctors;
    }
    ctors = clazz.getConstructors();
    constructorsCache.put(clazz, ctors);
    return ctors;
  }

  // ========== CLASS CACHE ==========

  /**
   * Get a class by name, using cache.
   */
  public static Class<?> getClass(String className) throws ClassNotFoundException {
    Class<?> clazz = classCache.get(className);
    if (clazz != null) {
      return clazz;
    }
    clazz = Class.forName(className);
    classCache.put(className, clazz);
    return clazz;
  }

  /**
   * Try to get a class by name without throwing. Returns null if not found.
   */
  public static Class<?> tryGetClass(String className) {
    Class<?> clazz = classCache.get(className);
    if (clazz != null) {
      return clazz;
    }
    try {
      clazz = Class.forName(className);
      classCache.put(className, clazz);
      return clazz;
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  // ========== METHOD RESOLUTION ==========

  /**
   * Get a cached method, or null if not cached.
   */
  public static CachedMethod getCachedMethod(MethodCacheKey key) {
    return methodCache.get(key);
  }

  /**
   * Cache a resolved method.
   */
  public static void cacheMethod(MethodCacheKey key, CachedMethod cached) {
    methodCache.put(key, cached);
  }

  // ========== CONSTRUCTOR RESOLUTION ==========

  /**
   * Get a cached constructor, or null if not cached.
   */
  public static CachedConstructor getCachedConstructor(ConstructorCacheKey key) {
    return constructorCache.get(key);
  }

  /**
   * Cache a resolved constructor.
   */
  public static void cacheConstructor(ConstructorCacheKey key, CachedConstructor cached) {
    constructorCache.put(key, cached);
  }

  // ========== CACHE KEY CLASSES ==========

  /** Key for method cache lookup. */
  public static final class MethodCacheKey {
    private final Class<?> clazz;
    private final String methodName;
    private final Class<?>[] argTypes;
    private final int hashCode;

    public MethodCacheKey(Class<?> clazz, String methodName, Class<?>[] argTypes) {
      this.clazz = clazz;
      this.methodName = methodName;
      this.argTypes = argTypes;
      // Pre-compute hash for faster lookups
      int h = clazz.hashCode() * 31 + methodName.hashCode();
      for (Class<?> t : argTypes) {
        h = h * 31 + (t != null ? t.hashCode() : 0);
      }
      this.hashCode = h;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof MethodCacheKey other)) return false;
      if (clazz != other.clazz) return false;
      if (!methodName.equals(other.methodName)) return false;
      if (argTypes.length != other.argTypes.length) return false;
      for (int i = 0; i < argTypes.length; i++) {
        if (argTypes[i] != other.argTypes[i]) return false;
      }
      return true;
    }
  }

  /** Cached method resolution result with MethodHandle for fast invocation. */
  public static final class CachedMethod {
    public final java.lang.reflect.Method method;
    public final MethodHandle handle;
    public final boolean isVarArgs;
    public final int fixedArgCount;
    public final Class<?> varargComponentType;
    public final boolean isStatic;

    public CachedMethod(java.lang.reflect.Method method) {
      this.method = method;
      this.isVarArgs = method.isVarArgs();
      if (isVarArgs) {
        Class<?>[] paramTypes = method.getParameterTypes();
        this.fixedArgCount = paramTypes.length - 1;
        this.varargComponentType = paramTypes[fixedArgCount].getComponentType();
      } else {
        this.fixedArgCount = 0;
        this.varargComponentType = null;
      }
      // Convert to MethodHandle for faster invocation
      MethodHandle h = null;
      try {
        h = METHOD_LOOKUP.unreflect(method);
      } catch (IllegalAccessException e) {
        // Fall back to null - will use Method.invoke
      }
      this.handle = h;
      this.isStatic = java.lang.reflect.Modifier.isStatic(method.getModifiers());
    }

    /** Prepare arguments for invocation, repacking varargs if needed. */
    public Object[] prepareArgs(Object[] args) {
      if (!isVarArgs) {
        return args;
      }
      // Repack: fixed args + varargs array
      Object[] newArgs = new Object[fixedArgCount + 1];
      for (int i = 0; i < fixedArgCount; i++) {
        newArgs[i] = args[i];
      }
      int varargCount = args.length - fixedArgCount;
      Object varargArray = java.lang.reflect.Array.newInstance(varargComponentType, varargCount);
      for (int i = 0; i < varargCount; i++) {
        java.lang.reflect.Array.set(varargArray, i, args[fixedArgCount + i]);
      }
      newArgs[fixedArgCount] = varargArray;
      return newArgs;
    }

    /** Invoke the method using MethodHandle if available, otherwise fall back to reflection. */
    public Object invoke(Object target, Object[] args) throws Throwable {
      if (handle != null) {
        // Use invokeWithArguments for flexibility with different arg counts
        if (isStatic) {
          return handle.invokeWithArguments(args);
        } else {
          // For instance methods, prepend target to args
          Object[] fullArgs = new Object[args.length + 1];
          fullArgs[0] = target;
          System.arraycopy(args, 0, fullArgs, 1, args.length);
          return handle.invokeWithArguments(fullArgs);
        }
      }
      return method.invoke(target, args);
    }
  }

  /** Key for constructor cache lookup. */
  public static final class ConstructorCacheKey {
    private final Class<?> clazz;
    private final Class<?>[] argTypes;
    private final int hashCode;

    public ConstructorCacheKey(Class<?> clazz, Class<?>[] argTypes) {
      this.clazz = clazz;
      this.argTypes = argTypes;
      int h = clazz.hashCode();
      for (Class<?> t : argTypes) {
        h = h * 31 + (t != null ? t.hashCode() : 0);
      }
      this.hashCode = h;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ConstructorCacheKey other)) return false;
      if (clazz != other.clazz) return false;
      if (argTypes.length != other.argTypes.length) return false;
      for (int i = 0; i < argTypes.length; i++) {
        if (argTypes[i] != other.argTypes[i]) return false;
      }
      return true;
    }
  }

  /** Cached constructor resolution result. */
  public static final class CachedConstructor {
    public final java.lang.reflect.Constructor<?> constructor;
    public final boolean isVarArgs;
    public final int fixedArgCount;
    public final Class<?> varargComponentType;

    public CachedConstructor(java.lang.reflect.Constructor<?> constructor) {
      this.constructor = constructor;
      this.isVarArgs = constructor.isVarArgs();
      if (isVarArgs) {
        Class<?>[] paramTypes = constructor.getParameterTypes();
        this.fixedArgCount = paramTypes.length - 1;
        this.varargComponentType = paramTypes[fixedArgCount].getComponentType();
      } else {
        this.fixedArgCount = 0;
        this.varargComponentType = null;
      }
    }

    /** Prepare arguments for invocation, repacking varargs if needed. */
    public Object[] prepareArgs(Object[] args) {
      if (!isVarArgs) {
        return args;
      }
      Object[] newArgs = new Object[fixedArgCount + 1];
      for (int i = 0; i < fixedArgCount; i++) {
        newArgs[i] = args[i];
      }
      int varargCount = args.length - fixedArgCount;
      Object varargArray = java.lang.reflect.Array.newInstance(varargComponentType, varargCount);
      for (int i = 0; i < varargCount; i++) {
        java.lang.reflect.Array.set(varargArray, i, args[fixedArgCount + i]);
      }
      newArgs[fixedArgCount] = varargArray;
      return newArgs;
    }
  }
}
