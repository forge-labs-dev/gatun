package org.gatun.server;

import org.gatun.server.ReflectionCache.CachedConstructor;
import org.gatun.server.ReflectionCache.CachedMethod;
import org.gatun.server.ReflectionCache.ConstructorCacheKey;
import org.gatun.server.ReflectionCache.MethodCacheKey;
import org.gatun.server.observability.GatunEvents;
import org.gatun.server.observability.StructuredLogger;

/**
 * Handles method and constructor resolution with overload matching.
 *
 * <p>This class provides:
 * <ul>
 *   <li>Method resolution with specificity scoring for overload selection</li>
 *   <li>Constructor resolution with similar scoring</li>
 *   <li>Varargs handling and argument repacking</li>
 *   <li>Type compatibility checking including primitive widening</li>
 * </ul>
 *
 * <p>All methods are static and thread-safe.
 */
public final class MethodResolver {

  private MethodResolver() {} // Prevent instantiation

  // ========== METHOD RESOLUTION ==========

  /** Result of method resolution with prepared arguments. */
  public static final class MethodWithArgs {
    public final CachedMethod cached;
    public final Object[] args;

    public MethodWithArgs(CachedMethod cached, Object[] args) {
      this.cached = cached;
      this.args = args;
    }
  }

  /**
   * Find a method by name and compatible argument types.
   *
   * <p>Uses caching for faster repeated lookups. Handles varargs repacking.
   */
  public static MethodWithArgs findMethodWithArgs(
      Class<?> clazz, String name, Class<?>[] argTypes, Object[] args)
      throws NoSuchMethodException {
    // Check cache first
    MethodCacheKey cacheKey = new MethodCacheKey(clazz, name, argTypes);
    CachedMethod cached = ReflectionCache.getCachedMethod(cacheKey);
    if (cached != null) {
      return new MethodWithArgs(cached, cached.prepareArgs(args));
    }

    // Not in cache - do full resolution
    java.lang.reflect.Method resolvedMethod = resolveMethod(clazz, name, argTypes, args);

    // Cache the result
    CachedMethod toCache = new CachedMethod(resolvedMethod);
    ReflectionCache.cacheMethod(cacheKey, toCache);

    return new MethodWithArgs(toCache, toCache.prepareArgs(args));
  }

  /**
   * Resolve method without caching (called on cache miss).
   *
   * <p>Uses specificity scoring to select the best matching overload:
   * <ul>
   *   <li>Non-varargs methods get higher priority (base 1000)</li>
   *   <li>Exact type matches get 10 points per parameter</li>
   *   <li>Compatible specific types get 5 points</li>
   *   <li>Object parameters get 0 points (least specific)</li>
   * </ul>
   */
  private static java.lang.reflect.Method resolveMethod(
      Class<?> clazz, String name, Class<?>[] argTypes, Object[] args)
      throws NoSuchMethodException {
    // First try exact match
    try {
      return clazz.getMethod(name, argTypes);
    } catch (NoSuchMethodException e) {
      // Fall through to search
    }

    // Search for compatible method - prioritize non-varargs over varargs
    // and methods with fewer fixed parameters
    java.lang.reflect.Method bestMatch = null;
    int bestScore = Integer.MIN_VALUE;

    for (java.lang.reflect.Method m : ReflectionCache.getCachedMethods(clazz)) {
      if (!m.getName().equals(name)) continue;
      Class<?>[] paramTypes = m.getParameterTypes();

      // Check for exact parameter count match (non-varargs only)
      if (!m.isVarArgs() && paramTypes.length == argTypes.length) {
        boolean match = true;
        int specificity = 0; // Higher = more specific type matches
        for (int i = 0; i < paramTypes.length; i++) {
          int typeScore = getTypeSpecificity(paramTypes[i], argTypes[i]);
          if (typeScore == 0) {
            // Check if still assignable (for edge cases)
            if (!isAssignable(paramTypes[i], argTypes[i])) {
              match = false;
              break;
            }
          }
          specificity += typeScore;
        }
        if (match) {
          // Score: base 1000 for non-varargs, plus specificity bonus
          int score = 1000 + specificity;
          if (bestScore < score) {
            bestMatch = m;
            bestScore = score;
          }
        }
      }

      // Check for varargs match
      if (m.isVarArgs() && argTypes.length >= paramTypes.length - 1) {
        if (isVarargsCompatible(paramTypes, argTypes)) {
          // Score varargs with specificity (but lower base than non-varargs)
          int fixedCount = paramTypes.length - 1;
          int score = getVarargsSpecificity(paramTypes, argTypes, fixedCount);
          if (bestScore < score) {
            bestMatch = m;
            bestScore = score;
          }
        }
      }
    }

    if (bestMatch != null) {
      // Emit trace logging and JFR event for method resolution
      if (StructuredLogger.isTraceEnabled()) {
        int candidateCount = countCandidates(clazz, name);
        String chosenMethod = formatMethod(bestMatch);
        StructuredLogger.logMethodResolution(
            clazz.getName(), name, candidateCount, chosenMethod, bestScore, argTypes);
        GatunEvents.emitMethodResolution(
            clazz.getName(), name, candidateCount, chosenMethod, bestScore, formatArgTypes(argTypes));
      }
      return bestMatch;
    }

    throw new NoSuchMethodException(
        "No matching method: " + name + " with " + argTypes.length + " args");
  }

  /** Count candidate methods with matching name (for tracing). */
  private static int countCandidates(Class<?> clazz, String name) {
    int count = 0;
    for (java.lang.reflect.Method m : ReflectionCache.getCachedMethods(clazz)) {
      if (m.getName().equals(name)) count++;
    }
    return count;
  }

  /** Format method signature for logging. */
  private static String formatMethod(java.lang.reflect.Method m) {
    StringBuilder sb = new StringBuilder();
    sb.append(m.getName()).append("(");
    Class<?>[] params = m.getParameterTypes();
    for (int i = 0; i < params.length; i++) {
      if (i > 0) sb.append(",");
      sb.append(params[i].getSimpleName());
      if (m.isVarArgs() && i == params.length - 1) {
        sb.setLength(sb.length() - 2); // Remove "[]"
        sb.append("...");
      }
    }
    sb.append(")");
    return sb.toString();
  }

  /** Format argument types for logging. */
  private static String formatArgTypes(Class<?>[] argTypes) {
    if (argTypes == null || argTypes.length == 0) return "[]";
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < argTypes.length; i++) {
      if (i > 0) sb.append(",");
      sb.append(argTypes[i] != null ? argTypes[i].getSimpleName() : "null");
    }
    sb.append("]");
    return sb.toString();
  }

  // ========== CONSTRUCTOR RESOLUTION ==========

  /** Result of constructor resolution with cached MethodHandle. */
  public static final class ConstructorWithArgs {
    public final CachedConstructor cached;
    public final Object[] args;

    public ConstructorWithArgs(CachedConstructor cached, Object[] args) {
      this.cached = cached;
      this.args = args;
    }
  }

  /**
   * Find a constructor with compatible argument types.
   *
   * <p>Uses caching for faster repeated lookups. Handles varargs repacking.
   */
  public static ConstructorWithArgs findConstructorWithArgs(
      Class<?> clazz, Class<?>[] argTypes, Object[] args) throws NoSuchMethodException {
    // Check cache first
    ConstructorCacheKey cacheKey = new ConstructorCacheKey(clazz, argTypes);
    CachedConstructor cached = ReflectionCache.getCachedConstructor(cacheKey);
    if (cached != null) {
      return new ConstructorWithArgs(cached, cached.prepareArgs(args));
    }

    // Not in cache - do full resolution
    java.lang.reflect.Constructor<?> resolvedCtor = resolveConstructor(clazz, argTypes, args);

    // Cache the result
    CachedConstructor toCache = new CachedConstructor(resolvedCtor);
    ReflectionCache.cacheConstructor(cacheKey, toCache);

    return new ConstructorWithArgs(toCache, toCache.prepareArgs(args));
  }

  /**
   * Resolve constructor without caching (called on cache miss).
   */
  private static java.lang.reflect.Constructor<?> resolveConstructor(
      Class<?> clazz, Class<?>[] argTypes, Object[] args) throws NoSuchMethodException {
    // First try exact match
    try {
      return clazz.getConstructor(argTypes);
    } catch (NoSuchMethodException e) {
      // Fall through to search
    }

    // Search for compatible constructor with specificity scoring
    java.lang.reflect.Constructor<?> bestMatch = null;
    int bestScore = Integer.MIN_VALUE;

    for (java.lang.reflect.Constructor<?> c : ReflectionCache.getCachedConstructors(clazz)) {
      Class<?>[] paramTypes = c.getParameterTypes();

      // Check for varargs match
      if (c.isVarArgs() && argTypes.length >= paramTypes.length - 1) {
        if (isVarargsCompatible(paramTypes, argTypes)) {
          int fixedCount = paramTypes.length - 1;
          int score = getVarargsSpecificity(paramTypes, argTypes, fixedCount);
          if (bestScore < score) {
            bestMatch = c;
            bestScore = score;
          }
        }
      }

      // Check for exact parameter count match
      if (paramTypes.length != argTypes.length) continue;

      boolean match = true;
      int specificity = 0;
      for (int i = 0; i < paramTypes.length; i++) {
        int typeScore = getTypeSpecificity(paramTypes[i], argTypes[i]);
        if (typeScore == 0) {
          if (!isAssignable(paramTypes[i], argTypes[i])) {
            match = false;
            break;
          }
        }
        specificity += typeScore;
      }
      if (match) {
        int score = 1000 + specificity;
        if (bestScore < score) {
          bestMatch = c;
          bestScore = score;
        }
      }
    }

    if (bestMatch != null) {
      return bestMatch;
    }

    throw new NoSuchMethodException(
        "No matching constructor for " + clazz.getName() + " with " + argTypes.length + " args");
  }

  // ========== VARARGS HELPERS ==========

  /**
   * Check if argTypes are compatible with varargs method/constructor params.
   *
   * <p>Handles two cases:
   * <ol>
   *   <li>"Packed" case: m(String... xs) called as m(new String[]{"a","b"}) - array passed directly</li>
   *   <li>"Spread" case: m(String... xs) called as m("a", "b") - individual args spread</li>
   * </ol>
   */
  public static boolean isVarargsCompatible(Class<?>[] paramTypes, Class<?>[] argTypes) {
    int fixedCount = paramTypes.length - 1;
    Class<?> varargArrayType = paramTypes[fixedCount];
    Class<?> varargComponentType = varargArrayType.getComponentType();

    // Check fixed parameters match
    for (int i = 0; i < fixedCount; i++) {
      if (!isAssignable(paramTypes[i], argTypes[i])) {
        return false;
      }
    }

    // Case 1: "Packed" - array passed directly to varargs parameter
    // e.g., m(String... xs) called with m(new String[]{"a","b"})
    if (argTypes.length == paramTypes.length) {
      Class<?> lastArgType = argTypes[fixedCount];
      if (isAssignable(varargArrayType, lastArgType)) {
        return true;
      }
    }

    // Case 2: "Spread" - individual args that need to be packed into array
    for (int i = fixedCount; i < argTypes.length; i++) {
      if (!isAssignable(varargComponentType, argTypes[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Calculate specificity score for varargs method/constructor.
   *
   * <p>Score components:
   * <ul>
   *   <li>Base: 100 (lower than non-varargs base of 1000)</li>
   *   <li>Fixed params: sum of getTypeSpecificity for each</li>
   *   <li>Varargs: specificity of packed array or spread elements</li>
   *   <li>Penalty: -1 per fixed param (prefer fewer fixed params on tie)</li>
   * </ul>
   */
  public static int getVarargsSpecificity(Class<?>[] paramTypes, Class<?>[] argTypes, int fixedCount) {
    int specificity = 0;

    // Score fixed parameters
    for (int i = 0; i < fixedCount; i++) {
      specificity += getTypeSpecificity(paramTypes[i], argTypes[i]);
    }

    Class<?> varargArrayType = paramTypes[fixedCount];
    Class<?> varargComponentType = varargArrayType.getComponentType();

    // Check if "packed" case (array passed directly)
    if (argTypes.length == paramTypes.length) {
      Class<?> lastArgType = argTypes[fixedCount];
      if (isAssignable(varargArrayType, lastArgType)) {
        // Packed: score the array type match
        specificity += getTypeSpecificity(varargArrayType, lastArgType);
        return 100 + specificity - fixedCount;
      }
    }

    // "Spread" case: score each vararg element
    for (int i = fixedCount; i < argTypes.length; i++) {
      specificity += getTypeSpecificity(varargComponentType, argTypes[i]);
    }

    return 100 + specificity - fixedCount;
  }

  /**
   * Try to match varargs method and repack arguments.
   *
   * <p>Handles two cases:
   * <ol>
   *   <li>"Packed" case: m(String... xs) called as m(new String[]{"a","b"}) - array passed directly</li>
   *   <li>"Spread" case: m(String... xs) called as m("a", "b") - individual args need repacking</li>
   * </ol>
   *
   * @return repacked arguments if compatible, null otherwise
   */
  public static Object[] tryVarargsMatch(
      Class<?>[] paramTypes, Class<?>[] argTypes, Object[] args) {
    int fixedCount = paramTypes.length - 1;
    Class<?> varargArrayType = paramTypes[fixedCount];
    Class<?> varargComponentType = varargArrayType.getComponentType();

    // Check fixed parameters match
    for (int i = 0; i < fixedCount; i++) {
      if (!isAssignable(paramTypes[i], argTypes[i])) {
        return null;
      }
    }

    // Case 1: "Packed" - array passed directly to varargs parameter
    // e.g., m(String... xs) called with m(new String[]{"a","b"})
    // In this case, pass args through without repacking
    if (argTypes.length == paramTypes.length) {
      Class<?> lastArgType = argTypes[fixedCount];
      if (isAssignable(varargArrayType, lastArgType)) {
        // Args are already in correct form, return as-is
        return args;
      }
    }

    // Case 2: "Spread" - individual args that need to be packed into array
    // Check vararg parameters match component type
    for (int i = fixedCount; i < argTypes.length; i++) {
      if (!isAssignable(varargComponentType, argTypes[i])) {
        return null;
      }
    }

    // Repack arguments: fixed args + varargs array
    Object[] newArgs = new Object[paramTypes.length];
    for (int i = 0; i < fixedCount; i++) {
      newArgs[i] = args[i];
    }

    // Create varargs array
    int varargCount = argTypes.length - fixedCount;
    Object varargArray = java.lang.reflect.Array.newInstance(varargComponentType, varargCount);
    for (int i = 0; i < varargCount; i++) {
      java.lang.reflect.Array.set(varargArray, i, args[fixedCount + i]);
    }
    newArgs[fixedCount] = varargArray;

    return newArgs;
  }

  // ========== TYPE CHECKING ==========

  // Boxing/unboxing pairs
  private static final java.util.Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER;
  private static final java.util.Map<Class<?>, Class<?>> WRAPPER_TO_PRIMITIVE;

  static {
    PRIMITIVE_TO_WRAPPER = new java.util.HashMap<>();
    PRIMITIVE_TO_WRAPPER.put(boolean.class, Boolean.class);
    PRIMITIVE_TO_WRAPPER.put(byte.class, Byte.class);
    PRIMITIVE_TO_WRAPPER.put(char.class, Character.class);
    PRIMITIVE_TO_WRAPPER.put(short.class, Short.class);
    PRIMITIVE_TO_WRAPPER.put(int.class, Integer.class);
    PRIMITIVE_TO_WRAPPER.put(long.class, Long.class);
    PRIMITIVE_TO_WRAPPER.put(float.class, Float.class);
    PRIMITIVE_TO_WRAPPER.put(double.class, Double.class);

    WRAPPER_TO_PRIMITIVE = new java.util.HashMap<>();
    for (var entry : PRIMITIVE_TO_WRAPPER.entrySet()) {
      WRAPPER_TO_PRIMITIVE.put(entry.getValue(), entry.getKey());
    }
  }

  /**
   * Check if argType can be assigned to paramType.
   *
   * <p>Handles (in order of preference for scoring):
   * <ol>
   *   <li>Exact match</li>
   *   <li>Boxing/unboxing (int <-> Integer)</li>
   *   <li>Primitive widening (byte -> short -> int -> long -> float -> double)</li>
   *   <li>Reference widening (subclass -> superclass/interface)</li>
   *   <li>Object accepts anything (except null to primitive)</li>
   * </ol>
   *
   * <p>Note: null (represented as Object.class with null value) cannot be assigned to primitives.
   */
  public static boolean isAssignable(Class<?> paramType, Class<?> argType) {
    // Exact match
    if (paramType == argType) return true;

    // Standard Java assignability (handles reference widening)
    if (paramType.isAssignableFrom(argType)) return true;

    // Boxing: primitive param accepts its wrapper
    if (paramType.isPrimitive()) {
      Class<?> wrapper = PRIMITIVE_TO_WRAPPER.get(paramType);
      if (wrapper != null && wrapper.isAssignableFrom(argType)) return true;
    }

    // Unboxing: wrapper param accepts its primitive
    if (!paramType.isPrimitive()) {
      Class<?> primitive = WRAPPER_TO_PRIMITIVE.get(paramType);
      if (primitive != null && primitive == argType) return true;
    }

    // Primitive widening conversions (JLS 5.1.2)
    if (paramType.isPrimitive() && argType.isPrimitive()) {
      return isPrimitiveWidening(paramType, argType);
    }

    // Boxed widening: Integer -> Long parameter (requires unbox + widen + box)
    // Java doesn't do this implicitly, but we support it for convenience
    Class<?> paramPrimitive = WRAPPER_TO_PRIMITIVE.get(paramType);
    Class<?> argPrimitive = WRAPPER_TO_PRIMITIVE.get(argType);
    if (paramPrimitive != null && argPrimitive != null) {
      if (isPrimitiveWidening(paramPrimitive, argPrimitive)) return true;
    }

    // Primitive param with boxed arg that can widen
    if (paramType.isPrimitive() && argPrimitive != null) {
      if (isPrimitiveWidening(paramType, argPrimitive)) return true;
    }

    // Boxed param with primitive arg that can widen
    if (paramPrimitive != null && argType.isPrimitive()) {
      if (isPrimitiveWidening(paramPrimitive, argType)) return true;
    }

    // Object accepts anything (primitives auto-box at runtime)
    if (paramType == Object.class) return true;

    // argType=Object (from null or untyped) matches any reference type
    // Note: null to primitive is handled by caller (convertArgument)
    if (argType == Object.class && !paramType.isPrimitive()) return true;

    return false;
  }

  /**
   * Check if primitive widening conversion is valid (JLS 5.1.2).
   * Widening: byte -> short -> int -> long -> float -> double
   *           char -> int -> long -> float -> double
   */
  private static boolean isPrimitiveWidening(Class<?> to, Class<?> from) {
    if (to == from) return true;

    // byte widens to short, int, long, float, double
    if (from == byte.class) {
      return to == short.class || to == int.class || to == long.class
          || to == float.class || to == double.class;
    }
    // short widens to int, long, float, double
    if (from == short.class) {
      return to == int.class || to == long.class || to == float.class || to == double.class;
    }
    // char widens to int, long, float, double
    if (from == char.class) {
      return to == int.class || to == long.class || to == float.class || to == double.class;
    }
    // int widens to long, float, double
    if (from == int.class) {
      return to == long.class || to == float.class || to == double.class;
    }
    // long widens to float, double
    if (from == long.class) {
      return to == float.class || to == double.class;
    }
    // float widens to double
    if (from == float.class) {
      return to == double.class;
    }
    return false;
  }

  /**
   * Calculate specificity score for a type match. Higher = more specific = preferred.
   *
   * <p>Scoring (based on Java method resolution order):
   * <ul>
   *   <li>100: Exact match</li>
   *   <li>90: Boxing/unboxing match</li>
   *   <li>80: Primitive widening (closer types score higher)</li>
   *   <li>70: Boxed type widening</li>
   *   <li>50: Specific interface/superclass match</li>
   *   <li>10: Object parameter (least specific)</li>
   *   <li>0: No match</li>
   * </ul>
   */
  public static int getTypeSpecificity(Class<?> paramType, Class<?> argType) {
    // Exact match - highest priority
    if (paramType == argType) return 100;

    // Boxing/unboxing - second highest
    if (paramType.isPrimitive()) {
      Class<?> wrapper = PRIMITIVE_TO_WRAPPER.get(paramType);
      if (wrapper == argType) return 90;
    }
    if (!paramType.isPrimitive()) {
      Class<?> primitive = WRAPPER_TO_PRIMITIVE.get(paramType);
      if (primitive == argType) return 90;
    }

    // Primitive widening - score based on "distance"
    if (paramType.isPrimitive() && argType.isPrimitive() && isPrimitiveWidening(paramType, argType)) {
      return 80 - getPrimitiveWideningDistance(paramType, argType);
    }

    // Boxed widening
    Class<?> paramPrimitive = WRAPPER_TO_PRIMITIVE.get(paramType);
    Class<?> argPrimitive = WRAPPER_TO_PRIMITIVE.get(argType);
    if (paramPrimitive != null && argPrimitive != null && isPrimitiveWidening(paramPrimitive, argPrimitive)) {
      return 70 - getPrimitiveWideningDistance(paramPrimitive, argPrimitive);
    }

    // Mixed primitive/boxed widening
    if (paramType.isPrimitive() && argPrimitive != null && isPrimitiveWidening(paramType, argPrimitive)) {
      return 75 - getPrimitiveWideningDistance(paramType, argPrimitive);
    }
    if (paramPrimitive != null && argType.isPrimitive() && isPrimitiveWidening(paramPrimitive, argType)) {
      return 75 - getPrimitiveWideningDistance(paramPrimitive, argType);
    }

    // Reference type hierarchy
    if (paramType.isAssignableFrom(argType)) {
      if (paramType == Object.class) return 10; // Object is least specific
      if (paramType.isInterface()) return 50;   // Interface match
      return 60; // Superclass match
    }

    // Object parameter accepts primitives (auto-boxing at runtime)
    if (paramType == Object.class && argType.isPrimitive()) {
      return 10; // Same as Object accepting reference types
    }

    // Object.class arg (null or untyped) to reference type
    if (argType == Object.class && !paramType.isPrimitive()) {
      return paramType == Object.class ? 10 : 30;
    }

    return 0; // No match
  }

  /**
   * Get the "distance" for primitive widening (used to prefer closer conversions).
   * Lower distance = less widening = more preferred.
   */
  private static int getPrimitiveWideningDistance(Class<?> to, Class<?> from) {
    // Order: byte(0) < short(1) < int(2) < long(3) < float(4) < double(5)
    // char is treated as level 2 (same as int for widening purposes)
    int fromLevel = getPrimitiveLevel(from);
    int toLevel = getPrimitiveLevel(to);
    return toLevel - fromLevel;
  }

  private static int getPrimitiveLevel(Class<?> type) {
    if (type == byte.class) return 0;
    if (type == short.class) return 1;
    if (type == char.class) return 2;
    if (type == int.class) return 2;
    if (type == long.class) return 3;
    if (type == float.class) return 4;
    if (type == double.class) return 5;
    return -1;
  }

  /**
   * Check if result should be auto-converted (not wrapped as ObjectRef).
   *
   * <p>Note: Only primitive arrays (int[], double[], etc.) are auto-converted.
   * Object arrays (String[], ArrayList[], etc.) are kept as ObjectRef so they
   * can be manipulated in Java via Array.set/get. This is necessary because
   * auto-converting an Object[] copies its contents to Python, and subsequent
   * Array.set calls would modify a temporary copy instead of the original.
   */
  public static boolean isAutoConvertible(Object obj) {
    if (obj instanceof String
        || obj instanceof Number
        || obj instanceof Boolean
        || obj instanceof Character
        || obj instanceof java.util.List
        || obj instanceof java.util.Map) {
      return true;
    }
    // Only auto-convert primitive arrays, not Object arrays
    if (obj.getClass().isArray()) {
      Class<?> componentType = obj.getClass().getComponentType();
      return componentType.isPrimitive();
    }
    return false;
  }
}
