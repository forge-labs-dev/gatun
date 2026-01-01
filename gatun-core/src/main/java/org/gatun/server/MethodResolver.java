package org.gatun.server;

import org.gatun.server.ReflectionCache.CachedConstructor;
import org.gatun.server.ReflectionCache.CachedMethod;
import org.gatun.server.ReflectionCache.ConstructorCacheKey;
import org.gatun.server.ReflectionCache.MethodCacheKey;

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
          if (!isAssignable(paramTypes[i], argTypes[i])) {
            match = false;
            break;
          }
          // Score specificity: exact type match gets 10 points, Object param gets 0
          if (paramTypes[i] == argTypes[i]) {
            specificity += 10;
          } else if (paramTypes[i] != Object.class && paramTypes[i].isAssignableFrom(argTypes[i])) {
            // Specific type that accepts our arg gets 5 points
            specificity += 5;
          }
          // Object.class as param type gets 0 points (least specific)
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
          // Score: varargs gets lower priority (100 - fixedCount)
          int fixedCount = paramTypes.length - 1;
          int score = 100 - fixedCount;
          if (bestScore < score) {
            bestMatch = m;
            bestScore = score;
          }
        }
      }
    }

    if (bestMatch != null) {
      return bestMatch;
    }

    throw new NoSuchMethodException(
        "No matching method: " + name + " with " + argTypes.length + " args");
  }

  // ========== CONSTRUCTOR RESOLUTION ==========

  /** Result of constructor resolution. */
  public static final class ConstructorWithArgs {
    public final java.lang.reflect.Constructor<?> constructor;
    public final Object[] args;

    public ConstructorWithArgs(java.lang.reflect.Constructor<?> constructor, Object[] args) {
      this.constructor = constructor;
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
      return new ConstructorWithArgs(cached.constructor, cached.prepareArgs(args));
    }

    // Not in cache - do full resolution
    java.lang.reflect.Constructor<?> resolvedCtor = resolveConstructor(clazz, argTypes, args);

    // Cache the result
    CachedConstructor toCache = new CachedConstructor(resolvedCtor);
    ReflectionCache.cacheConstructor(cacheKey, toCache);

    return new ConstructorWithArgs(resolvedCtor, toCache.prepareArgs(args));
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
          int score = 100; // Varargs gets lower priority
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
        if (!isAssignable(paramTypes[i], argTypes[i])) {
          match = false;
          break;
        }
        // Score specificity: exact type match gets 10 points
        if (paramTypes[i] == argTypes[i]) {
          specificity += 10;
        } else if (paramTypes[i] != Object.class && paramTypes[i].isAssignableFrom(argTypes[i])) {
          specificity += 5;
        }
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
   */
  public static boolean isVarargsCompatible(Class<?>[] paramTypes, Class<?>[] argTypes) {
    int fixedCount = paramTypes.length - 1;
    Class<?> varargComponentType = paramTypes[fixedCount].getComponentType();

    // Check fixed parameters match
    for (int i = 0; i < fixedCount; i++) {
      if (!isAssignable(paramTypes[i], argTypes[i])) {
        return false;
      }
    }

    // Check vararg parameters match
    for (int i = fixedCount; i < argTypes.length; i++) {
      if (!isAssignable(varargComponentType, argTypes[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Try to match varargs method and repack arguments.
   *
   * @return repacked arguments if compatible, null otherwise
   */
  public static Object[] tryVarargsMatch(
      Class<?>[] paramTypes, Class<?>[] argTypes, Object[] args) {
    int fixedCount = paramTypes.length - 1;
    Class<?> varargComponentType = paramTypes[fixedCount].getComponentType();

    // Check fixed parameters match
    for (int i = 0; i < fixedCount; i++) {
      if (!isAssignable(paramTypes[i], argTypes[i])) {
        return null;
      }
    }

    // Check vararg parameters match
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

  /**
   * Check if argType can be assigned to paramType.
   *
   * <p>Handles:
   * <ul>
   *   <li>Standard Java assignability</li>
   *   <li>Object accepting anything</li>
   *   <li>Primitive widening conversions (int -> long -> double)</li>
   * </ul>
   */
  public static boolean isAssignable(Class<?> paramType, Class<?> argType) {
    if (paramType.isAssignableFrom(argType)) return true;
    // Handle Object accepting anything (used for String args and object refs)
    if (paramType == Object.class) return true;
    // Handle argType=Object matching any reference type (String args come as Object.class)
    if (argType == Object.class && !paramType.isPrimitive()) return true;
    // Handle primitive widening conversions (Java allows these implicitly)
    if (paramType == int.class && argType == int.class) return true;
    if (paramType == long.class && (argType == int.class || argType == long.class)) return true;
    // Allow int/long -> double widening (e.g., Math.pow(2, 10) should work)
    if (paramType == double.class
        && (argType == double.class
            || argType == float.class
            || argType == int.class
            || argType == long.class)) return true;
    if (paramType == float.class
        && (argType == float.class || argType == int.class || argType == long.class)) return true;
    if (paramType == boolean.class && argType == boolean.class) return true;
    return false;
  }

  /**
   * Check if result should be auto-converted (not wrapped as ObjectRef).
   */
  public static boolean isAutoConvertible(Object obj) {
    return obj instanceof String
        || obj instanceof Number
        || obj instanceof Boolean
        || obj instanceof Character
        || obj instanceof java.util.List
        || obj instanceof java.util.Map
        || obj.getClass().isArray();
  }
}
