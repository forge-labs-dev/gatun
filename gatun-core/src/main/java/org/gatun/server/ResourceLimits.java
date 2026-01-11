package org.gatun.server;

/**
 * Resource limits to prevent buggy clients from exhausting server resources.
 *
 * <p>All limits are configurable via system properties with sensible defaults. Limits are enforced
 * per-session to provide isolation between clients.
 *
 * <p>System properties:
 *
 * <ul>
 *   <li>gatun.limits.max_objects - Max objects per session (default: 100,000)
 *   <li>gatun.limits.max_string_length - Max string length in bytes (default: 10MB)
 *   <li>gatun.limits.max_list_entries - Max entries in list/map arguments (default: 100,000)
 *   <li>gatun.limits.max_nesting_depth - Max nesting depth for collections (default: 100)
 *   <li>gatun.limits.max_batch_size - Max commands in a batch (default: 10,000)
 *   <li>gatun.limits.max_method_calls - Max calls in invoke_methods (default: 10,000)
 *   <li>gatun.limits.session_idle_timeout_ms - Session idle timeout (default: 0 = disabled)
 *   <li>gatun.limits.max_arrow_bytes - Max Arrow buffer bytes per session (default: 1GB)
 * </ul>
 */
public final class ResourceLimits {

  // ========== OBJECT LIMITS ==========

  /** Maximum number of objects that can be held per session. */
  public static final int MAX_OBJECTS_PER_SESSION =
      Integer.getInteger("gatun.limits.max_objects", 100_000);

  // ========== STRING/COLLECTION LIMITS ==========

  /** Maximum string length in bytes (10 MB default). */
  public static final int MAX_STRING_LENGTH =
      Integer.getInteger("gatun.limits.max_string_length", 10 * 1024 * 1024);

  /** Maximum entries in a list or map argument. */
  public static final int MAX_LIST_ENTRIES =
      Integer.getInteger("gatun.limits.max_list_entries", 100_000);

  /** Maximum nesting depth for collections (prevents stack overflow). */
  public static final int MAX_NESTING_DEPTH =
      Integer.getInteger("gatun.limits.max_nesting_depth", 100);

  // ========== BATCH/VECTORIZED LIMITS ==========

  /** Maximum commands in a single batch. */
  public static final int MAX_BATCH_SIZE =
      Integer.getInteger("gatun.limits.max_batch_size", 10_000);

  /** Maximum method calls in invoke_methods. */
  public static final int MAX_METHOD_CALLS =
      Integer.getInteger("gatun.limits.max_method_calls", 10_000);

  /** Maximum objects to create in create_objects. */
  public static final int MAX_CREATE_OBJECTS =
      Integer.getInteger("gatun.limits.max_create_objects", 10_000);

  // ========== SESSION LIMITS ==========

  /** Session idle timeout in milliseconds (0 = disabled). */
  public static final long SESSION_IDLE_TIMEOUT_MS =
      Long.getLong("gatun.limits.session_idle_timeout_ms", 0);

  // ========== ARROW LIMITS ==========

  /** Maximum Arrow buffer bytes per session (1 GB default). */
  public static final long MAX_ARROW_BYTES_PER_SESSION =
      Long.getLong("gatun.limits.max_arrow_bytes", 1024L * 1024 * 1024);

  // ========== EXCEPTION CLASSES ==========

  /** Thrown when a resource limit is exceeded. */
  public static class ResourceLimitExceededException extends RuntimeException {
    public ResourceLimitExceededException(String message) {
      super(message);
    }
  }

  /** Thrown when session object limit is exceeded. */
  public static class ObjectLimitExceededException extends ResourceLimitExceededException {
    public ObjectLimitExceededException(int current, int max) {
      super(
          String.format(
              "Object limit exceeded: session has %d objects (max: %d). "
                  + "Free unused objects or increase gatun.limits.max_objects.",
              current, max));
    }
  }

  /** Thrown when string is too long. */
  public static class StringTooLongException extends ResourceLimitExceededException {
    public StringTooLongException(int length, int max) {
      super(
          String.format(
              "String too long: %d bytes (max: %d). "
                  + "Increase gatun.limits.max_string_length if needed.",
              length, max));
    }
  }

  /** Thrown when list/map has too many entries. */
  public static class CollectionTooLargeException extends ResourceLimitExceededException {
    public CollectionTooLargeException(int size, int max) {
      super(
          String.format(
              "Collection too large: %d entries (max: %d). "
                  + "Increase gatun.limits.max_list_entries if needed.",
              size, max));
    }
  }

  /** Thrown when nesting is too deep. */
  public static class NestingTooDeepException extends ResourceLimitExceededException {
    public NestingTooDeepException(int depth, int max) {
      super(
          String.format(
              "Nesting too deep: %d levels (max: %d). "
                  + "Flatten data structure or increase gatun.limits.max_nesting_depth.",
              depth, max));
    }
  }

  /** Thrown when batch is too large. */
  public static class BatchTooLargeException extends ResourceLimitExceededException {
    public BatchTooLargeException(int size, int max) {
      super(
          String.format(
              "Batch too large: %d commands (max: %d). "
                  + "Split into smaller batches or increase gatun.limits.max_batch_size.",
              size, max));
    }
  }

  /** Thrown when Arrow buffer limit is exceeded. */
  public static class ArrowBufferLimitExceededException extends ResourceLimitExceededException {
    public ArrowBufferLimitExceededException(long current, long max) {
      super(
          String.format(
              "Arrow buffer limit exceeded: %d bytes (max: %d). "
                  + "Reset arena or increase gatun.limits.max_arrow_bytes.",
              current, max));
    }
  }

  /** Thrown when session times out due to inactivity. */
  public static class SessionTimeoutException extends ResourceLimitExceededException {
    public SessionTimeoutException(long idleMs, long timeoutMs) {
      super(
          String.format(
              "Session timed out after %d ms idle (timeout: %d ms). "
                  + "Increase gatun.limits.session_idle_timeout_ms or keep session active.",
              idleMs, timeoutMs));
    }
  }

  // ========== VALIDATION METHODS ==========

  /** Check object count limit. */
  public static void checkObjectLimit(int currentCount) {
    if (currentCount >= MAX_OBJECTS_PER_SESSION) {
      throw new ObjectLimitExceededException(currentCount, MAX_OBJECTS_PER_SESSION);
    }
  }

  /** Check string length limit. */
  public static void checkStringLength(String s) {
    if (s != null && s.length() > MAX_STRING_LENGTH) {
      throw new StringTooLongException(s.length(), MAX_STRING_LENGTH);
    }
  }

  /** Check collection size limit. */
  public static void checkCollectionSize(int size) {
    if (size > MAX_LIST_ENTRIES) {
      throw new CollectionTooLargeException(size, MAX_LIST_ENTRIES);
    }
  }

  /** Check nesting depth limit. */
  public static void checkNestingDepth(int depth) {
    if (depth > MAX_NESTING_DEPTH) {
      throw new NestingTooDeepException(depth, MAX_NESTING_DEPTH);
    }
  }

  /** Check batch size limit. */
  public static void checkBatchSize(int size) {
    if (size > MAX_BATCH_SIZE) {
      throw new BatchTooLargeException(size, MAX_BATCH_SIZE);
    }
  }

  /** Check method calls limit. */
  public static void checkMethodCallsSize(int size) {
    if (size > MAX_METHOD_CALLS) {
      throw new BatchTooLargeException(size, MAX_METHOD_CALLS);
    }
  }

  /** Check create objects limit. */
  public static void checkCreateObjectsSize(int size) {
    if (size > MAX_CREATE_OBJECTS) {
      throw new BatchTooLargeException(size, MAX_CREATE_OBJECTS);
    }
  }

  /** Check Arrow buffer limit. */
  public static void checkArrowBufferLimit(long currentBytes) {
    if (currentBytes > MAX_ARROW_BYTES_PER_SESSION) {
      throw new ArrowBufferLimitExceededException(currentBytes, MAX_ARROW_BYTES_PER_SESSION);
    }
  }

  /** Check session idle timeout. Returns true if session should be terminated. */
  public static boolean isSessionTimedOut(long lastActivityNanos) {
    if (SESSION_IDLE_TIMEOUT_MS <= 0) return false;
    long idleMs = (System.nanoTime() - lastActivityNanos) / 1_000_000;
    return idleMs > SESSION_IDLE_TIMEOUT_MS;
  }

  /** Get a summary of current limits for logging. */
  public static String getSummary() {
    return String.format(
        "ResourceLimits: max_objects=%d, max_string=%d, max_list=%d, "
            + "max_nesting=%d, max_batch=%d, idle_timeout=%dms, max_arrow=%d",
        MAX_OBJECTS_PER_SESSION,
        MAX_STRING_LENGTH,
        MAX_LIST_ENTRIES,
        MAX_NESTING_DEPTH,
        MAX_BATCH_SIZE,
        SESSION_IDLE_TIMEOUT_MS,
        MAX_ARROW_BYTES_PER_SESSION);
  }

  private ResourceLimits() {} // Prevent instantiation
}
