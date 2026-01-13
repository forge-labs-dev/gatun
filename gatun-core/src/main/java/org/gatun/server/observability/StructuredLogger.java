package org.gatun.server.observability;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Structured logger that outputs key-value pairs for easy parsing.
 *
 * <p>Log format: {@code [timestamp] [level] [sessionId=N requestId=N action=X target=Y latency_us=Z
 * ...]}
 *
 * <p>This format is:
 *
 * <ul>
 *   <li>Human readable for debugging
 *   <li>Easy to parse with grep/awk for ad-hoc analysis
 *   <li>Compatible with log aggregation systems (Splunk, ELK, etc.)
 * </ul>
 */
public class StructuredLogger {
  private static final Logger LOG = Logger.getLogger("org.gatun.server");

  /** Rate limit for ring buffer dumps: minimum interval in nanoseconds (1 second). */
  private static final long RING_BUFFER_DUMP_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

  /** Last time a ring buffer was dumped (global rate limit). */
  private static final AtomicLong lastRingBufferDumpNanos = new AtomicLong(0);

  /**
   * Client errors that don't warrant diagnostic ring buffer dumps. These are expected errors from
   * invalid client input, not server-side issues that need debugging context.
   */
  private static final Set<String> CLIENT_ERROR_TYPES =
      Set.of(
          "java.lang.NumberFormatException",
          "java.lang.IllegalArgumentException",
          "java.lang.SecurityException",
          "java.lang.NoSuchMethodException",
          "java.lang.NoSuchFieldException",
          "java.lang.ClassNotFoundException",
          "java.lang.IndexOutOfBoundsException",
          "java.lang.NullPointerException",
          "java.lang.ArrayIndexOutOfBoundsException",
          "java.lang.StringIndexOutOfBoundsException");

  /** Enable trace mode for verbose method resolution logging. */
  private static boolean traceMode =
      Boolean.parseBoolean(System.getProperty("gatun.trace", "false"));

  /** Enable metrics logging (periodic stats dump). */
  private static boolean metricsEnabled =
      Boolean.parseBoolean(System.getProperty("gatun.metrics", "false"));

  /** Check if trace mode is enabled. */
  public static boolean isTraceEnabled() {
    return traceMode;
  }

  /** Enable or disable trace mode. */
  public static void setTraceMode(boolean enabled) {
    traceMode = enabled;
    LOG.info("Trace mode " + (enabled ? "enabled" : "disabled"));
  }

  /** Check if metrics are enabled. */
  public static boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  /** Enable or disable metrics. */
  public static void setMetricsEnabled(boolean enabled) {
    metricsEnabled = enabled;
  }

  /** Log a completed request with structured fields. */
  public static void logRequest(RequestContext ctx, boolean success, String errorType) {
    if (!LOG.isLoggable(Level.FINE)) return;

    StringBuilder sb = new StringBuilder();
    sb.append("sessionId=").append(ctx.sessionId());
    sb.append(" requestId=").append(ctx.requestId());
    sb.append(" action=").append(ctx.actionName());

    if (ctx.target() != null && !ctx.target().isEmpty()) {
      sb.append(" target=").append(truncate(ctx.target(), 100));
    }

    sb.append(" latency_us=").append(ctx.elapsedMicros());

    if (!success) {
      sb.append(" status=ERROR");
      if (errorType != null) {
        sb.append(" errorType=").append(errorType);
      }
    } else {
      sb.append(" status=OK");
    }

    LOG.fine(sb.toString());
  }

  /** Log a callback invocation with structured fields. */
  public static void logCallback(
      long sessionId,
      long callbackId,
      String methodName,
      int argCount,
      long latencyMicros,
      boolean success,
      String errorMessage) {
    if (!LOG.isLoggable(Level.FINE)) return;

    StringBuilder sb = new StringBuilder();
    sb.append("sessionId=").append(sessionId);
    sb.append(" callbackId=").append(callbackId);
    sb.append(" method=").append(methodName);
    sb.append(" args=").append(argCount);
    sb.append(" latency_us=").append(latencyMicros);

    if (!success) {
      sb.append(" status=ERROR");
      if (errorMessage != null) {
        sb.append(" error=").append(truncate(errorMessage, 100));
      }
    } else {
      sb.append(" status=OK");
    }

    LOG.fine("CALLBACK " + sb.toString());
  }

  /** Log method resolution decision (only in trace mode). */
  public static void logMethodResolution(
      String className,
      String methodName,
      int candidateCount,
      String chosenMethod,
      int score,
      Class<?>[] argTypes) {
    if (!traceMode || !LOG.isLoggable(Level.FINER)) return;

    StringBuilder sb = new StringBuilder();
    sb.append("METHOD_RESOLUTION class=").append(className);
    sb.append(" method=").append(methodName);
    sb.append(" candidates=").append(candidateCount);
    sb.append(" chosen=").append(chosenMethod);
    sb.append(" score=").append(score);

    if (argTypes != null && argTypes.length > 0) {
      sb.append(" argTypes=[");
      for (int i = 0; i < argTypes.length; i++) {
        if (i > 0) sb.append(",");
        sb.append(argTypes[i] != null ? argTypes[i].getSimpleName() : "null");
      }
      sb.append("]");
    }

    LOG.finer(sb.toString());
  }

  /**
   * Log an error with context for debugging.
   *
   * <p>Ring buffer dumps are only performed for server errors (not client errors like
   * NumberFormatException) and are rate-limited to avoid flooding logs.
   *
   * @param sessionId The session ID
   * @param requestId The request ID
   * @param action The action being performed
   * @param target The target (class/method name)
   * @param errorType The fully qualified exception class name
   * @param errorMessage The error message
   * @param ringBuffer The request ring buffer (may be null)
   * @param isFatal Whether this error will end the session
   */
  public static void logError(
      long sessionId,
      long requestId,
      String action,
      String target,
      String errorType,
      String errorMessage,
      RequestRingBuffer ringBuffer,
      boolean isFatal) {
    // Determine if this is a client error (expected) vs server error (unexpected)
    boolean isClientError = CLIENT_ERROR_TYPES.contains(errorType);

    StringBuilder sb = new StringBuilder();
    sb.append("ERROR sessionId=").append(sessionId);
    sb.append(" requestId=").append(requestId);
    sb.append(" action=").append(action);
    if (target != null) {
      sb.append(" target=").append(truncate(target, 100));
    }
    sb.append(" error=").append(errorType);
    sb.append(" message=").append(truncate(errorMessage, 200));
    sb.append(" type=").append(isClientError ? "CLIENT" : "SERVER");

    // Client errors are logged at FINE level, server errors at WARNING
    if (isClientError) {
      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine(sb.toString());
      }
    } else {
      LOG.warning(sb.toString());
    }

    // Only dump ring buffer for:
    // 1. Server errors (not client errors)
    // 2. Fatal errors that will end the session
    // 3. Rate-limited to once per second to avoid log flooding
    boolean shouldDumpRingBuffer =
        ringBuffer != null
            && (!isClientError || isFatal)
            && LOG.isLoggable(Level.INFO)
            && tryAcquireRingBufferDumpPermit();

    if (shouldDumpRingBuffer) {
      LOG.info("Request history for session " + sessionId + ":\n" + ringBuffer.dump());
    }
  }

  /**
   * Try to acquire permission to dump ring buffer (rate-limited).
   *
   * @return true if dump is allowed, false if rate-limited
   */
  private static boolean tryAcquireRingBufferDumpPermit() {
    long now = System.nanoTime();
    long last = lastRingBufferDumpNanos.get();

    // Check if enough time has passed since last dump
    if (now - last < RING_BUFFER_DUMP_INTERVAL_NANOS) {
      return false;
    }

    // Try to update the timestamp (CAS to handle concurrent access)
    return lastRingBufferDumpNanos.compareAndSet(last, now);
  }

  /** Log session start. */
  public static void logSessionStart(long sessionId, String shmPath) {
    if (!LOG.isLoggable(Level.FINE)) return;
    LOG.fine("SESSION_START sessionId=" + sessionId + " shm=" + shmPath);
  }

  /** Log session end with summary. */
  public static void logSessionEnd(
      long sessionId, int objectCount, long requestCount, long durationMillis) {
    if (!LOG.isLoggable(Level.FINE)) return;

    StringBuilder sb = new StringBuilder();
    sb.append("SESSION_END sessionId=").append(sessionId);
    sb.append(" objects=").append(objectCount);
    sb.append(" requests=").append(requestCount);
    sb.append(" duration_ms=").append(durationMillis);

    LOG.fine(sb.toString());
  }

  /** Log Arrow data transfer. */
  public static void logArrowTransfer(
      long sessionId, String direction, long rowCount, long bytesCopied, boolean zeroCopy) {
    if (!LOG.isLoggable(Level.FINE)) return;

    StringBuilder sb = new StringBuilder();
    sb.append("ARROW sessionId=").append(sessionId);
    sb.append(" direction=").append(direction);
    sb.append(" rows=").append(rowCount);
    sb.append(" bytes=").append(bytesCopied);
    sb.append(" zeroCopy=").append(zeroCopy);

    LOG.fine(sb.toString());
  }

  /** Log object registry change. */
  public static void logObjectRegistry(
      long sessionId, long objectId, String operation, String objectType, int registrySize) {
    if (!LOG.isLoggable(Level.FINER)) return;

    StringBuilder sb = new StringBuilder();
    sb.append("OBJECT sessionId=").append(sessionId);
    sb.append(" objectId=").append(objectId);
    sb.append(" op=").append(operation);
    if (objectType != null) {
      sb.append(" type=").append(objectType);
    }
    sb.append(" registrySize=").append(registrySize);

    LOG.finer(sb.toString());
  }

  /** Dump metrics to log (called periodically if metrics enabled). */
  public static void dumpMetrics() {
    if (!metricsEnabled) return;

    String report = Metrics.getInstance().report();
    LOG.info(report);
  }

  private static String truncate(String s, int maxLen) {
    if (s == null) return "";
    return s.length() <= maxLen ? s : s.substring(0, maxLen - 3) + "...";
  }
}
