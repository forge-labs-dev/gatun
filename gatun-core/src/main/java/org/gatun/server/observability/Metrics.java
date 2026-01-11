package org.gatun.server.observability;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.gatun.protocol.Action;

/**
 * Collects and exposes metrics for monitoring Gatun server performance.
 *
 * <p>Metrics include:
 *
 * <ul>
 *   <li>Request counts and rates per action type
 *   <li>Latency percentiles (p50, p99) per action
 *   <li>Object registry counts
 *   <li>Arrow data transfer metrics
 *   <li>Callback metrics
 * </ul>
 *
 * <p>Uses lock-free data structures for minimal overhead during request processing.
 */
public class Metrics {
  private static final Metrics INSTANCE = new Metrics();

  // Request counts per action type
  private final Map<Byte, LongAdder> requestCounts = new ConcurrentHashMap<>();
  private final Map<Byte, LongAdder> errorCounts = new ConcurrentHashMap<>();

  // Latency tracking using HdrHistogram-style approach (simplified)
  // We use a two-level structure: per-action -> latency buckets
  private final Map<Byte, LatencyTracker> latencyTrackers = new ConcurrentHashMap<>();

  // Global counters
  private final LongAdder totalRequests = new LongAdder();
  private final LongAdder totalErrors = new LongAdder();
  private final LongAdder totalBytesCopied = new LongAdder();
  private final LongAdder totalArrowRows = new LongAdder();
  private final LongAdder totalCallbackInvocations = new LongAdder();
  private final LongAdder totalCallbackErrors = new LongAdder();

  // Object tracking
  private final AtomicLong currentObjectCount = new AtomicLong(0);
  private final AtomicLong peakObjectCount = new AtomicLong(0);
  private final AtomicLong currentSessionCount = new AtomicLong(0);

  // Timestamp for rate calculations
  private final long startTimeNanos = System.nanoTime();

  private Metrics() {}

  public static Metrics getInstance() {
    return INSTANCE;
  }

  /** Record a completed request. */
  public void recordRequest(byte action, long latencyMicros, boolean success, String errorType) {
    totalRequests.increment();
    requestCounts.computeIfAbsent(action, k -> new LongAdder()).increment();

    if (!success) {
      totalErrors.increment();
      errorCounts.computeIfAbsent(action, k -> new LongAdder()).increment();
    }

    // Track latency
    latencyTrackers.computeIfAbsent(action, k -> new LatencyTracker()).record(latencyMicros);
  }

  /** Record Arrow data transfer. */
  public void recordArrowTransfer(long numRows, long bytesCopied) {
    totalArrowRows.add(numRows);
    totalBytesCopied.add(bytesCopied);
  }

  /** Record a callback invocation. */
  public void recordCallback(boolean success) {
    totalCallbackInvocations.increment();
    if (!success) {
      totalCallbackErrors.increment();
    }
  }

  /** Update object count (called on create/free). */
  public void updateObjectCount(long delta) {
    long newCount = currentObjectCount.addAndGet(delta);
    // Update peak if needed (may have race, but approximate is fine)
    long peak = peakObjectCount.get();
    while (newCount > peak && !peakObjectCount.compareAndSet(peak, newCount)) {
      peak = peakObjectCount.get();
    }
  }

  /** Update session count. */
  public void updateSessionCount(long delta) {
    currentSessionCount.addAndGet(delta);
  }

  /** Get total request count. */
  public long getTotalRequests() {
    return totalRequests.sum();
  }

  /** Get total error count. */
  public long getTotalErrors() {
    return totalErrors.sum();
  }

  /** Get requests per second since start. */
  public double getRequestsPerSecond() {
    long elapsedNanos = System.nanoTime() - startTimeNanos;
    if (elapsedNanos <= 0) return 0;
    return totalRequests.sum() * 1_000_000_000.0 / elapsedNanos;
  }

  /** Get current object count. */
  public long getCurrentObjectCount() {
    return currentObjectCount.get();
  }

  /** Get peak object count. */
  public long getPeakObjectCount() {
    return peakObjectCount.get();
  }

  /** Get current session count. */
  public long getCurrentSessionCount() {
    return currentSessionCount.get();
  }

  /** Get latency percentile for an action. */
  public long getLatencyPercentile(byte action, double percentile) {
    LatencyTracker tracker = latencyTrackers.get(action);
    return tracker != null ? tracker.getPercentile(percentile) : 0;
  }

  /** Get request count for an action. */
  public long getRequestCount(byte action) {
    LongAdder counter = requestCounts.get(action);
    return counter != null ? counter.sum() : 0;
  }

  /** Get error count for an action. */
  public long getErrorCount(byte action) {
    LongAdder counter = errorCounts.get(action);
    return counter != null ? counter.sum() : 0;
  }

  /** Get Arrow rows transferred. */
  public long getTotalArrowRows() {
    return totalArrowRows.sum();
  }

  /** Get total bytes copied. */
  public long getTotalBytesCopied() {
    return totalBytesCopied.sum();
  }

  /** Get callback invocation count. */
  public long getTotalCallbackInvocations() {
    return totalCallbackInvocations.sum();
  }

  /** Get callback error count. */
  public long getTotalCallbackErrors() {
    return totalCallbackErrors.sum();
  }

  /** Format all metrics as a human-readable report. */
  public String report() {
    StringBuilder sb = new StringBuilder();
    sb.append("=== Gatun Server Metrics ===\n");

    // Global stats
    sb.append("\nGlobal:\n");
    sb.append(String.format("  total_requests: %d\n", getTotalRequests()));
    sb.append(String.format("  total_errors: %d\n", getTotalErrors()));
    sb.append(String.format("  requests_per_sec: %.2f\n", getRequestsPerSecond()));
    sb.append(String.format("  current_sessions: %d\n", getCurrentSessionCount()));
    sb.append(String.format("  current_objects: %d\n", getCurrentObjectCount()));
    sb.append(String.format("  peak_objects: %d\n", getPeakObjectCount()));

    // Arrow stats
    sb.append("\nArrow:\n");
    sb.append(String.format("  total_rows: %d\n", getTotalArrowRows()));
    sb.append(String.format("  total_bytes_copied: %d\n", getTotalBytesCopied()));

    // Callback stats
    sb.append("\nCallbacks:\n");
    sb.append(String.format("  total_invocations: %d\n", getTotalCallbackInvocations()));
    sb.append(String.format("  total_errors: %d\n", getTotalCallbackErrors()));

    // Per-action stats
    sb.append("\nPer-Action Latency (microseconds):\n");
    for (byte action : requestCounts.keySet()) {
      String actionName = getActionName(action);
      long count = getRequestCount(action);
      long errors = getErrorCount(action);
      long p50 = getLatencyPercentile(action, 0.50);
      long p99 = getLatencyPercentile(action, 0.99);

      sb.append(
          String.format(
              "  %s: count=%d errors=%d p50=%d p99=%d\n", actionName, count, errors, p50, p99));
    }

    return sb.toString();
  }

  /** Get action name for display. */
  private static String getActionName(byte action) {
    return switch (action) {
      case Action.CreateObject -> "CreateObject";
      case Action.FreeObject -> "FreeObject";
      case Action.InvokeMethod -> "InvokeMethod";
      case Action.InvokeStaticMethod -> "InvokeStaticMethod";
      case Action.GetField -> "GetField";
      case Action.SetField -> "SetField";
      case Action.GetStaticField -> "GetStaticField";
      case Action.SetStaticField -> "SetStaticField";
      case Action.SendArrowBatch -> "SendArrowBatch";
      case Action.SendArrowBuffers -> "SendArrowBuffers";
      case Action.GetArrowData -> "GetArrowData";
      case Action.ResetPayloadArena -> "ResetPayloadArena";
      case Action.RegisterCallback -> "RegisterCallback";
      case Action.UnregisterCallback -> "UnregisterCallback";
      case Action.Cancel -> "Cancel";
      case Action.IsInstanceOf -> "IsInstanceOf";
      case Action.Reflect -> "Reflect";
      case Action.GetFields -> "GetFields";
      case Action.InvokeMethods -> "InvokeMethods";
      case Action.CreateObjects -> "CreateObjects";
      case Action.Batch -> "Batch";
      case Action.GetMetrics -> "GetMetrics";
      default -> "Unknown(" + action + ")";
    };
  }

  /**
   * Simple latency tracker using bucketed histogram. Provides approximate percentiles with minimal
   * overhead.
   */
  private static class LatencyTracker {
    // Buckets: 0-10us, 10-100us, 100-1000us, 1-10ms, 10-100ms, 100ms-1s, >1s
    private static final long[] BUCKET_BOUNDS = {
      10, 100, 1000, 10_000, 100_000, 1_000_000, Long.MAX_VALUE
    };
    private final LongAdder[] buckets = new LongAdder[BUCKET_BOUNDS.length];
    private final LongAdder totalCount = new LongAdder();
    private final LongAdder totalSum = new LongAdder();

    LatencyTracker() {
      for (int i = 0; i < buckets.length; i++) {
        buckets[i] = new LongAdder();
      }
    }

    void record(long latencyMicros) {
      totalCount.increment();
      totalSum.add(latencyMicros);

      for (int i = 0; i < BUCKET_BOUNDS.length; i++) {
        if (latencyMicros < BUCKET_BOUNDS[i]) {
          buckets[i].increment();
          break;
        }
      }
    }

    long getPercentile(double percentile) {
      long total = totalCount.sum();
      if (total == 0) return 0;

      long target = (long) (total * percentile);
      long cumulative = 0;

      for (int i = 0; i < buckets.length; i++) {
        cumulative += buckets[i].sum();
        if (cumulative >= target) {
          // Return the upper bound of this bucket (approximate)
          return BUCKET_BOUNDS[i];
        }
      }
      return BUCKET_BOUNDS[BUCKET_BOUNDS.length - 1];
    }

    long getMean() {
      long count = totalCount.sum();
      return count > 0 ? totalSum.sum() / count : 0;
    }
  }

  /** Reset all metrics (for testing). */
  public void reset() {
    totalRequests.reset();
    totalErrors.reset();
    totalBytesCopied.reset();
    totalArrowRows.reset();
    totalCallbackInvocations.reset();
    totalCallbackErrors.reset();
    requestCounts.clear();
    errorCounts.clear();
    latencyTrackers.clear();
    currentObjectCount.set(0);
    peakObjectCount.set(0);
    currentSessionCount.set(0);
  }
}
