package org.gatun.server.observability;

import java.util.ArrayList;
import java.util.List;

/**
 * Thread-safe ring buffer that stores the last N requests for a session.
 *
 * <p>On fatal errors, this buffer can be dumped to help diagnose what led to the failure.
 * Each session gets its own ring buffer, sized appropriately for debugging without
 * excessive memory overhead.
 */
public class RequestRingBuffer {
  /** Default capacity - stores last 64 requests per session. */
  public static final int DEFAULT_CAPACITY = 64;

  private final RequestRecord[] buffer;
  private final int capacity;
  private int writeIndex = 0;
  private int count = 0;

  /** A single recorded request with its outcome. */
  public record RequestRecord(
      long requestId,
      byte action,
      String actionName,
      String target,
      long latencyMicros,
      boolean success,
      String errorType,
      String errorMessage,
      long timestampNanos) {

    /** Format as a structured log line. */
    public String toLogLine() {
      StringBuilder sb = new StringBuilder();
      sb.append("[req=").append(requestId);
      sb.append(" action=").append(actionName);
      if (target != null && !target.isEmpty()) {
        sb.append(" target=").append(truncate(target, 80));
      }
      sb.append(" latency_us=").append(latencyMicros);
      if (!success) {
        sb.append(" ERROR=").append(errorType);
        if (errorMessage != null) {
          sb.append(" msg=").append(truncate(errorMessage, 100));
        }
      }
      sb.append("]");
      return sb.toString();
    }

    private static String truncate(String s, int maxLen) {
      if (s == null) return "";
      return s.length() <= maxLen ? s : s.substring(0, maxLen - 3) + "...";
    }
  }

  public RequestRingBuffer() {
    this(DEFAULT_CAPACITY);
  }

  public RequestRingBuffer(int capacity) {
    this.capacity = capacity;
    this.buffer = new RequestRecord[capacity];
  }

  /**
   * Record a completed request.
   *
   * @param ctx The request context (captures timing and action info)
   * @param success Whether the request succeeded
   * @param errorType Error class name if failed, null otherwise
   * @param errorMessage Error message if failed, null otherwise
   */
  public synchronized void record(
      RequestContext ctx, boolean success, String errorType, String errorMessage) {
    RequestRecord record =
        new RequestRecord(
            ctx.requestId(),
            ctx.action(),
            ctx.actionName(),
            ctx.target(),
            ctx.elapsedMicros(),
            success,
            errorType,
            errorMessage,
            System.nanoTime());

    buffer[writeIndex] = record;
    writeIndex = (writeIndex + 1) % capacity;
    if (count < capacity) {
      count++;
    }
  }

  /**
   * Get all recorded requests in chronological order.
   *
   * @return List of records from oldest to newest
   */
  public synchronized List<RequestRecord> getRecords() {
    List<RequestRecord> result = new ArrayList<>(count);
    if (count == 0) return result;

    // Calculate start index (oldest entry)
    int start = (count < capacity) ? 0 : writeIndex;

    for (int i = 0; i < count; i++) {
      int idx = (start + i) % capacity;
      if (buffer[idx] != null) {
        result.add(buffer[idx]);
      }
    }
    return result;
  }

  /**
   * Dump the ring buffer contents as a formatted string.
   * Useful for including in error reports or logs.
   */
  public synchronized String dump() {
    List<RequestRecord> records = getRecords();
    if (records.isEmpty()) {
      return "  (no requests recorded)";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("  Last ").append(records.size()).append(" requests:\n");
    for (RequestRecord r : records) {
      sb.append("    ").append(r.toLogLine()).append("\n");
    }
    return sb.toString();
  }

  /** Clear all recorded requests. */
  public synchronized void clear() {
    for (int i = 0; i < capacity; i++) {
      buffer[i] = null;
    }
    writeIndex = 0;
    count = 0;
  }

  /** Get the number of recorded requests. */
  public synchronized int size() {
    return count;
  }
}
