package org.gatun.server;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks resource usage for a single session.
 *
 * <p>Each client session gets its own quota tracker that enforces limits and provides usage
 * statistics. The quota is thread-safe but designed for single-session use (one client = one thread
 * typically).
 */
public class SessionQuota {
  private final long sessionId;

  // Object tracking
  private final AtomicInteger objectCount = new AtomicInteger(0);
  private final AtomicInteger peakObjectCount = new AtomicInteger(0);
  private final AtomicLong totalObjectsCreated = new AtomicLong(0);

  // Arrow buffer tracking
  private final AtomicLong arrowBytesAllocated = new AtomicLong(0);
  private final AtomicLong peakArrowBytes = new AtomicLong(0);
  private final AtomicLong totalArrowBytesTransferred = new AtomicLong(0);

  // Activity tracking for idle timeout
  private volatile long lastActivityNanos = System.nanoTime();

  // Request tracking
  private final AtomicLong requestCount = new AtomicLong(0);

  public SessionQuota(long sessionId) {
    this.sessionId = sessionId;
  }

  // ========== OBJECT QUOTA ==========

  /**
   * Try to allocate space for a new object.
   *
   * @throws ResourceLimits.ObjectLimitExceededException if limit would be exceeded
   */
  public void allocateObject() {
    int current = objectCount.get();
    ResourceLimits.checkObjectLimit(current);

    int newCount = objectCount.incrementAndGet();
    totalObjectsCreated.incrementAndGet();

    // Update peak (may race, but approximate is fine)
    int peak = peakObjectCount.get();
    while (newCount > peak && !peakObjectCount.compareAndSet(peak, newCount)) {
      peak = peakObjectCount.get();
    }

    touchActivity();
  }

  /** Release an object slot. */
  public void releaseObject() {
    objectCount.decrementAndGet();
    touchActivity();
  }

  /** Get current object count. */
  public int getObjectCount() {
    return objectCount.get();
  }

  /** Get peak object count for this session. */
  public int getPeakObjectCount() {
    return peakObjectCount.get();
  }

  /** Get total objects created (including freed ones). */
  public long getTotalObjectsCreated() {
    return totalObjectsCreated.get();
  }

  // ========== ARROW BUFFER QUOTA ==========

  /**
   * Try to allocate Arrow buffer space.
   *
   * @param bytes Number of bytes to allocate
   * @throws ResourceLimits.ArrowBufferLimitExceededException if limit would be exceeded
   */
  public void allocateArrowBytes(long bytes) {
    long current = arrowBytesAllocated.get();
    ResourceLimits.checkArrowBufferLimit(current + bytes);

    long newTotal = arrowBytesAllocated.addAndGet(bytes);
    totalArrowBytesTransferred.addAndGet(bytes);

    // Update peak
    long peak = peakArrowBytes.get();
    while (newTotal > peak && !peakArrowBytes.compareAndSet(peak, newTotal)) {
      peak = peakArrowBytes.get();
    }

    touchActivity();
  }

  /** Release Arrow buffer space (e.g., on arena reset). */
  public void releaseArrowBytes(long bytes) {
    arrowBytesAllocated.addAndGet(-bytes);
    touchActivity();
  }

  /** Reset Arrow buffer quota (on arena reset). */
  public void resetArrowBytes() {
    arrowBytesAllocated.set(0);
    touchActivity();
  }

  /** Get current Arrow bytes allocated. */
  public long getArrowBytesAllocated() {
    return arrowBytesAllocated.get();
  }

  /** Get peak Arrow bytes for this session. */
  public long getPeakArrowBytes() {
    return peakArrowBytes.get();
  }

  /** Get total Arrow bytes transferred. */
  public long getTotalArrowBytesTransferred() {
    return totalArrowBytesTransferred.get();
  }

  // ========== ACTIVITY TRACKING ==========

  /** Update last activity timestamp. */
  public void touchActivity() {
    lastActivityNanos = System.nanoTime();
  }

  /** Get nanoseconds since last activity. */
  public long getIdleNanos() {
    return System.nanoTime() - lastActivityNanos;
  }

  /** Get milliseconds since last activity. */
  public long getIdleMs() {
    return getIdleNanos() / 1_000_000;
  }

  /** Check if session has timed out due to inactivity. */
  public boolean isTimedOut() {
    return ResourceLimits.isSessionTimedOut(lastActivityNanos);
  }

  // ========== REQUEST TRACKING ==========

  /** Record a request. */
  public void recordRequest() {
    requestCount.incrementAndGet();
    touchActivity();
  }

  /** Get request count. */
  public long getRequestCount() {
    return requestCount.get();
  }

  // ========== REPORTING ==========

  /** Get session ID. */
  public long getSessionId() {
    return sessionId;
  }

  /** Get a summary of quota usage for logging. */
  public String getSummary() {
    return String.format(
        "Session %d: objects=%d (peak=%d, total=%d), "
            + "arrow_bytes=%d (peak=%d, total=%d), requests=%d, idle_ms=%d",
        sessionId,
        objectCount.get(),
        peakObjectCount.get(),
        totalObjectsCreated.get(),
        arrowBytesAllocated.get(),
        peakArrowBytes.get(),
        totalArrowBytesTransferred.get(),
        requestCount.get(),
        getIdleMs());
  }

  @Override
  public String toString() {
    return getSummary();
  }
}
