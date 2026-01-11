package org.gatun.server;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * A thread-safe bounded cache with LRU eviction.
 *
 * <p>This cache evicts the least recently used entries when the size exceeds the maximum. Designed
 * for use in long-running processes like Spark drivers where unbounded caches can cause memory
 * issues with dynamically generated classes and methods.
 *
 * <p>Thread-safety is achieved with a read-write lock:
 *
 * <ul>
 *   <li>Reads (get) acquire read lock - allows concurrent reads
 *   <li>Writes (put, computeIfAbsent) acquire write lock - exclusive access
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class BoundedCache<K, V> {
  private final int maxSize;
  private final LinkedHashMap<K, V> map;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // Statistics for monitoring
  private long hits = 0;
  private long misses = 0;
  private long evictions = 0;

  /**
   * Create a bounded cache with the specified maximum size.
   *
   * @param maxSize maximum number of entries before eviction (must be > 0)
   */
  public BoundedCache(int maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("maxSize must be positive: " + maxSize);
    }
    this.maxSize = maxSize;
    // accessOrder=true makes it LRU (access order rather than insertion order)
    this.map =
        new LinkedHashMap<>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            boolean shouldRemove = size() > BoundedCache.this.maxSize;
            if (shouldRemove) {
              evictions++;
            }
            return shouldRemove;
          }
        };
  }

  /**
   * Get a value from the cache.
   *
   * @param key the key to look up
   * @return the cached value, or null if not present
   */
  public V get(K key) {
    lock.readLock().lock();
    try {
      V value = map.get(key);
      if (value != null) {
        hits++;
      } else {
        misses++;
      }
      return value;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Put a value into the cache.
   *
   * @param key the key
   * @param value the value
   */
  public void put(K key, V value) {
    lock.writeLock().lock();
    try {
      map.put(key, value);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get or compute a value if absent.
   *
   * <p>Note: The mapping function is called while holding the write lock. Keep computation
   * lightweight to avoid blocking other threads.
   *
   * @param key the key
   * @param mappingFunction function to compute the value if absent
   * @return the existing or computed value
   */
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    // First try read-only path
    lock.readLock().lock();
    try {
      V value = map.get(key);
      if (value != null) {
        hits++;
        return value;
      }
    } finally {
      lock.readLock().unlock();
    }

    // Need to compute - acquire write lock
    lock.writeLock().lock();
    try {
      // Double-check after acquiring write lock
      V value = map.get(key);
      if (value != null) {
        hits++;
        return value;
      }
      misses++;
      value = mappingFunction.apply(key);
      if (value != null) {
        map.put(key, value);
      }
      return value;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Get the current size of the cache. */
  public int size() {
    lock.readLock().lock();
    try {
      return map.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Clear all entries from the cache. */
  public void clear() {
    lock.writeLock().lock();
    try {
      map.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Get cache statistics as a formatted string. */
  public String getStats() {
    lock.readLock().lock();
    try {
      long total = hits + misses;
      double hitRate = total > 0 ? (100.0 * hits / total) : 0.0;
      return String.format(
          "size=%d/%d hits=%d misses=%d evictions=%d hitRate=%.1f%%",
          map.size(), maxSize, hits, misses, evictions, hitRate);
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Get the number of cache hits. */
  public long getHits() {
    lock.readLock().lock();
    try {
      return hits;
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Get the number of cache misses. */
  public long getMisses() {
    lock.readLock().lock();
    try {
      return misses;
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Get the number of evictions. */
  public long getEvictions() {
    lock.readLock().lock();
    try {
      return evictions;
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Get the maximum size of the cache. */
  public int getMaxSize() {
    return maxSize;
  }
}
