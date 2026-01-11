package org.gatun.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OwnershipTransferResult;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.gatun.protocol.ArrowBatchDescriptor;
import org.gatun.protocol.BufferDescriptor;
import org.gatun.protocol.FieldNode;

/**
 * Handles Arrow memory operations for zero-copy data transfer.
 *
 * <p>This class manages:
 *
 * <ul>
 *   <li>Arrow IPC batch reading from shared memory
 *   <li>Zero-copy buffer reconstruction from buffer descriptors using VectorLoader
 *   <li>Schema caching for repeated transfers
 * </ul>
 */
public class ArrowMemoryHandler {
  private static final Logger LOG = Logger.getLogger(ArrowMemoryHandler.class.getName());

  private final BufferAllocator allocator;

  // Schema cache: hash -> parsed Schema object
  // We store hashes as provided by Python to maintain hash consistency across roundtrips.
  // Python and Java may serialize schemas differently, producing different bytes, so we
  // must use Python's hash (computed from Python-serialized bytes) in both directions.
  private final Map<Long, Schema> schemaCache = new HashMap<>();

  // Reverse lookup: Schema -> hash (for finding the canonical hash of a schema)
  // Uses schema.equals() for comparison, which checks field names/types, not serialized bytes
  private final Map<Schema, Long> schemaToHash = new HashMap<>();

  // Current VectorSchemaRoot from buffer reconstruction (kept for reference until reset)
  private VectorSchemaRoot currentRoot = null;

  // VectorLoader for efficient batch loading
  private VectorLoader vectorLoader = null;

  // Arena epoch for lifetime safety - incremented on each reset
  private long arenaEpoch = 0;

  public ArrowMemoryHandler(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /** Get the current arena epoch. */
  public long getArenaEpoch() {
    return arenaEpoch;
  }

  /**
   * ReferenceManager for foreign memory (shared memory owned by Python).
   *
   * <p>This manager implements proper reference counting so Arrow's lifecycle management works
   * normally. The actual memory is never freed since Python owns it, but reference counting allows
   * Arrow objects to clean up properly.
   *
   * <p>A single manager is created per batch/payload segment and shared by all buffers in that
   * batch to reduce object allocation overhead.
   */
  private static class ForeignMemoryReferenceManager implements ReferenceManager {
    private final java.util.concurrent.atomic.AtomicInteger refCnt =
        new java.util.concurrent.atomic.AtomicInteger(1);
    private final BufferAllocator allocator;
    private final long capacity;

    /**
     * Create a reference manager for foreign memory.
     *
     * @param capacity Total capacity of the memory region
     * @param allocator Parent allocator (for compatibility, not used for allocation)
     */
    ForeignMemoryReferenceManager(long capacity, BufferAllocator allocator) {
      this.capacity = capacity;
      this.allocator = allocator;
    }

    @Override
    public int getRefCount() {
      return refCnt.get();
    }

    @Override
    public boolean release() {
      return release(1);
    }

    @Override
    public boolean release(int decrement) {
      if (decrement < 1) {
        throw new IllegalArgumentException("decrement must be >= 1");
      }
      int newRefCnt = refCnt.addAndGet(-decrement);
      if (newRefCnt < 0) {
        throw new IllegalStateException("Reference count went negative: " + newRefCnt);
      }
      // Return true when refcount hits zero (signals "released")
      // We don't actually free memory since Python owns it
      return newRefCnt == 0;
    }

    @Override
    public void retain() {
      retain(1);
    }

    @Override
    public void retain(int increment) {
      if (increment < 1) {
        throw new IllegalArgumentException("increment must be >= 1");
      }
      int oldRefCnt = refCnt.getAndAdd(increment);
      if (oldRefCnt < 1) {
        throw new IllegalStateException("Cannot retain after release: refCnt was " + oldRefCnt);
      }
    }

    @Override
    public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
      // Retain the buffer and return it
      // VectorLoader calls this when loading buffers into vectors
      retain();
      return srcBuffer;
    }

    @Override
    public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, long index, long length) {
      // Create a derived buffer pointing to a slice of the same memory
      // The derived buffer shares this reference manager
      long newAddress = sourceBuffer.memoryAddress() + index;
      retain(); // Derived buffer holds a reference
      return new ArrowBuf(this, null, length, newAddress);
    }

    @Override
    public OwnershipTransferResult transferOwnership(
        ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
      // Foreign memory cannot transfer ownership
      throw new UnsupportedOperationException("Cannot transfer ownership of foreign memory");
    }

    @Override
    public BufferAllocator getAllocator() {
      return allocator;
    }

    @Override
    public long getSize() {
      return capacity;
    }

    @Override
    public long getAccountedSize() {
      // Not accounted in any allocator since Python owns it
      return 0;
    }
  }

  /**
   * Process an Arrow IPC stream from shared memory.
   *
   * @param payloadSlice Memory segment containing the Arrow IPC data
   * @return Number of rows in the batch
   */
  public int processArrowIpcBatch(MemorySegment payloadSlice) throws Exception {
    ByteBuffer arrowBuf = payloadSlice.asByteBuffer();

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new GatunServer.ByteBufferInputStream(arrowBuf), allocator)) {

      if (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        int rows = root.getRowCount();
        LOG.fine("Arrow IPC batch received: " + rows + " rows");
        return rows;
      } else {
        throw new RuntimeException("Arrow Stream Empty");
      }
    }
  }

  /**
   * Process zero-copy Arrow buffers from buffer descriptors.
   *
   * <p>This method receives buffer locations in payload shared memory and reconstructs Arrow
   * vectors using VectorLoader (zero-copy on Java side).
   *
   * <p><b>Epoch validation:</b> The descriptor's arena_epoch must match our current arenaEpoch. A
   * mismatch indicates the sender is using stale buffer offsets from before a reset, which would
   * cause data corruption.
   *
   * @param batchDesc The Arrow batch descriptor containing buffer locations
   * @param payloadShm Memory segment for the payload shared memory
   * @return Number of rows in the batch
   * @throws IllegalStateException if epoch doesn't match (use-after-reset)
   */
  public long processArrowBuffers(ArrowBatchDescriptor batchDesc, MemorySegment payloadShm) {
    // Validate epoch to prevent use-after-reset corruption
    long descriptorEpoch = batchDesc.arenaEpoch();
    if (descriptorEpoch != arenaEpoch) {
      throw new IllegalStateException(
          String.format(
              "Arrow batch epoch mismatch: descriptor has epoch %d, but current epoch is %d. "
                  + "This usually means the sender is using stale buffer offsets from before a reset.",
              descriptorEpoch, arenaEpoch));
    }

    long schemaHash = batchDesc.schemaHash();
    long numRows = batchDesc.numRows();
    int numNodes = batchDesc.nodesLength();
    int numBuffers = batchDesc.buffersLength();

    LOG.fine(
        String.format(
            "Arrow buffers received: schema_hash=%d, rows=%d, nodes=%d, buffers=%d, epoch=%d",
            schemaHash, numRows, numNodes, numBuffers, descriptorEpoch));

    // 1. Get or deserialize schema
    Schema schema = schemaCache.get(schemaHash);
    if (schema == null && batchDesc.schemaBytesLength() > 0) {
      byte[] schemaBytes = new byte[batchDesc.schemaBytesLength()];
      for (int i = 0; i < schemaBytes.length; i++) {
        schemaBytes[i] = batchDesc.schemaBytes(i);
      }
      try {
        schema = deserializeSchema(schemaBytes);
        schemaCache.put(schemaHash, schema);
        // Also store reverse lookup: Schema -> hash (using Python's hash for consistency)
        schemaToHash.put(schema, schemaHash);
        LOG.fine(
            "Cached schema with hash: " + schemaHash + ", fields: " + schema.getFields().size());
      } catch (IOException e) {
        LOG.warning("Failed to deserialize schema: " + e.getMessage());
        return numRows;
      }
    }

    if (schema == null) {
      LOG.warning("No schema available for hash: " + schemaHash);
      return numRows;
    }

    // 2. Validate node and buffer counts against schema expectations
    // This catches protocol errors early before VectorLoader produces corrupted data
    int expectedNodes = countExpectedNodes(schema);
    int expectedBuffers = countExpectedBuffers(schema);

    if (numNodes != expectedNodes) {
      throw new IllegalStateException(
          String.format(
              "Node count mismatch: received %d nodes but schema expects %d nodes. "
                  + "Schema has %d fields. This indicates a protocol error.",
              numNodes, expectedNodes, schema.getFields().size()));
    }

    if (numBuffers != expectedBuffers) {
      throw new IllegalStateException(
          String.format(
              "Buffer count mismatch: received %d buffers but schema expects %d buffers. "
                  + "This indicates a protocol error or unsupported type.",
              numBuffers, expectedBuffers));
    }

    // 3. Get base address of payload shared memory
    long baseAddress = payloadShm.address();

    // 3. Create a single shared ReferenceManager for all buffers in this batch
    // This reduces object allocation and properly represents the shared memory region
    ReferenceManager sharedRefManager =
        new ForeignMemoryReferenceManager(payloadShm.byteSize(), allocator);

    // 4. Create ArrowBuf wrappers for each buffer descriptor
    // Note: Absent buffers (e.g., no validity bitmap when no nulls) are represented
    // as zero-length buffers. We still create ArrowBuf objects for them because
    // VectorLoader expects the exact buffer count from TypeLayout and doesn't handle
    // null/absent buffers well. See: https://issues.apache.org/jira/browse/ARROW-8803
    List<ArrowBuf> arrowBuffers = new ArrayList<>();
    for (int i = 0; i < numBuffers; i++) {
      BufferDescriptor bufDesc = batchDesc.buffers(i);
      long offset = bufDesc.offset();
      long length = bufDesc.length();

      // Create ArrowBuf backed by the shared memory region
      // All buffers share the same reference manager
      // For zero-length buffers, we still create a valid ArrowBuf (address doesn't matter)
      long bufferAddress = baseAddress + offset;
      // Retain for each additional buffer (first buffer uses initial refcount of 1)
      if (i > 0) {
        sharedRefManager.retain();
      }
      ArrowBuf arrowBuf = new ArrowBuf(sharedRefManager, null, length, bufferAddress);
      arrowBuffers.add(arrowBuf);

      LOG.finest(
          String.format(
              "  Buffer %d: offset=%d, length=%d, address=0x%x", i, offset, length, bufferAddress));
    }

    // 5. Build ArrowFieldNode list from FieldNode descriptors
    List<ArrowFieldNode> fieldNodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      FieldNode node = batchDesc.nodes(i);
      fieldNodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
      LOG.finest(
          String.format("  Node %d: length=%d, null_count=%d", i, node.length(), node.nullCount()));
    }

    // 6. Create ArrowRecordBatch from buffers and field nodes
    ArrowRecordBatch recordBatch = new ArrowRecordBatch((int) numRows, fieldNodes, arrowBuffers);

    // 7. Create or reuse VectorSchemaRoot and load with VectorLoader
    if (currentRoot != null) {
      currentRoot.close();
    }
    currentRoot = VectorSchemaRoot.create(schema, allocator);
    vectorLoader = new VectorLoader(currentRoot);
    vectorLoader.load(recordBatch);

    LOG.fine(
        "Loaded VectorSchemaRoot with "
            + schema.getFields().size()
            + " fields, "
            + numRows
            + " rows using VectorLoader");

    // 8. Verify by reading sample data (for debugging)
    if (LOG.isLoggable(java.util.logging.Level.FINE) && currentRoot.getRowCount() > 0) {
      StringBuilder verification = new StringBuilder();
      verification.append("Sample data: ");
      for (int i = 0; i < Math.min(schema.getFields().size(), 2); i++) {
        String fieldName = schema.getFields().get(i).getName();
        Object firstVal = currentRoot.getVector(fieldName).getObject(0);
        verification.append(String.format("%s[0]=%s ", fieldName, firstVal));
      }
      LOG.fine(verification.toString());
    }

    return numRows;
  }

  /** Get the current VectorSchemaRoot (if any). */
  public VectorSchemaRoot getCurrentRoot() {
    return currentRoot;
  }

  /** Deserialize an Arrow schema from IPC stream bytes. */
  private Schema deserializeSchema(byte[] schemaBytes) throws IOException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(schemaBytes);
        ReadChannel channel = new ReadChannel(Channels.newChannel(bais))) {
      return MessageSerializer.deserializeSchema(channel);
    }
  }

  /**
   * Reset the handler, clearing any cached state.
   *
   * <p>Called when the Python side resets the payload arena.
   */
  public void reset() {
    // Close current root if any - the buffers it references are about to be overwritten
    if (currentRoot != null) {
      currentRoot.close();
      currentRoot = null;
    }
    vectorLoader = null;
    // Increment epoch to invalidate any tables referencing old buffers
    arenaEpoch++;
    // Note: We keep the schema cache since schemas are typically stable
    LOG.fine("Arrow memory handler reset, epoch now " + arenaEpoch);
  }

  /** Clear the schema cache. */
  public void clearSchemaCache() {
    schemaCache.clear();
    LOG.fine("Schema cache cleared");
  }

  /** Check if a schema is cached. */
  public boolean hasSchema(long schemaHash) {
    return schemaCache.containsKey(schemaHash);
  }

  /** Get a cached schema. */
  public Schema getSchema(long schemaHash) {
    return schemaCache.get(schemaHash);
  }

  // --- Java -> Python Arrow Transfer ---

  /**
   * Result of writing Arrow data to the payload zone. Contains buffer descriptors and field nodes
   * for the protocol message.
   */
  public static class ArrowWriteResult {
    public final long schemaHash;
    public final byte[] schemaBytes; // null if schema was cached
    public final long numRows;
    public final List<long[]> bufferDescriptors; // List of [offset, length]
    public final List<long[]> fieldNodes; // List of [length, nullCount]
    public final long totalBytesWritten;
    public final long arenaEpoch; // Epoch for lifetime safety

    ArrowWriteResult(
        long schemaHash,
        byte[] schemaBytes,
        long numRows,
        List<long[]> bufferDescriptors,
        List<long[]> fieldNodes,
        long totalBytesWritten,
        long arenaEpoch) {
      this.schemaHash = schemaHash;
      this.schemaBytes = schemaBytes;
      this.numRows = numRows;
      this.bufferDescriptors = bufferDescriptors;
      this.fieldNodes = fieldNodes;
      this.totalBytesWritten = totalBytesWritten;
      this.arenaEpoch = arenaEpoch;
    }
  }

  /**
   * Write the current VectorSchemaRoot to the payload shared memory zone.
   *
   * <p>This method uses VectorUnloader to extract buffers and writes them to the payload zone with
   * 64-byte alignment. It returns buffer descriptors that can be sent to Python for zero-copy
   * reconstruction.
   *
   * <p><b>PAYLOAD ZONE LAYOUT:</b> The payload zone is logically divided into two halves:
   *
   * <ul>
   *   <li>First half [0, size/2): Used by Python -> Java transfers
   *   <li>Second half [size/2, size): Used by Java -> Python transfers
   * </ul>
   *
   * This avoids overwriting source data during bidirectional transfers. See ArrowBatchDescriptor in
   * commands.fbs for protocol documentation.
   *
   * @param root The VectorSchemaRoot containing the data to write
   * @param payloadShm Memory segment for the payload shared memory
   * @param pythonSchemaCache Set of schema hashes that Python has cached
   * @return ArrowWriteResult containing buffer descriptors and metadata
   */
  public ArrowWriteResult writeArrowBuffers(
      VectorSchemaRoot root, MemorySegment payloadShm, java.util.Set<Long> pythonSchemaCache) {

    Schema schema = root.getSchema();

    // Try to use Python's cached hash for consistency.
    // Python and Java may serialize schemas differently, producing different hashes.
    // By using Python's original hash, we ensure cache lookups work correctly.
    Long cachedHash = schemaToHash.get(schema);
    long schemaHash;
    if (cachedHash != null) {
      schemaHash = cachedHash;
      LOG.fine("Using cached Python hash for schema: " + schemaHash);
    } else {
      // Fallback to computing our own hash (first time seeing this schema from Java side)
      schemaHash = computeSchemaHash(schema);
      LOG.fine("Computing new hash for schema: " + schemaHash);
    }

    // Check if Python has this schema cached
    byte[] schemaBytes = null;
    if (pythonSchemaCache == null || !pythonSchemaCache.contains(schemaHash)) {
      schemaBytes = serializeSchema(schema);
      if (pythonSchemaCache != null) {
        pythonSchemaCache.add(schemaHash);
      }
    }

    // Use VectorUnloader to extract the record batch
    VectorUnloader unloader = new VectorUnloader(root);
    ArrowRecordBatch recordBatch = unloader.getRecordBatch();

    try {
      List<long[]> bufferDescriptors = new ArrayList<>();
      List<long[]> fieldNodes = new ArrayList<>();

      // Extract field nodes
      for (ArrowFieldNode node : recordBatch.getNodes()) {
        fieldNodes.add(new long[] {node.getLength(), node.getNullCount()});
      }

      // Write buffers to payload shm with 64-byte alignment
      // Java -> Python: write to SECOND HALF of payload zone [size/2, size)
      // This avoids overwriting Python -> Java data in the first half
      long halfPayloadSize = payloadShm.byteSize() / 2;
      long currentOffset = halfPayloadSize;

      for (ArrowBuf buffer : recordBatch.getBuffers()) {
        long length = buffer.readableBytes();

        // Align offset to 64 bytes
        long alignedOffset = (currentOffset + ALIGNMENT - 1) & ~(ALIGNMENT - 1);

        if (length > 0) {
          // Copy buffer data to payload shm using bulk copy.
          // We use nioBuffer() to get a ByteBuffer view of ArrowBuf's native memory
          // (zero-copy), then wrap it as MemorySegment. This avoids using
          // MemorySegment.ofAddress().reinterpret() which is a restricted operation
          // requiring --enable-native-access.
          MemorySegment dest = payloadShm.asSlice(alignedOffset, length);
          MemorySegment src = MemorySegment.ofBuffer(buffer.nioBuffer(0, (int) length));
          dest.copyFrom(src);
        }

        bufferDescriptors.add(new long[] {alignedOffset, length});
        currentOffset = alignedOffset + length;

        LOG.finest(String.format("  Buffer written: offset=%d, length=%d", alignedOffset, length));
      }

      LOG.fine(
          String.format(
              "Arrow buffers written: rows=%d, buffers=%d, bytes=%d",
              root.getRowCount(), bufferDescriptors.size(), currentOffset));

      return new ArrowWriteResult(
          schemaHash,
          schemaBytes,
          root.getRowCount(),
          bufferDescriptors,
          fieldNodes,
          currentOffset,
          arenaEpoch);
    } finally {
      recordBatch.close();
    }
  }

  /**
   * Compute a hash of the schema for caching purposes. Uses FNV-1a hash algorithm for consistency
   * with Python side.
   */
  private long computeSchemaHash(Schema schema) {
    byte[] bytes = serializeSchema(schema);
    long h = 0xcbf29ce484222325L; // FNV offset basis
    for (byte b : bytes) {
      h ^= (b & 0xFF);
      h *= 0x100000001b3L; // FNV prime
    }
    return h;
  }

  /** Serialize an Arrow schema to IPC format bytes. */
  private byte[] serializeSchema(Schema schema) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      WriteChannel channel = new WriteChannel(Channels.newChannel(baos));
      MessageSerializer.serialize(channel, schema);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize schema", e);
    }
  }

  // Alignment for Arrow buffers (64 bytes for SIMD operations)
  private static final int ALIGNMENT = 64;

  // --- Validation helpers for node/buffer count verification ---

  /**
   * Count expected field nodes for a schema.
   *
   * <p>Arrow IPC format uses depth-first pre-order traversal, so nested types contribute additional
   * nodes for their children. Each array (including nested children) gets exactly one field node.
   *
   * @param schema Arrow schema to analyze
   * @return Expected number of field nodes
   */
  private static int countExpectedNodes(Schema schema) {
    int count = 0;
    for (Field field : schema.getFields()) {
      count += countNodesForField(field);
    }
    return count;
  }

  /** Count nodes for a single field (including nested children). */
  private static int countNodesForField(Field field) {
    int count = 1; // This field itself
    for (Field child : field.getChildren()) {
      count += countNodesForField(child);
    }
    return count;
  }

  /**
   * Count expected buffers for a schema.
   *
   * <p>Uses TypeLayout to get the exact buffer count for each type, then recursively counts buffers
   * for nested types.
   *
   * @param schema Arrow schema to analyze
   * @return Expected number of buffers
   */
  private static int countExpectedBuffers(Schema schema) {
    int count = 0;
    for (Field field : schema.getFields()) {
      count += countBuffersForField(field);
    }
    return count;
  }

  /** Count buffers for a single field (including nested children). */
  private static int countBuffersForField(Field field) {
    ArrowType type = field.getType();

    // Get buffer count for this type from TypeLayout
    int count = TypeLayout.getTypeBufferCount(type);

    // Add buffers for nested children (recursively)
    for (Field child : field.getChildren()) {
      count += countBuffersForField(child);
    }

    return count;
  }
}
