package org.gatun.server;

import java.io.ByteArrayInputStream;
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
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.gatun.protocol.ArrowBatchDescriptor;
import org.gatun.protocol.BufferDescriptor;
import org.gatun.protocol.FieldNode;

/**
 * Handles Arrow memory operations for zero-copy data transfer.
 *
 * <p>This class manages:
 * <ul>
 *   <li>Arrow IPC batch reading from shared memory</li>
 *   <li>Zero-copy buffer reconstruction from buffer descriptors using VectorLoader</li>
 *   <li>Schema caching for repeated transfers</li>
 * </ul>
 */
public class ArrowMemoryHandler {
  private static final Logger LOG = Logger.getLogger(ArrowMemoryHandler.class.getName());

  private final BufferAllocator allocator;

  // Schema cache: hash -> parsed Schema object
  private final Map<Long, Schema> schemaCache = new HashMap<>();

  // Current VectorSchemaRoot from buffer reconstruction (kept for reference until reset)
  private VectorSchemaRoot currentRoot = null;

  // VectorLoader for efficient batch loading
  private VectorLoader vectorLoader = null;

  public ArrowMemoryHandler(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * A no-op ReferenceManager for foreign memory.
   *
   * <p>This is used to wrap shared memory that is owned by Python.
   * We don't actually manage the memory lifecycle - Python does.
   */
  private static class ForeignMemoryReferenceManager implements ReferenceManager {
    private final long capacity;

    ForeignMemoryReferenceManager(long capacity) {
      this.capacity = capacity;
    }

    @Override
    public int getRefCount() {
      return 1; // Always appears to have one reference
    }

    @Override
    public boolean release() {
      return false; // Never actually releases - Python owns the memory
    }

    @Override
    public boolean release(int decrement) {
      return false;
    }

    @Override
    public void retain() {
      // No-op
    }

    @Override
    public void retain(int increment) {
      // No-op
    }

    @Override
    public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
      // For foreign memory, return the same buffer - we don't actually manage ownership
      // VectorLoader calls this when loading buffers into vectors
      return srcBuffer;
    }

    @Override
    public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, long index, long length) {
      // Create a derived buffer pointing to a slice of the same memory
      long newAddress = sourceBuffer.memoryAddress() + index;
      return new ArrowBuf(this, null, length, newAddress);
    }

    @Override
    public OwnershipTransferResult transferOwnership(ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
      throw new UnsupportedOperationException("Cannot transfer ownership of foreign memory");
    }

    @Override
    public BufferAllocator getAllocator() {
      return null; // Foreign memory has no allocator
    }

    @Override
    public long getSize() {
      return capacity;
    }

    @Override
    public long getAccountedSize() {
      return 0; // Not accounted in any allocator
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
   * <p>This method receives buffer locations in payload shared memory and
   * reconstructs Arrow vectors using VectorLoader (zero-copy on Java side).
   *
   * @param batchDesc The Arrow batch descriptor containing buffer locations
   * @param payloadShm Memory segment for the payload shared memory
   * @return Number of rows in the batch
   */
  public long processArrowBuffers(ArrowBatchDescriptor batchDesc, MemorySegment payloadShm) {
    long schemaHash = batchDesc.schemaHash();
    long numRows = batchDesc.numRows();
    int numNodes = batchDesc.nodesLength();
    int numBuffers = batchDesc.buffersLength();

    LOG.fine(String.format(
        "Arrow buffers received: schema_hash=%d, rows=%d, nodes=%d, buffers=%d",
        schemaHash, numRows, numNodes, numBuffers));

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
        LOG.fine("Cached schema with hash: " + schemaHash + ", fields: " + schema.getFields().size());
      } catch (IOException e) {
        LOG.warning("Failed to deserialize schema: " + e.getMessage());
        return numRows;
      }
    }

    if (schema == null) {
      LOG.warning("No schema available for hash: " + schemaHash);
      return numRows;
    }

    // 2. Get base address of payload shared memory
    long baseAddress = payloadShm.address();

    // 3. Create ArrowBuf wrappers for each buffer descriptor
    List<ArrowBuf> arrowBuffers = new ArrayList<>();
    for (int i = 0; i < numBuffers; i++) {
      BufferDescriptor bufDesc = batchDesc.buffers(i);
      long offset = bufDesc.offset();
      long length = bufDesc.length();

      // Create ArrowBuf backed by the shared memory region
      // Note: VectorLoader requires non-null buffers, so we create empty buffers for zero-length
      long bufferAddress = baseAddress + offset;
      ReferenceManager refManager = new ForeignMemoryReferenceManager(length);
      ArrowBuf arrowBuf = new ArrowBuf(refManager, null, length, bufferAddress);
      arrowBuffers.add(arrowBuf);

      LOG.finest(String.format("  Buffer %d: offset=%d, length=%d, address=0x%x",
          i, offset, length, bufferAddress));
    }

    // 4. Build ArrowFieldNode list from FieldNode descriptors
    List<ArrowFieldNode> fieldNodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      FieldNode node = batchDesc.nodes(i);
      fieldNodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
      LOG.finest(String.format("  Node %d: length=%d, null_count=%d",
          i, node.length(), node.nullCount()));
    }

    // 5. Create ArrowRecordBatch from buffers and field nodes
    ArrowRecordBatch recordBatch = new ArrowRecordBatch(
        (int) numRows,
        fieldNodes,
        arrowBuffers
    );

    // 6. Create or reuse VectorSchemaRoot and load with VectorLoader
    if (currentRoot != null) {
      currentRoot.close();
    }
    currentRoot = VectorSchemaRoot.create(schema, allocator);
    vectorLoader = new VectorLoader(currentRoot);
    vectorLoader.load(recordBatch);

    LOG.fine("Loaded VectorSchemaRoot with " + schema.getFields().size() +
             " fields, " + numRows + " rows using VectorLoader");

    // 7. Verify by reading sample data (for debugging)
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

  /**
   * Get the current VectorSchemaRoot (if any).
   */
  public VectorSchemaRoot getCurrentRoot() {
    return currentRoot;
  }

  /**
   * Deserialize an Arrow schema from IPC stream bytes.
   */
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
    // Note: We keep the schema cache since schemas are typically stable
    LOG.fine("Arrow memory handler reset");
  }

  /**
   * Clear the schema cache.
   */
  public void clearSchemaCache() {
    schemaCache.clear();
    LOG.fine("Schema cache cleared");
  }

  /**
   * Check if a schema is cached.
   */
  public boolean hasSchema(long schemaHash) {
    return schemaCache.containsKey(schemaHash);
  }

  /**
   * Get a cached schema.
   */
  public Schema getSchema(long schemaHash) {
    return schemaCache.get(schemaHash);
  }
}
