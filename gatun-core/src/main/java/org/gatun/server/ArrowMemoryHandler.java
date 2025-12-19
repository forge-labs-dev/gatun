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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
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
 * <ul>
 *   <li>Arrow IPC batch reading from shared memory</li>
 *   <li>Zero-copy buffer reconstruction from buffer descriptors</li>
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
      // For foreign memory, we don't support transfer to another allocator
      throw new UnsupportedOperationException("Cannot retain foreign memory buffer to another allocator");
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
   * reconstructs Arrow vectors by wrapping those buffers directly (zero-copy on Java side).
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

      if (length == 0) {
        // Zero-length buffer (e.g., no validity bitmap)
        arrowBuffers.add(null);
      } else {
        // Create ArrowBuf backed by the shared memory region
        long bufferAddress = baseAddress + offset;
        ReferenceManager refManager = new ForeignMemoryReferenceManager(length);
        ArrowBuf arrowBuf = new ArrowBuf(refManager, null, length, bufferAddress);
        arrowBuffers.add(arrowBuf);
      }

      LOG.finest(String.format("  Buffer %d: offset=%d, length=%d, address=0x%x",
          i, offset, length, baseAddress + offset));
    }

    // 4. Log field nodes for debugging
    for (int i = 0; i < numNodes; i++) {
      FieldNode node = batchDesc.nodes(i);
      LOG.finest(String.format("  Node %d: length=%d, null_count=%d",
          i, node.length(), node.nullCount()));
    }

    // 5. Reconstruct vectors from buffers
    // Each field consumes buffers based on its type (validity + data for primitives,
    // validity + offsets + data for variable-width types)
    List<FieldVector> vectors = new ArrayList<>();
    int bufferIndex = 0;
    int nodeIndex = 0;

    for (Field field : schema.getFields()) {
      ArrowType type = field.getType();
      FieldNode node = batchDesc.nodes(nodeIndex++);
      int valueCount = (int) node.length();

      FieldVector vector = reconstructVector(field, type, arrowBuffers, bufferIndex, valueCount);
      if (vector != null) {
        vectors.add(vector);
        // Advance buffer index based on type
        bufferIndex += getBufferCount(type);
      } else {
        LOG.warning("Failed to reconstruct vector for field: " + field.getName() + " type=" + type);
      }
    }

    // 6. Build VectorSchemaRoot
    if (!vectors.isEmpty()) {
      // Close any existing root
      if (currentRoot != null) {
        currentRoot.close();
      }
      currentRoot = new VectorSchemaRoot(schema, vectors, (int) numRows);
      LOG.fine("Reconstructed VectorSchemaRoot with " + vectors.size() + " vectors, " + numRows + " rows");
    }

    // 7. Verify by reading sample data
    StringBuilder verification = new StringBuilder();
    verification.append("Reconstructed ").append(vectors.size()).append(" vectors");

    if (currentRoot != null && currentRoot.getRowCount() > 0) {
      for (int i = 0; i < Math.min(vectors.size(), 2); i++) {
        FieldVector vec = vectors.get(i);
        String fieldName = schema.getFields().get(i).getName();
        Object firstVal = vec.getObject(0);
        verification.append(String.format(", %s[0]=%s", fieldName, firstVal));
      }
    }

    LOG.fine(verification.toString());

    return numRows;
  }

  /**
   * Reconstruct a FieldVector from ArrowBuf wrappers.
   */
  private FieldVector reconstructVector(Field field, ArrowType type, List<ArrowBuf> buffers,
                                        int bufferIndex, int valueCount) {
    try {
      if (type instanceof ArrowType.Int intType) {
        return reconstructIntVector(field.getName(), intType, buffers, bufferIndex, valueCount);
      } else if (type instanceof ArrowType.FloatingPoint fpType) {
        return reconstructFloatVector(field.getName(), fpType, buffers, bufferIndex, valueCount);
      } else if (type instanceof ArrowType.Utf8) {
        return reconstructVarCharVector(field.getName(), buffers, bufferIndex, valueCount);
      } else if (type instanceof ArrowType.Bool) {
        return reconstructBitVector(field.getName(), buffers, bufferIndex, valueCount);
      } else {
        LOG.fine("Unsupported Arrow type for reconstruction: " + type.getTypeID().name());
        return null;
      }
    } catch (Exception e) {
      LOG.warning("Error reconstructing vector " + field.getName() + ": " + e.getMessage());
      return null;
    }
  }

  /**
   * Reconstruct an integer vector (Int8, Int16, Int32, Int64).
   */
  private FieldVector reconstructIntVector(String name, ArrowType.Int intType,
                                           List<ArrowBuf> buffers, int bufferIndex, int valueCount) {
    int bitWidth = intType.getBitWidth();
    ArrowBuf validityBuf = buffers.get(bufferIndex);
    ArrowBuf dataBuf = buffers.get(bufferIndex + 1);

    switch (bitWidth) {
      case 8 -> {
        TinyIntVector vec = new TinyIntVector(name, allocator);
        vec.allocateNew(valueCount);
        loadBuffersIntoVector(vec, validityBuf, dataBuf, valueCount);
        return vec;
      }
      case 16 -> {
        SmallIntVector vec = new SmallIntVector(name, allocator);
        vec.allocateNew(valueCount);
        loadBuffersIntoVector(vec, validityBuf, dataBuf, valueCount);
        return vec;
      }
      case 32 -> {
        IntVector vec = new IntVector(name, allocator);
        vec.allocateNew(valueCount);
        loadBuffersIntoVector(vec, validityBuf, dataBuf, valueCount);
        return vec;
      }
      case 64 -> {
        BigIntVector vec = new BigIntVector(name, allocator);
        vec.allocateNew(valueCount);
        loadBuffersIntoVector(vec, validityBuf, dataBuf, valueCount);
        return vec;
      }
      default -> {
        LOG.warning("Unsupported int bit width: " + bitWidth);
        return null;
      }
    }
  }

  /**
   * Reconstruct a floating point vector (Float32, Float64).
   */
  private FieldVector reconstructFloatVector(String name, ArrowType.FloatingPoint fpType,
                                             List<ArrowBuf> buffers, int bufferIndex, int valueCount) {
    ArrowBuf validityBuf = buffers.get(bufferIndex);
    ArrowBuf dataBuf = buffers.get(bufferIndex + 1);

    switch (fpType.getPrecision()) {
      case SINGLE -> {
        Float4Vector vec = new Float4Vector(name, allocator);
        vec.allocateNew(valueCount);
        loadBuffersIntoVector(vec, validityBuf, dataBuf, valueCount);
        return vec;
      }
      case DOUBLE -> {
        Float8Vector vec = new Float8Vector(name, allocator);
        vec.allocateNew(valueCount);
        loadBuffersIntoVector(vec, validityBuf, dataBuf, valueCount);
        return vec;
      }
      default -> {
        LOG.warning("Unsupported float precision: " + fpType.getPrecision());
        return null;
      }
    }
  }

  /**
   * Reconstruct a VarChar (String) vector.
   */
  private FieldVector reconstructVarCharVector(String name, List<ArrowBuf> buffers,
                                               int bufferIndex, int valueCount) {
    ArrowBuf validityBuf = buffers.get(bufferIndex);
    ArrowBuf offsetBuf = buffers.get(bufferIndex + 1);
    ArrowBuf dataBuf = buffers.get(bufferIndex + 2);

    VarCharVector vec = new VarCharVector(name, allocator);
    vec.allocateNew(valueCount);

    // Load buffers - VarChar has 3 buffers: validity, offsets, data
    if (validityBuf != null) {
      vec.getValidityBuffer().setBytes(0, validityBuf, 0, validityBuf.capacity());
    }
    if (offsetBuf != null) {
      vec.getOffsetBuffer().setBytes(0, offsetBuf, 0, offsetBuf.capacity());
    }
    if (dataBuf != null) {
      vec.getDataBuffer().setBytes(0, dataBuf, 0, dataBuf.capacity());
    }
    vec.setValueCount(valueCount);

    return vec;
  }

  /**
   * Reconstruct a BitVector (Boolean).
   */
  private FieldVector reconstructBitVector(String name, List<ArrowBuf> buffers,
                                           int bufferIndex, int valueCount) {
    ArrowBuf validityBuf = buffers.get(bufferIndex);
    ArrowBuf dataBuf = buffers.get(bufferIndex + 1);

    BitVector vec = new BitVector(name, allocator);
    vec.allocateNew(valueCount);
    loadBuffersIntoVector(vec, validityBuf, dataBuf, valueCount);

    return vec;
  }

  /**
   * Load validity and data buffers into a fixed-width vector.
   */
  private void loadBuffersIntoVector(FieldVector vec, ArrowBuf validityBuf,
                                     ArrowBuf dataBuf, int valueCount) {
    if (validityBuf != null) {
      vec.getValidityBuffer().setBytes(0, validityBuf, 0, validityBuf.capacity());
    }
    if (dataBuf != null) {
      vec.getDataBuffer().setBytes(0, dataBuf, 0, dataBuf.capacity());
    }
    vec.setValueCount(valueCount);
  }

  /**
   * Get the number of buffers for a given Arrow type.
   */
  private int getBufferCount(ArrowType type) {
    if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.Binary ||
        type instanceof ArrowType.LargeUtf8 || type instanceof ArrowType.LargeBinary) {
      return 3; // validity + offsets + data
    }
    return 2; // validity + data for fixed-width types
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
