package org.gatun.server;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.gatun.protocol.*; // Import all generated classes
import org.gatun.server.MethodResolver.MethodWithArgs;
import org.gatun.server.observability.Metrics;
import org.gatun.server.observability.SessionObserver;
import org.gatun.server.observability.StructuredLogger;

public class GatunServer {
  private static final Logger LOG = Logger.getLogger(GatunServer.class.getName());

  // Protocol version - increment when making breaking changes to the protocol
  // Version 2: Per-client shared memory (handshake includes SHM path)
  public static final int PROTOCOL_VERSION = 2;

  private final Path socketPath;
  private final long memorySize;
  private final long responseOffset;
  private final ExecutorService threadPool;

  // Memory zones
  private static final int COMMAND_OFFSET = 0;
  private static final int PAYLOAD_OFFSET =
      65536; // 64KB - increased to handle large pickled functions
  private static final int RESPONSE_ZONE_SIZE = 65536; // 64KB - increased to handle large responses

  // --- SECURITY: Allowlist of classes that can be instantiated or used for static methods ---
  private static final Set<String> ALLOWED_CLASSES =
      Set.of(
          "java.util.ArrayList",
          "java.util.LinkedList",
          "java.util.HashMap",
          "java.util.LinkedHashMap",
          "java.util.HashSet",
          "java.util.LinkedHashSet",
          "java.util.TreeMap",
          "java.util.TreeSet",
          "java.util.Collections",
          "java.util.Arrays",
          "java.lang.String",
          "java.lang.Integer",
          "java.lang.Long",
          "java.lang.Double",
          "java.lang.Float",
          "java.lang.Boolean",
          "java.lang.Byte",
          "java.lang.Short",
          "java.lang.Character",
          "java.lang.Math",
          "java.lang.StringBuilder",
          "java.lang.StringBuffer",
          "java.lang.System", // For setting system properties (e.g., spark.master)
          "java.lang.Class", // For reflection-based array creation
          "java.lang.reflect.Array", // For array operations
          "java.sql.Timestamp", // For datetime conversion
          "java.sql.Date", // For date conversion
          "java.sql.Time"); // For time conversion

  // --- SECURITY: Prefixes for allowed class packages (e.g., for Spark integration) ---
  private static final Set<String> ALLOWED_PREFIXES =
      Set.of(
          "org.apache.spark.", // Apache Spark
          "org.apache.log4j.", // Log4J (used by Spark)
          "scala." // Scala standard library
          );

  private static boolean isClassAllowed(String className) {
    if (ALLOWED_CLASSES.contains(className)) {
      return true;
    }
    for (String prefix : ALLOWED_PREFIXES) {
      if (className.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  // --- THE OBJECT REGISTRY ---
  private final Map<Long, Object> objectRegistry = new ConcurrentHashMap<>();
  private final AtomicLong objectIdCounter = new AtomicLong(1);

  // Session counter for unique per-client SHM paths
  private final AtomicLong sessionCounter = new AtomicLong(0);

  // --- CALLBACK REGISTRY ---
  // Maps callback ID -> CallbackInfo (interface name + proxy object + session context)
  // Note: callback IDs are allocated from objectIdCounter to ensure uniqueness
  private final Map<Long, CallbackInfo> callbackRegistry = new ConcurrentHashMap<>();

  /**
   * Holds session context for callback invocations. This is captured by the callback proxy so that
   * callbacks can be invoked from any thread, not just the handleClient thread.
   */
  private static class SessionContext {
    final SocketChannel channel;
    final MemorySegment sharedMem;
    final Object callbackLock; // Lock for serializing callback round-trips
    final Set<Long> sessionObjectIds; // Track object IDs for cleanup

    SessionContext(SocketChannel channel, MemorySegment sharedMem, Set<Long> sessionObjectIds) {
      this.channel = channel;
      this.sharedMem = sharedMem;
      this.callbackLock = new Object();
      this.sessionObjectIds = sessionObjectIds;
    }
  }

  // Per-session context stored in ThreadLocal for the handleClient thread
  private static final ThreadLocal<SessionContext> sessionContext = new ThreadLocal<>();

  // Per-session: current request ID for cancellation support
  private static final ThreadLocal<Long> currentRequestId = new ThreadLocal<>();

  // Per-session: cancelled request IDs
  private static final ThreadLocal<Set<Long>> cancelledRequests =
      ThreadLocal.withInitial(HashSet::new);

  // Per-request: nesting depth counter for convertArgument recursion
  private static final ThreadLocal<Integer> argumentNestingDepth = ThreadLocal.withInitial(() -> 0);

  // --- REUSABLE FLATBUFFER TEMP OBJECTS ---
  // These are ThreadLocal to avoid per-request allocation overhead.
  // FlatBuffer table wrappers are lightweight and can be reused via Init().
  private static final ThreadLocal<StringVal> tempStringVal =
      ThreadLocal.withInitial(StringVal::new);
  private static final ThreadLocal<IntVal> tempIntVal = ThreadLocal.withInitial(IntVal::new);
  private static final ThreadLocal<DoubleVal> tempDoubleVal =
      ThreadLocal.withInitial(DoubleVal::new);
  private static final ThreadLocal<BoolVal> tempBoolVal = ThreadLocal.withInitial(BoolVal::new);
  private static final ThreadLocal<CharVal> tempCharVal = ThreadLocal.withInitial(CharVal::new);
  private static final ThreadLocal<ObjectRef> tempObjectRef =
      ThreadLocal.withInitial(ObjectRef::new);
  private static final ThreadLocal<ListVal> tempListVal = ThreadLocal.withInitial(ListVal::new);
  private static final ThreadLocal<MapVal> tempMapVal = ThreadLocal.withInitial(MapVal::new);
  private static final ThreadLocal<ArrayVal> tempArrayVal = ThreadLocal.withInitial(ArrayVal::new);
  private static final ThreadLocal<CallbackRef> tempCallbackRef =
      ThreadLocal.withInitial(CallbackRef::new);

  private final BufferAllocator allocator = new RootAllocator();
  private final ArrowMemoryHandler arrowHandler = new ArrowMemoryHandler(allocator);

  // Delegate to ReflectionCache for all caching operations
  private static java.lang.reflect.Constructor<?> getNoArgConstructor(Class<?> clazz)
      throws NoSuchMethodException {
    return ReflectionCache.getNoArgConstructor(clazz);
  }

  private static MethodHandle getNoArgMethodHandle(Class<?> clazz, String methodName) {
    return ReflectionCache.getNoArgMethodHandle(clazz, methodName);
  }

  private static java.lang.reflect.Field getStaticField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    return ReflectionCache.getStaticField(clazz, fieldName);
  }

  private static java.lang.reflect.Field getInstanceField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    return ReflectionCache.getInstanceField(clazz, fieldName);
  }

  private static java.lang.invoke.VarHandle getInstanceVarHandle(Class<?> clazz, String fieldName) {
    return ReflectionCache.getInstanceVarHandle(clazz, fieldName);
  }

  private static ReflectionCache.CachedStaticVarHandle getStaticVarHandle(
      Class<?> clazz, String fieldName) {
    return ReflectionCache.getStaticVarHandle(clazz, fieldName);
  }

  private static java.lang.reflect.Method[] getCachedMethods(Class<?> clazz) {
    return ReflectionCache.getCachedMethods(clazz);
  }

  private static Class<?> getClass(String className) throws ClassNotFoundException {
    return ReflectionCache.getClass(className);
  }

  private static Class<?> tryGetClass(String className) {
    return ReflectionCache.tryGetClass(className);
  }

  /** Holds information about a registered callback. */
  private static class CallbackInfo {
    final long id;
    final String interfaceName;
    final Object proxyInstance;
    final SessionContext sessionContext; // Captured for cross-thread callback invocations

    CallbackInfo(long id, String interfaceName, Object proxyInstance, SessionContext ctx) {
      this.id = id;
      this.interfaceName = interfaceName;
      this.proxyInstance = proxyInstance;
      this.sessionContext = ctx;
    }
  }

  public GatunServer(String socketPathStr, long memorySize) {
    this.socketPath = Path.of(socketPathStr);
    this.memorySize = memorySize;
    this.responseOffset = memorySize - RESPONSE_ZONE_SIZE;
    this.threadPool = Executors.newVirtualThreadPerTaskExecutor();
  }

  public void start() throws IOException {
    Files.deleteIfExists(socketPath);

    // --- Register shutdown hook for cleanup ---
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutting down...");
                  threadPool.shutdownNow();
                  allocator.close();
                  try {
                    Files.deleteIfExists(socketPath);
                    // Note: Per-session SHM files are cleaned up when each session ends
                  } catch (IOException e) {
                    // Ignore cleanup errors on shutdown
                  }
                }));

    // --- START SOCKET SERVER ---
    try (ServerSocketChannel serverChannel =
        ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
      serverChannel.bind(UnixDomainSocketAddress.of(socketPath));
      LOG.info("Server ready at: " + socketPath);

      while (true) {
        SocketChannel client = serverChannel.accept();
        long sessionId = sessionCounter.incrementAndGet();
        threadPool.submit(() -> handleClient(client, sessionId));
      }
    }
  }

  private void handleClient(SocketChannel client, long sessionId) {
    Set<Long> sessionObjectIds = new HashSet<>();
    Set<Long> sessionCallbackIds = new HashSet<>();
    SessionObserver observer = new SessionObserver(sessionId);
    SessionQuota quota = new SessionQuota(sessionId);
    LOG.fine("New client session " + sessionId + " started");

    // Create per-session shared memory file
    Path sessionShmPath = Path.of(socketPath.toString() + "." + sessionId + ".shm");

    try (client;
        RandomAccessFile raf = new RandomAccessFile(sessionShmPath.toFile(), "rw");
        Arena arena = Arena.ofConfined()) {

      // Create and map the session's shared memory
      raf.setLength(this.memorySize);
      FileChannel channel = raf.getChannel();
      MemorySegment sharedMem =
          channel.map(FileChannel.MapMode.READ_WRITE, 0, this.memorySize, arena);
      sharedMem.fill((byte) 0);

      LOG.fine("Session " + sessionId + " SHM mapped at: " + sessionShmPath);

      // Set up session context for callbacks (captured by callback proxies)
      SessionContext ctx = new SessionContext(client, sharedMem, sessionObjectIds);
      sessionContext.set(ctx);

      // --- HANDSHAKE: Send Protocol Version, Memory Size, and SHM Path to Client ---
      // Format: [4 bytes: version] [4 bytes: arena_epoch] [8 bytes: memory size]
      //         [2 bytes: shm_path_length] [N bytes: shm_path (UTF-8)]
      byte[] shmPathBytes =
          sessionShmPath.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      ByteBuffer handshakeBuf = ByteBuffer.allocate(16 + 2 + shmPathBytes.length);
      handshakeBuf.order(ByteOrder.LITTLE_ENDIAN);
      handshakeBuf.putInt(PROTOCOL_VERSION);
      handshakeBuf.putInt((int) arrowHandler.getArenaEpoch()); // Current epoch for synchronization
      handshakeBuf.putLong(this.memorySize);
      handshakeBuf.putShort((short) shmPathBytes.length);
      handshakeBuf.put(shmPathBytes);
      handshakeBuf.flip();

      while (handshakeBuf.hasRemaining()) {
        client.write(handshakeBuf);
      }

      // Notify observer of session start
      observer.onSessionStart(sessionShmPath.toString());

      ByteBuffer lengthBuf = ByteBuffer.allocate(4);
      lengthBuf.order(ByteOrder.LITTLE_ENDIAN);

      // Reusable FlatBufferBuilder for responses (avoid allocation per request)
      FlatBufferBuilder responseBuilder = new FlatBufferBuilder(1024);

      while (readFully(client, lengthBuf)) {
        // Check idle timeout before processing request
        if (quota.isTimedOut()) {
          LOG.warning("Session " + sessionId + " timed out due to inactivity");
          break;
        }

        // Record request activity
        quota.recordRequest();

        // 1. Read Command Length
        lengthBuf.flip();
        int commandSize = lengthBuf.getInt();
        lengthBuf.clear();

        // Validate command size to prevent OOB access or DoS
        // Command zone is from COMMAND_OFFSET to PAYLOAD_OFFSET
        if (commandSize <= 0 || commandSize > PAYLOAD_OFFSET) {
          LOG.warning("Invalid command size: " + commandSize + " (max: " + PAYLOAD_OFFSET + ")");
          // Send error response and continue
          FlatBufferBuilder errBuilder = new FlatBufferBuilder(256);
          int errOffset =
              packError(
                  errBuilder,
                  "Invalid command size: " + commandSize + " bytes (max: " + PAYLOAD_OFFSET + ")",
                  "java.lang.IllegalArgumentException");
          errBuilder.finish(errOffset);
          ByteBuffer errBuf = errBuilder.dataBuffer();
          int errSize = errBuf.remaining();
          MemorySegment errSlice = sharedMem.asSlice(this.responseOffset, errSize);
          errSlice.copyFrom(MemorySegment.ofBuffer(errBuf));
          lengthBuf.putInt(errSize);
          lengthBuf.flip();
          client.write(lengthBuf);
          lengthBuf.clear();
          continue;
        }

        // --- Safe Buffer Slicing ---
        // We create a ByteBuffer isolated to just the command data.
        MemorySegment cmdSlice = sharedMem.asSlice(COMMAND_OFFSET, commandSize);
        ByteBuffer cmdBuf = cmdSlice.asByteBuffer();
        cmdBuf.order(ByteOrder.LITTLE_ENDIAN); // Essential for FlatBuffers

        // Define 'cmd' here so it is visible to the rest of the loop
        Command cmd = Command.getRootAsCommand(cmdBuf);

        // Handle Cancel action immediately (doesn't require response processing)
        if (cmd.action() == Action.Cancel) {
          long requestIdToCancel = cmd.requestId();
          cancelledRequests.get().add(requestIdToCancel);
          LOG.fine("Marked request " + requestIdToCancel + " for cancellation");

          // Send acknowledgement
          FlatBufferBuilder cancelBuilder = new FlatBufferBuilder(256);
          int cancelResponse = packSuccess(cancelBuilder, true);
          cancelBuilder.finish(cancelResponse);
          ByteBuffer cancelBuf = cancelBuilder.dataBuffer();
          int cancelSize = cancelBuf.remaining();

          MemorySegment cancelSlice = sharedMem.asSlice(this.responseOffset, cancelSize);
          cancelSlice.copyFrom(MemorySegment.ofBuffer(cancelBuf));

          lengthBuf.putInt(cancelSize);
          lengthBuf.flip();
          client.write(lengthBuf);
          lengthBuf.clear();
          continue;
        }

        // Track current request ID for cancellation checks
        long requestId = cmd.requestId();
        currentRequestId.set(requestId);

        // Start observability tracking for this request
        observer.onRequestStart(requestId, cmd.action(), cmd.targetName());

        // --- Standard Request/Response Logic ---
        // Reuse the response builder (clear resets position but keeps buffer)
        responseBuilder.clear();
        FlatBufferBuilder builder = responseBuilder;
        int responseOffset = 0;
        Object result = null;
        String errorType = null;
        String errorMessage = null;
        boolean requestSuccess = true;

        try {
          // Check if this request was already cancelled
          if (requestId != 0 && cancelledRequests.get().contains(requestId)) {
            throw new InterruptedException("Request " + requestId + " was cancelled");
          }

          if (cmd.action() == Action.CreateObject) {
            String className = cmd.targetName();
            if (!isClassAllowed(className)) {
              throw new SecurityException("Class not allowed: " + className);
            }

            // Check object limit before creating
            quota.allocateObject();

            Class<?> clazz = getClass(className);

            // Convert constructor arguments if provided
            int argCount = cmd.argsLength();
            Object instance;

            if (argCount == 0) {
              // No-arg constructor (cached)
              instance = getNoArgConstructor(clazz).newInstance();
            } else {
              // Constructor with arguments
              Object[] javaArgs = new Object[argCount];
              Class<?>[] argTypes = new Class<?>[argCount];

              for (int i = 0; i < argCount; i++) {
                Argument arg = cmd.args(i);
                Object[] converted = convertArgument(arg);
                javaArgs[i] = converted[0];
                argTypes[i] = (Class<?>) converted[1];
              }

              var ctorResult = MethodResolver.findConstructorWithArgs(clazz, argTypes, javaArgs);
              instance = ctorResult.cached.newInstance(ctorResult.args);
            }

            long newId = objectIdCounter.getAndIncrement();
            objectRegistry.put(newId, instance);
            sessionObjectIds.add(newId);
            result = new ObjectRefT(newId);
            LOG.fine("Created object ID " + newId + " of type " + className);

          } else if (cmd.action() == Action.FreeObject) {
            long targetId = cmd.targetId();
            if (sessionObjectIds.contains(targetId)) {
              objectRegistry.remove(targetId);
              sessionObjectIds.remove(targetId);
              quota.releaseObject();
            }
            result = null;
          } else if (cmd.action() == Action.InvokeMethod) {
            long targetId = cmd.targetId();
            Object target = objectRegistry.get(targetId);
            String methodName = cmd.targetName();

            if (target == null) throw new RuntimeException("Object " + targetId + " not found");

            int argCount = cmd.argsLength();

            // Fast path for no-arg methods (most common case: size(), isEmpty(), toString(), etc)
            // Uses MethodHandle for faster invocation after JIT warmup
            if (argCount == 0) {
              MethodHandle handle = getNoArgMethodHandle(target.getClass(), methodName);
              if (handle != null) {
                result = handle.invoke(target);
              } else {
                // Fallback: use normal resolution (handles inherited methods, etc)
                MethodWithArgs mwa =
                    MethodResolver.findMethodWithArgs(
                        target.getClass(),
                        methodName,
                        ReflectionCache.EMPTY_CLASS_ARRAY,
                        ReflectionCache.EMPTY_OBJECT_ARRAY);
                result = mwa.cached.invoke(target, mwa.args);
              }
            } else {
              // Convert FlatBuffer arguments to Java objects
              Object[] javaArgs = new Object[argCount];
              Class<?>[] argTypes = new Class<?>[argCount];

              for (int i = 0; i < argCount; i++) {
                Argument arg = cmd.args(i);
                Object[] converted = convertArgument(arg);
                javaArgs[i] = converted[0];
                argTypes[i] = (Class<?>) converted[1];
              }

              // Find and invoke method via MethodHandle (faster than reflection)
              MethodWithArgs mwa =
                  MethodResolver.findMethodWithArgs(
                      target.getClass(), methodName, argTypes, javaArgs);
              result = mwa.cached.invoke(target, mwa.args);
            }

            // Wrap returned objects in registry
            if (result != null && !MethodResolver.isAutoConvertible(result)) {
              quota.allocateObject();
              long newId = objectIdCounter.getAndIncrement();
              objectRegistry.put(newId, result);
              sessionObjectIds.add(newId);
              result = new ObjectRefT(newId);
            }
          } else if (cmd.action() == Action.InvokeStaticMethod) {
            // target_name format: "fully.qualified.ClassName.methodName"
            String fullName = cmd.targetName();
            int lastDot = fullName.lastIndexOf('.');
            if (lastDot == -1) {
              throw new IllegalArgumentException("Invalid static method format: " + fullName);
            }
            String className = fullName.substring(0, lastDot);
            String methodName = fullName.substring(lastDot + 1);

            if (!isClassAllowed(className)) {
              throw new SecurityException("Class not allowed: " + className);
            }

            Class<?> clazz = getClass(className);

            // Convert FlatBuffer arguments to Java objects
            int argCount = cmd.argsLength();
            Object[] javaArgs = new Object[argCount];
            Class<?>[] argTypes = new Class<?>[argCount];

            for (int i = 0; i < argCount; i++) {
              Argument arg = cmd.args(i);
              Object[] converted = convertArgument(arg);
              javaArgs[i] = converted[0];
              argTypes[i] = (Class<?>) converted[1];
            }

            // Find and invoke static method via MethodHandle (faster than reflection)
            MethodWithArgs mwa =
                MethodResolver.findMethodWithArgs(clazz, methodName, argTypes, javaArgs);
            result = mwa.cached.invoke(null, mwa.args);

            // Wrap returned objects in registry
            // If returnObjectRef is true, always wrap as ObjectRef (no auto-conversion)
            if (result != null
                && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
              quota.allocateObject();
              long newId = objectIdCounter.getAndIncrement();
              objectRegistry.put(newId, result);
              sessionObjectIds.add(newId);
              result = new ObjectRefT(newId);
            }
          } else if (cmd.action() == Action.GetField) {
            long targetId = cmd.targetId();
            Object target = objectRegistry.get(targetId);
            String fieldName = cmd.targetName();

            if (target == null) throw new RuntimeException("Object " + targetId + " not found");

            // Try VarHandle for faster access
            java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
            if (vh != null) {
              result = vh.get(target);
            } else {
              // Fallback to Field
              java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
              field.setAccessible(true);
              result = field.get(target);
            }

            // Wrap returned objects in registry
            if (result != null && !MethodResolver.isAutoConvertible(result)) {
              quota.allocateObject();
              long newId = objectIdCounter.getAndIncrement();
              objectRegistry.put(newId, result);
              sessionObjectIds.add(newId);
              result = new ObjectRefT(newId);
            }
          } else if (cmd.action() == Action.SetField) {
            long targetId = cmd.targetId();
            Object target = objectRegistry.get(targetId);
            String fieldName = cmd.targetName();

            if (target == null) throw new RuntimeException("Object " + targetId + " not found");

            if (cmd.argsLength() != 1)
              throw new IllegalArgumentException("SetField requires exactly one argument");

            Argument arg = cmd.args(0);
            Object[] converted = convertArgument(arg);
            Object value = converted[0];

            // Try VarHandle for faster access
            java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
            if (vh != null) {
              vh.set(target, value);
            } else {
              // Fallback to Field
              java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
              field.setAccessible(true);
              field.set(target, value);
            }
            result = null;
          }
          // --- ARROW BLOCK ---
          else if (cmd.action() == Action.SendArrowBatch) {
            LOG.fine("Processing Arrow batch...");
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            MemorySegment payloadSlice = sharedMem.asSlice(PAYLOAD_OFFSET, payloadSize);
            int rows = arrowHandler.processArrowIpcBatch(payloadSlice);
            result = "Received " + rows + " rows";
          }
          // --- ZERO-COPY ARROW BUFFERS ---
          else if (cmd.action() == Action.SendArrowBuffers) {
            ArrowBatchDescriptor batchDesc = cmd.arrowBatch();
            if (batchDesc == null) {
              throw new IllegalArgumentException(
                  "SendArrowBuffers requires arrow_batch descriptor");
            }

            // Calculate total bytes being transferred for quota tracking
            long totalBytes = 0;
            for (int i = 0; i < batchDesc.buffersLength(); i++) {
              totalBytes += batchDesc.buffers(i).length();
            }
            // Check and track Arrow buffer allocation
            quota.allocateArrowBytes(totalBytes);

            // For now, payload shm is the same as control shm (payloadSlice)
            // TODO: Support separate payload shm file
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            MemorySegment payloadSlice = sharedMem.asSlice(PAYLOAD_OFFSET, payloadSize);
            long numRows = arrowHandler.processArrowBuffers(batchDesc, payloadSlice);
            result = "Received " + numRows + " rows via zero-copy buffers";
          }
          // --- RESET PAYLOAD ARENA ---
          else if (cmd.action() == Action.ResetPayloadArena) {
            LOG.fine("Payload arena reset requested");
            arrowHandler.reset();
            // Reset Arrow buffer quota when arena is reset
            quota.resetArrowBytes();
            result = true;
          }
          // --- GET ARROW DATA (Java -> Python) ---
          else if (cmd.action() == Action.GetArrowData) {
            VectorSchemaRoot currentRoot = arrowHandler.getCurrentRoot();
            if (currentRoot == null) {
              throw new RuntimeException("No Arrow data available");
            }
            // Write Arrow buffers to payload zone
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            MemorySegment payloadSlice = sharedMem.asSlice(PAYLOAD_OFFSET, payloadSize);
            ArrowMemoryHandler.ArrowWriteResult writeResult =
                arrowHandler.writeArrowBuffers(currentRoot, payloadSlice, null);
            // Pack and send Arrow response (handled specially below)
            responseOffset = packArrowResponse(builder, writeResult);
            // Skip normal packSuccess
            builder.finish(responseOffset);
            ByteBuffer resBuf = builder.dataBuffer();
            int resSize = resBuf.remaining();
            MemorySegment responseSlice = sharedMem.asSlice(this.responseOffset, resSize);
            responseSlice.copyFrom(MemorySegment.ofBuffer(resBuf));
            lengthBuf.putInt(resSize);
            lengthBuf.flip();
            client.write(lengthBuf);
            lengthBuf.clear();
            continue; // Skip normal response handling
          }
          // --- CALLBACK BLOCK ---
          else if (cmd.action() == Action.RegisterCallback) {
            // Register a Python callback and create a Java proxy
            String interfaceName = cmd.targetName();

            // Check object limit before creating
            quota.allocateObject();

            // Use the same ID for both callback and object registry
            // This way Python can use the object_id as the callback_id
            long callbackId = objectIdCounter.getAndIncrement();

            // Create a dynamic proxy that will invoke Python when called
            // Use session context (captured at start of handleClient) for cross-thread callbacks
            Object proxy = createCallbackProxy(callbackId, interfaceName, ctx);

            // Store in callback registry with session context
            CallbackInfo info = new CallbackInfo(callbackId, interfaceName, proxy, ctx);
            callbackRegistry.put(callbackId, info);
            sessionCallbackIds.add(callbackId);

            // Also register proxy in object registry with the SAME ID
            objectRegistry.put(callbackId, proxy);
            sessionObjectIds.add(callbackId);

            LOG.fine("Registered callback ID " + callbackId + " for interface " + interfaceName);
            result = new ObjectRefT(callbackId);
          } else if (cmd.action() == Action.UnregisterCallback) {
            long callbackId = cmd.targetId();
            CallbackInfo info = callbackRegistry.remove(callbackId);
            if (info != null) {
              sessionCallbackIds.remove(callbackId);
              LOG.fine("Unregistered callback ID " + callbackId);
            }
            result = null;
          }
          // Note: CallbackResponse is handled directly in invokeCallback(), not here
          else if (cmd.action() == Action.IsInstanceOf) {
            // Check if an object is an instance of a class
            long targetId = cmd.targetId();
            String className = cmd.targetName();

            Object target = objectRegistry.get(targetId);
            if (target == null) throw new RuntimeException("Object " + targetId + " not found");

            // Try to load the class (no allowlist check - this is read-only)
            Class<?> clazz = getClass(className);
            result = clazz.isInstance(target);
          } else if (cmd.action() == Action.GetStaticField) {
            // Get static field: target_name = "pkg.Class.FIELD"
            String fullName = cmd.targetName();
            int lastDot = fullName.lastIndexOf('.');
            if (lastDot == -1) {
              throw new IllegalArgumentException("Invalid static field format: " + fullName);
            }
            String className = fullName.substring(0, lastDot);
            String fieldName = fullName.substring(lastDot + 1);

            if (!isClassAllowed(className)) {
              throw new SecurityException("Class not allowed: " + className);
            }

            Class<?> clazz = getClass(className);

            // Try VarHandle for faster static field access
            ReflectionCache.CachedStaticVarHandle svh = getStaticVarHandle(clazz, fieldName);
            if (svh != null) {
              result = svh.handle.get();
            } else {
              // Fallback to Field
              java.lang.reflect.Field field = getStaticField(clazz, fieldName);
              result = field.get(null);
            }

            // Wrap returned objects in registry
            if (result != null && !MethodResolver.isAutoConvertible(result)) {
              quota.allocateObject();
              long newId = objectIdCounter.getAndIncrement();
              objectRegistry.put(newId, result);
              sessionObjectIds.add(newId);
              result = new ObjectRefT(newId);
            }
          } else if (cmd.action() == Action.SetStaticField) {
            // Set static field: target_name = "pkg.Class.FIELD", args[0] = value
            String fullName = cmd.targetName();
            int lastDot = fullName.lastIndexOf('.');
            if (lastDot == -1) {
              throw new IllegalArgumentException("Invalid static field format: " + fullName);
            }
            String className = fullName.substring(0, lastDot);
            String fieldName = fullName.substring(lastDot + 1);

            if (!isClassAllowed(className)) {
              throw new SecurityException("Class not allowed: " + className);
            }

            if (cmd.argsLength() != 1) {
              throw new IllegalArgumentException("SetStaticField requires exactly one argument");
            }

            Argument arg = cmd.args(0);
            Object[] converted = convertArgument(arg);
            Object value = converted[0];

            Class<?> clazz = getClass(className);

            // Try VarHandle for faster static field access
            ReflectionCache.CachedStaticVarHandle svh = getStaticVarHandle(clazz, fieldName);
            if (svh != null) {
              svh.handle.set(value);
            } else {
              // Fallback to Field
              java.lang.reflect.Field field = getStaticField(clazz, fieldName);
              field.set(null, value);
            }
            result = null;
          } else if (cmd.action() == Action.Reflect) {
            // Reflection query: target_name is the fully qualified name to check
            // Returns a string:
            // - "class" if it's a loadable class
            // - "method" if the last segment is a method on the parent class
            // - "field" if the last segment is a field on the parent class
            // - "none" if not found
            String fullName = cmd.targetName();

            // First try: is the whole path a class?
            try {
              // Handle Scala objects: try both ClassName and ClassName$
              Class<?> clazz = tryGetClass(fullName);
              if (clazz == null) {
                // Try Scala object naming convention (append $)
                clazz = tryGetClass(fullName + "$");
              }

              if (clazz != null) {
                result = "class";
              } else {
                // Try as class.member
                int lastDot = fullName.lastIndexOf('.');
                if (lastDot > 0) {
                  String parentName = fullName.substring(0, lastDot);
                  String memberName = fullName.substring(lastDot + 1);

                  // Try to load parent as class
                  Class<?> parentClass = tryGetClass(parentName);
                  if (parentClass == null) {
                    // Try Scala object
                    parentClass = tryGetClass(parentName + "$");
                  }

                  if (parentClass != null) {
                    // Check if memberName is a method
                    boolean isMethod = false;
                    for (java.lang.reflect.Method m : getCachedMethods(parentClass)) {
                      if (m.getName().equals(memberName)) {
                        isMethod = true;
                        break;
                      }
                    }
                    if (isMethod) {
                      result = "method";
                    } else {
                      // Check if it's a field
                      try {
                        parentClass.getField(memberName);
                        result = "field";
                      } catch (NoSuchFieldException e) {
                        result = "none";
                      }
                    }
                  } else {
                    result = "none";
                  }
                } else {
                  result = "none";
                }
              }
            } catch (Exception e) {
              result = "none";
            }
          } else if (cmd.action() == Action.GetFields) {
            // Vectorized field reads: get multiple fields from one object
            long targetId = cmd.targetId();
            Object target = objectRegistry.get(targetId);
            if (target == null) {
              throw new RuntimeException("Object " + targetId + " not found");
            }

            GetFieldsRequest req = cmd.getFields();
            if (req == null) {
              throw new IllegalArgumentException("GetFields action requires get_fields field");
            }

            int fieldCount = req.fieldNamesLength();
            java.util.ArrayList<Object> results = new java.util.ArrayList<>(fieldCount);

            for (int i = 0; i < fieldCount; i++) {
              String fieldName = req.fieldNames(i);

              // Try VarHandle for faster access
              java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
              Object fieldValue;
              if (vh != null) {
                fieldValue = vh.get(target);
              } else {
                java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
                field.setAccessible(true);
                fieldValue = field.get(target);
              }

              // Wrap non-auto-convertible objects
              if (fieldValue != null && !MethodResolver.isAutoConvertible(fieldValue)) {
                quota.allocateObject();
                long newId = objectIdCounter.getAndIncrement();
                objectRegistry.put(newId, fieldValue);
                sessionObjectIds.add(newId);
                fieldValue = new ObjectRefT(newId);
              }
              results.add(fieldValue);
            }
            result = results; // Will be packed as ListVal

          } else if (cmd.action() == Action.InvokeMethods) {
            // Vectorized method calls: invoke multiple methods on same target
            long targetId = cmd.targetId();
            Object target = objectRegistry.get(targetId);
            if (target == null) {
              throw new RuntimeException("Object " + targetId + " not found");
            }

            InvokeMethodsRequest req = cmd.invokeMethods();
            if (req == null) {
              throw new IllegalArgumentException(
                  "InvokeMethods action requires invoke_methods field");
            }

            int callCount = req.methodCallsLength();
            ResourceLimits.checkMethodCallsSize(callCount);
            java.util.ArrayList<Object> results = new java.util.ArrayList<>(callCount);

            for (int i = 0; i < callCount; i++) {
              MethodCall call = req.methodCalls(i);
              String methodName = call.methodName();
              boolean returnObjectRef = call.returnObjectRef();

              // Convert arguments
              int argCount = call.argsLength();
              Object[] javaArgs = new Object[argCount];
              Class<?>[] argTypes = new Class<?>[argCount];
              for (int j = 0; j < argCount; j++) {
                Argument arg = call.args(j);
                Object[] converted = convertArgument(arg);
                javaArgs[j] = converted[0];
                argTypes[j] = (Class<?>) converted[1];
              }

              // Invoke method
              Object methodResult;
              if (argCount == 0) {
                MethodHandle handle = getNoArgMethodHandle(target.getClass(), methodName);
                if (handle != null) {
                  methodResult = handle.invoke(target);
                } else {
                  var mwa =
                      MethodResolver.findMethodWithArgs(
                          target.getClass(), methodName, argTypes, javaArgs);
                  methodResult = mwa.cached.invoke(target, mwa.args);
                }
              } else {
                var mwa =
                    MethodResolver.findMethodWithArgs(
                        target.getClass(), methodName, argTypes, javaArgs);
                methodResult = mwa.cached.invoke(target, mwa.args);
              }

              // Wrap result if needed
              if (methodResult != null
                  && (returnObjectRef || !MethodResolver.isAutoConvertible(methodResult))) {
                quota.allocateObject();
                long newId = objectIdCounter.getAndIncrement();
                objectRegistry.put(newId, methodResult);
                sessionObjectIds.add(newId);
                methodResult = new ObjectRefT(newId);
              }
              results.add(methodResult);
            }
            result = results; // Will be packed as ListVal

          } else if (cmd.action() == Action.CreateObjects) {
            // Vectorized object creation: create multiple objects
            CreateObjectsRequest req = cmd.createObjects();
            if (req == null) {
              throw new IllegalArgumentException(
                  "CreateObjects action requires create_objects field");
            }

            int objCount = req.objectsLength();
            ResourceLimits.checkCreateObjectsSize(objCount);
            java.util.ArrayList<Object> results = new java.util.ArrayList<>(objCount);

            for (int i = 0; i < objCount; i++) {
              ObjectSpec spec = req.objects(i);
              String className = spec.className();

              if (!isClassAllowed(className)) {
                throw new SecurityException("Class not allowed: " + className);
              }

              // Check object limit before creating
              quota.allocateObject();

              Class<?> clazz = getClass(className);
              Object instance;

              int argCount = spec.argsLength();
              if (argCount == 0) {
                instance = getNoArgConstructor(clazz).newInstance();
              } else {
                Object[] javaArgs = new Object[argCount];
                Class<?>[] argTypes = new Class<?>[argCount];
                for (int j = 0; j < argCount; j++) {
                  Argument arg = spec.args(j);
                  Object[] converted = convertArgument(arg);
                  javaArgs[j] = converted[0];
                  argTypes[j] = (Class<?>) converted[1];
                }

                var ctorResult = MethodResolver.findConstructorWithArgs(clazz, argTypes, javaArgs);
                instance = ctorResult.cached.newInstance(ctorResult.args);
              }

              long newId = objectIdCounter.getAndIncrement();
              objectRegistry.put(newId, instance);
              sessionObjectIds.add(newId);
              results.add(new ObjectRefT(newId));
            }
            result = results; // Will be packed as ListVal of ObjectRefs

          } else if (cmd.action() == Action.Batch) {
            // Batch command: execute multiple sub-commands in sequence
            BatchCommand batch = cmd.batch();
            if (batch == null) {
              throw new IllegalArgumentException("Batch action requires batch field");
            }

            int commandCount = batch.commandsLength();
            ResourceLimits.checkBatchSize(commandCount);
            boolean stopOnError = batch.stopOnError();

            // Execute each sub-command, collect response offsets
            int[] subResponseOffsets = new int[commandCount];
            int errorIndex = -1;
            int executedCount = 0;

            for (int i = 0; i < commandCount; i++) {
              Command subCmd = batch.commands(i);

              try {
                // Execute sub-command and get result
                Object subResult =
                    executeCommand(subCmd, sessionObjectIds, sessionCallbackIds, sharedMem, quota);
                subResponseOffsets[i] =
                    packSuccessWithSession(builder, subResult, sessionObjectIds);
                executedCount++;
              } catch (Throwable subT) {
                // Pack error for this sub-command
                Throwable subCause = subT;
                if (subT instanceof java.lang.reflect.InvocationTargetException
                    && subT.getCause() != null) {
                  subCause = subT.getCause();
                }
                String subMessage = formatException(subCause);
                String subErrorType = subCause.getClass().getName();
                subResponseOffsets[i] = packError(builder, subMessage, subErrorType);
                executedCount++;

                if (stopOnError) {
                  errorIndex = i;
                  break; // Stop executing remaining commands
                }
              }
            }

            // Build BatchResponse
            responseOffset =
                packBatchResponse(builder, subResponseOffsets, executedCount, errorIndex);

            // Skip normal packSuccess - we've already built the response
            builder.finish(responseOffset);
            ByteBuffer resBuf = builder.dataBuffer();
            int resSize = resBuf.remaining();

            // Validate response size fits in zone
            if (resSize > RESPONSE_ZONE_SIZE) {
              builder = new FlatBufferBuilder(256);
              String errorMsg =
                  String.format(
                      "Response too large: %d bytes exceeds %d byte limit",
                      resSize, RESPONSE_ZONE_SIZE);
              responseOffset = packError(builder, errorMsg, "org.gatun.PayloadTooLargeException");
              builder.finish(responseOffset);
              resBuf = builder.dataBuffer();
              resSize = resBuf.remaining();
            }

            MemorySegment responseSlice = sharedMem.asSlice(this.responseOffset, resSize);
            responseSlice.copyFrom(MemorySegment.ofBuffer(resBuf));

            // Signal Python
            lengthBuf.putInt(resSize);
            lengthBuf.flip();
            client.write(lengthBuf);
            lengthBuf.clear();

            // Clean up and continue to next command
            if (requestId != 0) {
              cancelledRequests.get().remove(requestId);
            }
            currentRequestId.remove();
            continue; // Skip the normal response logic below

          } else if (cmd.action() == Action.GetMetrics) {
            // Return server metrics as a string report
            result = Metrics.getInstance().report();
          } else if (cmd.action() == Action.Ping) {
            // Health check - just return true to confirm server is alive
            result = true;
          }

          // 3. Pack Success
          responseOffset = packSuccess(builder, result);

        } catch (Throwable t) {
          // Unwrap InvocationTargetException to get the real cause
          Throwable cause = t;
          if (t instanceof java.lang.reflect.InvocationTargetException && t.getCause() != null) {
            cause = t.getCause();
          }

          if (cause instanceof NoSuchMethodException
              || cause instanceof IllegalArgumentException
              || cause instanceof SecurityException
              || cause instanceof NoSuchFieldException) {
            LOG.log(Level.FINE, "Client error: " + cause.getMessage());
          } else {
            LOG.log(Level.WARNING, "Error processing command", cause);
          }

          errorMessage = formatException(cause);
          errorType = cause.getClass().getName();
          requestSuccess = false;
          responseOffset = packError(builder, errorMessage, errorType);
        }

        // Record request completion in observer
        observer.onRequestEnd(requestSuccess, errorType, errorMessage);

        // 4. Write Response to Zone C
        builder.finish(responseOffset);
        ByteBuffer resBuf = builder.dataBuffer();
        int resSize = resBuf.remaining();

        // Validate response size fits in zone
        if (resSize > RESPONSE_ZONE_SIZE) {
          // Response too large - send error instead
          builder = new FlatBufferBuilder(256);
          String errorMsg =
              String.format(
                  "Response too large: %d bytes exceeds %d byte limit",
                  resSize, RESPONSE_ZONE_SIZE);
          responseOffset = packError(builder, errorMsg, "org.gatun.PayloadTooLargeException");
          builder.finish(responseOffset);
          resBuf = builder.dataBuffer();
          resSize = resBuf.remaining();
        }

        MemorySegment responseSlice = sharedMem.asSlice(this.responseOffset, resSize);
        responseSlice.copyFrom(MemorySegment.ofBuffer(resBuf));

        // 5. Signal Python
        lengthBuf.putInt(resSize);
        lengthBuf.flip();
        client.write(lengthBuf);
        lengthBuf.clear();

        // Clean up completed request from cancelled set
        if (requestId != 0) {
          cancelledRequests.get().remove(requestId);
        }
        currentRequestId.remove();
      }
    } catch (IOException e) {
      LOG.fine("Client session " + sessionId + " ended: " + e.getMessage());
    } finally {
      // Notify observer of session end
      observer.onSessionEnd();

      // Log quota summary
      LOG.fine(quota.getSummary());

      // Cleanup orphans: remove session objects and callbacks from global registries
      for (Long id : sessionObjectIds) objectRegistry.remove(id);
      for (Long id : sessionCallbackIds) callbackRegistry.remove(id);
      // Clear thread-locals
      cancelledRequests.get().clear();
      currentRequestId.remove();
      argumentNestingDepth.remove();
      // Clean up session shared memory file
      try {
        Files.deleteIfExists(sessionShmPath);
        LOG.fine("Session " + sessionId + " SHM file deleted: " + sessionShmPath);
      } catch (IOException e) {
        LOG.warning("Failed to delete session SHM file: " + sessionShmPath);
      }
    }
  }

  /**
   * Check if the current request has been cancelled. Can be called from long-running operations to
   * support cooperative cancellation.
   *
   * @throws InterruptedException if the current request was cancelled
   */
  public static void checkCancelled() throws InterruptedException {
    Long reqId = currentRequestId.get();
    if (reqId != null && reqId != 0 && cancelledRequests.get().contains(reqId)) {
      throw new InterruptedException("Request " + reqId + " was cancelled");
    }
  }

  /**
   * Check if the current request has been cancelled without throwing.
   *
   * @return true if the current request was cancelled
   */
  public static boolean isCancelled() {
    Long reqId = currentRequestId.get();
    return reqId != null && reqId != 0 && cancelledRequests.get().contains(reqId);
  }

  /**
   * Execute a single command and return the result.
   *
   * <p>This method is used by batch processing to execute sub-commands. It handles all action types
   * except Batch (to prevent recursion) and Cancel (which is handled specially).
   *
   * @param cmd The command to execute
   * @param sessionObjectIds Set of object IDs owned by this session (for cleanup)
   * @param sessionCallbackIds Set of callback IDs owned by this session
   * @param sharedMem Shared memory segment for Arrow operations
   * @param quota Session quota tracker for resource limits
   * @return The result object (may be null, primitive, String, ObjectRefT, etc.)
   * @throws Throwable Any exception from command execution
   */
  private Object executeCommand(
      Command cmd,
      Set<Long> sessionObjectIds,
      Set<Long> sessionCallbackIds,
      MemorySegment sharedMem,
      SessionQuota quota)
      throws Throwable {

    // Disallow nested batch commands
    if (cmd.action() == Action.Batch) {
      throw new IllegalArgumentException("Nested batch commands are not supported");
    }

    // Disallow Cancel in batch (should be handled at top level)
    if (cmd.action() == Action.Cancel) {
      throw new IllegalArgumentException("Cancel action cannot be used in batch");
    }

    Object result = null;

    if (cmd.action() == Action.CreateObject) {
      String className = cmd.targetName();
      if (!isClassAllowed(className)) {
        throw new SecurityException("Class not allowed: " + className);
      }

      // Check object limit before creating
      quota.allocateObject();

      Class<?> clazz = getClass(className);

      int argCount = cmd.argsLength();
      Object instance;

      if (argCount == 0) {
        instance = getNoArgConstructor(clazz).newInstance();
      } else {
        Object[] javaArgs = new Object[argCount];
        Class<?>[] argTypes = new Class<?>[argCount];

        for (int i = 0; i < argCount; i++) {
          Argument arg = cmd.args(i);
          Object[] converted = convertArgument(arg);
          javaArgs[i] = converted[0];
          argTypes[i] = (Class<?>) converted[1];
        }

        var ctorResult = MethodResolver.findConstructorWithArgs(clazz, argTypes, javaArgs);
        instance = ctorResult.cached.newInstance(ctorResult.args);
      }

      long newId = objectIdCounter.getAndIncrement();
      objectRegistry.put(newId, instance);
      sessionObjectIds.add(newId);
      result = new ObjectRefT(newId);

    } else if (cmd.action() == Action.FreeObject) {
      long targetId = cmd.targetId();
      if (sessionObjectIds.contains(targetId)) {
        objectRegistry.remove(targetId);
        sessionObjectIds.remove(targetId);
        quota.releaseObject();
      }
      result = null;

    } else if (cmd.action() == Action.InvokeMethod) {
      long targetId = cmd.targetId();
      Object target = objectRegistry.get(targetId);
      String methodName = cmd.targetName();

      if (target == null) throw new RuntimeException("Object " + targetId + " not found");

      int argCount = cmd.argsLength();

      if (argCount == 0) {
        MethodHandle handle = getNoArgMethodHandle(target.getClass(), methodName);
        if (handle != null) {
          result = handle.invoke(target);
        } else {
          MethodWithArgs mwa =
              MethodResolver.findMethodWithArgs(
                  target.getClass(),
                  methodName,
                  ReflectionCache.EMPTY_CLASS_ARRAY,
                  ReflectionCache.EMPTY_OBJECT_ARRAY);
          result = mwa.cached.invoke(target, mwa.args);
        }
      } else {
        Object[] javaArgs = new Object[argCount];
        Class<?>[] argTypes = new Class<?>[argCount];

        for (int i = 0; i < argCount; i++) {
          Argument arg = cmd.args(i);
          Object[] converted = convertArgument(arg);
          javaArgs[i] = converted[0];
          argTypes[i] = (Class<?>) converted[1];
        }

        MethodWithArgs mwa =
            MethodResolver.findMethodWithArgs(target.getClass(), methodName, argTypes, javaArgs);
        result = mwa.cached.invoke(target, mwa.args);
      }

      // Wrap returned objects in registry
      if (result != null && !MethodResolver.isAutoConvertible(result)) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        objectRegistry.put(newId, result);
        sessionObjectIds.add(newId);
        result = new ObjectRefT(newId);
      }

    } else if (cmd.action() == Action.InvokeStaticMethod) {
      String fullName = cmd.targetName();
      int lastDot = fullName.lastIndexOf('.');
      if (lastDot == -1) {
        throw new IllegalArgumentException("Invalid static method format: " + fullName);
      }
      String className = fullName.substring(0, lastDot);
      String methodName = fullName.substring(lastDot + 1);

      if (!isClassAllowed(className)) {
        throw new SecurityException("Class not allowed: " + className);
      }

      Class<?> clazz = getClass(className);

      int argCount = cmd.argsLength();
      Object[] javaArgs = new Object[argCount];
      Class<?>[] argTypes = new Class<?>[argCount];

      for (int i = 0; i < argCount; i++) {
        Argument arg = cmd.args(i);
        Object[] converted = convertArgument(arg);
        javaArgs[i] = converted[0];
        argTypes[i] = (Class<?>) converted[1];
      }

      MethodWithArgs mwa = MethodResolver.findMethodWithArgs(clazz, methodName, argTypes, javaArgs);
      result = mwa.cached.invoke(null, mwa.args);

      // Wrap returned objects in registry
      if (result != null && (cmd.returnObjectRef() || !MethodResolver.isAutoConvertible(result))) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        objectRegistry.put(newId, result);
        sessionObjectIds.add(newId);
        result = new ObjectRefT(newId);
      }

    } else if (cmd.action() == Action.GetField) {
      long targetId = cmd.targetId();
      Object target = objectRegistry.get(targetId);
      String fieldName = cmd.targetName();

      if (target == null) throw new RuntimeException("Object " + targetId + " not found");

      // Try VarHandle for faster access
      java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
      if (vh != null) {
        result = vh.get(target);
      } else {
        java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
        field.setAccessible(true);
        result = field.get(target);
      }

      if (result != null && !MethodResolver.isAutoConvertible(result)) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        objectRegistry.put(newId, result);
        sessionObjectIds.add(newId);
        result = new ObjectRefT(newId);
      }

    } else if (cmd.action() == Action.SetField) {
      long targetId = cmd.targetId();
      Object target = objectRegistry.get(targetId);
      String fieldName = cmd.targetName();

      if (target == null) throw new RuntimeException("Object " + targetId + " not found");

      if (cmd.argsLength() != 1)
        throw new IllegalArgumentException("SetField requires exactly one argument");

      Argument arg = cmd.args(0);
      Object[] converted = convertArgument(arg);
      Object value = converted[0];

      // Try VarHandle for faster access
      java.lang.invoke.VarHandle vh = getInstanceVarHandle(target.getClass(), fieldName);
      if (vh != null) {
        vh.set(target, value);
      } else {
        java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
      }
      result = null;

    } else if (cmd.action() == Action.IsInstanceOf) {
      long targetId = cmd.targetId();
      Object target = objectRegistry.get(targetId);
      String className = cmd.targetName();

      if (target == null) throw new RuntimeException("Object " + targetId + " not found");

      Class<?> clazz = getClass(className);
      result = clazz.isInstance(target);

    } else if (cmd.action() == Action.GetStaticField) {
      String fullName = cmd.targetName();
      int lastDot = fullName.lastIndexOf('.');
      if (lastDot == -1) {
        throw new IllegalArgumentException("Invalid static field format: " + fullName);
      }
      String className = fullName.substring(0, lastDot);
      String fieldName = fullName.substring(lastDot + 1);

      if (!isClassAllowed(className)) {
        throw new SecurityException("Class not allowed: " + className);
      }

      Class<?> clazz = getClass(className);

      // Try VarHandle for faster access
      ReflectionCache.CachedStaticVarHandle svh = getStaticVarHandle(clazz, fieldName);
      if (svh != null) {
        result = svh.handle.get();
      } else {
        java.lang.reflect.Field field = getStaticField(clazz, fieldName);
        result = field.get(null);
      }

      if (result != null && !MethodResolver.isAutoConvertible(result)) {
        quota.allocateObject();
        long newId = objectIdCounter.getAndIncrement();
        objectRegistry.put(newId, result);
        sessionObjectIds.add(newId);
        result = new ObjectRefT(newId);
      }

    } else if (cmd.action() == Action.SetStaticField) {
      String fullName = cmd.targetName();
      int lastDot = fullName.lastIndexOf('.');
      if (lastDot == -1) {
        throw new IllegalArgumentException("Invalid static field format: " + fullName);
      }
      String className = fullName.substring(0, lastDot);
      String fieldName = fullName.substring(lastDot + 1);

      if (!isClassAllowed(className)) {
        throw new SecurityException("Class not allowed: " + className);
      }

      if (cmd.argsLength() != 1) {
        throw new IllegalArgumentException("SetStaticField requires exactly one argument");
      }

      Argument arg = cmd.args(0);
      Object[] converted = convertArgument(arg);
      Object value = converted[0];

      Class<?> clazz = getClass(className);

      // Try VarHandle for faster access
      ReflectionCache.CachedStaticVarHandle svh = getStaticVarHandle(clazz, fieldName);
      if (svh != null) {
        svh.handle.set(value);
      } else {
        java.lang.reflect.Field field = getStaticField(clazz, fieldName);
        field.set(null, value);
      }
      result = null;

    } else {
      // Actions not supported in batch: Arrow operations, callbacks, etc.
      throw new UnsupportedOperationException(
          "Action " + cmd.action() + " is not supported in batch mode");
    }

    return result;
  }

  // --- HELPER: Convert FlatBuffer Argument to Java Object ---
  private Object[] convertArgument(Argument arg) {
    // Track nesting depth to prevent stack overflow from deeply nested structures
    int depth = argumentNestingDepth.get();
    ResourceLimits.checkNestingDepth(depth);
    argumentNestingDepth.set(depth + 1);

    try {
      return convertArgumentInternal(arg);
    } finally {
      argumentNestingDepth.set(depth);
    }
  }

  private Object[] convertArgumentInternal(Argument arg) {
    byte valType = arg.valType();
    Object value;
    Class<?> type;

    // Check for explicit type hint - used for overload resolution
    String typeHint = arg.typeHint();

    if (valType == Value.StringVal) {
      StringVal sv = (StringVal) arg.val(tempStringVal.get());
      value = sv.v();
      // Check string length limit
      ResourceLimits.checkStringLength((String) value);
      type = String.class; // Use String for better overload resolution
    } else if (valType == Value.IntVal) {
      IntVal iv = (IntVal) arg.val(tempIntVal.get());
      long longVal = iv.v();
      // Check if value fits in int range; if so use int for common Java APIs
      // Otherwise use long to avoid overflow (e.g., epoch milliseconds for Timestamp)
      if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
        value = (int) longVal;
        type = int.class;
      } else {
        value = longVal;
        type = long.class;
      }
    } else if (valType == Value.DoubleVal) {
      DoubleVal dv = (DoubleVal) arg.val(tempDoubleVal.get());
      value = dv.v();
      type = double.class;
    } else if (valType == Value.BoolVal) {
      BoolVal bv = (BoolVal) arg.val(tempBoolVal.get());
      value = bv.v();
      type = boolean.class;
    } else if (valType == Value.CharVal) {
      CharVal cv = (CharVal) arg.val(tempCharVal.get());
      value = (char) cv.v(); // Convert ushort to char
      type = char.class;
    } else if (valType == Value.ObjectRef) {
      ObjectRef ref = (ObjectRef) arg.val(tempObjectRef.get());
      value = objectRegistry.get(ref.id());
      // Use actual object type for better overload resolution
      type = (value != null) ? value.getClass() : Object.class;
    } else if (valType == Value.ListVal) {
      // Convert Python list to Java ArrayList
      // Use fast path for homogeneous lists (all elements same type)
      ListVal lv = (ListVal) arg.val(tempListVal.get());
      int len = lv.itemsLength();
      // Check collection size limit
      ResourceLimits.checkCollectionSize(len);
      java.util.ArrayList<Object> list = new java.util.ArrayList<>(len);

      if (len > 0) {
        // Check first element type to determine if we can use fast path
        Argument first = lv.items(0);
        byte firstType = first.valType();

        // Fast paths for common homogeneous list types
        if (firstType == Value.StringVal) {
          // Try homogeneous string list
          boolean allStrings = true;
          for (int i = 1; i < len && allStrings; i++) {
            if (lv.items(i).valType() != Value.StringVal) allStrings = false;
          }
          if (allStrings) {
            StringVal sv = tempStringVal.get();
            for (int i = 0; i < len; i++) {
              lv.items(i).val(sv);
              list.add(sv.v());
            }
          } else {
            // Fall back to per-element conversion
            for (int i = 0; i < len; i++) {
              Object[] converted = convertArgument(lv.items(i));
              list.add(converted[0]);
            }
          }
        } else if (firstType == Value.IntVal) {
          // Try homogeneous int/long list
          boolean allInts = true;
          for (int i = 1; i < len && allInts; i++) {
            if (lv.items(i).valType() != Value.IntVal) allInts = false;
          }
          if (allInts) {
            IntVal iv = tempIntVal.get();
            for (int i = 0; i < len; i++) {
              lv.items(i).val(iv);
              long longVal = iv.v();
              if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
                list.add((int) longVal);
              } else {
                list.add(longVal);
              }
            }
          } else {
            for (int i = 0; i < len; i++) {
              Object[] converted = convertArgument(lv.items(i));
              list.add(converted[0]);
            }
          }
        } else if (firstType == Value.DoubleVal) {
          // Try homogeneous double list
          boolean allDoubles = true;
          for (int i = 1; i < len && allDoubles; i++) {
            if (lv.items(i).valType() != Value.DoubleVal) allDoubles = false;
          }
          if (allDoubles) {
            DoubleVal dv = tempDoubleVal.get();
            for (int i = 0; i < len; i++) {
              lv.items(i).val(dv);
              list.add(dv.v());
            }
          } else {
            for (int i = 0; i < len; i++) {
              Object[] converted = convertArgument(lv.items(i));
              list.add(converted[0]);
            }
          }
        } else if (firstType == Value.ObjectRef) {
          // Try homogeneous object ref list
          boolean allRefs = true;
          for (int i = 1; i < len && allRefs; i++) {
            if (lv.items(i).valType() != Value.ObjectRef) allRefs = false;
          }
          if (allRefs) {
            ObjectRef ref = tempObjectRef.get();
            for (int i = 0; i < len; i++) {
              lv.items(i).val(ref);
              list.add(objectRegistry.get(ref.id()));
            }
          } else {
            for (int i = 0; i < len; i++) {
              Object[] converted = convertArgument(lv.items(i));
              list.add(converted[0]);
            }
          }
        } else {
          // Other types or mixed: use generic conversion
          for (int i = 0; i < len; i++) {
            Object[] converted = convertArgument(lv.items(i));
            list.add(converted[0]);
          }
        }
      }
      value = list;
      type = java.util.ArrayList.class;
    } else if (valType == Value.MapVal) {
      // Convert Python dict to Java HashMap
      MapVal mv = (MapVal) arg.val(tempMapVal.get());
      int mapSize = mv.entriesLength();
      // Check collection size limit
      ResourceLimits.checkCollectionSize(mapSize);
      java.util.HashMap<Object, Object> map = new java.util.HashMap<>();
      for (int i = 0; i < mapSize; i++) {
        MapEntry entry = mv.entries(i);
        Object[] keyConverted = convertArgument(entry.key());
        Object[] valConverted = convertArgument(entry.value());
        map.put(keyConverted[0], valConverted[0]);
      }
      value = map;
      type = java.util.HashMap.class;
    } else if (valType == Value.NullVal) {
      value = null;
      // Null cannot be assigned to primitive types
      // If type hint specifies a primitive, this is an error
      if (typeHint != null && !typeHint.isEmpty()) {
        Class<?> hintedType = resolveTypeHint(typeHint);
        if (hintedType.isPrimitive()) {
          throw new IllegalArgumentException("Cannot pass null to primitive type: " + typeHint);
        }
        type = hintedType;
        // Skip the type hint resolution below since we already handled it
        return new Object[] {value, type};
      }
      type = Object.class;
    } else if (valType == Value.ArrayVal) {
      // Convert ArrayVal to Java array
      ArrayVal av = (ArrayVal) arg.val(tempArrayVal.get());
      value = convertArrayVal(av);
      type = value.getClass();
    } else {
      value = null;
      type = Object.class;
    }

    // If type hint is provided, use it for overload resolution instead of inferred type
    if (typeHint != null && !typeHint.isEmpty()) {
      type = resolveTypeHint(typeHint);
    }

    return new Object[] {value, type};
  }

  /**
   * Resolve a type hint string to a Class object. Supports primitives, boxed types, and fully
   * qualified class names.
   */
  private Class<?> resolveTypeHint(String typeHint) {
    // Handle primitive types
    return switch (typeHint) {
      case "int" -> int.class;
      case "long" -> long.class;
      case "double" -> double.class;
      case "float" -> float.class;
      case "boolean" -> boolean.class;
      case "byte" -> byte.class;
      case "short" -> short.class;
      case "char" -> char.class;
      case "void" -> void.class;
      // Handle common boxed types (shortcuts)
      case "Integer", "java.lang.Integer" -> Integer.class;
      case "Long", "java.lang.Long" -> Long.class;
      case "Double", "java.lang.Double" -> Double.class;
      case "Float", "java.lang.Float" -> Float.class;
      case "Boolean", "java.lang.Boolean" -> Boolean.class;
      case "Byte", "java.lang.Byte" -> Byte.class;
      case "Short", "java.lang.Short" -> Short.class;
      case "Character", "java.lang.Character" -> Character.class;
      case "String", "java.lang.String" -> String.class;
      case "Object", "java.lang.Object" -> Object.class;
      // Handle common collection interfaces (for better overload resolution)
      case "List", "java.util.List" -> java.util.List.class;
      case "Map", "java.util.Map" -> java.util.Map.class;
      case "Set", "java.util.Set" -> java.util.Set.class;
      case "Collection", "java.util.Collection" -> java.util.Collection.class;
      case "Iterable", "java.lang.Iterable" -> Iterable.class;
      default -> {
        // Try to load class by fully qualified name
        try {
          yield Class.forName(typeHint);
        } catch (ClassNotFoundException e) {
          LOG.warning("Type hint class not found: " + typeHint + ", using Object.class");
          yield Object.class;
        }
      }
    };
  }

  // --- HELPER: Convert ArrayVal to Java array ---
  private Object convertArrayVal(ArrayVal av) {
    byte elemType = av.elementType();

    if (elemType == ElementType.Int) {
      int len = av.intValuesLength();
      int[] arr = new int[len];
      for (int i = 0; i < len; i++) {
        arr[i] = av.intValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Long) {
      int len = av.longValuesLength();
      long[] arr = new long[len];
      for (int i = 0; i < len; i++) {
        arr[i] = av.longValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Double) {
      int len = av.doubleValuesLength();
      double[] arr = new double[len];
      for (int i = 0; i < len; i++) {
        arr[i] = av.doubleValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Float) {
      // Float was widened to double, narrow back
      int len = av.doubleValuesLength();
      float[] arr = new float[len];
      for (int i = 0; i < len; i++) {
        arr[i] = (float) av.doubleValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Bool) {
      int len = av.boolValuesLength();
      boolean[] arr = new boolean[len];
      for (int i = 0; i < len; i++) {
        arr[i] = av.boolValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Byte) {
      int len = av.byteValuesLength();
      byte[] arr = new byte[len];
      for (int i = 0; i < len; i++) {
        arr[i] = (byte) av.byteValues(i);
      }
      return arr;
    } else if (elemType == ElementType.Short) {
      // Short was widened to int, narrow back
      int len = av.intValuesLength();
      short[] arr = new short[len];
      for (int i = 0; i < len; i++) {
        arr[i] = (short) av.intValues(i);
      }
      return arr;
    } else if (elemType == ElementType.String) {
      // String array stored as object_values
      int len = av.objectValuesLength();
      String[] arr = new String[len];
      for (int i = 0; i < len; i++) {
        Argument item = av.objectValues(i);
        Object[] converted = convertArgument(item);
        arr[i] = (String) converted[0];
      }
      return arr;
    } else {
      // Object array
      int len = av.objectValuesLength();
      Object[] arr = new Object[len];
      for (int i = 0; i < len; i++) {
        Argument item = av.objectValues(i);
        Object[] converted = convertArgument(item);
        arr[i] = converted[0];
      }
      return arr;
    }
  }

  // --- HELPER: Format exception with full stack trace ---
  private static String formatException(Throwable t) {
    StringBuilder sb = new StringBuilder();
    sb.append(t.getClass().getName()).append(": ").append(t.getMessage());
    for (StackTraceElement ste : t.getStackTrace()) {
      sb.append("\n\tat ").append(ste.toString());
    }
    // Include cause chain
    Throwable cause = t.getCause();
    while (cause != null) {
      sb.append("\nCaused by: ")
          .append(cause.getClass().getName())
          .append(": ")
          .append(cause.getMessage());
      for (StackTraceElement ste : cause.getStackTrace()) {
        sb.append("\n\tat ").append(ste.toString());
      }
      cause = cause.getCause();
    }
    return sb.toString();
  }

  // --- HELPER: Pack a single value and return [type, offset] ---
  private int[] packValue(FlatBufferBuilder builder, Object value, Set<Long> sessionObjectIds) {
    byte type;
    int valueOffset;

    if (value == null) {
      type = Value.NullVal;
      NullVal.startNullVal(builder);
      valueOffset = NullVal.endNullVal(builder);
    } else if (value instanceof Boolean) {
      type = Value.BoolVal;
      valueOffset = BoolVal.createBoolVal(builder, (Boolean) value);
    } else if (value instanceof Character) {
      type = Value.CharVal;
      valueOffset = CharVal.createCharVal(builder, (Character) value);
    } else if (value instanceof Integer
        || value instanceof Long
        || value instanceof Short
        || value instanceof Byte) {
      type = Value.IntVal;
      valueOffset = IntVal.createIntVal(builder, ((Number) value).longValue());
    } else if (value instanceof Double || value instanceof Float) {
      type = Value.DoubleVal;
      valueOffset = DoubleVal.createDoubleVal(builder, ((Number) value).doubleValue());
    } else if (value instanceof String) {
      type = Value.StringVal;
      int sOff = builder.createString((String) value);
      valueOffset = StringVal.createStringVal(builder, sOff);
    } else if (value instanceof ObjectRefT) {
      type = Value.ObjectRef;
      valueOffset = ObjectRef.createObjectRef(builder, ((ObjectRefT) value).id);
    } else if (value instanceof List<?> list) {
      // Auto-convert List to ListVal
      type = Value.ListVal;
      int[] itemOffsets = new int[list.size()];
      for (int i = 0; i < list.size(); i++) {
        int[] packed = packValue(builder, list.get(i), sessionObjectIds);
        Argument.startArgument(builder);
        Argument.addValType(builder, (byte) packed[0]);
        Argument.addVal(builder, packed[1]);
        itemOffsets[i] = Argument.endArgument(builder);
      }
      int itemsVec = ListVal.createItemsVector(builder, itemOffsets);
      valueOffset = ListVal.createListVal(builder, itemsVec);
    } else if (value instanceof Map<?, ?> map) {
      // Auto-convert Map to MapVal
      type = Value.MapVal;
      int[] entryOffsets = new int[map.size()];
      int idx = 0;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        int[] keyPacked = packValue(builder, entry.getKey(), sessionObjectIds);
        int[] valPacked = packValue(builder, entry.getValue(), sessionObjectIds);

        Argument.startArgument(builder);
        Argument.addValType(builder, (byte) keyPacked[0]);
        Argument.addVal(builder, keyPacked[1]);
        int keyArgOffset = Argument.endArgument(builder);

        Argument.startArgument(builder);
        Argument.addValType(builder, (byte) valPacked[0]);
        Argument.addVal(builder, valPacked[1]);
        int valArgOffset = Argument.endArgument(builder);

        MapEntry.startMapEntry(builder);
        MapEntry.addKey(builder, keyArgOffset);
        MapEntry.addValue(builder, valArgOffset);
        entryOffsets[idx++] = MapEntry.endMapEntry(builder);
      }
      int entriesVec = MapVal.createEntriesVector(builder, entryOffsets);
      valueOffset = MapVal.createMapVal(builder, entriesVec);
    } else if (value.getClass().isArray()) {
      // Handle Java arrays
      type = Value.ArrayVal;
      valueOffset = packArray(builder, value, sessionObjectIds);
    } else {
      // Unknown type - wrap as object reference
      long newId = objectIdCounter.getAndIncrement();
      objectRegistry.put(newId, value);
      if (sessionObjectIds != null) {
        sessionObjectIds.add(newId);
      }
      type = Value.ObjectRef;
      valueOffset = ObjectRef.createObjectRef(builder, newId);
    }

    return new int[] {type, valueOffset};
  }

  // --- HELPER: Pack Java array into ArrayVal ---
  private int packArray(FlatBufferBuilder builder, Object array, Set<Long> sessionObjectIds) {
    Class<?> componentType = array.getClass().getComponentType();

    if (componentType == int.class) {
      int[] arr = (int[]) array;
      int vecOffset = ArrayVal.createIntValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Int);
      ArrayVal.addIntValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == long.class) {
      long[] arr = (long[]) array;
      int vecOffset = ArrayVal.createLongValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Long);
      ArrayVal.addLongValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == double.class) {
      double[] arr = (double[]) array;
      int vecOffset = ArrayVal.createDoubleValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Double);
      ArrayVal.addDoubleValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == float.class) {
      float[] arr = (float[]) array;
      // Widen float[] to double[]
      double[] widened = new double[arr.length];
      for (int i = 0; i < arr.length; i++) {
        widened[i] = arr[i];
      }
      int vecOffset = ArrayVal.createDoubleValuesVector(builder, widened);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Float);
      ArrayVal.addDoubleValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == boolean.class) {
      boolean[] arr = (boolean[]) array;
      int vecOffset = ArrayVal.createBoolValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Bool);
      ArrayVal.addBoolValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == byte.class) {
      byte[] arr = (byte[]) array;
      int vecOffset = ArrayVal.createByteValuesVector(builder, arr);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Byte);
      ArrayVal.addByteValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else if (componentType == short.class) {
      short[] arr = (short[]) array;
      // Widen short[] to int[]
      int[] widened = new int[arr.length];
      for (int i = 0; i < arr.length; i++) {
        widened[i] = arr[i];
      }
      int vecOffset = ArrayVal.createIntValuesVector(builder, widened);
      ArrayVal.startArrayVal(builder);
      ArrayVal.addElementType(builder, ElementType.Short);
      ArrayVal.addIntValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    } else {
      // Object array (String[], Object[], etc.)
      Object[] arr = (Object[]) array;
      int[] argOffsets = new int[arr.length];
      for (int i = 0; i < arr.length; i++) {
        int[] packed = packValue(builder, arr[i], sessionObjectIds);
        Argument.startArgument(builder);
        Argument.addValType(builder, (byte) packed[0]);
        Argument.addVal(builder, packed[1]);
        argOffsets[i] = Argument.endArgument(builder);
      }
      int vecOffset = ArrayVal.createObjectValuesVector(builder, argOffsets);
      ArrayVal.startArrayVal(builder);
      // Use String type for String[], Object for everything else
      if (componentType == String.class) {
        ArrayVal.addElementType(builder, ElementType.String);
      } else {
        ArrayVal.addElementType(builder, ElementType.Object);
      }
      ArrayVal.addObjectValues(builder, vecOffset);
      return ArrayVal.endArrayVal(builder);
    }
  }

  // --- CALLBACK HELPERS ---

  /**
   * Creates a dynamic proxy that implements the specified interface. When any method on the proxy
   * is called, it sends a callback request to Python and waits for the response.
   *
   * <p>The SessionContext is captured at proxy creation time so callbacks can be invoked from any
   * thread (e.g., Spark executor threads), not just the handleClient thread.
   */
  private Object createCallbackProxy(long callbackId, String interfaceName, SessionContext ctx)
      throws ClassNotFoundException {
    Class<?> interfaceClass = getClass(interfaceName);
    if (!interfaceClass.isInterface()) {
      throw new IllegalArgumentException(interfaceName + " is not an interface");
    }

    InvocationHandler handler =
        (proxy, method, args) -> {
          // Handle Object methods locally
          if (method.getDeclaringClass() == Object.class) {
            String methodName = method.getName();
            if ("toString".equals(methodName)) {
              return "PythonCallback[" + callbackId + "]@" + interfaceName;
            } else if ("hashCode".equals(methodName)) {
              return (int) callbackId;
            } else if ("equals".equals(methodName)) {
              return proxy == args[0];
            }
          }

          // Send callback request to Python and wait for response
          // Use captured session context so this works from any thread
          return invokeCallback(
              callbackId, method.getName(), args == null ? new Object[0] : args, ctx);
        };

    return Proxy.newProxyInstance(
        interfaceClass.getClassLoader(), new Class<?>[] {interfaceClass}, handler);
  }

  /**
   * Sends a callback invocation request to Python and blocks until Python responds. This is called
   * from the dynamic proxy when Java code invokes a method on the callback object.
   *
   * <p>The SessionContext is passed explicitly (captured at proxy creation time) so this method can
   * be called from any thread, not just the handleClient thread. A lock ensures thread-safe
   * round-trip communication when multiple threads invoke callbacks concurrently.
   */
  private Object invokeCallback(
      long callbackId, String methodName, Object[] args, SessionContext ctx) throws Exception {
    if (ctx == null || ctx.channel == null || ctx.sharedMem == null) {
      throw new IllegalStateException("Callback invoked with invalid session context");
    }

    SocketChannel channel = ctx.channel;
    MemorySegment sharedMem = ctx.sharedMem;

    // Lock to serialize callback round-trips from different threads
    // This prevents interleaved writes/reads on the same socket/SHM
    synchronized (ctx.callbackLock) {
      // Build the callback request response
      FlatBufferBuilder builder = new FlatBufferBuilder(1024);

      // Build callback arguments
      int[] argOffsets = new int[args.length];
      for (int i = 0; i < args.length; i++) {
        int[] packed = packValue(builder, args[i], ctx.sessionObjectIds);
        Argument.startArgument(builder);
        Argument.addValType(builder, (byte) packed[0]);
        Argument.addVal(builder, packed[1]);
        argOffsets[i] = Argument.endArgument(builder);
      }

      int argsVec = Response.createCallbackArgsVector(builder, argOffsets);
      int methodOff = builder.createString(methodName);

      Response.startResponse(builder);
      Response.addIsError(builder, false);
      Response.addIsCallback(builder, true);
      Response.addCallbackId(builder, callbackId);
      Response.addCallbackMethod(builder, methodOff);
      Response.addCallbackArgs(builder, argsVec);
      int responseOffset = Response.endResponse(builder);

      builder.finish(responseOffset);
      ByteBuffer resBuf = builder.dataBuffer();
      int resSize = resBuf.remaining();

      // Write to response zone
      MemorySegment responseSlice = sharedMem.asSlice(this.responseOffset, resSize);
      responseSlice.copyFrom(MemorySegment.ofBuffer(resBuf));

      // Signal Python with the response size
      ByteBuffer lengthBuf = ByteBuffer.allocate(4);
      lengthBuf.order(ByteOrder.LITTLE_ENDIAN);
      lengthBuf.putInt(resSize);
      lengthBuf.flip();
      while (lengthBuf.hasRemaining()) {
        channel.write(lengthBuf);
      }

      // Now we need to read the callback response directly from Python
      // Python sends a CallbackResponse command back to us
      lengthBuf.clear();
      if (!readFully(channel, lengthBuf)) {
        throw new IOException("Connection closed while waiting for callback response");
      }
      lengthBuf.flip();
      int commandSize = lengthBuf.getInt();

      // Validate command size
      if (commandSize <= 0 || commandSize > PAYLOAD_OFFSET) {
        throw new IOException("Invalid callback response size: " + commandSize);
      }

      // Read the command from command zone
      MemorySegment cmdSlice = sharedMem.asSlice(COMMAND_OFFSET, commandSize);
      ByteBuffer cmdBuf = cmdSlice.asByteBuffer();
      cmdBuf.order(ByteOrder.LITTLE_ENDIAN);

      Command cmd = Command.getRootAsCommand(cmdBuf);
      if (cmd.action() != Action.CallbackResponse) {
        throw new RuntimeException("Expected CallbackResponse but got action: " + cmd.action());
      }

      // Extract the result from the callback response
      Object returnValue = null;
      boolean isError = false;
      String errorMsg = null;

      if (cmd.argsLength() > 0) {
        Argument arg = cmd.args(0);
        Object[] converted = convertArgument(arg);
        returnValue = converted[0];
      }

      // Check if there's an error flag (second arg if present)
      if (cmd.argsLength() > 1) {
        Argument errorArg = cmd.args(1);
        Object[] converted = convertArgument(errorArg);
        if (converted[0] instanceof Boolean) {
          isError = (Boolean) converted[0];
        }
      }

      // Error message is in target_name for error responses
      if (isError) {
        errorMsg = cmd.targetName();
      }

      if (isError) {
        throw new RuntimeException("Callback error: " + errorMsg);
      }

      return returnValue;
    } // end synchronized
  }

  // --- HELPER: Pack Result into FlatBuffer ---
  private int packSuccess(FlatBufferBuilder builder, Object result) {
    return packSuccessWithSession(builder, result, null);
  }

  private int packSuccessWithSession(
      FlatBufferBuilder builder, Object result, Set<Long> sessionObjectIds) {
    int[] packed = packValue(builder, result, sessionObjectIds);

    Response.startResponse(builder);
    Response.addIsError(builder, false);
    Response.addReturnValType(builder, (byte) packed[0]);
    Response.addReturnVal(builder, packed[1]);
    return Response.endResponse(builder);
  }

  private int packError(FlatBufferBuilder builder, String msg, String errorType) {
    int errOff = builder.createString(msg == null ? "Unknown Error" : msg);
    int typeOff =
        builder.createString(errorType == null ? "java.lang.RuntimeException" : errorType);
    Response.startResponse(builder);
    Response.addIsError(builder, true);
    Response.addErrorMsg(builder, errOff);
    Response.addErrorType(builder, typeOff);
    return Response.endResponse(builder);
  }

  /**
   * Pack a batch of responses into a BatchResponse.
   *
   * @param builder The FlatBuffer builder
   * @param responseOffsets Array of Response offsets (already built)
   * @param count Number of responses that were actually built
   * @param errorIndex Index of first error (-1 if no errors)
   */
  private int packBatchResponse(
      FlatBufferBuilder builder, int[] responseOffsets, int count, int errorIndex) {
    // Create vector of response offsets (only include executed ones)
    int[] toInclude =
        count == responseOffsets.length
            ? responseOffsets
            : java.util.Arrays.copyOf(responseOffsets, count);
    int responsesVec = BatchResponse.createResponsesVector(builder, toInclude);

    // Build BatchResponse
    BatchResponse.startBatchResponse(builder);
    BatchResponse.addResponses(builder, responsesVec);
    BatchResponse.addErrorIndex(builder, errorIndex);
    int batchRespOffset = BatchResponse.endBatchResponse(builder);

    // Wrap in outer Response
    Response.startResponse(builder);
    Response.addIsError(builder, false);
    Response.addBatch(builder, batchRespOffset);
    return Response.endResponse(builder);
  }

  /** Pack an Arrow write result into a response with ArrowBatchDescriptor. */
  private int packArrowResponse(
      FlatBufferBuilder builder, ArrowMemoryHandler.ArrowWriteResult result) {
    // Build schema bytes vector if present
    int schemaBytesVec = 0;
    if (result.schemaBytes != null) {
      schemaBytesVec = ArrowBatchDescriptor.createSchemaBytesVector(builder, result.schemaBytes);
    }

    // Build buffer descriptors
    int[] bufferOffsets = new int[result.bufferDescriptors.size()];
    for (int i = 0; i < result.bufferDescriptors.size(); i++) {
      long[] desc = result.bufferDescriptors.get(i);
      BufferDescriptor.startBufferDescriptor(builder);
      BufferDescriptor.addOffset(builder, desc[0]);
      BufferDescriptor.addLength(builder, desc[1]);
      bufferOffsets[i] = BufferDescriptor.endBufferDescriptor(builder);
    }
    int buffersVec = ArrowBatchDescriptor.createBuffersVector(builder, bufferOffsets);

    // Build field nodes
    int[] nodeOffsets = new int[result.fieldNodes.size()];
    for (int i = 0; i < result.fieldNodes.size(); i++) {
      long[] node = result.fieldNodes.get(i);
      FieldNode.startFieldNode(builder);
      FieldNode.addLength(builder, node[0]);
      FieldNode.addNullCount(builder, node[1]);
      nodeOffsets[i] = FieldNode.endFieldNode(builder);
    }
    int nodesVec = ArrowBatchDescriptor.createNodesVector(builder, nodeOffsets);

    // Build ArrowBatchDescriptor
    ArrowBatchDescriptor.startArrowBatchDescriptor(builder);
    ArrowBatchDescriptor.addSchemaHash(builder, result.schemaHash);
    if (schemaBytesVec != 0) {
      ArrowBatchDescriptor.addSchemaBytes(builder, schemaBytesVec);
    }
    ArrowBatchDescriptor.addNumRows(builder, result.numRows);
    ArrowBatchDescriptor.addNodes(builder, nodesVec);
    ArrowBatchDescriptor.addBuffers(builder, buffersVec);
    ArrowBatchDescriptor.addArenaEpoch(builder, result.arenaEpoch);
    int batchDescOffset = ArrowBatchDescriptor.endArrowBatchDescriptor(builder);

    // Build Response with arrow_batch
    Response.startResponse(builder);
    Response.addIsError(builder, false);
    Response.addArrowBatch(builder, batchDescOffset);
    return Response.endResponse(builder);
  }

  static class ObjectRefT {
    public long id;

    public ObjectRefT(long id) {
      this.id = id;
    }
  }

  /**
   * Reads exactly buffer.remaining() bytes from the channel. Returns true if successful, false on
   * EOF.
   */
  private static boolean readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
    buffer.clear();
    while (buffer.hasRemaining()) {
      int read = channel.read(buffer);
      if (read == -1) {
        return false; // EOF
      }
    }
    return true;
  }

  public static class ByteBufferInputStream extends java.io.InputStream {
    private final ByteBuffer buf;

    public ByteBufferInputStream(ByteBuffer buf) {
      this.buf = buf;
    }

    @Override
    public int read() {
      if (!buf.hasRemaining()) return -1;
      return buf.get() & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int off, int len) {
      if (!buf.hasRemaining()) return -1;
      len = Math.min(len, buf.remaining());
      buf.get(bytes, off, len);
      return len;
    }
  }

  // ========== OBSERVABILITY API ==========

  /**
   * Get the global Metrics instance for monitoring.
   *
   * <p>Use this to access request counts, latency percentiles, object counts, etc. Thread-safe and
   * can be called from any thread.
   */
  public static Metrics getMetrics() {
    return Metrics.getInstance();
  }

  /** Get a formatted metrics report for logging or debugging. */
  public static String getMetricsReport() {
    return Metrics.getInstance().report();
  }

  /**
   * Enable trace mode for verbose method resolution logging.
   *
   * <p>When enabled, logs detailed information about method overload resolution decisions. Useful
   * for debugging "wrong method called" issues.
   *
   * <p>Enable via: -Dgatun.trace=true or call this method.
   */
  public static void setTraceMode(boolean enabled) {
    StructuredLogger.setTraceMode(enabled);
  }

  /** Check if trace mode is enabled. */
  public static boolean isTraceMode() {
    return StructuredLogger.isTraceEnabled();
  }

  public static void main(String[] args) {
    try {
      // Configure logging based on system property
      configureLogging();

      // Log resource limits at startup for debugging
      LOG.info(ResourceLimits.getSummary());

      long size = 16 * 1024 * 1024;
      String path = System.getProperty("user.home") + "/gatun.sock";
      if (args.length > 0) size = Long.parseLong(args[0]);
      if (args.length > 1) path = args[1];
      new GatunServer(path, size).start();
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Server failed to start", t);
      System.exit(1);
    }
  }

  /**
   * Configure Java logging based on system properties.
   *
   * <p>Reads gatun.log.level to set the logging level for org.gatun.server. Valid levels: FINEST,
   * FINER, FINE, INFO, WARNING, SEVERE
   */
  private static void configureLogging() {
    String levelStr = System.getProperty("gatun.log.level");
    if (levelStr == null) return;

    try {
      Level level = Level.parse(levelStr.toUpperCase());

      // Get the root logger and our logger
      java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
      java.util.logging.Logger gatunLogger = java.util.logging.Logger.getLogger("org.gatun.server");

      // Set our logger's level
      gatunLogger.setLevel(level);

      // Also set the console handler level (otherwise it filters out FINE/FINER/FINEST)
      for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
        if (handler instanceof java.util.logging.ConsoleHandler) {
          handler.setLevel(level);
        }
      }

      LOG.info("Log level set to: " + level);
    } catch (IllegalArgumentException e) {
      LOG.warning("Invalid log level: " + levelStr + ". Using default.");
    }
  }
}
