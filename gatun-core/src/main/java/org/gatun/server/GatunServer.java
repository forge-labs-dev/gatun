package org.gatun.server;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
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

public class GatunServer {
  private static final Logger LOG = Logger.getLogger(GatunServer.class.getName());

  // Protocol version - increment when making breaking changes to the protocol
  public static final int PROTOCOL_VERSION = 1;

  private final Path socketPath;
  private final Path memoryPath;
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
          "java.lang.Boolean",
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

  // --- CALLBACK REGISTRY ---
  // Maps callback ID -> CallbackInfo (interface name + proxy object)
  // Note: callback IDs are allocated from objectIdCounter to ensure uniqueness
  private final Map<Long, CallbackInfo> callbackRegistry = new ConcurrentHashMap<>();

  // Per-session: the socket channel for sending callback requests
  private static final ThreadLocal<SocketChannel> sessionChannel = new ThreadLocal<>();

  // Per-session: shared memory segment
  private static final ThreadLocal<MemorySegment> sessionSharedMem = new ThreadLocal<>();

  // Per-session: current request ID for cancellation support
  private static final ThreadLocal<Long> currentRequestId = new ThreadLocal<>();

  // Per-session: cancelled request IDs
  private static final ThreadLocal<Set<Long>> cancelledRequests =
      ThreadLocal.withInitial(HashSet::new);

  private final BufferAllocator allocator = new RootAllocator();
  private final ArrowMemoryHandler arrowHandler = new ArrowMemoryHandler(allocator);

  // --- METHOD CACHE ---
  // Caches resolved methods to avoid repeated reflection lookups.
  // Key: (class, methodName, argTypes) -> CachedMethod (method + varargs info)
  private static final Map<MethodCacheKey, CachedMethod> methodCache = new ConcurrentHashMap<>();

  // --- CONSTRUCTOR CACHE ---
  // Caches resolved constructors to avoid repeated reflection lookups.
  // Key: (class, argTypes) -> CachedConstructor (constructor + varargs info)
  private static final Map<ConstructorCacheKey, CachedConstructor> constructorCache =
      new ConcurrentHashMap<>();

  // --- NO-ARG CONSTRUCTOR CACHE ---
  // Caches no-arg constructors separately for fast lookup (most common case)
  // Key: class -> no-arg Constructor
  private static final Map<Class<?>, java.lang.reflect.Constructor<?>> noArgConstructorCache =
      new ConcurrentHashMap<>();

  /**
   * Get no-arg constructor for a class, using cache.
   */
  private static java.lang.reflect.Constructor<?> getNoArgConstructor(Class<?> clazz)
      throws NoSuchMethodException {
    java.lang.reflect.Constructor<?> ctor = noArgConstructorCache.get(clazz);
    if (ctor != null) {
      return ctor;
    }
    ctor = clazz.getDeclaredConstructor();
    noArgConstructorCache.put(clazz, ctor);
    return ctor;
  }

  // --- NO-ARG METHOD HANDLE CACHE ---
  // Caches no-arg methods as MethodHandles for fast invocation (most common case: size(), isEmpty(), etc)
  // MethodHandles are significantly faster than Method.invoke() after JIT warmup
  // Uses a simple two-level map: Class -> methodName -> MethodHandle
  private static final Map<Class<?>, Map<String, MethodHandle>> noArgMethodHandleCache =
      new ConcurrentHashMap<>();

  /** Lookup for converting Methods to MethodHandles. */
  private static final MethodHandles.Lookup METHOD_LOOKUP = MethodHandles.publicLookup();

  /** Empty Class array constant to avoid allocation. */
  private static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];

  /** Empty Object array constant to avoid allocation. */
  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  /**
   * Get no-arg method handle for a class, using fast cache lookup.
   *
   * @return the MethodHandle, or null if not found (caller should fall back to normal resolution)
   */
  private static MethodHandle getNoArgMethodHandle(Class<?> clazz, String methodName) {
    Map<String, MethodHandle> classMethods = noArgMethodHandleCache.get(clazz);
    if (classMethods != null) {
      MethodHandle cached = classMethods.get(methodName);
      if (cached != null) {
        return cached;
      }
    }

    // Try to find the no-arg method and convert to MethodHandle
    try {
      java.lang.reflect.Method method = clazz.getMethod(methodName);
      MethodHandle handle = METHOD_LOOKUP.unreflect(method);
      // Cache it
      noArgMethodHandleCache.computeIfAbsent(clazz, k -> new ConcurrentHashMap<>()).put(methodName, handle);
      return handle;
    } catch (NoSuchMethodException | IllegalAccessException e) {
      // Not a simple no-arg method or not accessible - return null so caller uses normal resolution
      return null;
    }
  }

  // --- STATIC FIELD CACHE ---
  // Caches static field lookups to avoid repeated reflection overhead.
  // Key: "className.fieldName" -> Field object
  private static final Map<String, java.lang.reflect.Field> staticFieldCache =
      new ConcurrentHashMap<>();

  /**
   * Get a static field by class and name, using cache.
   */
  private static java.lang.reflect.Field getStaticField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    String key = clazz.getName() + "." + fieldName;
    java.lang.reflect.Field field = staticFieldCache.get(key);
    if (field != null) {
      return field;
    }
    field = clazz.getField(fieldName);
    staticFieldCache.put(key, field);
    return field;
  }

  // --- INSTANCE FIELD CACHE ---
  // Caches instance field lookups (including hierarchy search) to avoid repeated reflection.
  // Key: "className.fieldName" -> Field object (or sentinel for not found)
  private static final Map<String, java.lang.reflect.Field> instanceFieldCache =
      new ConcurrentHashMap<>();

  /**
   * Get an instance field by class and name, using cache.
   * Searches the class hierarchy and caches the result.
   */
  private static java.lang.reflect.Field getInstanceField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    String key = clazz.getName() + "." + fieldName;
    java.lang.reflect.Field field = instanceFieldCache.get(key);
    if (field != null) {
      return field;
    }
    // Search up the class hierarchy
    Class<?> current = clazz;
    while (current != null) {
      try {
        field = current.getDeclaredField(fieldName);
        instanceFieldCache.put(key, field);
        return field;
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException("No field named '" + fieldName + "' in " + clazz.getName());
  }

  // --- METHODS CACHE ---
  // Caches getMethods() results to avoid repeated reflection overhead.
  // Key: Class -> Method[] array
  private static final Map<Class<?>, java.lang.reflect.Method[]> methodsCache =
      new ConcurrentHashMap<>();

  /**
   * Get all public methods for a class, using cache.
   */
  private static java.lang.reflect.Method[] getCachedMethods(Class<?> clazz) {
    java.lang.reflect.Method[] methods = methodsCache.get(clazz);
    if (methods != null) {
      return methods;
    }
    methods = clazz.getMethods();
    methodsCache.put(clazz, methods);
    return methods;
  }

  // --- CONSTRUCTORS CACHE ---
  // Caches getConstructors() results to avoid repeated reflection overhead.
  // Key: Class -> Constructor[] array
  private static final Map<Class<?>, java.lang.reflect.Constructor<?>[]> constructorsCache =
      new ConcurrentHashMap<>();

  /**
   * Get all public constructors for a class, using cache.
   */
  private static java.lang.reflect.Constructor<?>[] getCachedConstructors(Class<?> clazz) {
    java.lang.reflect.Constructor<?>[] constructors = constructorsCache.get(clazz);
    if (constructors != null) {
      return constructors;
    }
    constructors = clazz.getConstructors();
    constructorsCache.put(clazz, constructors);
    return constructors;
  }

  // --- CLASS CACHE ---
  // Caches Class.forName lookups to avoid repeated class loading overhead.
  // Key: fully qualified class name -> Class object
  private static final Map<String, Class<?>> classCache = new ConcurrentHashMap<>();

  /**
   * Get a Class object by name, using cache to avoid repeated Class.forName overhead.
   */
  private static Class<?> getClass(String className) throws ClassNotFoundException {
    Class<?> clazz = classCache.get(className);
    if (clazz != null) {
      return clazz;
    }
    clazz = Class.forName(className);
    classCache.put(className, clazz);
    return clazz;
  }

  /**
   * Try to get a Class object by name, returning null if not found.
   * Uses cache for faster lookups.
   */
  private static Class<?> tryGetClass(String className) {
    Class<?> clazz = classCache.get(className);
    if (clazz != null) {
      return clazz;
    }
    try {
      clazz = Class.forName(className);
      classCache.put(className, clazz);
      return clazz;
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  /** Key for method cache lookup. */
  private static final class MethodCacheKey {
    private final Class<?> clazz;
    private final String methodName;
    private final Class<?>[] argTypes;
    private final int hashCode;

    MethodCacheKey(Class<?> clazz, String methodName, Class<?>[] argTypes) {
      this.clazz = clazz;
      this.methodName = methodName;
      this.argTypes = argTypes;
      // Pre-compute hash for faster lookups
      int h = clazz.hashCode() * 31 + methodName.hashCode();
      for (Class<?> t : argTypes) {
        h = h * 31 + (t != null ? t.hashCode() : 0);
      }
      this.hashCode = h;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof MethodCacheKey other)) return false;
      if (clazz != other.clazz) return false;
      if (!methodName.equals(other.methodName)) return false;
      if (argTypes.length != other.argTypes.length) return false;
      for (int i = 0; i < argTypes.length; i++) {
        if (argTypes[i] != other.argTypes[i]) return false;
      }
      return true;
    }
  }

  /** Cached method resolution result with MethodHandle for fast invocation. */
  private static final class CachedMethod {
    final java.lang.reflect.Method method;
    final MethodHandle handle; // MethodHandle for faster invocation
    final boolean isVarArgs;
    final int fixedArgCount; // For varargs: number of fixed args before vararg array
    final Class<?> varargComponentType; // For varargs: component type of vararg array

    CachedMethod(java.lang.reflect.Method method) {
      this.method = method;
      this.isVarArgs = method.isVarArgs();
      if (isVarArgs) {
        Class<?>[] paramTypes = method.getParameterTypes();
        this.fixedArgCount = paramTypes.length - 1;
        this.varargComponentType = paramTypes[fixedArgCount].getComponentType();
      } else {
        this.fixedArgCount = 0;
        this.varargComponentType = null;
      }
      // Convert to MethodHandle for faster invocation
      MethodHandle h = null;
      try {
        h = METHOD_LOOKUP.unreflect(method);
      } catch (IllegalAccessException e) {
        // Fall back to null - will use Method.invoke
      }
      this.handle = h;
      this.isStatic = java.lang.reflect.Modifier.isStatic(method.getModifiers());
    }

    final boolean isStatic;

    /** Prepare arguments for invocation, repacking varargs if needed. */
    Object[] prepareArgs(Object[] args) {
      if (!isVarArgs) {
        return args;
      }
      // Repack: fixed args + varargs array
      Object[] newArgs = new Object[fixedArgCount + 1];
      for (int i = 0; i < fixedArgCount; i++) {
        newArgs[i] = args[i];
      }
      int varargCount = args.length - fixedArgCount;
      Object varargArray = java.lang.reflect.Array.newInstance(varargComponentType, varargCount);
      for (int i = 0; i < varargCount; i++) {
        java.lang.reflect.Array.set(varargArray, i, args[fixedArgCount + i]);
      }
      newArgs[fixedArgCount] = varargArray;
      return newArgs;
    }

    /** Invoke the method using MethodHandle if available, otherwise fall back to reflection. */
    Object invoke(Object target, Object[] args) throws Throwable {
      if (handle != null) {
        // Use invokeWithArguments for flexibility with different arg counts
        if (isStatic) {
          return handle.invokeWithArguments(args);
        } else {
          // For instance methods, prepend target to args
          Object[] fullArgs = new Object[args.length + 1];
          fullArgs[0] = target;
          System.arraycopy(args, 0, fullArgs, 1, args.length);
          return handle.invokeWithArguments(fullArgs);
        }
      }
      return method.invoke(target, args);
    }
  }

  /** Key for constructor cache lookup. */
  private static final class ConstructorCacheKey {
    private final Class<?> clazz;
    private final Class<?>[] argTypes;
    private final int hashCode;

    ConstructorCacheKey(Class<?> clazz, Class<?>[] argTypes) {
      this.clazz = clazz;
      this.argTypes = argTypes;
      // Pre-compute hash for faster lookups
      int h = clazz.hashCode();
      for (Class<?> t : argTypes) {
        h = h * 31 + (t != null ? t.hashCode() : 0);
      }
      this.hashCode = h;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ConstructorCacheKey other)) return false;
      if (clazz != other.clazz) return false;
      if (argTypes.length != other.argTypes.length) return false;
      for (int i = 0; i < argTypes.length; i++) {
        if (argTypes[i] != other.argTypes[i]) return false;
      }
      return true;
    }
  }

  /** Cached constructor resolution result. */
  private static final class CachedConstructor {
    final java.lang.reflect.Constructor<?> constructor;
    final boolean isVarArgs;
    final int fixedArgCount;
    final Class<?> varargComponentType;

    CachedConstructor(java.lang.reflect.Constructor<?> constructor) {
      this.constructor = constructor;
      this.isVarArgs = constructor.isVarArgs();
      if (isVarArgs) {
        Class<?>[] paramTypes = constructor.getParameterTypes();
        this.fixedArgCount = paramTypes.length - 1;
        this.varargComponentType = paramTypes[fixedArgCount].getComponentType();
      } else {
        this.fixedArgCount = 0;
        this.varargComponentType = null;
      }
    }

    /** Prepare arguments for invocation, repacking varargs if needed. */
    Object[] prepareArgs(Object[] args) {
      if (!isVarArgs) {
        return args;
      }
      // Repack: fixed args + varargs array
      Object[] newArgs = new Object[fixedArgCount + 1];
      for (int i = 0; i < fixedArgCount; i++) {
        newArgs[i] = args[i];
      }
      int varargCount = args.length - fixedArgCount;
      Object varargArray = java.lang.reflect.Array.newInstance(varargComponentType, varargCount);
      for (int i = 0; i < varargCount; i++) {
        java.lang.reflect.Array.set(varargArray, i, args[fixedArgCount + i]);
      }
      newArgs[fixedArgCount] = varargArray;
      return newArgs;
    }
  }

  /** Holds information about a registered callback. */
  private static class CallbackInfo {
    final long id;
    final String interfaceName;
    final Object proxyInstance;

    CallbackInfo(long id, String interfaceName, Object proxyInstance) {
      this.id = id;
      this.interfaceName = interfaceName;
      this.proxyInstance = proxyInstance;
    }
  }

  public GatunServer(String socketPathStr, long memorySize) {
    this.socketPath = Path.of(socketPathStr);
    this.memoryPath = Path.of(socketPathStr + ".shm");
    this.memorySize = memorySize;
    this.responseOffset = memorySize - RESPONSE_ZONE_SIZE;
    this.threadPool = Executors.newVirtualThreadPerTaskExecutor();
  }

  public void start() throws IOException {
    Files.deleteIfExists(socketPath);

    // --- 1. SETUP SHARED MEMORY (PANAMA) ---
    try (RandomAccessFile raf = new RandomAccessFile(memoryPath.toFile(), "rw");
        Arena arena = Arena.ofShared()) {
      raf.setLength(this.memorySize);

      FileChannel channel = raf.getChannel();
      MemorySegment sharedMem =
          channel.map(FileChannel.MapMode.READ_WRITE, 0, this.memorySize, arena);

      sharedMem.fill((byte) 0);
      LOG.info("Shared memory mapped at: " + memoryPath);

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
                      Files.deleteIfExists(memoryPath);
                    } catch (IOException e) {
                      // Ignore cleanup errors on shutdown
                    }
                  }));

      // --- 2. START SOCKET SERVER ---
      try (ServerSocketChannel serverChannel =
          ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
        serverChannel.bind(UnixDomainSocketAddress.of(socketPath));
        LOG.info("Server ready at: " + socketPath);

        while (true) {
          SocketChannel client = serverChannel.accept();
          threadPool.submit(() -> handleClient(client, sharedMem));
        }
      }
    }
  }

  private void handleClient(SocketChannel client, MemorySegment sharedMem) {
    Set<Long> sessionObjectIds = new HashSet<>();
    Set<Long> sessionCallbackIds = new HashSet<>();
    LOG.fine("New client session started");

    // Set up thread-local session context for callbacks
    sessionChannel.set(client);
    sessionSharedMem.set(sharedMem);

    try (client) {

      // --- HANDSHAKE: Send Protocol Version and Memory Size to Client ---
      // Format: [4 bytes: version] [4 bytes: arena_epoch] [8 bytes: memory size]
      ByteBuffer handshakeBuf = ByteBuffer.allocate(16);
      handshakeBuf.order(ByteOrder.LITTLE_ENDIAN);
      handshakeBuf.putInt(PROTOCOL_VERSION);
      handshakeBuf.putInt((int) arrowHandler.getArenaEpoch()); // Current epoch for synchronization
      handshakeBuf.putLong(this.memorySize);
      handshakeBuf.flip();

      while (handshakeBuf.hasRemaining()) {
        client.write(handshakeBuf);
      }

      ByteBuffer lengthBuf = ByteBuffer.allocate(4);
      lengthBuf.order(ByteOrder.LITTLE_ENDIAN);

      // Reusable FlatBufferBuilder for responses (avoid allocation per request)
      FlatBufferBuilder responseBuilder = new FlatBufferBuilder(1024);

      while (readFully(client, lengthBuf)) {
        // 1. Read Command Length
        lengthBuf.flip();
        int commandSize = lengthBuf.getInt();
        lengthBuf.clear();

        // --- CRITICAL FIX: Safe Buffer Slicing ---
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

        // --- Standard Request/Response Logic ---
        // Reuse the response builder (clear resets position but keeps buffer)
        responseBuilder.clear();
        FlatBufferBuilder builder = responseBuilder;
        int responseOffset = 0;
        Object result = null;

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

              Object[] ctorAndArgs = findConstructorWithArgs(clazz, argTypes, javaArgs);
              java.lang.reflect.Constructor<?> ctor =
                  (java.lang.reflect.Constructor<?>) ctorAndArgs[0];
              Object[] finalArgs = (Object[]) ctorAndArgs[1];
              instance = ctor.newInstance(finalArgs);
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
                    findMethodWithArgs(target.getClass(), methodName, EMPTY_CLASS_ARRAY, EMPTY_OBJECT_ARRAY);
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
                  findMethodWithArgs(target.getClass(), methodName, argTypes, javaArgs);
              result = mwa.cached.invoke(target, mwa.args);
            }

            // Wrap returned objects in registry
            if (result != null && !isAutoConvertible(result)) {
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
            MethodWithArgs mwa = findMethodWithArgs(clazz, methodName, argTypes, javaArgs);
            result = mwa.cached.invoke(null, mwa.args);

            // Wrap returned objects in registry
            // If returnObjectRef is true, always wrap as ObjectRef (no auto-conversion)
            if (result != null && (cmd.returnObjectRef() || !isAutoConvertible(result))) {
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

            java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
            field.setAccessible(true);
            result = field.get(target);

            // Wrap returned objects in registry
            if (result != null && !isAutoConvertible(result)) {
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

            java.lang.reflect.Field field = getInstanceField(target.getClass(), fieldName);
            field.setAccessible(true);
            field.set(target, value);
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
            // Use the same ID for both callback and object registry
            // This way Python can use the object_id as the callback_id
            long callbackId = objectIdCounter.getAndIncrement();

            // Create a dynamic proxy that will invoke Python when called
            Object proxy = createCallbackProxy(callbackId, interfaceName);

            // Store in callback registry
            CallbackInfo info = new CallbackInfo(callbackId, interfaceName, proxy);
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
            java.lang.reflect.Field field = getStaticField(clazz, fieldName);
            result = field.get(null); // null for static field

            // Wrap returned objects in registry
            if (result != null && !isAutoConvertible(result)) {
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
            java.lang.reflect.Field field = getStaticField(clazz, fieldName);
            field.set(null, value); // null for static field
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

          String message = formatException(cause);
          String errorType = cause.getClass().getName();
          responseOffset = packError(builder, message, errorType);
        }

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
      LOG.fine("Client session ended");
    } finally {
      // Cleanup orphans...
      for (Long id : sessionObjectIds) objectRegistry.remove(id);
      // Clear thread-locals
      cancelledRequests.get().clear();
      currentRequestId.remove();
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

  // --- HELPER: Convert FlatBuffer Argument to Java Object ---
  private Object[] convertArgument(Argument arg) {
    byte valType = arg.valType();
    Object value;
    Class<?> type;

    if (valType == Value.StringVal) {
      StringVal sv = (StringVal) arg.val(new StringVal());
      value = sv.v();
      type = String.class; // Use String for better overload resolution
    } else if (valType == Value.IntVal) {
      IntVal iv = (IntVal) arg.val(new IntVal());
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
      DoubleVal dv = (DoubleVal) arg.val(new DoubleVal());
      value = dv.v();
      type = double.class;
    } else if (valType == Value.BoolVal) {
      BoolVal bv = (BoolVal) arg.val(new BoolVal());
      value = bv.v();
      type = boolean.class;
    } else if (valType == Value.ObjectRef) {
      ObjectRef ref = (ObjectRef) arg.val(new ObjectRef());
      value = objectRegistry.get(ref.id());
      // Use actual object type for better overload resolution
      type = (value != null) ? value.getClass() : Object.class;
    } else if (valType == Value.ListVal) {
      // Convert Python list to Java ArrayList
      // Use fast path for homogeneous lists (all elements same type)
      ListVal lv = (ListVal) arg.val(new ListVal());
      int len = lv.itemsLength();
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
            for (int i = 0; i < len; i++) {
              StringVal sv = (StringVal) lv.items(i).val(new StringVal());
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
            for (int i = 0; i < len; i++) {
              IntVal iv = (IntVal) lv.items(i).val(new IntVal());
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
            for (int i = 0; i < len; i++) {
              DoubleVal dv = (DoubleVal) lv.items(i).val(new DoubleVal());
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
            for (int i = 0; i < len; i++) {
              ObjectRef ref = (ObjectRef) lv.items(i).val(new ObjectRef());
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
      MapVal mv = (MapVal) arg.val(new MapVal());
      java.util.HashMap<Object, Object> map = new java.util.HashMap<>();
      for (int i = 0; i < mv.entriesLength(); i++) {
        MapEntry entry = mv.entries(i);
        Object[] keyConverted = convertArgument(entry.key());
        Object[] valConverted = convertArgument(entry.value());
        map.put(keyConverted[0], valConverted[0]);
      }
      value = map;
      type = java.util.HashMap.class;
    } else if (valType == Value.NullVal) {
      value = null;
      type = Object.class;
    } else if (valType == Value.ArrayVal) {
      // Convert ArrayVal to Java array
      ArrayVal av = (ArrayVal) arg.val(new ArrayVal());
      value = convertArrayVal(av);
      type = value.getClass();
    } else {
      value = null;
      type = Object.class;
    }

    return new Object[] {value, type};
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

  // --- HELPER: Find method by name and compatible argument types ---
  // Returns Object[] { Method, Object[] adjustedArgs } to handle varargs repacking
  // Uses method cache for faster repeated lookups.
  /** Result of method resolution with prepared arguments. */
  private static final class MethodWithArgs {
    final CachedMethod cached;
    final Object[] args;

    MethodWithArgs(CachedMethod cached, Object[] args) {
      this.cached = cached;
      this.args = args;
    }
  }

  private static MethodWithArgs findMethodWithArgs(
      Class<?> clazz, String name, Class<?>[] argTypes, Object[] args)
      throws NoSuchMethodException {
    // Check cache first
    MethodCacheKey cacheKey = new MethodCacheKey(clazz, name, argTypes);
    CachedMethod cached = methodCache.get(cacheKey);
    if (cached != null) {
      return new MethodWithArgs(cached, cached.prepareArgs(args));
    }

    // Not in cache - do full resolution
    java.lang.reflect.Method resolvedMethod = resolveMethod(clazz, name, argTypes, args);

    // Cache the result
    CachedMethod toCache = new CachedMethod(resolvedMethod);
    methodCache.put(cacheKey, toCache);

    return new MethodWithArgs(toCache, toCache.prepareArgs(args));
  }

  // --- HELPER: Resolve method without caching (called on cache miss) ---
  private static java.lang.reflect.Method resolveMethod(
      Class<?> clazz, String name, Class<?>[] argTypes, Object[] args)
      throws NoSuchMethodException {
    // First try exact match
    try {
      return clazz.getMethod(name, argTypes);
    } catch (NoSuchMethodException e) {
      // Fall through to search
    }

    // Search for compatible method - prioritize non-varargs over varargs
    // and methods with fewer fixed parameters
    java.lang.reflect.Method bestMatch = null;
    int bestScore = Integer.MIN_VALUE;

    for (java.lang.reflect.Method m : getCachedMethods(clazz)) {
      if (!m.getName().equals(name)) continue;
      Class<?>[] paramTypes = m.getParameterTypes();

      // Check for exact parameter count match (non-varargs only)
      if (!m.isVarArgs() && paramTypes.length == argTypes.length) {
        boolean match = true;
        int specificity = 0; // Higher = more specific type matches
        for (int i = 0; i < paramTypes.length; i++) {
          if (!isAssignable(paramTypes[i], argTypes[i])) {
            match = false;
            break;
          }
          // Score specificity: exact type match gets 10 points, Object param gets 0
          if (paramTypes[i] == argTypes[i]) {
            specificity += 10;
          } else if (paramTypes[i] != Object.class && paramTypes[i].isAssignableFrom(argTypes[i])) {
            // Specific type that accepts our arg gets 5 points
            specificity += 5;
          }
          // Object.class as param type gets 0 points (least specific)
        }
        if (match) {
          // Score: base 1000 for non-varargs, plus specificity bonus
          int score = 1000 + specificity;
          if (bestScore < score) {
            bestMatch = m;
            bestScore = score;
          }
        }
      }

      // Check for varargs match
      if (m.isVarArgs() && argTypes.length >= paramTypes.length - 1) {
        if (isVarargsCompatible(paramTypes, argTypes)) {
          // Score: varargs gets lower priority (100 - fixedCount)
          int fixedCount = paramTypes.length - 1;
          int score = 100 - fixedCount;
          if (bestScore < score) {
            bestMatch = m;
            bestScore = score;
          }
        }
      }
    }

    if (bestMatch != null) {
      return bestMatch;
    }

    throw new NoSuchMethodException(
        "No matching method: " + name + " with " + argTypes.length + " args");
  }

  // --- HELPER: Check if argTypes are compatible with varargs method params ---
  private static boolean isVarargsCompatible(Class<?>[] paramTypes, Class<?>[] argTypes) {
    int fixedCount = paramTypes.length - 1;
    Class<?> varargComponentType = paramTypes[fixedCount].getComponentType();

    // Check fixed parameters match
    for (int i = 0; i < fixedCount; i++) {
      if (!isAssignable(paramTypes[i], argTypes[i])) {
        return false;
      }
    }

    // Check vararg parameters match
    for (int i = fixedCount; i < argTypes.length; i++) {
      if (!isAssignable(varargComponentType, argTypes[i])) {
        return false;
      }
    }
    return true;
  }

  // --- HELPER: Try to match varargs method and repack arguments ---
  private static Object[] tryVarargsMatch(
      Class<?>[] paramTypes, Class<?>[] argTypes, Object[] args) {
    int fixedCount = paramTypes.length - 1;
    Class<?> varargComponentType = paramTypes[fixedCount].getComponentType();

    // Check fixed parameters match
    for (int i = 0; i < fixedCount; i++) {
      if (!isAssignable(paramTypes[i], argTypes[i])) {
        return null;
      }
    }

    // Check vararg parameters match
    for (int i = fixedCount; i < argTypes.length; i++) {
      if (!isAssignable(varargComponentType, argTypes[i])) {
        return null;
      }
    }

    // Repack arguments: fixed args + varargs array
    Object[] newArgs = new Object[paramTypes.length];
    for (int i = 0; i < fixedCount; i++) {
      newArgs[i] = args[i];
    }

    // Create varargs array
    int varargCount = argTypes.length - fixedCount;
    Object varargArray = java.lang.reflect.Array.newInstance(varargComponentType, varargCount);
    for (int i = 0; i < varargCount; i++) {
      java.lang.reflect.Array.set(varargArray, i, args[fixedCount + i]);
    }
    newArgs[fixedCount] = varargArray;

    return newArgs;
  }

  // --- HELPER: Find constructor with compatible argument types ---
  // Returns Object[] { Constructor, Object[] adjustedArgs } to handle varargs repacking
  // Uses constructor cache for faster repeated lookups.
  private static Object[] findConstructorWithArgs(
      Class<?> clazz, Class<?>[] argTypes, Object[] args) throws NoSuchMethodException {
    // Check cache first
    ConstructorCacheKey cacheKey = new ConstructorCacheKey(clazz, argTypes);
    CachedConstructor cached = constructorCache.get(cacheKey);
    if (cached != null) {
      return new Object[] {cached.constructor, cached.prepareArgs(args)};
    }

    // Not in cache - do full resolution
    java.lang.reflect.Constructor<?> resolvedCtor = resolveConstructor(clazz, argTypes, args);

    // Cache the result
    CachedConstructor toCache = new CachedConstructor(resolvedCtor);
    constructorCache.put(cacheKey, toCache);

    return new Object[] {resolvedCtor, toCache.prepareArgs(args)};
  }

  // --- HELPER: Resolve constructor without caching (called on cache miss) ---
  private static java.lang.reflect.Constructor<?> resolveConstructor(
      Class<?> clazz, Class<?>[] argTypes, Object[] args) throws NoSuchMethodException {
    // First try exact match
    try {
      return clazz.getConstructor(argTypes);
    } catch (NoSuchMethodException e) {
      // Fall through to search
    }

    // Search for compatible constructor with specificity scoring
    java.lang.reflect.Constructor<?> bestMatch = null;
    int bestScore = Integer.MIN_VALUE;

    for (java.lang.reflect.Constructor<?> c : getCachedConstructors(clazz)) {
      Class<?>[] paramTypes = c.getParameterTypes();

      // Check for varargs match
      if (c.isVarArgs() && argTypes.length >= paramTypes.length - 1) {
        if (isVarargsCompatible(paramTypes, argTypes)) {
          int score = 100; // Varargs gets lower priority
          if (bestScore < score) {
            bestMatch = c;
            bestScore = score;
          }
        }
      }

      // Check for exact parameter count match
      if (paramTypes.length != argTypes.length) continue;

      boolean match = true;
      int specificity = 0;
      for (int i = 0; i < paramTypes.length; i++) {
        if (!isAssignable(paramTypes[i], argTypes[i])) {
          match = false;
          break;
        }
        // Score specificity: exact type match gets 10 points
        if (paramTypes[i] == argTypes[i]) {
          specificity += 10;
        } else if (paramTypes[i] != Object.class && paramTypes[i].isAssignableFrom(argTypes[i])) {
          specificity += 5;
        }
      }
      if (match) {
        int score = 1000 + specificity;
        if (bestScore < score) {
          bestMatch = c;
          bestScore = score;
        }
      }
    }

    if (bestMatch != null) {
      return bestMatch;
    }

    throw new NoSuchMethodException(
        "No matching constructor for " + clazz.getName() + " with " + argTypes.length + " args");
  }

  // --- HELPER: Check if argType can be assigned to paramType ---
  private static boolean isAssignable(Class<?> paramType, Class<?> argType) {
    if (paramType.isAssignableFrom(argType)) return true;
    // Handle Object accepting anything (used for String args and object refs)
    if (paramType == Object.class) return true;
    // Handle argType=Object matching any reference type (String args come as Object.class)
    if (argType == Object.class && !paramType.isPrimitive()) return true;
    // Handle primitive widening conversions (Java allows these implicitly)
    if (paramType == int.class && argType == int.class) return true;
    if (paramType == long.class && (argType == int.class || argType == long.class)) return true;
    // Allow int/long -> double widening (e.g., Math.pow(2, 10) should work)
    if (paramType == double.class
        && (argType == double.class
            || argType == float.class
            || argType == int.class
            || argType == long.class)) return true;
    if (paramType == float.class
        && (argType == float.class || argType == int.class || argType == long.class)) return true;
    if (paramType == boolean.class && argType == boolean.class) return true;
    return false;
  }

  // --- HELPER: Check if result should be auto-converted (not wrapped as ObjectRef) ---
  private static boolean isAutoConvertible(Object obj) {
    return obj instanceof String
        || obj instanceof Number
        || obj instanceof Boolean
        || obj instanceof Character
        || obj instanceof List
        || obj instanceof Map
        || obj.getClass().isArray();
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
   */
  private Object createCallbackProxy(long callbackId, String interfaceName)
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
          return invokeCallback(callbackId, method.getName(), args == null ? new Object[0] : args);
        };

    return Proxy.newProxyInstance(
        interfaceClass.getClassLoader(), new Class<?>[] {interfaceClass}, handler);
  }

  /**
   * Sends a callback invocation request to Python and blocks until Python responds. This is called
   * from the dynamic proxy when Java code invokes a method on the callback object.
   */
  private Object invokeCallback(long callbackId, String methodName, Object[] args)
      throws Exception {
    SocketChannel channel = sessionChannel.get();
    MemorySegment sharedMem = sessionSharedMem.get();

    if (channel == null || sharedMem == null) {
      throw new IllegalStateException("Callback invoked outside of session context");
    }

    // Build the callback request response
    FlatBufferBuilder builder = new FlatBufferBuilder(1024);

    // Build callback arguments
    int[] argOffsets = new int[args.length];
    for (int i = 0; i < args.length; i++) {
      int[] packed = packValue(builder, args[i], null);
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

  public static void main(String[] args) {
    try {
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
}
