package org.gatun.server;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
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
  private static final int PAYLOAD_OFFSET = 4096; // 4KB
  private static final int RESPONSE_ZONE_SIZE = 4096; // 4KB

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
          "java.lang.StringBuffer");

  // --- THE OBJECT REGISTRY ---
  private final Map<Long, Object> objectRegistry = new ConcurrentHashMap<>();
  private final AtomicLong objectIdCounter = new AtomicLong(1);

  private final BufferAllocator allocator = new RootAllocator();

  public GatunServer(String socketPathStr, long memorySize) {
    this.socketPath = Path.of(socketPathStr);
    this.memoryPath = Path.of(socketPathStr + ".shm");
    this.memorySize = memorySize;
    this.responseOffset = memorySize - 4096;
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
    LOG.fine("New client session started");

    try (client) {

      // --- HANDSHAKE: Send Protocol Version and Memory Size to Client ---
      // Format: [4 bytes: version] [4 bytes: reserved] [8 bytes: memory size]
      ByteBuffer handshakeBuf = ByteBuffer.allocate(16);
      handshakeBuf.order(ByteOrder.LITTLE_ENDIAN);
      handshakeBuf.putInt(PROTOCOL_VERSION);
      handshakeBuf.putInt(0); // Reserved for future use
      handshakeBuf.putLong(this.memorySize);
      handshakeBuf.flip();

      while (handshakeBuf.hasRemaining()) {
        client.write(handshakeBuf);
      }

      ByteBuffer lengthBuf = ByteBuffer.allocate(4);
      lengthBuf.order(ByteOrder.LITTLE_ENDIAN);

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

        // --- Standard Request/Response Logic ---
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int responseOffset = 0;
        Object result = null;

        try {
          if (cmd.action() == Action.CreateObject) {
            String className = cmd.targetName();
            if (!ALLOWED_CLASSES.contains(className)) {
              throw new SecurityException("Class not allowed: " + className);
            }
            Class<?> clazz = Class.forName(className);

            // Convert constructor arguments if provided
            int argCount = cmd.argsLength();
            Object instance;

            if (argCount == 0) {
              // No-arg constructor
              instance = clazz.getDeclaredConstructor().newInstance();
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

            // Find and invoke method via reflection (with varargs support)
            Object[] methodAndArgs =
                findMethodWithArgs(target.getClass(), methodName, argTypes, javaArgs);
            java.lang.reflect.Method method = (java.lang.reflect.Method) methodAndArgs[0];
            Object[] finalArgs = (Object[]) methodAndArgs[1];
            result = method.invoke(target, finalArgs);

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

            if (!ALLOWED_CLASSES.contains(className)) {
              throw new SecurityException("Class not allowed: " + className);
            }

            Class<?> clazz = Class.forName(className);

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

            // Find and invoke static method via reflection (with varargs support)
            Object[] methodAndArgs = findMethodWithArgs(clazz, methodName, argTypes, javaArgs);
            java.lang.reflect.Method method = (java.lang.reflect.Method) methodAndArgs[0];
            Object[] finalArgs = (Object[]) methodAndArgs[1];
            result = method.invoke(null, finalArgs);

            // Wrap returned objects in registry
            if (result != null && !isAutoConvertible(result)) {
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

            java.lang.reflect.Field field = findField(target.getClass(), fieldName);
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

            java.lang.reflect.Field field = findField(target.getClass(), fieldName);
            field.setAccessible(true);
            field.set(target, value);
            result = null;
          }
          // --- ARROW BLOCK ---
          else if (cmd.action() == Action.SendArrowBatch) {
            LOG.fine("Processing Arrow batch...");
            long payloadSize = this.responseOffset - PAYLOAD_OFFSET;
            MemorySegment payloadSlice = sharedMem.asSlice(PAYLOAD_OFFSET, payloadSize);
            ByteBuffer arrowBuf = payloadSlice.asByteBuffer();

            try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteBufferInputStream(arrowBuf), allocator)) {

              if (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                int rows = root.getRowCount();
                LOG.fine("Arrow batch received: " + rows + " rows");
                result = "Received " + rows + " rows";
              } else {
                throw new RuntimeException("Arrow Stream Empty");
              }
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
          responseOffset = packError(builder, message);
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
          responseOffset = packError(builder, errorMsg);
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
      }
    } catch (IOException e) {
      LOG.fine("Client session ended");
    } finally {
      // Cleanup orphans...
      for (Long id : sessionObjectIds) objectRegistry.remove(id);
    }
  }

  // --- HELPER: Convert FlatBuffer Argument to Java Object ---
  private Object[] convertArgument(Argument arg) {
    byte valType = arg.valType();
    Object value;
    Class<?> type;

    if (valType == Value.StringVal) {
      StringVal sv = (StringVal) arg.val(new StringVal());
      value = sv.v();
      type = Object.class; // Use Object for flexibility with overloaded methods
    } else if (valType == Value.IntVal) {
      IntVal iv = (IntVal) arg.val(new IntVal());
      value = (int) iv.v(); // Convert long to int for common Java APIs
      type = int.class;
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
      type = Object.class;
    } else if (valType == Value.ListVal) {
      // Convert Python list to Java ArrayList
      ListVal lv = (ListVal) arg.val(new ListVal());
      java.util.ArrayList<Object> list = new java.util.ArrayList<>();
      for (int i = 0; i < lv.itemsLength(); i++) {
        Object[] converted = convertArgument(lv.items(i));
        list.add(converted[0]);
      }
      value = list;
      type = Object.class;
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
      type = Object.class;
    } else if (valType == Value.NullVal) {
      value = null;
      type = Object.class;
    } else {
      value = null;
      type = Object.class;
    }

    return new Object[] {value, type};
  }

  // --- HELPER: Find method by name and compatible argument types ---
  // Returns Object[] { Method, Object[] adjustedArgs } to handle varargs repacking
  private static Object[] findMethodWithArgs(
      Class<?> clazz, String name, Class<?>[] argTypes, Object[] args)
      throws NoSuchMethodException {
    // First try exact match
    try {
      return new Object[] {clazz.getMethod(name, argTypes), args};
    } catch (NoSuchMethodException e) {
      // Fall through to search
    }

    // Search for compatible method - prioritize non-varargs over varargs
    // and methods with fewer fixed parameters
    java.lang.reflect.Method bestMatch = null;
    Object[] bestArgs = null;
    int bestScore = Integer.MIN_VALUE;

    for (java.lang.reflect.Method m : clazz.getMethods()) {
      if (!m.getName().equals(name)) continue;
      Class<?>[] paramTypes = m.getParameterTypes();

      // Check for exact parameter count match (non-varargs only)
      if (!m.isVarArgs() && paramTypes.length == argTypes.length) {
        boolean match = true;
        for (int i = 0; i < paramTypes.length; i++) {
          if (!isAssignable(paramTypes[i], argTypes[i])) {
            match = false;
            break;
          }
        }
        if (match) {
          // Score: prefer non-varargs (score = 1000), then by specificity
          int score = 1000;
          if (bestScore < score) {
            bestMatch = m;
            bestArgs = args;
            bestScore = score;
          }
        }
      }

      // Check for varargs match (always repack args into array)
      if (m.isVarArgs() && argTypes.length >= paramTypes.length - 1) {
        Object[] result = tryVarargsMatch(paramTypes, argTypes, args);
        if (result != null) {
          // Score: varargs gets lower priority (100 - fixedCount)
          // Methods with fewer fixed params are preferred (e.g., asList(T...) over format(Locale,
          // String, Object...))
          int fixedCount = paramTypes.length - 1;
          int score = 100 - fixedCount;
          if (bestScore < score) {
            bestMatch = m;
            bestArgs = result;
            bestScore = score;
          }
        }
      }
    }

    if (bestMatch != null) {
      return new Object[] {bestMatch, bestArgs};
    }

    throw new NoSuchMethodException(
        "No matching method: " + name + " with " + argTypes.length + " args");
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

  // --- HELPER: Find field by name (searches class hierarchy) ---
  private static java.lang.reflect.Field findField(Class<?> clazz, String name)
      throws NoSuchFieldException {
    // Search up the class hierarchy
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredField(name);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException("No field named '" + name + "' in " + clazz.getName());
  }

  // --- HELPER: Find constructor with compatible argument types ---
  // Returns Object[] { Constructor, Object[] adjustedArgs } to handle varargs repacking
  private static Object[] findConstructorWithArgs(
      Class<?> clazz, Class<?>[] argTypes, Object[] args) throws NoSuchMethodException {
    // First try exact match
    try {
      return new Object[] {clazz.getConstructor(argTypes), args};
    } catch (NoSuchMethodException e) {
      // Fall through to search
    }

    // Search for compatible constructor (handles Object params, boxing, varargs, etc.)
    for (java.lang.reflect.Constructor<?> c : clazz.getConstructors()) {
      Class<?>[] paramTypes = c.getParameterTypes();

      // Check for varargs match
      if (c.isVarArgs() && argTypes.length >= paramTypes.length - 1) {
        Object[] result = tryVarargsMatch(paramTypes, argTypes, args);
        if (result != null) {
          return new Object[] {c, result};
        }
      }

      // Check for exact parameter count match
      if (paramTypes.length != argTypes.length) continue;

      boolean match = true;
      for (int i = 0; i < paramTypes.length; i++) {
        if (!isAssignable(paramTypes[i], argTypes[i])) {
          match = false;
          break;
        }
      }
      if (match) return new Object[] {c, args};
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
    // Handle primitive boxing
    if (paramType == int.class && argType == int.class) return true;
    if (paramType == long.class && (argType == int.class || argType == long.class)) return true;
    if (paramType == double.class && (argType == double.class || argType == float.class))
      return true;
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
        || obj instanceof Map;
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

  private int packError(FlatBufferBuilder builder, String msg) {
    int errOff = builder.createString(msg == null ? "Unknown Error" : msg);
    Response.startResponse(builder);
    Response.addIsError(builder, true);
    Response.addErrorMsg(builder, errOff);
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
