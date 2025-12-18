package org.gatun.server;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.flatbuffers.FlatBufferBuilder;
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

    // Split memory in half
    private static final int COMMAND_OFFSET = 0;
    private static final int PAYLOAD_OFFSET = 4096; // 4KB

    // --- SECURITY: Allowlist of classes that can be instantiated or used for static methods ---
    private static final Set<String> ALLOWED_CLASSES = Set.of(
        "java.util.ArrayList",
        "java.util.LinkedList",
        "java.util.HashMap",
        "java.util.LinkedHashMap",
        "java.util.HashSet",
        "java.util.LinkedHashSet",
        "java.util.TreeMap",
        "java.util.TreeSet",
        "java.util.Collections",
        "java.lang.String",
        "java.lang.Integer",
        "java.lang.Long",
        "java.lang.Double",
        "java.lang.Boolean",
        "java.lang.Math",
        "java.lang.StringBuilder",
        "java.lang.StringBuffer"
    );

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
            MemorySegment sharedMem = channel.map(FileChannel.MapMode.READ_WRITE, 0, this.memorySize, arena);

            sharedMem.fill((byte) 0);
            LOG.info("Shared memory mapped at: " + memoryPath);

            // --- Register shutdown hook for cleanup ---
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
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
            try (ServerSocketChannel serverChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
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

                            java.lang.reflect.Constructor<?> ctor = findConstructor(clazz, argTypes);
                            instance = ctor.newInstance(javaArgs);
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
                    }
                    else if (cmd.action() == Action.InvokeMethod) {
                        long targetId = cmd.targetId();
                        Object target = objectRegistry.get(targetId);
                        String methodName = cmd.targetName();

                        if (target == null)
                            throw new RuntimeException("Object " + targetId + " not found");

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

                        // Find and invoke method via reflection
                        java.lang.reflect.Method method = findMethod(target.getClass(), methodName, argTypes);
                        result = method.invoke(target, javaArgs);

                        // Wrap returned objects in registry
                        if (result != null && !isPrimitive(result)) {
                            long newId = objectIdCounter.getAndIncrement();
                            objectRegistry.put(newId, result);
                            sessionObjectIds.add(newId);
                            result = new ObjectRefT(newId);
                        }
                    }
                    else if (cmd.action() == Action.InvokeStaticMethod) {
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

                        // Find and invoke static method via reflection
                        java.lang.reflect.Method method = findMethod(clazz, methodName, argTypes);
                        result = method.invoke(null, javaArgs);

                        // Wrap returned objects in registry
                        if (result != null && !isPrimitive(result)) {
                            long newId = objectIdCounter.getAndIncrement();
                            objectRegistry.put(newId, result);
                            sessionObjectIds.add(newId);
                            result = new ObjectRefT(newId);
                        }
                    }
                    else if (cmd.action() == Action.GetField) {
                        long targetId = cmd.targetId();
                        Object target = objectRegistry.get(targetId);
                        String fieldName = cmd.targetName();

                        if (target == null)
                            throw new RuntimeException("Object " + targetId + " not found");

                        java.lang.reflect.Field field = findField(target.getClass(), fieldName);
                        field.setAccessible(true);
                        result = field.get(target);

                        // Wrap returned objects in registry
                        if (result != null && !isPrimitive(result)) {
                            long newId = objectIdCounter.getAndIncrement();
                            objectRegistry.put(newId, result);
                            sessionObjectIds.add(newId);
                            result = new ObjectRefT(newId);
                        }
                    }
                    else if (cmd.action() == Action.SetField) {
                        long targetId = cmd.targetId();
                        Object target = objectRegistry.get(targetId);
                        String fieldName = cmd.targetName();

                        if (target == null)
                            throw new RuntimeException("Object " + targetId + " not found");

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

                        try (ArrowStreamReader reader = new ArrowStreamReader(
                                new ByteBufferInputStream(arrowBuf),
                                allocator)) {

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

                    if (cause instanceof NoSuchMethodException || cause instanceof IllegalArgumentException
                            || cause instanceof SecurityException || cause instanceof NoSuchFieldException) {
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
            for (Long id : sessionObjectIds)
                objectRegistry.remove(id);
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
        } else {
            value = null;
            type = Object.class;
        }

        return new Object[]{value, type};
    }

    // --- HELPER: Find method by name and compatible argument types ---
    private static java.lang.reflect.Method findMethod(Class<?> clazz, String name, Class<?>[] argTypes)
            throws NoSuchMethodException {
        // First try exact match
        try {
            return clazz.getMethod(name, argTypes);
        } catch (NoSuchMethodException e) {
            // Fall through to search
        }

        // Search for compatible method (handles Object params, boxing, etc.)
        for (java.lang.reflect.Method m : clazz.getMethods()) {
            if (!m.getName().equals(name)) continue;
            Class<?>[] paramTypes = m.getParameterTypes();
            if (paramTypes.length != argTypes.length) continue;

            boolean match = true;
            for (int i = 0; i < paramTypes.length; i++) {
                if (!isAssignable(paramTypes[i], argTypes[i])) {
                    match = false;
                    break;
                }
            }
            if (match) return m;
        }

        throw new NoSuchMethodException("No matching method: " + name + " with " + argTypes.length + " args");
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
    private static java.lang.reflect.Constructor<?> findConstructor(Class<?> clazz, Class<?>[] argTypes)
            throws NoSuchMethodException {
        // First try exact match
        try {
            return clazz.getConstructor(argTypes);
        } catch (NoSuchMethodException e) {
            // Fall through to search
        }

        // Search for compatible constructor (handles Object params, boxing, etc.)
        for (java.lang.reflect.Constructor<?> c : clazz.getConstructors()) {
            Class<?>[] paramTypes = c.getParameterTypes();
            if (paramTypes.length != argTypes.length) continue;

            boolean match = true;
            for (int i = 0; i < paramTypes.length; i++) {
                if (!isAssignable(paramTypes[i], argTypes[i])) {
                    match = false;
                    break;
                }
            }
            if (match) return c;
        }

        throw new NoSuchMethodException("No matching constructor for " + clazz.getName() + " with " + argTypes.length + " args");
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
        if (paramType == double.class && (argType == double.class || argType == float.class)) return true;
        if (paramType == boolean.class && argType == boolean.class) return true;
        return false;
    }

    // --- HELPER: Check if result is a primitive/string (not an object to wrap) ---
    private static boolean isPrimitive(Object obj) {
        return obj instanceof String
            || obj instanceof Number
            || obj instanceof Boolean
            || obj instanceof Character;
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
            sb.append("\nCaused by: ").append(cause.getClass().getName())
              .append(": ").append(cause.getMessage());
            for (StackTraceElement ste : cause.getStackTrace()) {
                sb.append("\n\tat ").append(ste.toString());
            }
            cause = cause.getCause();
        }
        return sb.toString();
    }

    // --- HELPER: Pack Result into FlatBuffer ---
    private int packSuccess(FlatBufferBuilder builder, Object result) {
        byte type = Value.NullVal;
        int valueOffset = 0;

        if (result == null) {
            type = Value.NullVal;
            NullVal.startNullVal(builder);
            valueOffset = NullVal.endNullVal(builder);
        } else if (result instanceof Boolean) {
            type = Value.BoolVal;
            valueOffset = BoolVal.createBoolVal(builder, (Boolean) result);
        } else if (result instanceof Integer || result instanceof Long) {
            type = Value.IntVal;
            valueOffset = IntVal.createIntVal(builder, ((Number) result).longValue());
        } else if (result instanceof Double || result instanceof Float) {
            type = Value.DoubleVal;
            valueOffset = DoubleVal.createDoubleVal(builder, ((Number) result).doubleValue());
        } else if (result instanceof String) {
            type = Value.StringVal;
            int sOff = builder.createString((String) result);
            valueOffset = StringVal.createStringVal(builder, sOff);
        } else if (result instanceof ObjectRefT) { 
            type = Value.ObjectRef;
            valueOffset = ObjectRef.createObjectRef(builder, ((ObjectRefT) result).id);
        }

        Response.startResponse(builder);
        Response.addIsError(builder, false);
        Response.addReturnValType(builder, type);
        Response.addReturnVal(builder, valueOffset);
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
        public ObjectRefT(long id) { this.id = id; }
    }

    /**
     * Reads exactly buffer.remaining() bytes from the channel.
     * Returns true if successful, false on EOF.
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
        public ByteBufferInputStream(ByteBuffer buf) { this.buf = buf; }
        @Override public int read() { if (!buf.hasRemaining()) return -1; return buf.get() & 0xFF; }
        @Override public int read(byte[] bytes, int off, int len) {
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