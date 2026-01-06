package org.gatun.server.observability;

import jdk.jfr.*;

/**
 * JFR (Java Flight Recorder) events for profiling and diagnostics.
 *
 * <p>These events integrate with JFR tooling (JDK Mission Control, async-profiler, etc.)
 * to provide deep visibility into Gatun server behavior without code changes.
 *
 * <p>Enable with: -XX:StartFlightRecording=filename=gatun.jfr,settings=profile
 */
public class GatunEvents {

  /** Base category for all Gatun events. */
  private static final String CATEGORY = "Gatun";

  /**
   * Event emitted when a request starts and ends.
   * Captures timing, action type, and target information.
   */
  @Name("org.gatun.Request")
  @Label("Request")
  @Category(CATEGORY)
  @Description("A Gatun RPC request from Python to Java")
  @StackTrace(false)
  public static class RequestEvent extends Event {
    @Label("Session ID")
    public long sessionId;

    @Label("Request ID")
    public long requestId;

    @Label("Action")
    public String action;

    @Label("Target")
    public String target;

    @Label("Success")
    public boolean success;

    @Label("Error Type")
    public String errorType;
  }

  /**
   * Event emitted during method resolution to track overload selection.
   */
  @Name("org.gatun.MethodResolution")
  @Label("Method Resolution")
  @Category(CATEGORY)
  @Description("Method overload resolution decision")
  @StackTrace(false)
  public static class MethodResolutionEvent extends Event {
    @Label("Class")
    public String className;

    @Label("Method Name")
    public String methodName;

    @Label("Candidate Count")
    public int candidateCount;

    @Label("Chosen Method")
    public String chosenMethod;

    @Label("Score")
    public int score;

    @Label("Argument Types")
    public String argumentTypes;
  }

  /**
   * Event emitted when objects are added/removed from the registry.
   */
  @Name("org.gatun.ObjectRegistry")
  @Label("Object Registry")
  @Category(CATEGORY)
  @Description("Object added to or removed from registry")
  @StackTrace(false)
  public static class ObjectRegistryEvent extends Event {
    @Label("Session ID")
    public long sessionId;

    @Label("Object ID")
    public long objectId;

    @Label("Operation")
    public String operation; // "add" or "remove"

    @Label("Object Type")
    public String objectType;

    @Label("Registry Size")
    public int registrySize;
  }

  /**
   * Event emitted for callback round-trips (Java -> Python -> Java).
   */
  @Name("org.gatun.Callback")
  @Label("Callback")
  @Category(CATEGORY)
  @Description("Python callback invocation from Java")
  @StackTrace(false)
  public static class CallbackEvent extends Event {
    @Label("Session ID")
    public long sessionId;

    @Label("Callback ID")
    public long callbackId;

    @Label("Method Name")
    public String methodName;

    @Label("Argument Count")
    public int argumentCount;

    @Label("Success")
    public boolean success;

    @Label("Error Message")
    public String errorMessage;
  }

  /**
   * Event emitted for Arrow data transfers.
   */
  @Name("org.gatun.ArrowTransfer")
  @Label("Arrow Transfer")
  @Category(CATEGORY)
  @Description("Arrow data transfer between Python and Java")
  @StackTrace(false)
  public static class ArrowTransferEvent extends Event {
    @Label("Session ID")
    public long sessionId;

    @Label("Direction")
    public String direction; // "python_to_java" or "java_to_python"

    @Label("Row Count")
    public long rowCount;

    @Label("Bytes Copied")
    public long bytesCopied;

    @Label("Zero Copy")
    public boolean zeroCopy;
  }

  /**
   * Event emitted when a session starts or ends.
   */
  @Name("org.gatun.Session")
  @Label("Session")
  @Category(CATEGORY)
  @Description("Client session lifecycle")
  @StackTrace(false)
  public static class SessionEvent extends Event {
    @Label("Session ID")
    public long sessionId;

    @Label("Operation")
    public String operation; // "start" or "end"

    @Label("Objects Created")
    public int objectsCreated;

    @Label("Requests Processed")
    public long requestsProcessed;
  }

  // --- Static helper methods for easy event emission ---

  /** Emit a request start event and return it for later commit. */
  public static RequestEvent beginRequest(
      long sessionId, long requestId, String action, String target) {
    RequestEvent event = new RequestEvent();
    event.sessionId = sessionId;
    event.requestId = requestId;
    event.action = action;
    event.target = target;
    event.begin();
    return event;
  }

  /** Complete and commit a request event. */
  public static void endRequest(RequestEvent event, boolean success, String errorType) {
    event.success = success;
    event.errorType = errorType;
    event.end();
    event.commit();
  }

  /** Emit a method resolution event. */
  public static void emitMethodResolution(
      String className,
      String methodName,
      int candidateCount,
      String chosenMethod,
      int score,
      String argumentTypes) {
    MethodResolutionEvent event = new MethodResolutionEvent();
    event.className = className;
    event.methodName = methodName;
    event.candidateCount = candidateCount;
    event.chosenMethod = chosenMethod;
    event.score = score;
    event.argumentTypes = argumentTypes;
    event.commit();
  }

  /** Emit an object registry event. */
  public static void emitObjectRegistry(
      long sessionId, long objectId, String operation, String objectType, int registrySize) {
    ObjectRegistryEvent event = new ObjectRegistryEvent();
    event.sessionId = sessionId;
    event.objectId = objectId;
    event.operation = operation;
    event.objectType = objectType;
    event.registrySize = registrySize;
    event.commit();
  }

  /** Begin a callback event. */
  public static CallbackEvent beginCallback(
      long sessionId, long callbackId, String methodName, int argumentCount) {
    CallbackEvent event = new CallbackEvent();
    event.sessionId = sessionId;
    event.callbackId = callbackId;
    event.methodName = methodName;
    event.argumentCount = argumentCount;
    event.begin();
    return event;
  }

  /** Complete a callback event. */
  public static void endCallback(CallbackEvent event, boolean success, String errorMessage) {
    event.success = success;
    event.errorMessage = errorMessage;
    event.end();
    event.commit();
  }

  /** Emit an Arrow transfer event. */
  public static void emitArrowTransfer(
      long sessionId, String direction, long rowCount, long bytesCopied, boolean zeroCopy) {
    ArrowTransferEvent event = new ArrowTransferEvent();
    event.sessionId = sessionId;
    event.direction = direction;
    event.rowCount = rowCount;
    event.bytesCopied = bytesCopied;
    event.zeroCopy = zeroCopy;
    event.commit();
  }

  /** Emit a session lifecycle event. */
  public static void emitSession(
      long sessionId, String operation, int objectsCreated, long requestsProcessed) {
    SessionEvent event = new SessionEvent();
    event.sessionId = sessionId;
    event.operation = operation;
    event.objectsCreated = objectsCreated;
    event.requestsProcessed = requestsProcessed;
    event.commit();
  }
}
