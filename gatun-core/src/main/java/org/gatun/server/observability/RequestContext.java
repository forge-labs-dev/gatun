package org.gatun.server.observability;

import org.gatun.protocol.Action;

/**
 * Captures context for a single request, used for structured logging and diagnostics.
 *
 * <p>This record is immutable and captures all relevant information at request start time.
 */
public record RequestContext(
    long sessionId, long requestId, byte action, String target, long startNanos) {

  /** Create a new request context with current timestamp. */
  public static RequestContext create(long sessionId, long requestId, byte action, String target) {
    return new RequestContext(sessionId, requestId, action, target, System.nanoTime());
  }

  /** Get action name for logging. */
  public String actionName() {
    return switch (action) {
      case Action.CreateObject -> "CreateObject";
      case Action.FreeObject -> "FreeObject";
      case Action.InvokeMethod -> "InvokeMethod";
      case Action.InvokeStaticMethod -> "InvokeStaticMethod";
      case Action.GetField -> "GetField";
      case Action.SetField -> "SetField";
      case Action.GetStaticField -> "GetStaticField";
      case Action.SetStaticField -> "SetStaticField";
      case Action.SendArrowBatch -> "SendArrowBatch";
      case Action.SendArrowBuffers -> "SendArrowBuffers";
      case Action.GetArrowData -> "GetArrowData";
      case Action.ResetPayloadArena -> "ResetPayloadArena";
      case Action.RegisterCallback -> "RegisterCallback";
      case Action.UnregisterCallback -> "UnregisterCallback";
      case Action.Cancel -> "Cancel";
      case Action.IsInstanceOf -> "IsInstanceOf";
      case Action.Reflect -> "Reflect";
      case Action.GetFields -> "GetFields";
      case Action.InvokeMethods -> "InvokeMethods";
      case Action.CreateObjects -> "CreateObjects";
      case Action.Batch -> "Batch";
      default -> "Unknown(" + action + ")";
    };
  }

  /** Calculate elapsed time in microseconds. */
  public long elapsedMicros() {
    return (System.nanoTime() - startNanos) / 1000;
  }
}
