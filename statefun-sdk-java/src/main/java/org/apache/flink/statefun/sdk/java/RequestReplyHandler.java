package org.apache.flink.statefun.sdk.java;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class RequestReplyHandler {
  private final StatefulFunctions functions;

  public RequestReplyHandler(StatefulFunctions functions) {
    this.functions = Objects.requireNonNull(functions);
  }

  public CompletableFuture<byte[]> handle(byte[] input) {
    // do something with functions
    System.out.println("Gordon! " + functions);
    return CompletableFuture.completedFuture(new byte[0]);
  }
}
