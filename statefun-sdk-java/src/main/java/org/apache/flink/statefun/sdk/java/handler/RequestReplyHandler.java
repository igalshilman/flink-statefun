package org.apache.flink.statefun.sdk.java.handler;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.slice.Slice;

public interface RequestReplyHandler {

  CompletableFuture<Slice> handle(Slice input);
}
