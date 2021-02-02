package org.apache.flink.statefun.sdk.java;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.message.Message;

public interface StatefulFunction {

  CompletableFuture<?> apply(Context context, Message argument) throws Throwable;
}
