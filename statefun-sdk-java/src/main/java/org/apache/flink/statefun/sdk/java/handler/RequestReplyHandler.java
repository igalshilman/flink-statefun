package org.apache.flink.statefun.sdk.java.handler;

import static org.apache.flink.statefun.sdk.java.handler.MoreFutures.applySequentially;
import static org.apache.flink.statefun.sdk.java.handler.ProtoUtils.sdkAddressFromProto;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.MessageWrapper;
import org.apache.flink.statefun.sdk.java.storage.ConcurrentAddressScopedStorage;
import org.apache.flink.statefun.sdk.java.storage.IncompleteStateValuesException;
import org.apache.flink.statefun.sdk.java.storage.StateValueContexts;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class RequestReplyHandler {
  private final Map<TypeName, StatefulFunctionSpec> functionSpecs;

  public RequestReplyHandler(Map<TypeName, StatefulFunctionSpec> functionSpecs) {
    this.functionSpecs = functionSpecs;
  }

  CompletableFuture<FromFunction> handle(ToFunction request) {
    if (!request.hasInvocation()) {
      return CompletableFuture.completedFuture(FromFunction.getDefaultInstance());
    }
    ToFunction.InvocationBatchRequest batchRequest = request.getInvocation();
    final Address self = sdkAddressFromProto(batchRequest.getTarget());
    StatefulFunctionSpec targetSpec = functionSpecs.get(self.type());
    if (targetSpec == null) {
      throw new IllegalStateException("Unknown target type " + self);
    }
    final StatefulFunction function = targetSpec.supplier().get();
    if (function == null) {
      throw new NullPointerException("supplier for " + self + " supplied NULL function.");
    }
    Either<ConcurrentAddressScopedStorage, Set<ValueSpec<?>>> maybeStorage =
        tryGetStorage(targetSpec, batchRequest.getStateList());
    if (maybeStorage.right != null) {
      // not enough information to compute this batch.
      FromFunction res = buildIncompleteInvocationResponse(maybeStorage.right);
      return CompletableFuture.completedFuture(res);
    }
    final ConcurrentAddressScopedStorage storage = maybeStorage.left;
    return executeBatch(batchRequest, self, storage, function);
  }

  private CompletableFuture<FromFunction> executeBatch(
      ToFunction.InvocationBatchRequest inputBatch,
      Address self,
      ConcurrentAddressScopedStorage storage,
      StatefulFunction function) {

    final FromFunction.InvocationResponse.Builder responseBuilder =
        FromFunction.InvocationResponse.newBuilder();

    final ConcurrentContext context = new ConcurrentContext(self, responseBuilder, storage);

    CompletableFuture<Void> allDone =
        applySequentially(
            inputBatch.getInvocationsList(), invocation -> apply(function, context, invocation));

    return allDone.thenApply(unused -> finalizeResponse(storage, responseBuilder));
  }

  private static final class Either<L, R> {
    final L left;
    final R right;

    public Either(L left, R right) {
      this.left = left;
      this.right = right;
    }
  }

  private static Either<ConcurrentAddressScopedStorage, Set<ValueSpec<?>>> tryGetStorage(
      StatefulFunctionSpec targetSpec, List<ToFunction.PersistedValue> actualStates) {
    try {
      Map<String, StateValueContexts.StateValueContext<?>> resolved =
          StateValueContexts.resolve(targetSpec.knownValues(), actualStates);
      ConcurrentAddressScopedStorage storage = new ConcurrentAddressScopedStorage(resolved);
      return new Either<>(storage, null);
    } catch (IncompleteStateValuesException e) {
      Set<ValueSpec<?>> missing = e.getStatesWithMissingValue();
      return new Either<>(null, missing);
    }
  }

  private static FromFunction buildIncompleteInvocationResponse(Set<ValueSpec<?>> missing) {
    FromFunction.IncompleteInvocationContext.Builder result =
        FromFunction.IncompleteInvocationContext.newBuilder();

    for (ValueSpec<?> v : missing) {
      result.addMissingValues(ProtoUtils.protoFromValueSpec(v));
    }

    return FromFunction.newBuilder().setIncompleteInvocationContext(result).build();
  }

  private static CompletableFuture<Void> apply(
      StatefulFunction function, ConcurrentContext context, ToFunction.Invocation invocation)
      throws Throwable {
    TypedValue argument = invocation.getArgument();
    MessageWrapper wrapper = new MessageWrapper(context.self(), argument);
    context.setCaller(sdkAddressFromProto(invocation.getCaller()));
    return function.apply(context, wrapper);
  }

  private static FromFunction finalizeResponse(
      ConcurrentAddressScopedStorage storage, FromFunction.InvocationResponse.Builder builder) {
    storage.addMutations(builder::addStateMutations);
    return FromFunction.newBuilder().setInvocationResult(builder).build();
  }
}
