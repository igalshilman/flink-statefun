package org.apache.flink.statefun.sdk.java.handler;

import static org.apache.flink.statefun.sdk.java.handler.ProtoUtils.protoAddressFromSdk;

import com.sun.istack.internal.Nullable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.ApiExtension;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageWrapper;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageWrapper;
import org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil;
import org.apache.flink.statefun.sdk.java.storage.ConcurrentAddressScopedStorage;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

final class ConcurrentContext implements Context {
  private final org.apache.flink.statefun.sdk.java.Address self;
  private final FromFunction.InvocationResponse.Builder responseBuilder;
  private final ConcurrentAddressScopedStorage storage;

  @Nullable private Address caller;

  public ConcurrentContext(
      org.apache.flink.statefun.sdk.java.Address self,
      FromFunction.InvocationResponse.Builder responseBuilder,
      ConcurrentAddressScopedStorage storage) {
    this.self = self;
    this.responseBuilder = responseBuilder;
    this.storage = storage;
  }

  @Override
  public org.apache.flink.statefun.sdk.java.Address self() {
    return self;
  }

  void setCaller(@Nullable Address address) {
    this.caller = address;
  }

  @Override
  public Optional<Address> caller() {
    return Optional.ofNullable(caller);
  }

  @Override
  public void send(Message message) {
    Objects.requireNonNull(message);

    FromFunction.Invocation.Builder outInvocation =
        FromFunction.Invocation.newBuilder()
            .setArgument(getTypedValue(message))
            .setTarget(protoAddressFromSdk(message.targetAddress()));

    synchronized (responseBuilder) {
      responseBuilder.addOutgoingMessages(outInvocation);
    }
  }

  @Override
  public void sendAfter(Duration duration, Message message) {
    Objects.requireNonNull(duration);
    Objects.requireNonNull(message);

    FromFunction.DelayedInvocation.Builder outInvocation =
        FromFunction.DelayedInvocation.newBuilder()
            .setArgument(getTypedValue(message))
            .setTarget(protoAddressFromSdk(message.targetAddress()))
            .setDelayInMs(duration.toMillis());

    synchronized (responseBuilder) {
      responseBuilder.addDelayedInvocations(outInvocation);
    }
  }

  @Override
  public void send(EgressMessage message) {
    Objects.requireNonNull(message);

    TypeName target = message.targetEgressId();

    FromFunction.EgressMessage.Builder outInvocation =
        FromFunction.EgressMessage.newBuilder()
            .setArgument(getTypedValue(message))
            .setEgressNamespace(target.namespace())
            .setEgressType(target.name());

    synchronized (responseBuilder) {
      responseBuilder.addOutgoingEgresses(outInvocation);
    }
  }

  @Override
  public AddressScopedStorage storage() {
    return storage;
  }

  private static TypedValue getTypedValue(Message message) {
    if (message instanceof MessageWrapper) {
      return ((MessageWrapper) message).typedValue();
    }
    return TypedValue.newBuilder()
        .setTypenameBytes(ApiExtension.typeNameByteString(message.valueTypeName()))
        .setValue(SliceProtobufUtil.asByteString(message.rawValue()))
        .build();
  }

  private static TypedValue getTypedValue(EgressMessage message) {
    if (message instanceof EgressMessageWrapper) {
      return ((EgressMessageWrapper) message).typedValue();
    }
    return TypedValue.newBuilder()
        .setTypenameBytes(ApiExtension.typeNameByteString(message.egressMessageValueType()))
        .setValue(SliceProtobufUtil.asByteString(message.egressMessageValueBytes()))
        .build();
  }
}
