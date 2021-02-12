/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.sdk.java.handler;

import static org.apache.flink.statefun.sdk.java.handler.ProtoUtils.getTypedValue;
import static org.apache.flink.statefun.sdk.java.handler.ProtoUtils.protoAddressFromSdk;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.AddressScopedStorage;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.storage.ConcurrentAddressScopedStorage;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;

final class ConcurrentContext implements Context {
  private final org.apache.flink.statefun.sdk.java.Address self;
  private final FromFunction.InvocationResponse.Builder responseBuilder;
  private final ConcurrentAddressScopedStorage storage;

  private Address caller;

  public ConcurrentContext(
      org.apache.flink.statefun.sdk.java.Address self,
      FromFunction.InvocationResponse.Builder responseBuilder,
      ConcurrentAddressScopedStorage storage) {
    this.self = Objects.requireNonNull(self);
    this.responseBuilder = Objects.requireNonNull(responseBuilder);
    this.storage = Objects.requireNonNull(storage);
  }

  @Override
  public org.apache.flink.statefun.sdk.java.Address self() {
    return self;
  }

  void setCaller(Address address) {
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
}
