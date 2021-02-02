package org.apache.flink.statefun.sdk.java.message;

import java.util.Objects;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

final class MessageWrapper implements Message {
  private final TypedValue transportMessage;
  private final Address targetAddress;

  MessageWrapper(Address targetAddress, TypedValue transportMessage) {
    this.targetAddress = Objects.requireNonNull(targetAddress);
    this.transportMessage = Objects.requireNonNull(transportMessage);
  }

  @Override
  public Address targetAddress() {
    return targetAddress;
  }

  @Override
  public boolean isLong() {
    return is(Types.longType());
  }

  @Override
  public long asLong() {
    return as(Types.longType());
  }

  @Override
  public boolean isUtf8String() {
    return is(Types.stringType());
  }

  @Override
  public String asUtf8String() {
    return as(Types.stringType());
  }

  @Override
  public boolean isInt() {
    return is(Types.integerType());
  }

  @Override
  public int asInt() {
    return as(Types.integerType());
  }

  @Override
  public boolean isBoolean() {
    return is(Types.booleanType());
  }

  @Override
  public boolean asBoolean() {
    return as(Types.booleanType());
  }

  @Override
  public boolean isFloat() {
    return is(Types.floatType());
  }

  @Override
  public float asFloat() {
    return as(Types.floatType());
  }

  @Override
  public boolean isDouble() {
    return is(Types.doubleType());
  }

  @Override
  public double asDouble() {
    return as(Types.doubleType());
  }

  @Override
  public <T> boolean is(Type<T> type) {
    TypeName typeName = TypeName.typeNameFromString(transportMessage.getTypename());
    return Objects.equals(typeName, type.typeName());
  }

  @Override
  public <T> T as(Type<T> type) {
    byte[] input = transportMessage.getValue().toByteArray();
    return type.typeSerializer().deserialize(input);
  }
}
