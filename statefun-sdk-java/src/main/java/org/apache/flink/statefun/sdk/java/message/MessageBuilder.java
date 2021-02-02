package org.apache.flink.statefun.sdk.java.message;

import com.google.protobuf.ByteString;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.Address;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.Type;
import org.apache.flink.statefun.sdk.java.types.Types;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class MessageBuilder {
  private final TypedValue.Builder builder;
  private final Address targetAddress;

  private MessageBuilder(TypeName functionType, String id) {
    Objects.requireNonNull(functionType);
    Objects.requireNonNull(id);
    this.targetAddress = new Address(functionType, id);
    this.builder = TypedValue.newBuilder();
  }

  public static MessageBuilder forAddress(TypeName functionType, String id) {
    return new MessageBuilder(functionType, id);
  }

  public static MessageBuilder forAddress(Address address) {
    Objects.requireNonNull(address);
    return new MessageBuilder(address.type(), address.id());
  }

  public MessageBuilder withValue(long value) {
    return withCustomType(Types.longType(), value);
  }

  public MessageBuilder withValue(int value) {
    return withCustomType(Types.integerType(), value);
  }

  public MessageBuilder withValue(boolean value) {
    return withCustomType(Types.booleanType(), value);
  }

  public MessageBuilder withValue(String value) {
    return withCustomType(Types.stringType(), value);
  }

  public MessageBuilder withValue(float value) {
    return withCustomType(Types.floatType(), value);
  }

  public MessageBuilder withValue(double value) {
    return withCustomType(Types.doubleType(), value);
  }

  public <T> MessageBuilder withCustomType(Type<T> customType, T element) {
    Objects.requireNonNull(customType);
    Objects.requireNonNull(element);
    try {
      byte[] valueBytes = customType.typeSerializer().serialize(element);
      ByteString byteString = ByteString.copyFrom(valueBytes);
      builder.setTypename(customType.typeName().asTypeNameString());
      builder.setValue(byteString);
      return this;
    } catch (Throwable throwable) {
      throw new IllegalStateException(throwable);
    }
  }

  public Message build() {
    return new MessageWrapper(targetAddress, builder.build());
  }
}
