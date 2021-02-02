package org.apache.flink.statefun.sdk.java.types;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.types.generated.BooleanWrapper;
import org.apache.flink.statefun.sdk.types.generated.DoubleWrapper;
import org.apache.flink.statefun.sdk.types.generated.FloatWrapper;
import org.apache.flink.statefun.sdk.types.generated.IntWrapper;
import org.apache.flink.statefun.sdk.types.generated.LongWrapper;
import org.apache.flink.statefun.sdk.types.generated.StringWrapper;

public final class Types {
  private Types() {}

  public static final TypeName BOOLEAN_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/bool");
  public static final TypeName INTEGER_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/int");
  public static final TypeName FLOAT_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/float");
  public static final TypeName LONG_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/long");
  public static final TypeName DOUBLE_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/double");
  public static final TypeName STRING_TYPENAME =
      TypeName.typeNameFromString("io.statefun.types/string");

  public static Type<Long> longType() {
    return LongType.INSTANCE;
  }

  public static Type<String> stringType() {
    return StringType.INSTANCE;
  }

  public static Type<Integer> integerType() {
    return IntegerType.INSTANCE;
  }

  public static Type<Boolean> booleanType() {
    return BooleanType.INSTANCE;
  }

  public static Type<Float> floatType() {
    return FloatType.INSTANCE;
  }

  public static Type<Double> doubleType() {
    return DoubleType.INSTANCE;
  }

  private static final class LongType implements Type<Long> {

    static final Type<Long> INSTANCE = new LongType();

    private final TypeSerializer<Long> serializer = new LongTypeSerializer();

    @Override
    public TypeName typeName() {
      return LONG_TYPENAME;
    }

    @Override
    public TypeSerializer<Long> typeSerializer() {
      return serializer;
    }
  }

  private static final class LongTypeSerializer implements TypeSerializer<Long> {

    @Override
    public byte[] serialize(Long element) {
      LongWrapper wrapper = LongWrapper.newBuilder().setValue(element).build();
      return wrapper.toByteArray();
    }

    @Override
    public Long deserialize(byte[] input) {
      try {
        LongWrapper wrapper = LongWrapper.parseFrom(input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  private static final class StringType implements Type<String> {

    static final Type<String> INSTANCE = new StringType();

    private final TypeSerializer<String> serializer = new StringTypeSerializer();

    @Override
    public TypeName typeName() {
      return STRING_TYPENAME;
    }

    @Override
    public TypeSerializer<String> typeSerializer() {
      return serializer;
    }
  }

  private static final class StringTypeSerializer implements TypeSerializer<String> {

    @Override
    public byte[] serialize(String element) {
      StringWrapper wrapper = StringWrapper.newBuilder().setValue(element).build();
      return wrapper.toByteArray();
    }

    @Override
    public String deserialize(byte[] input) {
      try {
        StringWrapper wrapper = StringWrapper.parseFrom(input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  private static final class IntegerType implements Type<Integer> {

    static final Type<Integer> INSTANCE = new IntegerType();

    private final TypeSerializer<Integer> serializer = new IntegerTypeSerializer();

    @Override
    public TypeName typeName() {
      return INTEGER_TYPENAME;
    }

    @Override
    public TypeSerializer<Integer> typeSerializer() {
      return serializer;
    }
  }

  private static final class IntegerTypeSerializer implements TypeSerializer<Integer> {

    @Override
    public byte[] serialize(Integer element) {
      IntWrapper wrapper = IntWrapper.newBuilder().setValue(element).build();
      return wrapper.toByteArray();
    }

    @Override
    public Integer deserialize(byte[] input) {
      try {
        IntWrapper wrapper = IntWrapper.parseFrom(input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  private static final class BooleanType implements Type<Boolean> {

    static final Type<Boolean> INSTANCE = new BooleanType();

    private final TypeSerializer<Boolean> serializer = new BooleanTypeSerializer();

    @Override
    public TypeName typeName() {
      return BOOLEAN_TYPENAME;
    }

    @Override
    public TypeSerializer<Boolean> typeSerializer() {
      return serializer;
    }
  }

  private static final class BooleanTypeSerializer implements TypeSerializer<Boolean> {

    @Override
    public byte[] serialize(Boolean element) {
      BooleanWrapper wrapper = BooleanWrapper.newBuilder().setValue(element).build();
      return wrapper.toByteArray();
    }

    @Override
    public Boolean deserialize(byte[] input) {
      try {
        BooleanWrapper wrapper = BooleanWrapper.parseFrom(input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  private static final class FloatType implements Type<Float> {

    static final Type<Float> INSTANCE = new FloatType();

    private final TypeSerializer<Float> serializer = new FloatTypeSerializer();

    @Override
    public TypeName typeName() {
      return FLOAT_TYPENAME;
    }

    @Override
    public TypeSerializer<Float> typeSerializer() {
      return serializer;
    }
  }

  private static final class FloatTypeSerializer implements TypeSerializer<Float> {

    @Override
    public byte[] serialize(Float element) {
      FloatWrapper wrapper = FloatWrapper.newBuilder().setValue(element).build();
      return wrapper.toByteArray();
    }

    @Override
    public Float deserialize(byte[] input) {
      try {
        FloatWrapper wrapper = FloatWrapper.parseFrom(input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  private static final class DoubleType implements Type<Double> {

    static final Type<Double> INSTANCE = new DoubleType();

    private final TypeSerializer<Double> serializer = new DoubleTypeSerializer();

    @Override
    public TypeName typeName() {
      return DOUBLE_TYPENAME;
    }

    @Override
    public TypeSerializer<Double> typeSerializer() {
      return serializer;
    }
  }

  private static final class DoubleTypeSerializer implements TypeSerializer<Double> {

    @Override
    public byte[] serialize(Double element) {
      DoubleWrapper wrapper = DoubleWrapper.newBuilder().setValue(element).build();
      return wrapper.toByteArray();
    }

    @Override
    public Double deserialize(byte[] input) {
      try {
        DoubleWrapper wrapper = DoubleWrapper.parseFrom(input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
