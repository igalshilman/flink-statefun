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
package org.apache.flink.statefun.sdk.java.types;

import static org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil.parseFrom;
import static org.apache.flink.statefun.sdk.java.slice.SliceProtobufUtil.toSlice;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.types.generated.BooleanWrapper;
import org.apache.flink.statefun.sdk.types.generated.DoubleWrapper;
import org.apache.flink.statefun.sdk.types.generated.FloatWrapper;
import org.apache.flink.statefun.sdk.types.generated.IntWrapper;
import org.apache.flink.statefun.sdk.types.generated.LongWrapper;
import org.apache.flink.statefun.sdk.types.generated.StringWrapper;

public final class Types {
  private Types() {}

  // primitives
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

  // common characteristics
  private static final Set<TypeCharacteristics> IMMUTABLE_TYPE_CHARS =
      Collections.unmodifiableSet(EnumSet.of(TypeCharacteristics.IMMUTABLE_VALUES));

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

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class LongTypeSerializer implements TypeSerializer<Long> {
    private static final Slice[] CACHE;

    static {
      Slice[] cache = new Slice[4096];
      for (int i = 0; i < cache.length; i++) {
        cache[i] = toSlice(LongWrapper.newBuilder().setValue(i).build());
      }
      CACHE = cache;
    }

    @Override
    public Slice serialize(Long element) {
      if (element >= 0 && element < CACHE.length) {
        return CACHE[element.intValue()];
      }
      LongWrapper wrapper = LongWrapper.newBuilder().setValue(element).build();
      return toSlice(wrapper);
    }

    @Override
    public Long deserialize(Slice input) {
      try {
        LongWrapper longWrapper = parseFrom(LongWrapper.parser(), input);
        return longWrapper.getValue();
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

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class StringTypeSerializer implements TypeSerializer<String> {

    @Override
    public Slice serialize(String element) {
      StringWrapper wrapper = StringWrapper.newBuilder().setValue(element).build();
      return Slices.wrap(wrapper.toByteArray());
    }

    @Override
    public String deserialize(Slice input) {
      try {
        StringWrapper wrapper = parseFrom(StringWrapper.parser(), input);
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

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class IntegerTypeSerializer implements TypeSerializer<Integer> {
    // cache a small range of int slices.
    private static final Slice[] CACHE;

    static {
      Slice[] cache = new Slice[4096];
      for (int i = 0; i < cache.length; i++) {
        cache[i] = toSlice(IntWrapper.newBuilder().setValue(i).build());
      }
      CACHE = cache;
    }

    @Override
    public Slice serialize(Integer element) {
      if (element >= 0 && element < CACHE.length) {
        return CACHE[element];
      }
      IntWrapper wrapper = IntWrapper.newBuilder().setValue(element).build();
      return toSlice(wrapper);
    }

    @Override
    public Integer deserialize(Slice input) {
      try {
        IntWrapper wrapper = parseFrom(IntWrapper.parser(), input);
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

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class BooleanTypeSerializer implements TypeSerializer<Boolean> {
    private static final Slice TRUE_SLICE =
        toSlice(BooleanWrapper.newBuilder().setValue(true).build());
    private static final Slice FALSE_SLICE =
        toSlice(BooleanWrapper.newBuilder().setValue(true).build());

    @Override
    public Slice serialize(Boolean element) {
      return element ? TRUE_SLICE : FALSE_SLICE;
    }

    @Override
    public Boolean deserialize(Slice input) {
      try {
        BooleanWrapper wrapper = parseFrom(BooleanWrapper.parser(), input);
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

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class FloatTypeSerializer implements TypeSerializer<Float> {

    @Override
    public Slice serialize(Float element) {
      FloatWrapper wrapper = FloatWrapper.newBuilder().setValue(element).build();
      return toSlice(wrapper);
    }

    @Override
    public Float deserialize(Slice input) {
      try {
        FloatWrapper wrapper = parseFrom(FloatWrapper.parser(), input);
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

    public Set<TypeCharacteristics> typeCharacteristics() {
      return IMMUTABLE_TYPE_CHARS;
    }
  }

  private static final class DoubleTypeSerializer implements TypeSerializer<Double> {

    @Override
    public Slice serialize(Double element) {
      DoubleWrapper wrapper = DoubleWrapper.newBuilder().setValue(element).build();
      return toSlice(wrapper);
    }

    @Override
    public Double deserialize(Slice input) {
      try {
        DoubleWrapper wrapper = parseFrom(DoubleWrapper.parser(), input);
        return wrapper.getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
