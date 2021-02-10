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

import static org.junit.Assert.assertEquals;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.apache.flink.statefun.sdk.types.generated.BooleanWrapper;
import org.apache.flink.statefun.sdk.types.generated.IntWrapper;
import org.apache.flink.statefun.sdk.types.generated.LongWrapper;
import org.junit.Ignore;
import org.junit.Test;

public class SanityPrimitiveTypeTest {

  @Test
  public void testBoolean() {
    assertRoundTrip(Types.booleanType(), Boolean.TRUE);
    assertRoundTrip(Types.booleanType(), Boolean.FALSE);
  }

  @Test
  public void testInt() {
    assertRoundTrip(Types.integerType(), 1);
    assertRoundTrip(Types.integerType(), 1048576);
    assertRoundTrip(Types.integerType(), Integer.MIN_VALUE);
    assertRoundTrip(Types.integerType(), Integer.MAX_VALUE);
    assertRoundTrip(Types.integerType(), -1);
  }

  @Test
  public void testLong() {
    assertRoundTrip(Types.longType(), -1L);
    assertRoundTrip(Types.longType(), 0L);
    assertRoundTrip(Types.longType(), Long.MIN_VALUE);
    assertRoundTrip(Types.longType(), Long.MAX_VALUE);
  }

  @Test
  public void testFloat() {
    assertRoundTrip(Types.floatType(), Float.MIN_VALUE);
    assertRoundTrip(Types.floatType(), Float.MAX_VALUE);
    assertRoundTrip(Types.floatType(), 2.1459f);
    assertRoundTrip(Types.floatType(), -1e-4f);
  }

  @Test
  public void testDouble() {
    assertRoundTrip(Types.doubleType(), Double.MIN_VALUE);
    assertRoundTrip(Types.doubleType(), Double.MAX_VALUE);
    assertRoundTrip(Types.doubleType(), 2.1459d);
    assertRoundTrip(Types.doubleType(), -1e-4d);
  }

  @Test
  public void testString() {
    assertRoundTrip(Types.stringType(), "");
    assertRoundTrip(Types.stringType(), "This is a string");
  }

  @Test
  public void testRandomCompatibilityWithAnIntegerWrapper() throws InvalidProtocolBufferException {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    TypeSerializer<Integer> serializer = Types.integerType().typeSerializer();
    for (int i = 0; i < 1_000_000; i++) {
      testCompatibilityWithAnIntegerWrapper(serializer, random.nextInt());
    }
  }

  @Test
  public void testCompatibilityWithABooleanWrapper() throws InvalidProtocolBufferException {
    TypeSerializer<Boolean> serializer = Types.booleanType().typeSerializer();
    testCompatibilityWithABooleanWrapper(serializer, true);
    testCompatibilityWithABooleanWrapper(serializer, false);
  }

  @Test
  public void testRandomCompatibilityWithALongWrapper() throws InvalidProtocolBufferException {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    TypeSerializer<Long> serializer = Types.longType().typeSerializer();
    for (int i = 0; i < 1_000_000; i++) {
      testCompatibilityWithALongWrapper(serializer, random.nextLong());
    }
  }

  @Ignore
  @Test
  public void testCompatibilityWithAnIntegerWrapper() throws InvalidProtocolBufferException {
    TypeSerializer<Integer> serializer = Types.integerType().typeSerializer();
    for (int expected = Integer.MIN_VALUE; expected != Integer.MAX_VALUE; expected++) {
      testCompatibilityWithAnIntegerWrapper(serializer, expected);
    }
  }

  private void testCompatibilityWithABooleanWrapper(
      TypeSerializer<Boolean> serializer, boolean expected) throws InvalidProtocolBufferException {
    // test round trip
    final Slice serialized = serializer.serialize(expected);
    final boolean got = serializer.deserialize(serialized);
    assertEquals(expected, got);

    // test that protobuf can parse what we wrote:
    final BooleanWrapper wrapper = BooleanWrapper.parseFrom(serialized.asReadOnlyByteBuffer());
    assertEquals(expected, wrapper.getValue());

    // test that we can parse what protobuf wrote:
    final Slice serializedByPb = Slices.wrap(wrapper.toByteArray());
    final boolean gotPb = serializer.deserialize(serializedByPb);
    assertEquals(gotPb, expected);

    // test that pb byte representation is equal to ours:
    assertEquals(serializedByPb.asReadOnlyByteBuffer(), serialized.asReadOnlyByteBuffer());
  }

  private void testCompatibilityWithAnIntegerWrapper(
      TypeSerializer<Integer> serializer, int expected) throws InvalidProtocolBufferException {
    // test round trip
    final Slice serialized = serializer.serialize(expected);
    final int got = serializer.deserialize(serialized);
    assertEquals(expected, got);

    // test that protobuf can parse what we wrote:
    final IntWrapper wrapper = IntWrapper.parseFrom(serialized.asReadOnlyByteBuffer());
    assertEquals(expected, wrapper.getValue());

    // test that we can parse what protobuf wrote:
    final Slice serializedByPb = Slices.wrap(wrapper.toByteArray());
    final int gotPb = serializer.deserialize(serializedByPb);
    assertEquals(gotPb, expected);

    // test that pb byte representation is equal to ours:
    assertEquals(serializedByPb.asReadOnlyByteBuffer(), serialized.asReadOnlyByteBuffer());
  }

  private void testCompatibilityWithALongWrapper(TypeSerializer<Long> serializer, long expected)
      throws InvalidProtocolBufferException {
    // test round trip
    final Slice serialized = serializer.serialize(expected);
    final long got = serializer.deserialize(serialized);
    assertEquals(expected, got);

    // test that protobuf can parse what we wrote:
    final LongWrapper wrapper = LongWrapper.parseFrom(serialized.asReadOnlyByteBuffer());
    assertEquals(expected, wrapper.getValue());

    // test that we can parse what protobuf wrote:
    final Slice serializedByPb = Slices.wrap(wrapper.toByteArray());
    final long gotPb = serializer.deserialize(serializedByPb);
    assertEquals(gotPb, expected);

    // test that pb byte representation is equal to ours:
    assertEquals(serializedByPb.asReadOnlyByteBuffer(), serialized.asReadOnlyByteBuffer());
  }

  public <T> void assertRoundTrip(Type<T> type, T element) {
    final Slice slice;
    {
      TypeSerializer<T> serializer = type.typeSerializer();
      slice = serializer.serialize(element);
    }
    TypeSerializer<T> serializer = type.typeSerializer();
    T got = serializer.deserialize(slice);
    assertEquals(element, got);
  }
}
